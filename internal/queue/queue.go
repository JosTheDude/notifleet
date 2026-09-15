// Package queue persists notification jobs to disk and delivers them with
// bounded retries. One exclusive writer owns a data directory; delivery runs
// on a single worker so a slow provider never blocks another.
package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"

	"notifleet/internal/config"
	"notifleet/internal/providers"
)

var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// ErrFull is returned by Enqueue when the queue is at max_jobs capacity.
var ErrFull = errors.New("queue capacity reached")

// ErrStorage is returned when the durable store cannot be written; the
// queue is poisoned (see Healthy) and stops accepting or delivering work.
var ErrStorage = errors.New("storage unavailable")

type Target struct {
	Name        string    `json:"name"`
	Fingerprint string    `json:"fingerprint"`
	State       string    `json:"state"`
	Attempts    int       `json:"attempts"`
	Code        string    `json:"code,omitempty"`
	NextAttempt time.Time `json:"next_attempt"`
}

type Job struct {
	ID          string            `json:"id"`
	CreatedAt   time.Time         `json:"created_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	Payload     providers.Message `json:"payload"`
	Targets     []Target          `json:"targets"`
}

type Queue struct {
	mu        sync.Mutex
	config    config.Config
	jobs      map[string]*Job
	cooldowns map[string]time.Time
	lock      *os.File
	unhealthy bool
}

// Open acquires the exclusive lock on c.DataDir, loads any existing queue
// records, and prunes terminal jobs past retention. It fails closed on any
// corrupt or invalid record rather than silently dropping it.
func Open(c config.Config) (*Queue, error) {
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(c.DataDir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(c.DataDir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("data directory is already in use")
	}
	q := &Queue{config: c, jobs: map[string]*Job{}, cooldowns: map[string]time.Time{}, lock: lock}
	entries, err := os.ReadDir(c.DataDir)
	if err != nil {
		q.Close()
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(c.DataDir, name))
		if err != nil {
			q.Close()
			return nil, err
		}
		var job Job
		if json.Unmarshal(data, &job) != nil || !idPattern.MatchString(job.ID) || name != job.ID+".json" || job.CreatedAt.IsZero() || len(job.Targets) == 0 {
			q.Close()
			return nil, fmt.Errorf("corrupt queue record %s; restore from backup before restarting", name)
		}
		for _, target := range job.Targets {
			if !config.NamePattern.MatchString(target.Name) || (target.State != "pending" && target.State != "delivered" && target.State != "failed") || target.Attempts < 0 || target.Attempts > 10 || target.NextAttempt.IsZero() {
				q.Close()
				return nil, fmt.Errorf("invalid target in queue record %s", name)
			}
			if target.Code == "http_429" && target.NextAttempt.After(q.cooldowns[target.Fingerprint]) {
				q.cooldowns[target.Fingerprint] = target.NextAttempt
			}
		}
		q.jobs[job.ID] = &job
	}
	if err := q.prune(time.Now()); err != nil {
		q.Close()
		return nil, err
	}
	if len(q.jobs) > c.MaxJobs {
		q.Close()
		return nil, errors.New("stored jobs exceed max_jobs; increase the configured limit")
	}
	return q, nil
}

func (q *Queue) Close() { q.lock.Close() }

// Healthy reports whether the queue can still accept and deliver work. A
// storage failure poisons the queue permanently for the life of this handle.
func (q *Queue) Healthy() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return !q.unhealthy
}

// Job returns a snapshot of one stored job. Never expose message content,
// destination fingerprints, or endpoints to a caller outside this package
// from the returned value without deliberate filtering.
func (q *Queue) Job(id string) (Job, bool) {
	if !idPattern.MatchString(id) {
		return Job{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[id]
	if !ok {
		return Job{}, false
	}
	cp := *j
	cp.Targets = append([]Target(nil), j.Targets...)
	return cp, true
}

// Jobs returns a snapshot of every stored job, for observability and tests.
func (q *Queue) Jobs() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		cp := *j
		cp.Targets = append([]Target(nil), j.Targets...)
		out = append(out, cp)
	}
	return out
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// File + directory fsync ensures acceptance is durable before returning 202.
// A persistence error poisons the queue: do not send with unrecorded state.
func (q *Queue) save(job *Job) (err error) {
	defer func() {
		if err != nil {
			q.unhealthy = true
		}
	}()
	f, err := os.CreateTemp(q.config.DataDir, ".job-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = json.NewEncoder(f).Encode(job); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), filepath.Join(q.config.DataDir, job.ID+".json")); err != nil {
		return err
	}
	return syncDirectory(q.config.DataDir)
}

func (q *Queue) prune(now time.Time) error {
	changed := false
	for id, job := range q.jobs {
		if job.CompletedAt != nil && now.Sub(*job.CompletedAt) >= time.Duration(q.config.RetentionHours)*time.Hour {
			if err := os.Remove(filepath.Join(q.config.DataDir, id+".json")); err != nil {
				q.unhealthy = true
				return err
			}
			delete(q.jobs, id)
			changed = true
		}
	}
	if changed {
		if err := syncDirectory(q.config.DataDir); err != nil {
			q.unhealthy = true
			return err
		}
	}
	return nil
}

// Prune removes terminal jobs past retention as of now. Open and Enqueue
// already call this; it is exported mainly so tests can exercise retention
// boundaries deterministically without waiting on real time.
func (q *Queue) Prune(now time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.prune(now)
}

func (q *Queue) Enqueue(m providers.Message) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.unhealthy {
		return "", ErrStorage
	}
	if err := q.prune(time.Now()); err != nil {
		return "", ErrStorage
	}
	if len(q.jobs) >= q.config.MaxJobs {
		return "", ErrFull
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", err
	}
	now := time.Now().UTC()
	job := &Job{ID: hex.EncodeToString(idBytes), CreatedAt: now, Payload: m}
	for _, name := range q.config.Routes[m.Route] {
		job.Targets = append(job.Targets, Target{Name: name, Fingerprint: q.config.Destinations[name].Fingerprint(), State: "pending", NextAttempt: now})
	}
	if len(job.Targets) == 0 {
		return "", errors.New("unknown route")
	}
	if err := q.save(job); err != nil {
		return "", ErrStorage
	}
	q.jobs[job.ID] = job
	return job.ID, nil
}

// step processes at most one ready delivery attempt. Single worker keeps
// provider concurrency bounded. Requests run outside the queue lock, so a
// slow provider never blocks ingress or status requests.
func (q *Queue) step(ctx context.Context, clients map[bool]*http.Client, now time.Time) (bool, error) {
	q.mu.Lock()
	if q.unhealthy {
		q.mu.Unlock()
		return false, ErrStorage
	}
	if err := q.prune(now); err != nil {
		q.mu.Unlock()
		return false, err
	}
	var job *Job
	index := 0
	for _, candidate := range q.jobs {
		for i, target := range candidate.Targets {
			if q.cooldowns[target.Fingerprint].After(now) {
				continue
			}
			if target.State == "pending" && !target.NextAttempt.After(now) && (job == nil || target.NextAttempt.Before(job.Targets[index].NextAttempt)) {
				job, index = candidate, i
			}
		}
	}
	if job == nil {
		q.mu.Unlock()
		return false, nil
	}
	target := &job.Targets[index]
	d, exists := q.config.Destinations[target.Name]
	valid := exists && d.Fingerprint() == target.Fingerprint
	result := providers.DeliveryResult{Code: "destination_changed"}
	if valid && target.Attempts >= q.config.MaxAttempts {
		valid = false
		result.Code = "attempts_exhausted"
	}
	if valid {
		// Persist before the external side effect; a crash cannot reset the budget.
		target.Attempts++
		if err := q.save(job); err != nil {
			q.mu.Unlock()
			return false, err
		}
	}
	q.mu.Unlock()
	if valid {
		result = providers.Deliver(ctx, clients[d.AllowPrivateNetwork], d, job.Payload)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	target.Code = result.Code
	if result.Retry {
		backoff := time.Second * time.Duration(1<<target.Attempts)
		backoff += time.Duration(randv2.Int64N(int64(time.Second)))
		target.NextAttempt = time.Now().UTC().Add(max(backoff, result.RetryAfter))
		if result.Code == "http_429" {
			q.cooldowns[target.Fingerprint] = target.NextAttempt
		}
	}
	switch {
	case result.Success:
		target.State = "delivered"
	case result.Retry && target.Attempts < q.config.MaxAttempts:
		target.State = "pending"
	default:
		target.State = "failed"
	}
	complete := true
	for _, t := range job.Targets {
		if t.State == "pending" {
			complete = false
		}
	}
	if complete {
		finished := time.Now().UTC()
		job.CompletedAt = &finished
	}
	if err := q.save(job); err != nil {
		return false, err
	}
	slog.Info("delivery", "job", job.ID, "destination", target.Name, "state", target.State, "attempt", target.Attempts, "code", target.Code)
	return true, nil
}

// Deliver processes exactly one ready delivery attempt using the given
// per-network-mode HTTP clients and clock. Run already calls this in a
// loop with real clients and the real clock; it is exported so tests can
// control provider mocking and time deterministically.
func (q *Queue) Deliver(ctx context.Context, clients map[bool]*http.Client, now time.Time) (bool, error) {
	return q.step(ctx, clients, now)
}

func (q *Queue) Run(ctx context.Context) error {
	clients := map[bool]*http.Client{false: providers.NewDeliveryClient(false), true: providers.NewDeliveryClient(true)}
	defer clients[false].CloseIdleConnections()
	defer clients[true].CloseIdleConnections()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		worked, err := q.step(ctx, clients, time.Now())
		if err != nil {
			return err
		}
		if worked {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
