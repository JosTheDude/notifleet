package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"notifleet/internal/config"
	"notifleet/internal/providers"
	"notifleet/internal/queue"
	"notifleet/internal/server"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func mockClient(status int, body string, headers http.Header) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		DataDir: t.TempDir(), APIKeys: []string{strings.Repeat("a", 64)}, MaxJobs: 100, MaxAttempts: 3, RetentionHours: 24, RequestsPerMinute: 1000,
		Destinations: map[string]config.Destination{"chat": {Provider: "discord", WebhookURL: "https://discord.com/api/webhooks/123/token"}},
		Routes:       map[string][]string{"default": {"chat"}, "alerts": {"chat"}},
	}
}

func request(h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestQueueRestartAndPartialFailure(t *testing.T) {
	c := testConfig(t)
	c.Destinations["bad"] = config.Destination{Provider: "slack", WebhookURL: "https://hooks.slack.com/services/T/B/bad"}
	c.Routes["default"] = []string{"chat", "bad"}
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.Enqueue(providers.Message{Route: "default", Message: "saved before acceptance"})
	if err != nil {
		t.Fatal(err)
	}
	if second, err := queue.Open(c); err == nil {
		second.Close()
		t.Fatal("second writer acquired queue")
	}
	q.Close()
	q, err = queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	job, ok := q.Job(id)
	if !ok || job.Payload.Message != "saved before acceptance" {
		t.Fatal("lost payload on restart")
	}
	info, err := os.Stat(filepath.Join(c.DataDir, id+".json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("queue file must be private")
	}
	counts := map[string]int{}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		counts[r.URL.Host]++
		status, body := 200, `{}`
		if r.URL.Host == "hooks.slack.com" {
			status, body = 400, "invalid_token"
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	for range 3 {
		if _, err := q.Deliver(context.Background(), map[bool]*http.Client{false: client}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	job, ok = q.Job(id)
	if !ok || job.Targets[0].State != "delivered" || job.Targets[1].State != "failed" || job.CompletedAt == nil || counts["discord.com"] != 1 || counts["hooks.slack.com"] != 1 {
		t.Fatalf("incorrect fan-out: %+v %v", job, counts)
	}
	w := request(server.NewHandler(c, q), "GET", "/v1/notifications/"+id, "", c.APIKeys[0])
	if !strings.Contains(w.Body.String(), `"status":"partial"`) {
		t.Fatal(w.Body.String())
	}
}

func TestRetryBudgetAndCooldownSurviveRestart(t *testing.T) {
	c := testConfig(t)
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.Enqueue(providers.Message{Route: "default", Message: "first"})
	if err != nil {
		t.Fatal(err)
	}
	clients := map[bool]*http.Client{false: mockClient(429, `{"retry_after":60}`, nil)}
	if _, err := q.Deliver(context.Background(), clients, time.Now()); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Job(id)
	next := job.Targets[0].NextAttempt
	if next.Before(time.Now().Add(59 * time.Second)) {
		t.Fatal("ignored Retry-After")
	}
	if _, err := q.Enqueue(providers.Message{Route: "default", Message: "second"}); err != nil {
		t.Fatal(err)
	}
	if worked, err := q.Deliver(context.Background(), clients, time.Now()); worked || err != nil {
		t.Fatal("another job bypassed destination cooldown")
	}
	q.Close()
	q, err = queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if worked, _ := q.Deliver(context.Background(), clients, time.Now()); worked {
		t.Fatal("restart bypassed cooldown")
	}
	job, _ = q.Job(id)
	if job.Targets[0].Attempts != 1 {
		t.Fatal("attempt count reset")
	}
	// Drive all pending jobs past their deadlines; each must stop at three calls.
	for range 10 {
		if _, err := q.Deliver(context.Background(), clients, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for _, job := range q.Jobs() {
		if job.Targets[0].Attempts != 3 || job.Targets[0].State != "failed" || job.CompletedAt == nil {
			t.Fatalf("retry budget wrong: %+v", job)
		}
	}
}

func TestChangedDestinationNeverReceivesPendingMessage(t *testing.T) {
	c := testConfig(t)
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.Enqueue(providers.Message{Route: "default", Message: "private"})
	if err != nil {
		t.Fatal(err)
	}
	q.Close()
	c.Destinations["chat"] = config.Destination{Provider: "discord", WebhookURL: "https://discord.com/api/webhooks/456/other"}
	q, err = queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// Nil client makes accidental delivery fail this test immediately.
	if _, err := q.Deliver(context.Background(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Job(id)
	target := job.Targets[0]
	if target.State != "failed" || target.Code != "destination_changed" || target.Attempts != 0 {
		t.Fatalf("rerouted queued message: %+v", target)
	}
}

func TestCapacityRetentionAndStorageFailure(t *testing.T) {
	c := testConfig(t)
	c.MaxJobs = 1
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	id, err := q.Enqueue(providers.Message{Route: "default", Message: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(providers.Message{Route: "default", Message: "second"}); !errors.Is(err, queue.ErrFull) {
		t.Fatal("unbounded queue")
	}
	if _, err := q.Deliver(context.Background(), map[bool]*http.Client{false: mockClient(200, `{}`, nil)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Job(id)
	finished := *job.CompletedAt
	if err := q.Prune(finished.Add(24*time.Hour - time.Nanosecond)); err != nil || len(q.Jobs()) != 1 {
		t.Fatal("pruned too early")
	}
	if err := q.Prune(finished.Add(24 * time.Hour)); err != nil || len(q.Jobs()) != 0 {
		t.Fatal("did not prune at retention boundary")
	}
	if _, err := os.Stat(filepath.Join(c.DataDir, id+".json")); !os.IsNotExist(err) {
		t.Fatal("payload remained on disk")
	}
	// Replace the data directory with a regular file: a deterministic
	// failure even when tests run as root (permission bits alone would not
	// stop a root-owned process from writing).
	if err := os.RemoveAll(c.DataDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.DataDir, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(providers.Message{Route: "default", Message: "must-not-accept"}); !errors.Is(err, queue.ErrStorage) || q.Healthy() {
		t.Fatal("accepted unpersisted job")
	}
	if request(server.NewHandler(c, q), "GET", "/healthz", "", "").Code != 503 {
		t.Fatal("storage failure reported healthy")
	}
	if _, err := q.Deliver(context.Background(), nil, time.Now()); !errors.Is(err, queue.ErrStorage) {
		t.Fatal("worker continued after storage failure")
	}
}

func TestCorruptQueueFailsClosed(t *testing.T) {
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.DataDir, strings.Repeat("a", 32)+".json"), []byte(`{"id":"../../escape"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if q, err := queue.Open(c); err == nil {
		q.Close()
		t.Fatal("corrupt queue accepted")
	}
}

func TestHTTPFanoutIntegration(t *testing.T) {
	received := make(chan map[string]any, 1)
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Header.Get("Authorization") != "Bearer ntfy-secret" {
			t.Error("missing ntfy authentication")
		}
		received <- body
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": "message-id", "event": "message"})
	}))
	defer provider.Close()
	c := testConfig(t)
	c.Destinations["chat"] = config.Destination{Provider: "ntfy", ServerURL: provider.URL, Topic: "builds", Token: "ntfy-secret"}
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	api := httptest.NewServer(server.NewHandler(c, q))
	defer api.Close()
	r, _ := http.NewRequest("POST", api.URL+"/v1/notify", strings.NewReader(`{"title":"Deploy","message":"Version 42 is ready"}`))
	r.Header.Set("Authorization", "Bearer "+c.APIKeys[0])
	r.Header.Set("Content-Type", "application/json")
	resp, err := api.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatal(resp.StatusCode)
	}
	if _, err := q.Deliver(context.Background(), map[bool]*http.Client{false: provider.Client()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-received:
		if body["title"] != "Deploy" || body["message"] != "Version 42 is ready" || body["topic"] != "builds" {
			t.Fatal(body)
		}
	default:
		t.Fatal("provider did not receive message")
	}
	status := request(server.NewHandler(c, q), "GET", resp.Header.Get("Location"), "", c.APIKeys[0])
	if !strings.Contains(status.Body.String(), `"status":"delivered"`) {
		t.Fatal(status.Body.String())
	}
}
