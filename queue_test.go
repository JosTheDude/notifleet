package main

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
)

func TestQueueRestartAndPartialFailure(t *testing.T) {
	c := testConfig(t)
	c.Destinations["bad"] = Destination{Provider: "slack", WebhookURL: "https://hooks.slack.com/services/T/B/bad"}
	c.Routes["default"] = []string{"chat", "bad"}
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.enqueue(Message{Route: "default", Message: "saved before acceptance"})
	if err != nil {
		t.Fatal(err)
	}
	if second, err := openQueue(c); err == nil {
		second.close()
		t.Fatal("second writer acquired queue")
	}
	q.close()
	q, err = openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	if q.jobs[id].Payload.Message != "saved before acceptance" {
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
		if _, err := q.step(context.Background(), map[bool]*http.Client{false: client}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	job := q.jobs[id]
	if job.Targets[0].State != "delivered" || job.Targets[1].State != "failed" || job.CompletedAt == nil || counts["discord.com"] != 1 || counts["hooks.slack.com"] != 1 {
		t.Fatalf("incorrect fan-out: %+v %v", job, counts)
	}
	w := request(newHandler(c, q), "GET", "/v1/notifications/"+id, "", c.APIKeys[0])
	if !strings.Contains(w.Body.String(), `"status":"partial"`) {
		t.Fatal(w.Body.String())
	}
}

func TestRetryBudgetAndCooldownSurviveRestart(t *testing.T) {
	c := testConfig(t)
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.enqueue(Message{Route: "default", Message: "first"})
	if err != nil {
		t.Fatal(err)
	}
	clients := map[bool]*http.Client{false: mockClient(429, `{"retry_after":60}`, nil)}
	if _, err := q.step(context.Background(), clients, time.Now()); err != nil {
		t.Fatal(err)
	}
	next := q.jobs[id].Targets[0].NextAttempt
	if next.Before(time.Now().Add(59 * time.Second)) {
		t.Fatal("ignored Retry-After")
	}
	if _, err := q.enqueue(Message{Route: "default", Message: "second"}); err != nil {
		t.Fatal(err)
	}
	if worked, err := q.step(context.Background(), clients, time.Now()); worked || err != nil {
		t.Fatal("another job bypassed destination cooldown")
	}
	q.close()
	q, err = openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	if worked, _ := q.step(context.Background(), clients, time.Now()); worked {
		t.Fatal("restart bypassed cooldown")
	}
	if q.jobs[id].Targets[0].Attempts != 1 {
		t.Fatal("attempt count reset")
	}
	// Drive all pending jobs past their deadlines; each must stop at three calls.
	for range 10 {
		if _, err := q.step(context.Background(), clients, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for _, job := range q.jobs {
		if job.Targets[0].Attempts != 3 || job.Targets[0].State != "failed" || job.CompletedAt == nil {
			t.Fatalf("retry budget wrong: %+v", job)
		}
	}
}

func TestChangedDestinationNeverReceivesPendingMessage(t *testing.T) {
	c := testConfig(t)
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.enqueue(Message{Route: "default", Message: "private"})
	if err != nil {
		t.Fatal(err)
	}
	q.close()
	c.Destinations["chat"] = Destination{Provider: "discord", WebhookURL: "https://discord.com/api/webhooks/456/other"}
	q, err = openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	// Nil client makes accidental delivery fail this test immediately.
	if _, err := q.step(context.Background(), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	target := q.jobs[id].Targets[0]
	if target.State != "failed" || target.Code != "destination_changed" || target.Attempts != 0 {
		t.Fatalf("rerouted queued message: %+v", target)
	}
}

func TestCapacityRetentionAndStorageFailure(t *testing.T) {
	c := testConfig(t)
	c.MaxJobs = 1
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	id, err := q.enqueue(Message{Route: "default", Message: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.enqueue(Message{Route: "default", Message: "second"}); !errors.Is(err, errFull) {
		t.Fatal("unbounded queue")
	}
	if _, err := q.step(context.Background(), map[bool]*http.Client{false: mockClient(200, `{}`, nil)}, time.Now()); err != nil {
		t.Fatal(err)
	}
	finished := *q.jobs[id].CompletedAt
	if err := q.prune(finished.Add(24*time.Hour - time.Nanosecond)); err != nil || len(q.jobs) != 1 {
		t.Fatal("pruned too early")
	}
	if err := q.prune(finished.Add(24 * time.Hour)); err != nil || len(q.jobs) != 0 {
		t.Fatal("did not prune at retention boundary")
	}
	if _, err := os.Stat(filepath.Join(c.DataDir, id+".json")); !os.IsNotExist(err) {
		t.Fatal("payload remained on disk")
	}
	// Point storage at a file, a deterministic failure even when tests run as root.
	file := filepath.Join(c.DataDir, "not-a-directory")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	q.config.DataDir = file
	if _, err := q.enqueue(Message{Route: "default", Message: "must-not-accept"}); !errors.Is(err, errStorage) || !q.unhealthy {
		t.Fatal("accepted unpersisted job")
	}
	if request(newHandler(c, q), "GET", "/healthz", "", "").Code != 503 {
		t.Fatal("storage failure reported healthy")
	}
	if _, err := q.step(context.Background(), nil, time.Now()); !errors.Is(err, errStorage) {
		t.Fatal("worker continued after storage failure")
	}
}

func TestCorruptQueueFailsClosed(t *testing.T) {
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.DataDir, strings.Repeat("a", 32)+".json"), []byte(`{"id":"../../escape"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if q, err := openQueue(c); err == nil {
		q.close()
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
		writeJSON(w, 200, map[string]string{"id": "message-id", "event": "message"})
	}))
	defer provider.Close()
	c := testConfig(t)
	c.Destinations["chat"] = Destination{Provider: "ntfy", ServerURL: provider.URL, Topic: "builds", Token: "ntfy-secret"}
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	api := httptest.NewServer(newHandler(c, q))
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
	if _, err := q.step(context.Background(), map[bool]*http.Client{false: provider.Client()}, time.Now()); err != nil {
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
	status := request(newHandler(c, q), "GET", resp.Header.Get("Location"), "", c.APIKeys[0])
	if !strings.Contains(status.Body.String(), `"status":"delivered"`) {
		t.Fatal(status.Body.String())
	}
}
