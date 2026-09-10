package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{DataDir: t.TempDir(), APIKeys: []string{strings.Repeat("a", 64)}, MaxJobs: 100, MaxAttempts: 3, RetentionHours: 24, RequestsPerMinute: 1000, Destinations: map[string]Destination{"chat": {Provider: "discord", WebhookURL: "https://discord.com/api/webhooks/123/token"}}, Routes: map[string][]string{"default": {"chat"}, "alerts": {"chat"}}}
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

func TestAPI(t *testing.T) {
	c := testConfig(t)
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	h := newHandler(c, q)
	for _, tc := range []struct {
		method, path, body, key string
		status                  int
	}{
		{"GET", "/healthz", "", "", 200},
		{"POST", "/v1/notify", `{"message":"hello"}`, "", 401},
		{"POST", "/v1/notify", `{"message":"hello"}`, "wrong", 401},
		{"POST", "/v1/notify?api_key=" + c.APIKeys[0], `{"message":"hello"}`, "", 401},
		{"GET", "/v1/notify", "", c.APIKeys[0], 405},
		{"POST", "/v1/notify", `{"message":"hello","url":"https://evil.test"}`, c.APIKeys[0], 400},
		{"POST", "/v1/notify", `{"message":"hello"} {}`, c.APIKeys[0], 400},
		{"POST", "/v1/notify", `null`, c.APIKeys[0], 422},
		{"POST", "/v1/notify", `{"message":"  "}`, c.APIKeys[0], 422},
		{"POST", "/v1/notify", `{"message":"hello","route":"missing"}`, c.APIKeys[0], 422},
		{"POST", "/v1/webhooks/alerts", `{"message":"hello","route":"default"}`, c.APIKeys[0], 400},
		{"POST", "/v1/notify", strings.Repeat("x", 32769), c.APIKeys[0], 413},
		{"POST", "/v1/notify", "{\"message\":\"\xff\"}", c.APIKeys[0], 400},
		{"GET", "/v1/notifications/invalid", "", c.APIKeys[0], 404},
	} {
		w := request(h, tc.method, tc.path, tc.body, tc.key)
		if w.Code != tc.status {
			t.Errorf("%s %s: got %d want %d: %s", tc.method, tc.path, w.Code, tc.status, w.Body)
		}
	}
	if len(q.jobs) != 0 {
		t.Fatal("invalid requests queued messages")
	}
	w := request(h, "POST", "/v1/webhooks/alerts", `{"title":"Test","message":"confidential-content"}`, c.APIKeys[0])
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	var accepted map[string]string
	json.Unmarshal(w.Body.Bytes(), &accepted)
	id := accepted["id"]
	if !idPattern.MatchString(id) || q.jobs[id].Payload.Route != "alerts" {
		t.Fatal(accepted)
	}
	status := request(h, "GET", w.Header().Get("Location"), "", c.APIKeys[0])
	if status.Code != 200 || !strings.Contains(status.Body.String(), `"status":"pending"`) {
		t.Fatal(status.Body.String())
	}
	for _, secret := range []string{"confidential-content", "fingerprint", "webhook", c.APIKeys[0]} {
		if strings.Contains(status.Body.String(), secret) {
			t.Fatalf("receipt leaks %s", secret)
		}
	}
	if request(h, "GET", w.Header().Get("Location"), "", "").Code != 401 {
		t.Fatal("receipt is public")
	}
	r := httptest.NewRequest("POST", "/v1/notify", strings.NewReader(`{"message":"hello"}`))
	r.Header.Set("Authorization", "Bearer "+c.APIKeys[0])
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal("accepted non-JSON content type")
	}
}

func TestRateLimitAndKeyRotation(t *testing.T) {
	c := testConfig(t)
	c.RequestsPerMinute = 1
	c.APIKeys = append(c.APIKeys, strings.Repeat("b", 64))
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	h := newHandler(c, q)
	if request(h, "POST", "/v1/notify", `{"message":"one"}`, c.APIKeys[1]).Code != 202 {
		t.Fatal("second key rejected")
	}
	if request(h, "POST", "/v1/notify", `{"message":"two"}`, c.APIKeys[0]).Code != 429 {
		t.Fatal("global rate limit bypassed")
	}
	if request(h, "GET", "/healthz", "", "").Code != 200 {
		t.Fatal("healthcheck rate limited")
	}
}

func TestMessageBoundaries(t *testing.T) {
	for _, tc := range []struct {
		provider, title, body string
		valid                 bool
	}{
		{"discord", "", strings.Repeat("😀", 1000), true},
		{"discord", "A", strings.Repeat("😀", 999), false},
		{"slack", "", strings.Repeat("a", 3000), true},
		{"slack", "", strings.Repeat("a", 3001), false},
		{"telegram", "", strings.Repeat("a", 4096), true},
		{"telegram", "A", strings.Repeat("a", 4096), false},
		{"pushover", "", strings.Repeat("界", 1024), true},
		{"pushover", "", strings.Repeat("界", 1025), false},
		{"ntfy", "", strings.Repeat("\n", 2100), false},
		{"ntfy", "", "hello", true},
		{"discord", strings.Repeat("t", 251), "hello", false},
	} {
		c := testConfig(t)
		c.Destinations["chat"] = Destination{Provider: tc.provider, Topic: "test"}
		err := validateMessage(Message{Route: "default", Title: tc.title, Message: tc.body}, c)
		if (err == nil) != tc.valid {
			t.Errorf("%s boundary: %v", tc.provider, err)
		}
	}
}

func TestConcurrentIngress(t *testing.T) {
	c := testConfig(t)
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	h := newHandler(c, q)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			w := request(h, "POST", "/v1/notify", `{"message":"hello"}`, c.APIKeys[0])
			if w.Code != 202 {
				t.Error(w.Code)
			}
		})
	}
	wg.Wait()
	if len(q.jobs) != 20 {
		t.Fatalf("lost jobs: %d", len(q.jobs))
	}
}
