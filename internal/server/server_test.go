package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"notifleet/internal/config"
	"notifleet/internal/queue"
	"notifleet/internal/server"
)

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

func TestAPI(t *testing.T) {
	c := testConfig(t)
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	h := server.NewHandler(c, q)
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
		{"POST", "/v1/notify", `{"title":"` + strings.Repeat("t", 251) + `","message":"hello"}`, c.APIKeys[0], 422},
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
	if len(q.Jobs()) != 0 {
		t.Fatal("invalid requests queued messages")
	}
	w := request(h, "POST", "/v1/webhooks/alerts", `{"title":"Test","message":"confidential-content"}`, c.APIKeys[0])
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	var accepted map[string]string
	json.Unmarshal(w.Body.Bytes(), &accepted)
	id := accepted["id"]
	job, ok := q.Job(id)
	if !ok || job.Payload.Route != "alerts" {
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
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r)
	if w2.Code != 415 {
		t.Fatal("accepted non-JSON content type")
	}
}

func TestRateLimitAndKeyRotation(t *testing.T) {
	c := testConfig(t)
	c.RequestsPerMinute = 1
	c.APIKeys = append(c.APIKeys, strings.Repeat("b", 64))
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	h := server.NewHandler(c, q)
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

func TestConcurrentIngress(t *testing.T) {
	c := testConfig(t)
	q, err := queue.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	h := server.NewHandler(c, q)
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
	if len(q.Jobs()) != 20 {
		t.Fatalf("lost jobs: %d", len(q.Jobs()))
	}
}
