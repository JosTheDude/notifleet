package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func apiError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func validateMessage(m Message, c Config) error {
	if strings.TrimSpace(m.Message) == "" || !utf8.ValidString(m.Message) || !utf8.ValidString(m.Title) || utf8.RuneCountInString(m.Title) > 250 || len(m.Message) > 16000 || strings.ContainsRune(m.text(), '\x00') {
		return errors.New("invalid message")
	}
	targets, ok := c.Routes[m.Route]
	if !ok {
		return errors.New("unknown route")
	}
	// Reject over-limit input rather than silently truncating notifications.
	for _, name := range targets {
		d := c.Destinations[name]
		switch d.Provider {
		case "discord":
			if len(utf16.Encode([]rune(m.text()))) > 2000 {
				return errors.New("Discord message exceeds 2000 UTF-16 units including title")
			}
		case "slack":
			if len(utf16.Encode([]rune(m.text()))) > 3000 {
				return errors.New("Slack message exceeds 3000 UTF-16 units including title")
			}
		case "telegram":
			if len(utf16.Encode([]rune(m.text()))) > 4096 {
				return errors.New("Telegram message exceeds 4096 UTF-16 units including title")
			}
		case "pushover":
			if utf8.RuneCountInString(m.Message) > 1024 {
				return errors.New("Pushover message exceeds 1024 characters")
			}
		case "ntfy":
			body, _ := json.Marshal(map[string]any{"topic": d.Topic, "title": m.Title, "message": m.Message})
			if len(body) > 4096 {
				return errors.New("ntfy JSON payload exceeds 4096 bytes")
			}
		}
	}
	return nil
}

func newHandler(c Config, q *Queue) http.Handler {
	keys := make([][32]byte, len(c.APIKeys))
	for i, key := range c.APIKeys {
		keys[i] = sha256.Sum256([]byte(key))
	}
	var rateMu sync.Mutex
	tokens, last := float64(c.RequestsPerMinute), time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		q.mu.Lock()
		healthy := !q.unhealthy
		q.mu.Unlock()
		if !healthy {
			apiError(w, 503, "storage_unavailable")
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	notify := func(w http.ResponseWriter, r *http.Request) {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			apiError(w, 415, "application_json_required")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				apiError(w, 413, "body_too_large")
			} else {
				apiError(w, 400, "invalid_body")
			}
			return
		}
		var m Message
		if !utf8.Valid(body) || decodeStrict(strings.NewReader(string(body)), &m) != nil {
			apiError(w, 400, "invalid_json")
			return
		}
		if route := r.PathValue("route"); route != "" {
			if m.Route != "" && m.Route != route {
				apiError(w, 400, "route_mismatch")
				return
			}
			m.Route = route
		}
		if m.Route == "" {
			m.Route = "default"
		}
		if err := validateMessage(m, c); err != nil {
			apiError(w, 422, err.Error())
			return
		}
		id, err := q.enqueue(m)
		if err != nil {
			apiError(w, 503, "queue_unavailable")
			return
		}
		w.Header().Set("Location", "/v1/notifications/"+id)
		writeJSON(w, 202, map[string]string{"id": id, "status": "queued"})
	}
	mux.HandleFunc("POST /v1/notify", notify)
	mux.HandleFunc("POST /v1/webhooks/{route}", notify)
	mux.HandleFunc("GET /v1/notifications/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !idPattern.MatchString(id) {
			apiError(w, 404, "not_found")
			return
		}
		q.mu.Lock()
		job, ok := q.jobs[id]
		if !ok {
			q.mu.Unlock()
			apiError(w, 404, "not_found")
			return
		}
		// Never expose message contents, credential fingerprints or endpoints.
		targets := make([]map[string]any, 0, len(job.Targets))
		pending, failed := false, 0
		for _, t := range job.Targets {
			v := map[string]any{"name": t.Name, "state": t.State, "attempts": t.Attempts, "code": t.Code}
			if t.State == "pending" {
				pending = true
				v["next_attempt"] = t.NextAttempt
			}
			if t.State == "failed" {
				failed++
			}
			targets = append(targets, v)
		}
		state := "delivered"
		if pending {
			state = "pending"
		} else if failed == len(targets) {
			state = "failed"
		} else if failed > 0 {
			state = "partial"
		}
		response := map[string]any{"id": job.ID, "status": state, "created_at": job.CreatedAt, "completed_at": job.CompletedAt, "targets": targets}
		q.mu.Unlock()
		writeJSON(w, 200, response)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		if r.URL.Path != "/healthz" {
			// All configured keys are checked; neither length nor position of a
			// matching secret changes the comparison loop.
			header := r.Header.Get("Authorization")
			candidate := sha256.Sum256([]byte(strings.TrimPrefix(header, "Bearer ")))
			match := 0
			for _, key := range keys {
				match |= subtle.ConstantTimeCompare(candidate[:], key[:])
			}
			if !strings.HasPrefix(header, "Bearer ") || match != 1 || len(r.Header.Values("Authorization")) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				apiError(w, 401, "unauthorized")
				return
			}
			rateMu.Lock()
			now := time.Now()
			tokens = min(float64(c.RequestsPerMinute), tokens+now.Sub(last).Seconds()*float64(c.RequestsPerMinute)/60)
			last = now
			allowed := tokens >= 1
			if allowed {
				tokens--
			}
			rateMu.Unlock()
			if !allowed {
				w.Header().Set("Retry-After", "60")
				apiError(w, 429, "rate_limited")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
