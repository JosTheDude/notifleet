// Package server implements Notifleet's JSON API: bearer auth, rate
// limiting, message validation, and sanitized delivery receipts.
package server

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
	"unicode/utf8"

	"notifleet/internal/config"
	"notifleet/internal/providers"
	"notifleet/internal/queue"
)

func decodeStrict(r io.Reader, v any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected exactly one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func apiError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func validateMessage(m providers.Message, c config.Config) error {
	if strings.TrimSpace(m.Message) == "" || !utf8.ValidString(m.Message) || !utf8.ValidString(m.Title) || utf8.RuneCountInString(m.Title) > 250 || len(m.Message) > 16000 || strings.ContainsRune(m.Message, '\x00') || strings.ContainsRune(m.Title, '\x00') {
		return errors.New("invalid message")
	}
	return providers.ValidateMessage(m, c)
}

// NewHandler builds the full Notifleet HTTP API, including bearer auth,
// per-process rate limiting, and security headers on every response.
func NewHandler(c config.Config, q *queue.Queue) http.Handler {
	keys := make([][32]byte, len(c.APIKeys))
	for i, key := range c.APIKeys {
		keys[i] = sha256.Sum256([]byte(key))
	}
	var rateMu sync.Mutex
	tokens, last := float64(c.RequestsPerMinute), time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if !q.Healthy() {
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
		var m providers.Message
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
		id, err := q.Enqueue(m)
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
		job, ok := q.Job(r.PathValue("id"))
		if !ok {
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
		writeJSON(w, 200, map[string]any{"id": job.ID, "status": state, "created_at": job.CreatedAt, "completed_at": job.CompletedAt, "targets": targets})
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
