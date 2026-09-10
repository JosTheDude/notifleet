package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func mockClient(status int, body string, headers http.Header) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
}

func TestProviderRequests(t *testing.T) {
	m := Message{Title: "Backup & restore", Message: "Hello <!channel> @everyone\n世界"}
	for _, provider := range []string{"discord", "slack", "pushover", "telegram", "ntfy"} {
		t.Run(provider, func(t *testing.T) {
			d := Destination{Provider: provider, WebhookURL: "https://example.com/hook", Token: "123:secret", User: "user&key", ChatID: "-10042", ServerURL: "https://ntfy.sh", Topic: "backups"}
			r, err := providerRequest(context.Background(), d, m)
			if err != nil {
				t.Fatal(err)
			}
			if r.Method != "POST" {
				t.Fatal(r.Method)
			}
			body, _ := io.ReadAll(r.Body)
			if provider == "pushover" {
				v, err := url.ParseQuery(string(body))
				if err != nil || v.Get("message") != m.Message || v.Get("title") != m.Title || v.Get("token") != d.Token || v.Get("user") != d.User || r.URL.String() != "https://api.pushover.net/1/messages.json" {
					t.Fatalf("bad Pushover request: %s", body)
				}
				return
			}
			var v map[string]any
			if err := json.Unmarshal(body, &v); err != nil {
				t.Fatal(err)
			}
			switch provider {
			case "discord":
				if v["content"] != "Backup & restore\n\nHello <!channel> @everyone\n世界" || r.URL.Query().Get("wait") != "true" || len(v["allowed_mentions"].(map[string]any)["parse"].([]any)) != 0 {
					t.Fatal(string(body))
				}
			case "slack":
				if v["mrkdwn"] != false || strings.Contains(v["text"].(string), "<!channel>") {
					t.Fatal(string(body))
				}
				block := v["blocks"].([]any)[0].(map[string]any)["text"].(map[string]any)
				if block["type"] != "plain_text" || block["text"] != m.text() {
					t.Fatal(string(body))
				}
			case "telegram":
				if r.URL.String() != "https://api.telegram.org/bot123:secret/sendMessage" || v["chat_id"] != "-10042" || v["text"] != m.text() || v["parse_mode"] != nil {
					t.Fatal(string(body))
				}
			case "ntfy":
				if r.URL.String() != "https://ntfy.sh/" || v["topic"] != "backups" || v["message"] != m.Message || v["title"] != m.Title || r.Header.Get("Authorization") != "Bearer 123:secret" {
					t.Fatal(string(body))
				}
			}
		})
	}
}

func TestProviderResponses(t *testing.T) {
	for _, tc := range []struct {
		provider       string
		status         int
		body           string
		success, retry bool
	}{
		{"discord", 200, `{}`, true, false},
		{"slack", 200, "ok", true, false},
		{"slack", 200, "invalid_token", false, false},
		{"pushover", 200, `{"status":1}`, true, false},
		{"pushover", 200, `{"status":0}`, false, false},
		{"telegram", 200, `{"ok":true}`, true, false},
		{"telegram", 200, `{"ok":false}`, false, false},
		{"ntfy", 200, `{"id":"abc","event":"message"}`, true, false},
		{"ntfy", 200, `<html>login</html>`, false, false},
		{"discord", 400, `secret-token`, false, false},
		{"discord", 401, `secret-token`, false, false},
		{"discord", 302, `secret-token`, false, false},
		{"discord", 429, `{"retry_after":1.5}`, false, true},
		{"telegram", 429, `{"parameters":{"retry_after":45}}`, false, true},
		{"discord", 503, `secret-token`, false, true},
	} {
		t.Run(tc.provider+"/"+tc.body, func(t *testing.T) {
			d := Destination{Provider: tc.provider, WebhookURL: "https://example.com", ServerURL: "https://ntfy.sh"}
			r := deliver(context.Background(), mockClient(tc.status, tc.body, nil), d, Message{Message: "hello"})
			if r.Success != tc.success || r.Retry != tc.retry || strings.Contains(r.Code, "secret") {
				t.Fatalf("unexpected result: %+v", r)
			}
			if tc.status == 429 && r.RetryAfter <= 0 {
				t.Fatal("ignored provider retry delay")
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for raw, want := range map[string]time.Duration{"15": 15 * time.Second, "-1": 0, "junk": 0, "9999999": 24 * time.Hour, now.Add(90 * time.Second).Format(http.TimeFormat): 90 * time.Second} {
		if got := retryAfter(raw, now); got != want {
			t.Errorf("%q: %v != %v", raw, got, want)
		}
	}
}

func TestNetworkBoundary(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.2.3.4", "172.16.1.2", "192.168.0.1", "169.254.169.254", "100.100.100.200", "0.0.0.0", "224.0.0.1", "255.255.255.255", "::1", "::ffff:127.0.0.1", "fe80::1", "fec0::1", "fc00::1", "::127.0.0.1", "64:ff9b::7f00:1", "2002:7f00:1::", "2001:20::1"} {
		if publicIP(netip.MustParseAddr(address)) {
			t.Errorf("allowed %s", address)
		}
	}
	for _, address := range []string{"1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"} {
		if !publicIP(netip.MustParseAddr(address)) {
			t.Errorf("blocked %s", address)
		}
	}
	client := newDeliveryClient(false)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if conn, err := client.Transport.(*http.Transport).DialContext(ctx, "tcp", "127.0.0.1:80"); err == nil {
		conn.Close()
		t.Fatal("dialed loopback")
	}
	if client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("environment proxy enabled")
	}
}

func TestRedirectDoesNotForwardSecrets(t *testing.T) {
	received := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received = true }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer origin.Close()
	client := newDeliveryClient(true)
	defer client.CloseIdleConnections()
	r := deliver(context.Background(), client, Destination{Provider: "ntfy", ServerURL: origin.URL, Token: "private-token"}, Message{Message: "private-message"})
	if r.Success || r.Retry || received {
		t.Fatalf("redirect followed: %+v", r)
	}
}
