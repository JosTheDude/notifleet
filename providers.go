package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Block non-public addresses at dial time, after DNS resolution. Dial the
// checked IP itself to avoid DNS-rebinding races. Environment proxies are off.
func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001::/23", "2001:db8::/32", "2002::/16"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}

func newDeliveryClient(allowPrivate bool) *http.Client {
	tr := &http.Transport{
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:    5 * time.Second,
		ResponseHeaderTimeout:  10 * time.Second,
		MaxResponseHeaderBytes: 32 << 10,
		IdleConnTimeout:        60 * time.Second,
		MaxIdleConns:           16,
		MaxIdleConnsPerHost:    2,
	}
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		var last error = &net.AddrError{Err: "no permitted destination address", Addr: "redacted"}
		for _, ip := range ips {
			if !allowPrivate && !publicIP(ip) {
				return nil, last
			}
		}
		for _, ip := range ips {
			conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}
	return &http.Client{Transport: tr, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type Message struct {
	Route   string `json:"route,omitempty"`
	Title   string `json:"title,omitempty"`
	Message string `json:"message"`
}

func (m Message) text() string {
	if m.Title == "" {
		return m.Message
	}
	return m.Title + "\n\n" + m.Message
}

func providerRequest(ctx context.Context, d Destination, m Message) (*http.Request, error) {
	endpoint, contentType := d.WebhookURL, "application/json"
	var payload any
	var body []byte
	switch d.Provider {
	case "discord":
		endpoint += "?wait=true"
		payload = map[string]any{"content": m.text(), "allowed_mentions": map[string]any{"parse": []string{}}}
	case "slack":
		// Escape the fallback too: notification previews must not parse mentions.
		escaped := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(m.text())
		payload = map[string]any{"text": escaped, "mrkdwn": false, "unfurl_links": false, "unfurl_media": false, "blocks": []any{map[string]any{"type": "section", "text": map[string]any{"type": "plain_text", "text": m.text(), "emoji": false}}}}
	case "pushover":
		endpoint, contentType = "https://api.pushover.net/1/messages.json", "application/x-www-form-urlencoded"
		body = []byte(url.Values{"token": {d.Token}, "user": {d.User}, "title": {m.Title}, "message": {m.Message}}.Encode())
	case "telegram":
		endpoint = "https://api.telegram.org/bot" + d.Token + "/sendMessage"
		payload = map[string]any{"chat_id": d.ChatID, "text": m.text(), "link_preview_options": map[string]any{"is_disabled": true}}
	case "ntfy":
		endpoint = strings.TrimRight(d.ServerURL, "/") + "/"
		payload = map[string]any{"topic": d.Topic, "title": m.Title, "message": m.Message}
	}
	if payload != nil {
		body, _ = json.Marshal(payload)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("User-Agent", "Notifleet/1")
	if d.Provider == "ntfy" && d.Token != "" {
		r.Header.Set("Authorization", "Bearer "+d.Token)
	}
	return r, nil
}

type DeliveryResult struct {
	Success    bool
	Retry      bool
	Code       string
	RetryAfter time.Duration
}

func retryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(raw, 10, 32); err == nil && seconds > 0 {
		return min(time.Duration(seconds)*time.Second, 24*time.Hour)
	}
	if date, err := http.ParseTime(raw); err == nil && date.After(now) {
		return min(date.Sub(now), 24*time.Hour)
	}
	return 0
}

func deliver(ctx context.Context, client *http.Client, d Destination, m Message) DeliveryResult {
	r, err := providerRequest(ctx, d, m)
	if err != nil {
		return DeliveryResult{Code: "invalid_destination"}
	}
	resp, err := client.Do(r)
	if err != nil {
		return DeliveryResult{Retry: true, Code: "network_error"}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return DeliveryResult{Retry: true, Code: "response_read_error"}
	}
	result := DeliveryResult{Code: "http_" + strconv.Itoa(resp.StatusCode), RetryAfter: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
	if resp.StatusCode == 429 || resp.StatusCode == 408 || resp.StatusCode >= 500 {
		result.Retry = true
		if resp.StatusCode == 429 {
			var limit struct {
				RetryAfter float64 `json:"retry_after"`
				Parameters struct {
					RetryAfter int64 `json:"retry_after"`
				} `json:"parameters"`
			}
			if json.Unmarshal(body, &limit) == nil {
				seconds := max(limit.RetryAfter, float64(limit.Parameters.RetryAfter))
				if seconds > 0 {
					result.RetryAfter = max(result.RetryAfter, time.Duration(min(seconds, 86400)*float64(time.Second)))
				}
			}
		}
		return result
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result
	}
	switch d.Provider {
	case "pushover":
		var v struct {
			Status int `json:"status"`
		}
		if json.Unmarshal(body, &v) != nil || v.Status != 1 {
			return DeliveryResult{Code: "provider_rejected"}
		}
	case "telegram":
		var v struct {
			OK bool `json:"ok"`
		}
		if json.Unmarshal(body, &v) != nil || !v.OK {
			return DeliveryResult{Code: "provider_rejected"}
		}
	case "slack":
		if strings.TrimSpace(string(body)) != "ok" {
			return DeliveryResult{Code: "provider_rejected"}
		}
	case "ntfy":
		var v struct {
			ID    string `json:"id"`
			Event string `json:"event"`
		}
		if json.Unmarshal(body, &v) != nil || v.ID == "" || v.Event != "message" {
			return DeliveryResult{Code: "provider_rejected"}
		}
	}
	return DeliveryResult{Success: true, Code: "delivered"}
}
