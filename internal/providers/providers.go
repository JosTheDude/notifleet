// Package providers builds and sends outbound requests to each supported
// destination, and validates that a message fits every provider it will be
// routed to.
package providers

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"notifleet/internal/config"
)

var (
	errUnknownRoute  = errors.New("unknown route")
	errDiscordLimit  = errors.New("Discord message exceeds 2000 UTF-16 units including title")
	errSlackLimit    = errors.New("Slack message exceeds 3000 UTF-16 units including title")
	errTelegramLimit = errors.New("Telegram message exceeds 4096 UTF-16 units including title")
	errPushoverLimit = errors.New("Pushover message exceeds 1024 characters")
	errNtfyLimit     = errors.New("ntfy JSON payload exceeds 4096 bytes")
)

// publicIP blocks non-public addresses at dial time, after DNS resolution.
// Dial the checked IP itself to avoid DNS-rebinding races. Environment
// proxies are off.
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

// NewDeliveryClient returns an HTTP client hardened against SSRF: it dials
// only public IPs unless allowPrivate is set (for a trusted self-hosted
// ntfy destination), refuses to follow redirects so a compromised or
// misconfigured endpoint cannot exfiltrate a bearer token via 3xx, and
// disables environment proxies. Feed fetches use the same client with
// allowPrivate always false.
func NewDeliveryClient(allowPrivate bool) *http.Client {
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

// ValidateMessage rejects a message that would exceed any provider's limit
// among the destinations its route targets, rather than silently truncating
// it. c.Routes[m.Route] must already be known to exist to the caller (an
// unknown route is itself rejected here too).
func ValidateMessage(m Message, c config.Config) error {
	targets, ok := c.Routes[m.Route]
	if !ok {
		return errUnknownRoute
	}
	for _, name := range targets {
		d := c.Destinations[name]
		switch d.Provider {
		case "discord":
			n := len(utf16.Encode([]rune(m.text())))
			if d.PingEveryone {
				n += len(utf16.Encode([]rune("@everyone ")))
			}
			if n > 2000 {
				return errDiscordLimit
			}
		case "slack":
			if len(utf16.Encode([]rune(m.text()))) > 3000 {
				return errSlackLimit
			}
		case "telegram":
			if len(utf16.Encode([]rune(m.text()))) > 4096 {
				return errTelegramLimit
			}
		case "pushover":
			if utf8.RuneCountInString(m.Message) > 1024 {
				return errPushoverLimit
			}
		case "ntfy":
			body, _ := json.Marshal(map[string]any{"topic": d.Topic, "title": m.Title, "message": m.Message})
			if len(body) > 4096 {
				return errNtfyLimit
			}
		}
	}
	return nil
}

func providerRequest(ctx context.Context, d config.Destination, m Message) (*http.Request, error) {
	endpoint, contentType := d.WebhookURL, "application/json"
	var payload any
	var body []byte
	switch d.Provider {
	case "discord":
		endpoint += "?wait=true"
		content, parse := m.text(), []string{}
		if d.PingEveryone {
			content, parse = "@everyone "+content, []string{"everyone"}
		}
		payload = map[string]any{"content": content, "allowed_mentions": map[string]any{"parse": parse}}
	case "slack":
		// Escape the fallback too: notification previews must not parse mentions.
		escaped := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(m.text())
		payload = map[string]any{"text": escaped, "mrkdwn": false, "unfurl_links": false, "unfurl_media": false, "blocks": []any{map[string]any{"type": "section", "text": map[string]any{"type": "plain_text", "text": m.text(), "emoji": false}}}}
	case "pushover":
		endpoint, contentType = "https://api.pushover.net/1/messages.json", "application/x-www-form-urlencoded"
		form := url.Values{"token": {d.Token}, "user": {d.User}, "title": {m.Title}, "message": {m.Message}, "priority": {strconv.Itoa(d.Priority)}}
		if d.Priority == 2 {
			form.Set("retry", strconv.Itoa(d.RetrySeconds))
			form.Set("expire", strconv.Itoa(d.ExpireSeconds))
		}
		body = []byte(form.Encode())
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

// Deliver sends one message to one destination and classifies the outcome.
func Deliver(ctx context.Context, client *http.Client, d config.Destination, m Message) DeliveryResult {
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
