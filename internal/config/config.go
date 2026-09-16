// Package config loads and validates Notifleet's TOML configuration:
// server limits, destinations, routes, and feeds.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// NamePattern is the allowed charset for destination, route, and feed names.
// Exported because internal/queue re-validates target names loaded from disk.
var NamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

var discordPath = regexp.MustCompile(`^/api(?:/v[0-9]+)?/webhooks/[0-9]+/[A-Za-z0-9._-]+$`)
var slackPath = regexp.MustCompile(`^/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$`)
var botPattern = regexp.MustCompile(`^[0-9]+:[A-Za-z0-9_-]+$`)

type Config struct {
	Listen            string                 `toml:"listen"`
	DataDir           string                 `toml:"data_dir"`
	APIKeys           []string               `toml:"api_keys"`
	MaxJobs           int                    `toml:"max_jobs"`
	MaxAttempts       int                    `toml:"max_attempts"`
	RetentionHours    int                    `toml:"retention_hours"`
	RequestsPerMinute int                    `toml:"requests_per_minute"`
	Destinations      map[string]Destination `toml:"destinations"`
	Routes            map[string][]string    `toml:"routes"`
	Feeds             map[string]Feed        `toml:"feeds"`
}

type Destination struct {
	Provider            string `toml:"provider" json:"provider"`
	WebhookURL          string `toml:"webhook_url" json:"webhook_url,omitempty"`
	Token               string `toml:"token" json:"token,omitempty"`
	User                string `toml:"user" json:"user,omitempty"`
	Priority            int    `toml:"priority" json:"priority,omitempty"`
	RetrySeconds        int    `toml:"retry_seconds" json:"retry_seconds,omitempty"`
	ExpireSeconds       int    `toml:"expire_seconds" json:"expire_seconds,omitempty"`
	ChatID              string `toml:"chat_id" json:"chat_id,omitempty"`
	ServerURL           string `toml:"server_url" json:"server_url,omitempty"`
	Topic               string `toml:"topic" json:"topic,omitempty"`
	AllowPrivateNetwork bool   `toml:"allow_private_network" json:"allow_private_network,omitempty"`
	PingEveryone        bool   `toml:"ping_everyone" json:"ping_everyone,omitempty"`
}

// Feed polls an RSS 2.0 or Atom URL and enqueues one notification per new
// item to the named fleet (an existing route). PollSeconds and MaxItems are
// validated in Load.
type Feed struct {
	URL         string `toml:"url"`
	Fleet       string `toml:"fleet"`
	PollSeconds int    `toml:"poll_seconds"`
	MaxItems    int    `toml:"max_items"`
}

// Secret references are resolved after TOML decoding, so quotes in a secret
// cannot inject configuration. No shell interpolation or implicit dotenv loading.
func secret(value string) (string, error) {
	if !strings.HasPrefix(value, "env:") {
		return "", errors.New("secrets must use env:VARIABLE references")
	}
	name := strings.TrimPrefix(value, "env:")
	v, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("missing environment variable %q", name)
	}
	if strings.ContainsAny(v, "\r\n") {
		return "", errors.New("secret contains a newline")
	}
	return v, nil
}

func Load(path string) (Config, error) {
	c := Config{Listen: "127.0.0.1:8080", DataDir: "./data", MaxJobs: 10000, MaxAttempts: 5, RetentionHours: 24, RequestsPerMinute: 120}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return c, err
	}
	if len(data) > 1<<20 {
		return c, errors.New("configuration exceeds 1 MiB")
	}
	if err = toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&c); err != nil {
		return c, fmt.Errorf("invalid configuration: %w", err)
	}
	if c.Listen == "" || c.DataDir == "" || c.MaxJobs < 1 || c.MaxJobs > 100000 || c.MaxAttempts < 1 || c.MaxAttempts > 10 || c.RetentionHours < 1 || c.RetentionHours > 720 || c.RequestsPerMinute < 1 || c.RequestsPerMinute > 60000 {
		return c, errors.New("invalid server limits (jobs: 1–100000, attempts: 1–10, retention hours: 1–720, requests/minute: 1–60000)")
	}
	if len(c.APIKeys) == 0 || len(c.APIKeys) > 16 {
		return c, errors.New("configure 1–16 api_keys")
	}
	for i, key := range c.APIKeys {
		c.APIKeys[i], err = secret(key)
		if err != nil {
			return c, err
		}
		if len(c.APIKeys[i]) < 32 || len(c.APIKeys[i]) > 512 || strings.ContainsAny(c.APIKeys[i], " \t") {
			return c, errors.New("API keys must be 32–512 bytes without whitespace")
		}
	}
	if len(c.Destinations) == 0 || len(c.Destinations) > 64 || len(c.Routes) == 0 || len(c.Routes) > 64 {
		return c, errors.New("configure 1–64 destinations and routes")
	}
	for name, d := range c.Destinations {
		if !NamePattern.MatchString(name) {
			return c, errors.New("invalid destination name")
		}
		for _, field := range []*string{&d.WebhookURL, &d.Token, &d.User} {
			if *field != "" {
				*field, err = secret(*field)
				if err != nil {
					return c, fmt.Errorf("destination %s: %w", name, err)
				}
			}
		}
		if err = d.validate(); err != nil {
			return c, fmt.Errorf("destination %s: %w", name, err)
		}
		c.Destinations[name] = d
	}
	for name, targets := range c.Routes {
		if !NamePattern.MatchString(name) || len(targets) == 0 || len(targets) > 64 {
			return c, errors.New("invalid route name or target count")
		}
		seen := map[string]bool{}
		for _, target := range targets {
			if _, ok := c.Destinations[target]; !ok || seen[target] {
				return c, fmt.Errorf("route %s has an unknown or duplicate destination", name)
			}
			seen[target] = true
		}
	}
	if len(c.Feeds) > 32 {
		return c, errors.New("configure at most 32 feeds")
	}
	for name, feed := range c.Feeds {
		if !NamePattern.MatchString(name) {
			return c, errors.New("invalid feed name")
		}
		if _, err := feedURL(feed.URL); err != nil {
			return c, fmt.Errorf("feed %s: %w", name, err)
		}
		if _, ok := c.Routes[feed.Fleet]; !ok {
			return c, fmt.Errorf("feed %s: fleet %q is not a configured route", name, feed.Fleet)
		}
		if feed.PollSeconds < 60 || feed.PollSeconds > 86400 {
			return c, fmt.Errorf("feed %s: poll_seconds must be 60-86400", name)
		}
		if feed.MaxItems < 0 || feed.MaxItems > 50 {
			return c, fmt.Errorf("feed %s: max_items must be 0-50", name)
		}
	}
	return c, nil
}

// feedURL is deliberately looser than secureURL (query strings and non-443
// ports are common for RSS endpoints); it still requires HTTPS with no
// embedded credentials or fragment. SSRF protection is enforced separately
// at dial time in internal/providers via its dial-time public-IP check,
// applied to feed fetches too.
func feedURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("feed url must be an HTTPS URL without credentials or fragment")
	}
	return u, nil
}

func secureURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Port() != "" && u.Port() != "443") {
		return nil, errors.New("endpoint must be an HTTPS URL on port 443 without credentials, query, or fragment")
	}
	return u, nil
}

func (d Destination) validate() error {
	if d.AllowPrivateNetwork && d.Provider != "ntfy" {
		return errors.New("private network access is only supported for self-hosted ntfy")
	}
	if d.PingEveryone && d.Provider != "discord" {
		return errors.New("ping_everyone is only supported for discord")
	}
	if d.Provider != "pushover" && (d.Priority != 0 || d.RetrySeconds != 0 || d.ExpireSeconds != 0) {
		return errors.New("priority, retry_seconds and expire_seconds are only supported for Pushover")
	}
	switch d.Provider {
	case "discord", "slack":
		u, err := secureURL(d.WebhookURL)
		if err != nil {
			return err
		}
		if d.Provider == "discord" && (u.Hostname() != "discord.com" || !discordPath.MatchString(u.Path) || u.RawPath != "") {
			return errors.New("invalid Discord webhook endpoint")
		}
		if d.Provider == "slack" && (u.Hostname() != "hooks.slack.com" || !slackPath.MatchString(u.Path) || u.RawPath != "") {
			return errors.New("invalid Slack webhook endpoint")
		}
	case "pushover":
		if d.Token == "" || d.User == "" {
			return errors.New("Pushover requires token and user")
		}
		if d.Priority < -2 || d.Priority > 2 {
			return errors.New("Pushover priority must be -2 through 2")
		}
		if d.Priority == 2 {
			if d.RetrySeconds < 30 || d.ExpireSeconds < 1 || d.ExpireSeconds > 10800 {
				return errors.New("Pushover emergency priority requires retry_seconds >= 30 and expire_seconds 1-10800")
			}
		} else if d.RetrySeconds != 0 || d.ExpireSeconds != 0 {
			return errors.New("Pushover retry_seconds and expire_seconds require emergency priority 2")
		}
	case "telegram":
		if !botPattern.MatchString(d.Token) || d.ChatID == "" {
			return errors.New("Telegram requires a valid bot token and chat_id")
		}
	case "ntfy":
		if d.ServerURL == "" {
			return errors.New("ntfy requires server_url")
		}
		u, err := secureURL(d.ServerURL)
		if err != nil {
			return err
		}
		if u.Path != "" && u.Path != "/" {
			return errors.New("ntfy server_url must be a server root")
		}
		if !NamePattern.MatchString(d.Topic) {
			return errors.New("ntfy topic must contain 1–64 letters, digits, underscores or hyphens")
		}
	default:
		return errors.New("provider must be discord, slack, pushover, telegram or ntfy")
	}
	return nil
}

// Fingerprint identifies the exact destination configuration a queued
// target was created against, so config changes never redirect pending
// messages. Existing queue records depend on this JSON encoding.
func (d Destination) Fingerprint() string {
	b, _ := json.Marshal(d)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
