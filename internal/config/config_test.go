package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	t.Setenv("TEST_API_KEY", strings.Repeat("k", 64))
	t.Setenv("TEST_WEBHOOK", "https://discord.com/api/webhooks/123/secret")
	base := `# Comments and literal strings are supported.
api_keys = ['env:TEST_API_KEY']

[destinations.chat]
provider = "discord"
webhook_url = "env:TEST_WEBHOOK"

[routes]
default = ["chat"]
`
	for _, tc := range []struct {
		name, data string
		valid      bool
	}{
		{"valid", base, true},
		{"unknown field", strings.Replace(base, `api_keys`, `api_keyz`, 1), false},
		{"unknown nested field", strings.Replace(base, `webhook_url`, `webhok_url`, 1), false},
		{"unknown target", strings.Replace(base, `["chat"]`, `["missing"]`, 1), false},
		{"duplicate target", strings.Replace(base, `["chat"]`, `["chat","chat"]`, 1), false},
		{"literal secret", strings.Replace(base, `env:TEST_API_KEY`, strings.Repeat("a", 64), 1), false},
		{"missing secret", strings.Replace(base, `env:TEST_API_KEY`, "env:NOTIFLEET_TEST_UNSET", 1), false},
		{"trailing garbage", base + `{}`, false},
		{"duplicate key", base + "default = [\"chat\"]\n", false},
		{"duplicate table", base + "[routes]\n", false},
		{"zero attempts", "max_attempts = 0\n" + base, false},
		{"wrong type", "max_attempts = 'five'\n" + base, false},
		{"oversized config", base + "#" + strings.Repeat("x", 1<<20), false},
		{"old JSON rejected", `{"api_keys":["env:TEST_API_KEY"]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if tc.valid && (c.APIKeys[0] != strings.Repeat("k", 64) || c.MaxAttempts != 5 || c.Listen != "127.0.0.1:8080") {
				t.Fatal("incorrect defaults or environment resolution")
			}
		})
	}
}

func TestFeedValidation(t *testing.T) {
	t.Setenv("TEST_API_KEY", strings.Repeat("k", 64))
	t.Setenv("TEST_WEBHOOK", "https://discord.com/api/webhooks/123/secret")
	base := `api_keys = ['env:TEST_API_KEY']

[destinations.chat]
provider = "discord"
webhook_url = "env:TEST_WEBHOOK"

[routes]
default = ["chat"]

[feeds.news]
url = "https://example.com/feed.xml"
fleet = "default"
poll_seconds = 300
`
	for _, tc := range []struct {
		name, data string
		valid      bool
	}{
		{"valid", base, true},
		{"valid with query", strings.Replace(base, `feed.xml"`, `feed.xml?format=rss"`, 1), true},
		{"http rejected", strings.Replace(base, "https://", "http://", 1), false},
		{"credentials rejected", strings.Replace(base, "https://example.com", "https://user:pass@example.com", 1), false},
		{"unknown fleet", strings.Replace(base, `fleet = "default"`, `fleet = "missing"`, 1), false},
		{"poll too low", strings.Replace(base, "poll_seconds = 300", "poll_seconds = 1", 1), false},
		{"poll too high", strings.Replace(base, "poll_seconds = 300", "poll_seconds = 999999", 1), false},
		{"invalid feed name", strings.Replace(base, "feeds.news", "feeds.\"bad name\"", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestEndpointValidation(t *testing.T) {
	for _, tc := range []struct {
		provider, endpoint string
		valid              bool
	}{
		{"discord", "https://discord.com/api/webhooks/123/token", true},
		{"slack", "https://hooks.slack.com/services/T/B/token", true},
		{"discord", "http://discord.com/api/webhooks/123/token", false},
		{"discord", "https://discord.com.attacker.test/api/webhooks/123/token", false},
		{"discord", "https://discord.com@attacker.test/api/webhooks/123/token", false},
		{"discord", "https://discord.com/api/webhooks/123/token?x=y", false},
		{"discord", "https://discord.com/api/webhooks/123/token/../../other", false},
		{"slack", "https://hooks.slack.com:8443/services/T/B/token", false},
		{"ntfy", "https://notify.example.com", true},
		{"ntfy", "https://notify.example.com/topic", false},
	} {
		d := Destination{Provider: tc.provider, WebhookURL: tc.endpoint, ServerURL: tc.endpoint, Topic: "backups"}
		if err := d.validate(); (err == nil) != tc.valid {
			t.Errorf("%s: %v", tc.endpoint, err)
		}
	}
}

func TestPushoverPriorityValidation(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		priority, retry, expire int
		valid                   bool
	}{
		{"lowest", -2, 0, 0, true},
		{"high", 1, 0, 0, true},
		{"emergency", 2, 30, 10800, true},
		{"priority too low", -3, 0, 0, false},
		{"priority too high", 3, 0, 0, false},
		{"emergency retry too short", 2, 29, 1800, false},
		{"emergency expiry missing", 2, 60, 0, false},
		{"emergency expiry too long", 2, 60, 10801, false},
		{"retry on normal", 0, 60, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Destination{Provider: "pushover", Token: "app", User: "user", Priority: tc.priority, RetrySeconds: tc.retry, ExpireSeconds: tc.expire}
			if err := d.validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}

	if err := (Destination{Provider: "discord", WebhookURL: "https://discord.com/api/webhooks/123/token", Priority: 1}).validate(); err == nil {
		t.Fatal("accepted Pushover priority on Discord")
	}
}

func TestSecretResolutionCannotInjectTOML(t *testing.T) {
	t.Setenv("QUOTED_SECRET", `abc" # [routes.evil]`)
	got, err := secret("env:QUOTED_SECRET")
	if err != nil || got != `abc" # [routes.evil]` {
		t.Fatal("secret was interpreted as configuration")
	}
	t.Setenv("NEWLINE_SECRET", "abc\r\nInjected: header")
	if _, err := secret("env:NEWLINE_SECRET"); err == nil {
		t.Fatal("header injection accepted")
	}
}

func TestTOMLDestinationFields(t *testing.T) {
	t.Setenv("TEST_API_KEY", strings.Repeat("k", 64))
	t.Setenv("TEST_TOKEN", "ntfy-token")
	path := filepath.Join(t.TempDir(), "config.toml")
	data := `listen = "127.0.0.1:9090"
data_dir = "./custom-data"
api_keys = ["env:TEST_API_KEY"]
max_jobs = 321
max_attempts = 7
retention_hours = 48
requests_per_minute = 37
[destinations.lan]
provider = "ntfy"
server_url = "https://notify.example.com"
topic = "backups"
token = "env:TEST_TOKEN"
allow_private_network = true
[routes]
default = ["lan"]
`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:9090" || c.DataDir != "./custom-data" || c.MaxJobs != 321 || c.MaxAttempts != 7 || c.RetentionHours != 48 || c.RequestsPerMinute != 37 {
		t.Fatalf("lost settings: %+v", c)
	}
	d := c.Destinations["lan"]
	if !d.AllowPrivateNetwork || d.Token != "ntfy-token" || d.Topic != "backups" || d.ServerURL != "https://notify.example.com" {
		t.Fatal("lost destination settings")
	}
}
