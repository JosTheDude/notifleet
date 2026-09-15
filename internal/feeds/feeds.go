// Package feeds polls RSS 2.0 and Atom URLs and pushes new items into a
// fleet (an existing configured route) as ordinary notifications.
package feeds

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"html"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"notifleet/internal/config"
	"notifleet/internal/providers"
	"notifleet/internal/queue"
)

var htmlTagPattern = regexp.MustCompile(`<[^>]*>`)

// RSS 2.0 item.
type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	Description string `xml:"description"`
}

type rssFeed struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

// Atom entry.
type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

type atomEntry struct {
	Title   string     `xml:"title"`
	ID      string     `xml:"id"`
	Summary string     `xml:"summary"`
	Content string     `xml:"content"`
	Links   []atomLink `xml:"link"`
}

type atomFeed struct {
	Entries []atomEntry `xml:"entry"`
}

type feedItem struct {
	id, title, body, link string
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// parseFeed accepts RSS 2.0 or Atom. Go's encoding/xml never resolves
// external entities or DTD entity expansion, so this is not XXE/billion-laughs
// vulnerable by construction; a body-size cap is still enforced by the caller.
func parseFeed(body []byte) ([]feedItem, error) {
	var rf rssFeed
	rssErr := xml.Unmarshal(body, &rf)
	var af atomFeed
	atomErr := xml.Unmarshal(body, &af)
	if rssErr != nil && atomErr != nil {
		return nil, rssErr
	}
	items := make([]feedItem, 0, len(rf.Channel.Items)+len(af.Entries))
	for _, it := range rf.Channel.Items {
		id := firstNonEmpty(it.GUID, it.Link, it.Title)
		if id == "" {
			continue
		}
		items = append(items, feedItem{id: id, title: it.Title, body: it.Description, link: it.Link})
	}
	for _, e := range af.Entries {
		link := ""
		for _, l := range e.Links {
			if l.Rel == "" || l.Rel == "alternate" {
				link = l.Href
				break
			}
		}
		id := firstNonEmpty(e.ID, link, e.Title)
		if id == "" {
			continue
		}
		items = append(items, feedItem{id: id, title: e.Title, body: firstNonEmpty(e.Summary, e.Content), link: link})
	}
	return items, nil
}

func stripHTML(s string) string {
	s = htmlTagPattern.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}

func feedMessage(feedName string, feed config.Feed, item feedItem) providers.Message {
	title := truncateRunes(firstNonEmpty(strings.TrimSpace(item.title), feedName), 120)
	body := truncateRunes(stripHTML(item.body), 600)
	if item.link != "" {
		if body != "" {
			body += "\n"
		}
		body += item.link
	}
	if body == "" {
		body = feedName
	}
	return providers.Message{Route: feed.Fleet, Title: title, Message: body}
}

type feedState struct {
	Primed bool     `json:"primed"`
	Seen   []string `json:"seen"`
}

func feedStatePath(dataDir, name string) string {
	return filepath.Join(dataDir, "feeds", name+".json")
}

func loadFeedState(dataDir, name string) (feedState, error) {
	data, err := os.ReadFile(feedStatePath(dataDir, name))
	if errors.Is(err, os.ErrNotExist) {
		return feedState{}, nil
	}
	if err != nil {
		return feedState{}, err
	}
	var st feedState
	if json.Unmarshal(data, &st) != nil {
		return feedState{}, errors.New("corrupt feed state")
	}
	return st, nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// saveFeedState uses the same atomic temp-file-then-rename-plus-fsync pattern
// as the queue package's job persistence so feed dedup state survives a
// crash without corruption.
func saveFeedState(dataDir, name string, st feedState) error {
	dir := filepath.Join(dataDir, "feeds")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(st)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), feedStatePath(dataDir, name)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

const maxSeenItems = 2000

// pollFeed fetches and parses one feed, then enqueues notifications for items
// not previously seen. The first successful poll of a feed only primes the
// seen set (no backfill notification flood). Items are marked seen only
// after a successful enqueue, so a crash mid-poll causes at most a duplicate
// notification on the next poll, never a silently dropped one.
func pollFeed(ctx context.Context, client *http.Client, c config.Config, name string, feed config.Feed, q *queue.Queue) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feed.URL, nil)
	if err != nil {
		slog.Error("feed request build failed", "feed", name, "error", err.Error())
		return
	}
	req.Header.Set("User-Agent", "Notifleet/1")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml")
	resp, err := client.Do(req)
	if err != nil {
		slog.Warn("feed fetch failed", "feed", name, "error", err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Warn("feed fetch rejected", "feed", name, "status", resp.StatusCode)
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		slog.Warn("feed read failed", "feed", name, "error", err.Error())
		return
	}
	items, err := parseFeed(body)
	if err != nil {
		slog.Warn("feed parse failed", "feed", name, "error", err.Error())
		return
	}
	st, err := loadFeedState(c.DataDir, name)
	if err != nil {
		slog.Error("feed state unreadable", "feed", name, "error", err.Error())
		return
	}
	seen := make(map[string]bool, len(st.Seen))
	for _, id := range st.Seen {
		seen[id] = true
	}
	var toMark []string
	if !st.Primed {
		for _, it := range items {
			if !seen[it.id] {
				toMark = append(toMark, it.id)
			}
		}
		st.Primed = true
	} else {
		maxItems := feed.MaxItems
		if maxItems <= 0 {
			maxItems = 20
		}
		notified := 0
		for _, it := range items {
			if seen[it.id] {
				continue
			}
			if notified >= maxItems {
				break
			}
			m := feedMessage(name, feed, it)
			if err := providers.ValidateMessage(m, c); err != nil {
				slog.Warn("feed item skipped", "feed", name, "error", err.Error())
				toMark = append(toMark, it.id)
				continue
			}
			if _, err := q.Enqueue(m); err != nil {
				slog.Error("feed item enqueue failed", "feed", name, "error", err.Error())
				continue
			}
			toMark = append(toMark, it.id)
			notified++
		}
	}
	if len(toMark) == 0 {
		return
	}
	for _, id := range toMark {
		if !seen[id] {
			seen[id] = true
			st.Seen = append(st.Seen, id)
		}
	}
	if len(st.Seen) > maxSeenItems {
		st.Seen = st.Seen[len(st.Seen)-maxSeenItems:]
	}
	if err := saveFeedState(c.DataDir, name, st); err != nil {
		slog.Error("feed state save failed", "feed", name, "error", err.Error())
	}
}

// Run polls every configured feed on its own interval until ctx is
// cancelled. It uses the same public-IP-only, no-redirect delivery client as
// provider sends, so feed fetches get identical SSRF/TLS protections.
func Run(ctx context.Context, c config.Config, q *queue.Queue) {
	if len(c.Feeds) == 0 {
		return
	}
	client := providers.NewDeliveryClient(false)
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for name, feed := range c.Feeds {
		wg.Add(1)
		go func(name string, feed config.Feed) {
			defer wg.Done()
			pollFeed(ctx, client, c, name, feed, q)
			ticker := time.NewTicker(time.Duration(feed.PollSeconds) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					pollFeed(ctx, client, c, name, feed, q)
				}
			}
		}(name, feed)
	}
	wg.Wait()
}
