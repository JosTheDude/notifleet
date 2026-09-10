package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseFeedRSS(t *testing.T) {
	doc := `<?xml version="1.0"?>
<rss version="2.0"><channel>
<item><title>First &amp; Second</title><link>https://example.com/1</link><guid>guid-1</guid><description>&lt;p&gt;Hello&lt;/p&gt;</description></item>
<item><title>No GUID</title><link>https://example.com/2</link><description>Body two</description></item>
</channel></rss>`
	items, err := parseFeed([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].id != "guid-1" || items[0].title != "First & Second" {
		t.Fatalf("unexpected first item: %+v", items[0])
	}
	if items[1].id != "https://example.com/2" {
		t.Fatalf("expected link fallback id, got %q", items[1].id)
	}
}

func TestParseFeedAtom(t *testing.T) {
	doc := `<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>Entry</title><id>urn:entry-1</id><link href="https://example.com/e1" rel="alternate"/><summary>Sum</summary></entry>
</feed>`
	items, err := parseFeed([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].id != "urn:entry-1" || items[0].link != "https://example.com/e1" {
		t.Fatalf("unexpected items: %+v", items)
	}
}

func TestParseFeedInvalid(t *testing.T) {
	if _, err := parseFeed([]byte("not xml at all <<<")); err == nil {
		t.Fatal("expected error for garbage input")
	}
}

func TestTruncateRunes(t *testing.T) {
	if got := truncateRunes("hello", 10); got != "hello" {
		t.Fatalf("unexpected: %q", got)
	}
	got := truncateRunes(strings.Repeat("a", 20), 5)
	if len([]rune(got)) != 5 || !strings.HasSuffix(got, "…") {
		t.Fatalf("unexpected truncation: %q", got)
	}
}

func TestStripHTML(t *testing.T) {
	if got := stripHTML("<p>Hello&nbsp;<b>World</b></p>"); got != "Hello World" {
		t.Fatalf("unexpected: %q", got)
	}
}

func TestFeedStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := feedState{Primed: true, Seen: []string{"a", "b", "c"}}
	if err := saveFeedState(dir, "news", st); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadFeedState(dir, "news")
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Primed || len(loaded.Seen) != 3 {
		t.Fatalf("unexpected state: %+v", loaded)
	}
	if _, err := loadFeedState(dir, "missing"); err != nil {
		t.Fatalf("missing state should not error: %v", err)
	}
}

func TestPollFeedPrimesWithoutNotifying(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<rss version="2.0"><channel><item><guid>1</guid><title>One</title></item></channel></rss>`))
	}))
	defer srv.Close()

	c := testConfig(t)
	dir := c.DataDir
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()

	feed := Feed{URL: srv.URL, Fleet: "default", PollSeconds: 60, MaxItems: 10}
	pollFeed(context.Background(), srv.Client(), c, "news", feed, q)

	if len(q.jobs) != 0 {
		t.Fatalf("first poll should only prime state, got %d jobs", len(q.jobs))
	}
	st, err := loadFeedState(dir, "news")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Primed || len(st.Seen) != 1 {
		t.Fatalf("unexpected primed state: %+v", st)
	}

	// Second poll with a new item should enqueue exactly one notification.
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<rss version="2.0"><channel>
<item><guid>1</guid><title>One</title></item>
<item><guid>2</guid><title>Two</title><description>New item</description></item>
</channel></rss>`))
	})
	pollFeed(context.Background(), srv.Client(), c, "news", feed, q)
	if len(q.jobs) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(q.jobs))
	}

	// Polling again with the same items should not duplicate.
	pollFeed(context.Background(), srv.Client(), c, "news", feed, q)
	if len(q.jobs) != 1 {
		t.Fatalf("expected dedup, still 1 job, got %d", len(q.jobs))
	}
}

func TestPollFeedRespectsMaxItems(t *testing.T) {
	body := `<rss version="2.0"><channel>
<item><guid>1</guid><title>One</title></item>
</channel></rss>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
	defer srv.Close()

	c := testConfig(t)
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	feed := Feed{URL: srv.URL, Fleet: "default", PollSeconds: 60, MaxItems: 1}
	pollFeed(context.Background(), srv.Client(), c, "news", feed, q) // prime

	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<rss version="2.0"><channel>
<item><guid>1</guid><title>One</title></item>
<item><guid>2</guid><title>Two</title></item>
<item><guid>3</guid><title>Three</title></item>
</channel></rss>`))
	})
	pollFeed(context.Background(), srv.Client(), c, "news", feed, q)
	if len(q.jobs) != 1 {
		t.Fatalf("max_items=1 should cap enqueue at 1, got %d", len(q.jobs))
	}
}

func TestRunFeedsStopsOnContextCancel(t *testing.T) {
	c := testConfig(t)
	c.Feeds = map[string]Feed{}
	q, err := openQueue(c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	runFeeds(ctx, c, q) // no feeds configured: should return immediately
}
