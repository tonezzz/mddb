package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	json "mddb/internal/jsonx"
)

type statsTestResponse struct {
	TotalDocuments   int `json:"totalDocuments"`
	TotalRevisions   int `json:"totalRevisions"`
	TotalMetaIndices int `json:"totalMetaIndices"`
	Collections      []struct {
		Name           string `json:"name"`
		DocumentCount  int    `json:"documentCount"`
		RevisionCount  int    `json:"revisionCount"`
		MetaIndexCount int    `json:"metaIndexCount"`
		Checksum       string `json:"checksum"`
	} `json:"collections"`
}

func getStats(t *testing.T, s *Server, url string) statsTestResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	s.handleStats(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: expected 200, got %d: %s", url, rec.Code, rec.Body.String())
	}
	var resp statsTestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return resp
}

// The checksum folded into the single docs walk must be identical to what
// collectionChecksum() reports via its own prefix walk.
func TestStatsCache_ChecksumParity(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	addTestDoc(t, s, "blog", "p1", "en", "first document body", map[string][]string{"tag": {"go"}})
	addTestDoc(t, s, "blog", "p2", "en", "second document body", map[string][]string{"tag": {"rust"}})
	addTestDoc(t, s, "wiki", "w1", "en", "wiki page", nil)

	resp := getStats(t, s, "/v1/stats")
	seen := map[string]bool{}
	for _, c := range resp.Collections {
		seen[c.Name] = true
		want, _ := s.collectionChecksum(c.Name)
		if c.Checksum != want {
			t.Errorf("collection %s: stats checksum %s, collectionChecksum %s", c.Name, c.Checksum, want)
		}
	}
	if !seen["blog"] || !seen["wiki"] {
		t.Errorf("expected blog and wiki in stats, got %v", seen)
	}
}

// Within the TTL the handler serves the cached snapshot; ?refresh=1 recomputes.
func TestStatsCache_ServesCachedThenForcedRefresh(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	addTestDoc(t, s, "blog", "p1", "en", "one", nil)
	if got := getStats(t, s, "/v1/stats").TotalDocuments; got != 1 {
		t.Fatalf("expected 1 doc, got %d", got)
	}

	addTestDoc(t, s, "blog", "p2", "en", "two", nil)

	// Still inside the TTL: cached answer.
	if got := getStats(t, s, "/v1/stats").TotalDocuments; got != 1 {
		t.Fatalf("expected cached count 1, got %d", got)
	}

	// Forced recompute sees the new doc.
	if got := getStats(t, s, "/v1/stats?refresh=1").TotalDocuments; got != 2 {
		t.Fatalf("expected refreshed count 2, got %d", got)
	}
}

// Past the TTL the handler serves the stale snapshot immediately and kicks off
// one background recompute; a later request sees the new numbers.
func TestStatsCache_StaleWhileRevalidate(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()
	s.statsCache.ttl = time.Millisecond

	addTestDoc(t, s, "blog", "p1", "en", "one", nil)
	if got := getStats(t, s, "/v1/stats").TotalDocuments; got != 1 {
		t.Fatalf("expected 1 doc, got %d", got)
	}

	addTestDoc(t, s, "blog", "p2", "en", "two", nil)
	time.Sleep(2 * s.statsCache.ttl)

	// First request after expiry serves the stale snapshot and schedules a
	// refresh; poll until the refreshed snapshot lands.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := getStats(t, s, "/v1/stats").TotalDocuments
		if got == 2 {
			return
		}
		if got != 1 {
			t.Fatalf("unexpected doc count %d", got)
		}
		if time.Now().After(deadline) {
			t.Fatal("stale snapshot never refreshed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Concurrent cold-cache calls share the single in-flight compute.
func TestStatsCache_ConcurrentColdCalls(t *testing.T) {
	s, cleanup := newHandlerTestServer(t)
	defer cleanup()

	addTestDoc(t, s, "blog", "p1", "en", "one", nil)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cols, err := s.statsCache.snapshot(s)
			if err != nil {
				errs <- err
				return
			}
			if cols["blog"] == nil || cols["blog"].DocumentCount != 1 {
				errs <- errStatsUnavailable
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestStatsKeyCollection(t *testing.T) {
	cases := []struct {
		key      string
		wantName string
		wantDoc  bool
		wantNil  bool
	}{
		{"doc|blog|abc123", "blog", true, false},
		{"doc|blog|id|extra", "blog", true, false}, // id may contain '|'
		{"rev|blog|abc|00000000000000000001", "blog", false, false},
		{"meta|blog|tag|go|abc", "blog", false, false},
		{"doc|acme/docs|abc", "acme/docs", true, false},
		{"doc|blog", "blog", false, false}, // two fields: counted, not checksummed
		{"noseparator", "", false, true},
		{"doc||abc", "", true, false}, // empty collection still yields a name
	}
	for _, tc := range cases {
		name, isDoc := statsKeyCollection([]byte(tc.key))
		if tc.wantNil {
			if name != nil {
				t.Errorf("%q: expected nil name, got %q", tc.key, name)
			}
			continue
		}
		if string(name) != tc.wantName || isDoc != tc.wantDoc {
			t.Errorf("%q: got (%q, %v), want (%q, %v)", tc.key, name, isDoc, tc.wantName, tc.wantDoc)
		}
	}
}
