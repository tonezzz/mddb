package main

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// defaultStatsCacheTTL bounds how long a /v1/stats snapshot is served before it
// is refreshed. The snapshot walk is O(total keys) inside one bbolt read
// transaction — on multi-GB databases it takes seconds under load and starves
// sibling requests, so the endpoint must not recompute per request.
const defaultStatsCacheTTL = 60 * time.Second

var errStatsUnavailable = errors.New("stats unavailable")

// CollectionStats is the per-collection block of the /v1/stats response.
type CollectionStats struct {
	Name           string `json:"name"`
	DocumentCount  int    `json:"documentCount"`
	RevisionCount  int    `json:"revisionCount"`
	MetaIndexCount int    `json:"metaIndexCount"`
	Checksum       string `json:"checksum"`
	Type           string `json:"type,omitempty"`
	Description    string `json:"description,omitempty"`
	Icon           string `json:"icon,omitempty"`
	Color          string `json:"color,omitempty"`

	crc uint32 // checksum accumulator; rendered into Checksum after the walk
}

// statsSnapshot is one completed collection-stats computation.
type statsSnapshot struct {
	at   time.Time
	cols map[string]*CollectionStats
}

// statsCache memoizes the expensive part of /v1/stats. At most one recompute
// runs at a time; while it runs callers are served the previous snapshot
// (stale-while-revalidate), so the endpoint stays O(1) once warm.
type statsCache struct {
	mu        sync.Mutex
	snap      *statsSnapshot
	refreshCh chan struct{} // non-nil while a recompute runs; closed on completion
	err       error         // error of the most recent completed recompute
	ttl       time.Duration // 0 → defaultStatsCacheTTL (tests may shrink it)
}

func (c *statsCache) cacheTTL() time.Duration {
	if c.ttl > 0 {
		return c.ttl
	}
	return defaultStatsCacheTTL
}

// snapshot returns the cached collection stats. A cold cache computes
// synchronously (first request only); a stale snapshot is served immediately
// while a single background recompute runs.
func (c *statsCache) snapshot(s *Server) (map[string]*CollectionStats, error) {
	c.mu.Lock()
	if c.snap != nil && time.Since(c.snap.at) < c.cacheTTL() {
		defer c.mu.Unlock()
		return c.snap.cols, nil
	}
	if c.refreshCh != nil {
		done := c.refreshCh
		if c.snap != nil {
			defer c.mu.Unlock()
			return c.snap.cols, nil // stale while the in-flight refresh runs
		}
		c.mu.Unlock()
		<-done // cold cache: wait for the in-flight first compute
		return c.result()
	}
	c.refreshCh = make(chan struct{})
	if c.snap != nil {
		cols := c.snap.cols
		c.mu.Unlock()
		go c.recompute(s)
		return cols, nil
	}
	c.mu.Unlock()
	if err := c.recompute(s); err != nil {
		return nil, err
	}
	return c.result()
}

// refresh recomputes the snapshot synchronously (?refresh=1), piggybacking on
// a recompute that is already running instead of starting a second walk.
func (c *statsCache) refresh(s *Server) (map[string]*CollectionStats, error) {
	c.mu.Lock()
	if c.refreshCh != nil {
		done := c.refreshCh
		c.mu.Unlock()
		<-done
		return c.result()
	}
	c.refreshCh = make(chan struct{})
	c.mu.Unlock()
	if err := c.recompute(s); err != nil {
		return nil, err
	}
	return c.result()
}

// recompute performs the bucket walk and publishes the result. Callers must
// have registered c.refreshCh first; exactly one recompute runs at a time.
func (c *statsCache) recompute(s *Server) error {
	cols, err := s.collectCollectionStats()
	c.mu.Lock()
	if err == nil {
		c.snap = &statsSnapshot{at: time.Now(), cols: cols}
	}
	c.err = err
	done := c.refreshCh
	c.refreshCh = nil
	c.mu.Unlock()
	close(done)
	return err
}

// result returns the current snapshot or the last recompute error.
func (c *statsCache) result() (map[string]*CollectionStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snap != nil {
		return c.snap.cols, nil
	}
	if c.err != nil {
		return nil, c.err
	}
	return nil, errStatsUnavailable
}

// collectCollectionStats walks docs, rev and idxmeta once, inside a single
// read transaction, accumulating per-collection counts and the crc32 checksum
// collectionChecksum() reports — previously one extra prefix-walking read tx
// per collection.
func (s *Server) collectCollectionStats() (map[string]*CollectionStats, error) {
	cols := make(map[string]*CollectionStats)
	err := s.DBView(func(tx *bolt.Tx) error {
		entry := func(name []byte) *CollectionStats {
			cs := cols[string(name)]
			if cs == nil {
				cs = &CollectionStats{Name: string(name)}
				cols[cs.Name] = cs
			}
			return cs
		}

		// docs — key format: doc|collection|id
		if bDocs := tx.Bucket([]byte("docs")); bDocs != nil {
			c := bDocs.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				name, isDoc := statsKeyCollection(k)
				if name == nil {
					continue
				}
				cs := entry(name)
				cs.DocumentCount++
				if isDoc {
					// Same fold as collectionChecksum: key + first 64 bytes of value.
					h := crc32.ChecksumIEEE(k)
					if len(v) > 64 {
						h ^= crc32.ChecksumIEEE(v[:64])
					} else {
						h ^= crc32.ChecksumIEEE(v)
					}
					cs.crc ^= h
				}
			}
		}

		// rev — key format: rev|collection|docID|ts
		if bRev := tx.Bucket([]byte("rev")); bRev != nil {
			c := bRev.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if name, _ := statsKeyCollection(k); name != nil {
					entry(name).RevisionCount++
				}
			}
		}

		// idxmeta — key format: meta|collection|key|value|docID
		if bIdx := tx.Bucket([]byte("idxmeta")); bIdx != nil {
			c := bIdx.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if name, _ := statsKeyCollection(k); name != nil {
					entry(name).MetaIndexCount++
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, cs := range cols {
		cs.Checksum = fmt.Sprintf("%08x", cs.crc)
	}
	return cols, nil
}

// statsKeyCollection extracts the collection segment — the field between the
// first and second '|' — of a scanned bucket key, and reports whether the key
// is a document record (doc|<coll>|...), which is what the per-collection
// checksum covers. Keys without a '|' are ignored.
func statsKeyCollection(k []byte) (name []byte, isDoc bool) {
	i := bytes.IndexByte(k, '|')
	if i < 0 {
		return nil, false
	}
	rest := k[i+1:]
	j := bytes.IndexByte(rest, '|')
	if j < 0 {
		return rest, false
	}
	return rest[:j], i == 3 && k[0] == 'd' && k[1] == 'o' && k[2] == 'c'
}
