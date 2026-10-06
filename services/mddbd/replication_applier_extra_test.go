package main

import (
	"encoding/binary"
	"mddb/internal/binlog"
	"mddb/internal/cache"
	"mddb/internal/schema"
	"mddb/internal/vector"
	"mddb/internal/webhooks"
	"os"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// applierExtraTestServer creates a Server with VectorIndex, WebhookManager, schema.SchemaManager, and Cache
// for comprehensive applier tests.
func applierExtraTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "applier_extra_*.db")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	db, err := bolt.Open(f.Name(), 0600, nil)
	if err != nil {
		_ = os.Remove(f.Name())
		t.Fatal(err)
	}

	s := &Server{
		DB:   db,
		Path: f.Name(),
		Mode: ModeRW,
		BucketNames: BucketNames{
			Docs:    []byte("docs"),
			IdxMeta: []byte("idxmeta"),
			Rev:     []byte("rev"),
			ByKey:   []byte("bykey"),
		},
		Cache:         cache.NewDocumentCache(100, 60),
		LockFreeCache: cache.NewLockFreeCache(100, 60),
	}

	if err := s.ensureBuckets(); err != nil {
		_ = db.Close()
		_ = os.Remove(f.Name())
		t.Fatal(err)
	}

	// Additional buckets
	_ = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"vectors", "webhooks", "schemas"} {
			_, _ = tx.CreateBucketIfNotExists([]byte(name))
		}
		return nil
	})

	// VectorIndex
	s.VectorIndex = vector.NewVectorIndex()
	s.VectorIndex.SetReady()
	s.VectorSearchers = map[string]vector.VectorSearcher{
		"flat": s.VectorIndex,
	}

	// WebhookManager
	s.WebhookManager = webhooks.NewWebhookManager(db)
	_ = s.WebhookManager.EnsureBucket()

	// schema.SchemaManager
	s.SchemaManager = schema.NewSchemaManager(db)
	_ = s.SchemaManager.EnsureBucket()

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(f.Name())
	}
	return s, cleanup
}

// ---------------------------------------------------------------------------
// Test: ApplyBatch with empty entries
// ---------------------------------------------------------------------------

func TestApplyBatch_Empty(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	if err := applier.ApplyBatch(nil); err != nil {
		t.Fatalf("ApplyBatch nil: %v", err)
	}
	if err := applier.ApplyBatch([]*binlog.BinlogEntry{}); err != nil {
		t.Fatalf("ApplyBatch empty: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: ApplyBatch with delete entries
// ---------------------------------------------------------------------------

func TestApplyBatch_WithDeletes(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	entries := []*binlog.BinlogEntry{
		{LSN: 1, Type: binlog.BinlogPut, BucketName: "docs", Key: []byte("doc|blog|x"), Value: []byte(`{"id":"x"}`)},
		{LSN: 2, Type: binlog.BinlogDelete, BucketName: "docs", Key: []byte("doc|blog|x")},
	}

	if err := applier.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	if applier.LastAppliedLSN() != 2 {
		t.Errorf("expected LastAppliedLSN=2, got %d", applier.LastAppliedLSN())
	}

	// Verify deleted
	var val []byte
	_ = s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("docs"))
		val = b.Get([]byte("doc|blog|x"))
		return nil
	})
	if val != nil {
		t.Error("expected document to be deleted")
	}
}

// ---------------------------------------------------------------------------
// Test: ApplyBatch with checkpoint entries (no-op)
// ---------------------------------------------------------------------------

func TestApplyBatch_WithCheckpoints(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	entries := []*binlog.BinlogEntry{
		{LSN: 1, Type: binlog.BinlogCheckpoint, BucketName: "docs"},
		{LSN: 2, Type: binlog.BinlogPut, BucketName: "docs", Key: []byte("doc|blog|y"), Value: []byte(`{"id":"y"}`)},
	}

	if err := applier.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	if applier.LastAppliedLSN() != 2 {
		t.Errorf("expected LastAppliedLSN=2, got %d", applier.LastAppliedLSN())
	}

	// The put should have been applied
	var val []byte
	_ = s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("docs"))
		val = b.Get([]byte("doc|blog|y"))
		return nil
	})
	if val == nil {
		t.Error("expected document to exist")
	}
}

// ---------------------------------------------------------------------------
// Test: ApplyBatch with DeleteBucket entries
// ---------------------------------------------------------------------------

func TestApplyBatch_WithDeleteBucket(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	// Create a test bucket
	_ = s.DB.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("tempbucket"))
		return b.Put([]byte("key"), []byte("val"))
	})

	applier := NewReplicationApplier(s)

	entries := []*binlog.BinlogEntry{
		{LSN: 1, Type: binlog.BinlogDeleteBucket, BucketName: "tempbucket"},
	}

	if err := applier.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	// Verify bucket is deleted
	_ = s.DB.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("tempbucket")) != nil {
			t.Error("expected bucket to be deleted")
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Test: Apply with Checkpoint type (no-op)
// ---------------------------------------------------------------------------

func TestApply_Checkpoint(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogCheckpoint,
		BucketName: "docs",
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply checkpoint: %v", err)
	}
	if applier.LastAppliedLSN() != 1 {
		t.Errorf("expected LastAppliedLSN=1, got %d", applier.LastAppliedLSN())
	}
}

// ---------------------------------------------------------------------------
// Test: Apply with DeleteBucket type
// ---------------------------------------------------------------------------

func TestApply_DeleteBucket(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	// Create bucket
	_ = s.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("tobedeleted"))
		return err
	})

	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogDeleteBucket,
		BucketName: "tobedeleted",
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply DeleteBucket: %v", err)
	}

	_ = s.DB.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("tobedeleted")) != nil {
			t.Error("expected bucket to be deleted")
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Test: updateInMemoryState - webhooks reload
// ---------------------------------------------------------------------------

func TestUpdateInMemoryState_Webhooks(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	// Register a webhook manually
	_, _ = s.WebhookManager.Register("http://example.com/hook", []string{"doc.added"}, "")
	if len(s.WebhookManager.List()) != 1 {
		t.Fatal("expected 1 webhook")
	}

	// Simulate a webhook entry being applied
	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "webhooks",
		Key:        []byte("wh|test"),
		Value:      []byte(`{"id":"test","url":"http://reloaded.com","events":["doc.added"]}`),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The webhook manager should have reloaded
	hooks := s.WebhookManager.List()
	if len(hooks) < 1 {
		t.Error("expected at least 1 webhook after reload")
	}
}

// ---------------------------------------------------------------------------
// Test: updateInMemoryState - schemas reload
// ---------------------------------------------------------------------------

func TestUpdateInMemoryState_Schemas(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	// Apply a schema entry
	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "schemas",
		Key:        []byte("schema|blog"),
		Value:      []byte(`{"required":["title"]}`),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Schema manager should have reloaded
	schemas := s.SchemaManager.List()
	if len(schemas) < 1 {
		t.Error("expected at least 1 schema after reload")
	}
}

// ---------------------------------------------------------------------------
// Test: updateInMemoryState - vectors
// ---------------------------------------------------------------------------

func TestUpdateInMemoryState_VectorsPut(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	// Create a valid embedding record
	rec := &vector.EmbeddingRecord{
		DocID:       "testdoc",
		Vector:      []float32{1.0, 2.0, 3.0},
		Model:       "test",
		Dimensions:  3,
		CreatedAt:   1000,
		ContentHash: "hash123",
	}
	data := vector.MarshalEmbeddingRecord(rec)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "vectors",
		Key:        []byte("vec|blog|testdoc"),
		Value:      data,
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply vector: %v", err)
	}

	// The vector index should have the entry
	results := s.VectorIndex.Search("blog", []float32{1.0, 2.0, 3.0}, 5, 0, nil)
	if len(results) != 1 {
		t.Errorf("expected 1 vector result, got %d", len(results))
	}
}

func TestUpdateInMemoryState_VectorsDelete(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	// Pre-add a vector
	if err := s.VectorIndex.Add("blog", "testdoc", []float32{1.0, 2.0, 3.0}); err != nil {
		t.Fatal(err)
	}

	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogDelete,
		BucketName: "vectors",
		Key:        []byte("vec|blog|testdoc"),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply vector delete: %v", err)
	}

	// The vector should be removed
	results := s.VectorIndex.Search("blog", []float32{1.0, 2.0, 3.0}, 5, 0, nil)
	if len(results) != 0 {
		t.Errorf("expected 0 vector results after delete, got %d", len(results))
	}
}

func TestUpdateInMemoryState_VectorsBadKey(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	// Key with < 3 parts should be ignored without error
	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "vectors",
		Key:        []byte("badkey"),
		Value:      []byte("invalid"),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func TestUpdateInMemoryState_VectorsNilIndex(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	s.VectorIndex = nil

	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "vectors",
		Key:        []byte("vec|blog|testdoc"),
		Value:      []byte("data"),
	}
	// Should not panic
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: invalidateDocCache with LockFreeCache
// ---------------------------------------------------------------------------

func TestInvalidateDocCache_LockFreeCache(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	// The caches are keyed by cache.BuildCacheKey(collection, key, lang); the
	// replicated doc carries key + lang so the applier derives that exact key
	// from the entry value (GO-002).
	cacheKey := cache.BuildCacheKey("blog", "post1", "en")
	s.Cache.Set(cacheKey, []byte(`{"id":"post1","key":"post1","lang":"en"}`))
	s.LockFreeCache.Set(cacheKey, []byte(`{"id":"post1","key":"post1","lang":"en"}`))

	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "docs",
		Key:        []byte("doc|blog|post1"),
		Value:      []byte(`{"id":"post1","key":"post1","lang":"en","contentMd":"updated"}`),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Both caches should be invalidated
	if _, ok := s.Cache.Get(cacheKey); ok {
		t.Error("expected Cache entry to be invalidated")
	}
	if _, ok := s.LockFreeCache.Get(cacheKey); ok {
		t.Error("expected LockFreeCache entry to be invalidated")
	}
}

// ---------------------------------------------------------------------------
// Test: invalidateDocCache with bad key
// ---------------------------------------------------------------------------

func TestInvalidateDocCache_BadKey(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)

	// Key with < 3 parts should be ignored without error
	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "docs",
		Key:        []byte("badkey"),
		Value:      []byte("data"),
	}
	// Should not panic
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: updateInMemoryState with nil managers
// ---------------------------------------------------------------------------

func TestUpdateInMemoryState_NilWebhookManager(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	s.WebhookManager = nil
	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "webhooks",
		Key:        []byte("wh|test"),
		Value:      []byte(`{}`),
	}
	// Should not panic
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func TestUpdateInMemoryState_NilSchemaManager(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	s.SchemaManager = nil
	applier := NewReplicationApplier(s)

	entry := &binlog.BinlogEntry{
		LSN:        1,
		Type:       binlog.BinlogPut,
		BucketName: "schemas",
		Key:        []byte("schema|test"),
		Value:      []byte(`{}`),
	}
	// Should not panic
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Persisted applied-LSN marker (_mddb_replication / last_applied_lsn)
// ---------------------------------------------------------------------------

func persistedLSN(t *testing.T, s *Server) uint64 {
	t.Helper()
	var lsn uint64
	var found bool
	if err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replMetaBucket)
		if b == nil {
			return nil
		}
		if v := b.Get(replMetaLSNKey); len(v) == 8 {
			lsn = binary.BigEndian.Uint64(v)
			found = true
		}
		return nil
	}); err != nil {
		t.Fatalf("read persisted LSN: %v", err)
	}
	if !found {
		t.Fatal("expected LSN marker in _mddb_replication bucket")
	}
	return lsn
}

func TestApplier_PersistsAppliedLSN(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	entry := &binlog.BinlogEntry{
		LSN:        42,
		Type:       binlog.BinlogPut,
		BucketName: "docs",
		Key:        []byte("doc|blog|x"),
		Value:      []byte(`{"id":"x"}`),
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := persistedLSN(t, s); got != 42 {
		t.Errorf("expected persisted LSN 42, got %d", got)
	}
}

func TestApplier_PersistsLSNOnCheckpoint(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	entry := &binlog.BinlogEntry{
		LSN:        77,
		Type:       binlog.BinlogCheckpoint,
		BucketName: "docs",
	}
	if err := applier.Apply(entry); err != nil {
		t.Fatalf("Apply checkpoint: %v", err)
	}
	if got := persistedLSN(t, s); got != 77 {
		t.Errorf("expected persisted LSN 77, got %d", got)
	}
}

func TestApplyBatch_PersistsLastLSN(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	entries := []*binlog.BinlogEntry{
		{LSN: 10, Type: binlog.BinlogPut, BucketName: "docs", Key: []byte("doc|a"), Value: []byte(`{"id":"a"}`)},
		{LSN: 11, Type: binlog.BinlogPut, BucketName: "docs", Key: []byte("doc|b"), Value: []byte(`{"id":"b"}`)},
		{LSN: 12, Type: binlog.BinlogDelete, BucketName: "docs", Key: []byte("doc|a")},
	}
	if err := applier.ApplyBatch(entries); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if got := persistedLSN(t, s); got != 12 {
		t.Errorf("expected persisted LSN 12, got %d", got)
	}
}

func TestApplier_LoadsPersistedLSNOnStart(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	// First "process": apply, which persists the marker.
	first := NewReplicationApplier(s)
	if err := first.Apply(&binlog.BinlogEntry{
		LSN: 99, Type: binlog.BinlogPut, BucketName: "docs",
		Key: []byte("doc|x"), Value: []byte(`{"id":"x"}`),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// "Restart": a fresh applier on the same database resumes from the marker
	// instead of 0, so the stream tails rather than re-snapshotting.
	second := NewReplicationApplier(s)
	if got := second.LastAppliedLSN(); got != 99 {
		t.Errorf("expected resumed LSN 99, got %d", got)
	}
}

func TestApplier_LoadsPersistedLSNAcrossReopen(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	if err := applier.Apply(&binlog.BinlogEntry{
		LSN: 55, Type: binlog.BinlogPut, BucketName: "docs",
		Key: []byte("doc|x"), Value: []byte(`{"id":"x"}`),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Full process-restart simulation: close the handle and reopen the file.
	if err := s.DB.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err := bolt.Open(s.Path, 0600, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()
	s.DB = db

	restarted := NewReplicationApplier(s)
	if got := restarted.LastAppliedLSN(); got != 55 {
		t.Errorf("expected LSN 55 after reopen, got %d", got)
	}
}

func TestApplier_NoMarkerStartsAtZero(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	if got := applier.LastAppliedLSN(); got != 0 {
		t.Errorf("expected LSN 0 without marker, got %d", got)
	}
}

func TestApplier_PersistLSNWritesMarker(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	if err := applier.PersistLSN(1234); err != nil {
		t.Fatalf("PersistLSN: %v", err)
	}
	if got := persistedLSN(t, s); got != 1234 {
		t.Errorf("expected persisted LSN 1234, got %d", got)
	}
}

func TestApplier_ReloadPersistedLSN(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	applier.SetLastAppliedLSN(500)

	// A restore swaps in a file whose marker differs from memory.
	if err := s.DB.Update(func(tx *bolt.Tx) error {
		return putAppliedLSN(tx, 321)
	}); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
	applier.ReloadPersistedLSN()
	if got := applier.LastAppliedLSN(); got != 321 {
		t.Errorf("expected reloaded LSN 321, got %d", got)
	}
}

func TestApplier_MarkerNotReplicatedAsData(t *testing.T) {
	s, cleanup := applierExtraTestServer(t)
	defer cleanup()

	applier := NewReplicationApplier(s)
	if err := applier.Apply(&binlog.BinlogEntry{
		LSN: 7, Type: binlog.BinlogPut, BucketName: "docs",
		Key: []byte("doc|x"), Value: []byte(`{"id":"x"}`),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The marker lives only in its own bucket — user buckets stay clean.
	if err := s.DB.View(func(tx *bolt.Tx) error {
		docs := tx.Bucket([]byte("docs"))
		if docs == nil || docs.Get(replMetaLSNKey) != nil {
			t.Error("LSN marker leaked into the docs bucket")
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
