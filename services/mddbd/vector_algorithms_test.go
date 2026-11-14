package main

import (
	"sort"
	"testing"

	"mddb/internal/vector"
)

// MDDB_VECTOR_ALGORITHMS exists because every stored vector is inserted into
// every searcher at startup and on write — indexes nobody queries still cost
// full build time and RAM. These tests pin the contract: unset keeps every
// algorithm (upstream behavior), a set keeps the named ones, and "flat" plus
// "quantized" always survive because the default query path and disk-only
// collections depend on them.

func testSearcherNames(m map[string]vector.VectorSearcher) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestVectorSearchersDefaultsToAllAlgorithms(t *testing.T) {
	searchers := newVectorSearchers(vector.NewVectorIndex(), nil, 100)
	got := testSearcherNames(searchers)
	want := []string{"bq", "flat", "hnsw", "ivf", "opq", "pq", "quantized", "sq", "sq4"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestVectorAlgorithmsAllowlist(t *testing.T) {
	t.Setenv("MDDB_VECTOR_ALGORITHMS", "flat,quantized")
	searchers := newVectorSearchers(vector.NewVectorIndex(), nil, 100)
	got := testSearcherNames(searchers)
	want := []string{"flat", "quantized"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestVectorAlgorithmsAlwaysKeepsFlatAndQuantized(t *testing.T) {
	// A list naming only hnsw must still leave the two mandatory searchers:
	// flat is the default query path and the fallback for unready indexes,
	// quantized backs disk-only collections and per-collection quantization.
	t.Setenv("MDDB_VECTOR_ALGORITHMS", "hnsw")
	searchers := newVectorSearchers(vector.NewVectorIndex(), nil, 100)
	got := testSearcherNames(searchers)
	want := []string{"flat", "hnsw", "quantized"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestVectorAlgorithmsIgnoresUnknownNames(t *testing.T) {
	// A typo must not silently disable the index — unknown names are ignored,
	// flat and quantized still built.
	t.Setenv("MDDB_VECTOR_ALGORITHMS", "flat, not-an-algorithm")
	searchers := newVectorSearchers(vector.NewVectorIndex(), nil, 100)
	got := testSearcherNames(searchers)
	want := []string{"flat", "quantized"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}
