// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This code is based on: https://github.com/golang/groupcache/

package cache

import "testing"

func noEvictionTyped(size int, key testKey, value int) bool {
	return false
}

func evictTwoOrMoreTyped(size int, key testKey, value int) bool {
	return size > 1
}

func evictThreeOrMoreTyped(size int, key testKey, value int) bool {
	return size > 2
}

func TestTypedUnorderedCacheGet(t *testing.T) {
	for _, tt := range getTests {
		mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{Policy: CacheLRU, ShouldEvict: noEvictionTyped})
		mc.Add(tt.keyToAdd, 1234)
		val, ok := mc.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestTypedUnderedCacheClear(t *testing.T) {
	mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{Policy: CacheLRU, ShouldEvict: noEvictionTyped})
	mc.Add(testKey("a"), 1)
	mc.Add(testKey("b"), 2)
	mc.Clear()
	if _, ok := mc.Get(testKey("a")); ok {
		t.Error("expected cache cleared")
	}
	if _, ok := mc.Get(testKey("b")); ok {
		t.Error("expected cache cleared")
	}
	mc.Add(testKey("a"), 1)
	if _, ok := mc.Get(testKey("a")); !ok {
		t.Error("expected reinsert to succeed")
	}
}

func TestTypedUnorderedCacheDel(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	mc.Add(testKey("myKey"), 1234)
	if val, ok := mc.Get(testKey("myKey")); !ok {
		t.Fatal("TestDel returned no match")
	} else if val != 1234 {
		t.Fatalf("TestDel failed. Expected %d, got %v", 1234, val)
	}

	mc.Del(testKey("myKey"))
	if _, ok := mc.Get(testKey("myKey")); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func TestTypedUnorderedCacheEviction(t *testing.T) {
	mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{Policy: CacheLRU, ShouldEvict: evictTwoOrMoreTyped})
	// Insert two keys into cache which only holds 1.
	mc.Add(testKey("a"), 1234)
	val, ok := mc.Get(testKey("a"))
	if !ok || val != 1234 {
		t.Fatal("expected get to succeed with value 1234")
	}
	mc.Add(testKey("b"), 4321)
	val, ok = mc.Get(testKey("b"))
	if !ok || val != 4321 {
		t.Fatal("expected get to succeed with value 4321")
	}
	// Verify eviction of first key.
	if _, ok = mc.Get(testKey("a")); ok {
		t.Fatal("unexpected success getting evicted key")
	}
}

func TestTypedUnorderedCacheLRU(t *testing.T) {
	mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{Policy: CacheLRU, ShouldEvict: evictThreeOrMoreTyped})
	// Insert two keys into cache.
	mc.Add(testKey("a"), 1)
	mc.Add(testKey("b"), 2)
	// Get "a" now to make it more recently used.
	if _, ok := mc.Get(testKey("a")); !ok {
		t.Fatal("failed to get key a")
	}
	// Add another entry to cause an eviction; should evict key "b".
	mc.Add(testKey("c"), 3)
	// Verify eviction of least recently used key "b".
	if _, ok := mc.Get(testKey("b")); ok {
		t.Fatal("unexpected success getting evicted key")
	}
}

func TestTypedUnorderedCacheFIFO(t *testing.T) {
	mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{
		Policy:      CacheFIFO,
		ShouldEvict: evictThreeOrMoreTyped,
	})
	// Insert two keys into cache.
	mc.Add(testKey("a"), 1)
	mc.Add(testKey("b"), 2)
	// Get "a" now to make it more recently used.
	if _, ok := mc.Get(testKey("a")); !ok {
		t.Fatal("failed to get key a")
	}
	// Add another entry to evict; should evict key "a" still, as that was first in.
	mc.Add(testKey("c"), 3)
	// Verify eviction of first key "a".
	if _, ok := mc.Get(testKey("a")); ok {
		t.Fatal("unexpected success getting evicted key")
	}
}

func BenchmarkTypedUnorderedCache(b *testing.B) {
	mc := NewTypedUnorderedCache[testKey, int](TypedConfig[testKey, int]{Policy: CacheLRU, ShouldEvict: noEvictionTyped})
	testKeys := []testKey{
		testKey("a"),
		testKey("b"),
		testKey("c"),
		testKey("d"),
		testKey("e"),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(testKeys); j++ {
			mc.Add(testKeys[j], j)
		}
		mc.Clear()
	}
}
