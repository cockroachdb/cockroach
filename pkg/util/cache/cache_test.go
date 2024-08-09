// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This code is based on: https://github.com/golang/groupcache/

package cache

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/biogo/store/llrb"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
)

type testKey string

// Compare implements llrb.Comparable.
func (tk testKey) Compare(b llrb.Comparable) int {
	return bytes.Compare([]byte(tk), []byte(b.(testKey)))
}

var getTests = []struct {
	name       string
	keyToAdd   testKey
	keyToGet   testKey
	expectedOk bool
}{
	{"string_hit", "myKey", "myKey", true},
	{"string_miss", "myKey", "nonsense", false},
}

func noEviction(size int, key, value interface{}) bool {
	return false
}

func evictTwoOrMore(size int, key, value interface{}) bool {
	return size > 1
}

func evictThreeOrMore(size int, key, value interface{}) bool {
	return size > 2
}

func TestCacheGet(t *testing.T) {
	for _, tt := range getTests {
		mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
		mc.Add(tt.keyToAdd, 1234)
		val, ok := mc.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != 1234 {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
	}
}

func TestCacheClear(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
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

func TestCacheDel(t *testing.T) {
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

func TestCacheAddDelEntry(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	e := &Entry{Key: testKey("myKey"), Value: 1234}
	mc.AddEntry(e)
	if val, ok := mc.Get(testKey("myKey")); !ok {
		t.Fatal("TestDel returned no match")
	} else if val != 1234 {
		t.Fatalf("TestDel failed. Expected %d, got %v", 1234, val)
	}

	mc.DelEntry(e)
	if _, ok := mc.Get(testKey("myKey")); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
}

func TestCacheEviction(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: evictTwoOrMore})
	// Insert two keys into cache which only holds 1.
	mc.Add(testKey("a"), 1234)
	val, ok := mc.Get(testKey("a"))
	if !ok || val.(int) != 1234 {
		t.Fatal("expected get to succeed with value 1234")
	}
	mc.Add(testKey("b"), 4321)
	val, ok = mc.Get(testKey("b"))
	if !ok || val.(int) != 4321 {
		t.Fatal("expected get to succeed with value 4321")
	}
	// Verify eviction of first key.
	if _, ok = mc.Get(testKey("a")); ok {
		t.Fatal("unexpected success getting evicted key")
	}
}

func TestCacheLRU(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: evictThreeOrMore})
	// Insert two keys into cache.
	mc.Add(testKey("a"), 1)
	mc.Add(testKey("b"), 2)
	// Get "a" now to make it more recently used.
	if _, ok := mc.Get(testKey("a")); !ok {
		t.Fatal("failed to get key a")
	}
	// Verify that a StealthyGet won't make b the most recently used.
	if _, ok := mc.StealthyGet(testKey("b")); !ok {
		t.Fatal("failed to get key b")
	}
	// Add another entry to cause an eviction; should evict key "b".
	mc.Add(testKey("c"), 3)
	// Verify eviction of least recently used key "b".
	if _, ok := mc.Get(testKey("b")); ok {
		t.Fatal("unexpected success getting evicted key")
	}
}

func TestCacheFIFO(t *testing.T) {
	mc := NewUnorderedCache(Config{Policy: CacheFIFO, ShouldEvict: evictThreeOrMore})
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

func TestOrderedCache(t *testing.T) {
	oc := NewOrderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	oc.Add(testKey("a"), 1)
	oc.Add(testKey("b"), 2)

	// Verify hit & miss.
	if v, ok := oc.Get(testKey("a")); !ok || v.(int) != 1 {
		t.Error("failed to fetch value for key \"a\"")
	}
	if _, ok := oc.Get(testKey("c")); ok {
		t.Error("unexpected success fetching \"c\"")
	}

	// Try binary searches for ceil and floor to key direct.
	if _, v, ok := oc.Ceil(testKey("a")); !ok || v.(int) != 1 {
		t.Error("expected success fetching key directly")
	}
	if _, v, ok := oc.Floor(testKey("a")); !ok || v.(int) != 1 {
		t.Error("expected success fetching key directly")
	}

	// Test ceil and floor operation with empty key.
	if _, v, ok := oc.Ceil(testKey("")); !ok || v.(int) != 1 {
		t.Error("expected fetch of key \"a\" for ceil of empty key")
	}
	if _, _, ok := oc.Floor(testKey("")); ok {
		t.Error("unexpected success fetching floor of empty key")
	}

	// Test ceil and floor operation with midway key.
	if _, v, ok := oc.Ceil(testKey("aa")); !ok || v.(int) != 2 {
		t.Error("expected fetch of key \"b\" for ceil of midway key")
	}
	if _, v, ok := oc.Floor(testKey("aa")); !ok || v.(int) != 1 {
		t.Error("expected fetch of key \"a\" for floor of midway key")
	}

	// Test ceil and floor operation with maximum key.
	if _, _, ok := oc.Ceil(testKey("c")); ok {
		t.Error("unexpected success fetching ceil of maximum key")
	}
	if _, v, ok := oc.Floor(testKey("c")); !ok || v.(int) != 2 {
		t.Error("expected fetch of key \"b\" for floor of maximum key")
	}

	// Test do over entire cache.
	expKeys, collectKeys := []string{"a", "b"}, []string{}
	expVals, collectVals := []int{1, 2}, []int{}
	collect := func(k, v interface{}) bool {
		collectKeys = append(collectKeys, string(k.(testKey)))
		collectVals = append(collectVals, v.(int))
		return false
	}

	oc.Do(collect)
	if !reflect.DeepEqual(expKeys, collectKeys) {
		t.Errorf("expected do to find keys %v, found %v", expKeys, collectKeys)
	}
	if !reflect.DeepEqual(expVals, collectVals) {
		t.Errorf("expected do to find values %v, found %v", expVals, collectVals)
	}

	// Test doRange over range ["a","b").
	expKeys, collectKeys = []string{"a"}, []string{}
	expVals, collectVals = []int{1}, []int{}

	oc.DoRange(collect, testKey("a"), testKey("b"))
	if !reflect.DeepEqual(expKeys, collectKeys) {
		t.Errorf("expected do to find keys %v, found %v", expKeys, collectKeys)
	}
	if !reflect.DeepEqual(expVals, collectVals) {
		t.Errorf("expected do to find values %v, found %v", expVals, collectVals)
	}
}

func TestOrderedCacheClear(t *testing.T) {
	oc := NewOrderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	oc.Add(testKey("a"), 1)
	oc.Add(testKey("b"), 2)
	oc.Clear()
	if _, ok := oc.Get(testKey("a")); ok {
		t.Error("expected cache cleared")
	}
	if _, ok := oc.Get(testKey("b")); ok {
		t.Error("expected cache cleared")
	}
	oc.Add(testKey("a"), 1)
	if _, ok := oc.Get(testKey("a")); !ok {
		t.Error("expected reinsert to succeed")
	}
}

func benchmarkCache(b *testing.B, c *baseCache, keys []interface{}) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(keys); j++ {
			c.Add(keys[j], j)
		}
		c.Clear()
	}
}

func BenchmarkUnorderedCache(b *testing.B) {
	mc := NewUnorderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	testKeys := []interface{}{
		testKey("a"),
		testKey("b"),
		testKey("c"),
		testKey("d"),
		testKey("e"),
	}
	benchmarkCache(b, &mc.baseCache, testKeys)
}

func BenchmarkOrderedCache(b *testing.B) {
	oc := NewOrderedCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	testKeys := []interface{}{
		testKey("a"),
		testKey("b"),
		testKey("c"),
		testKey("d"),
		testKey("e"),
	}
	benchmarkCache(b, &oc.baseCache, testKeys)
}
