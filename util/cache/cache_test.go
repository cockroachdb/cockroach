// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// This code is based on: https://github.com/golang/groupcache/
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package cache

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/biogo/store/interval"
	"github.com/biogo/store/llrb"

	// Define -logtostderr which is required by 'make test'`
	// TODO(bdarnell): remove this once we've gotten our log situation straightened out.
	_ "github.com/cockroachdb/cockroach/util/log"
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
	// Add another entry to evict; should evict key "b".
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

type rangeKey string

// Compare implements interval.Comparable.
func (rk rangeKey) Compare(b interval.Comparable) int {
	return bytes.Compare([]byte(rk), []byte(b.(rangeKey)))
}

func TestIntervalCache(t *testing.T) {
	ic := NewIntervalCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	key1 := ic.NewKey(rangeKey("a"), rangeKey("b"))
	key2 := ic.NewKey(rangeKey("a"), rangeKey("c"))
	key3 := ic.NewKey(rangeKey("d"), rangeKey("d\x00"))
	ic.Add(key1, 1)
	ic.Add(key2, 2)
	ic.Add(key3, 3)

	// Verify hit & miss.
	if v, ok := ic.Get(key1); !ok || v.(int) != 1 {
		t.Error("failed to fetch value for key \"a\"-\"b\"")
	}
	if v, ok := ic.Get(key2); !ok || v.(int) != 2 {
		t.Error("failed to fetch value for key \"a\"-\"c\"")
	}
	if v, ok := ic.Get(key3); !ok || v.(int) != 3 {
		t.Error("failed to fetch value for key \"d\"")
	}
	if _, ok := ic.Get(ic.NewKey(rangeKey("a"), rangeKey("a\x00"))); ok {
		t.Error("unexpected success fetching \"a\"")
	}

	// Verify replacement on adding identical key.
	ic.Add(key1, 3)
	if v, ok := ic.Get(key1); !ok || v.(int) != 3 {
		t.Error("failed to fetch value for key \"a\"-\"b\"")
	}
}

func TestIntervalCacheOverlap(t *testing.T) {
	ic := NewIntervalCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	ic.Add(ic.NewKey(rangeKey("a"), rangeKey("c")), 1)
	ic.Add(ic.NewKey(rangeKey("c"), rangeKey("e")), 2)
	ic.Add(ic.NewKey(rangeKey("b"), rangeKey("g")), 3)
	ic.Add(ic.NewKey(rangeKey("d"), rangeKey("e")), 4)
	ic.Add(ic.NewKey(rangeKey("b"), rangeKey("d")), 5)
	ic.Add(ic.NewKey(rangeKey("e"), rangeKey("g")), 6)
	ic.Add(ic.NewKey(rangeKey("f"), rangeKey("i")), 7)
	ic.Add(ic.NewKey(rangeKey("g"), rangeKey("i")), 8)
	ic.Add(ic.NewKey(rangeKey("f"), rangeKey("h")), 9)
	ic.Add(ic.NewKey(rangeKey("i"), rangeKey("j")), 10)

	expValues := []interface{}{3, 2, 4, 6, 7, 9}
	values := []interface{}{}
	for _, o := range ic.GetOverlaps(rangeKey("d"), rangeKey("g")) {
		values = append(values, o.Value)
	}
	if !reflect.DeepEqual(expValues, values) {
		t.Errorf("expected overlap values %+v, got %+v", expValues, values)
	}
}

func TestIntervalCacheClear(t *testing.T) {
	ic := NewIntervalCache(Config{Policy: CacheLRU, ShouldEvict: noEviction})
	key1 := ic.NewKey(rangeKey("a"), rangeKey("c"))
	key2 := ic.NewKey(rangeKey("c"), rangeKey("e"))
	ic.Add(key1, 1)
	ic.Add(key2, 2)
	ic.Clear()
	if _, ok := ic.Get(key1); ok {
		t.Error("expected cache cleared")
	}
	if _, ok := ic.Get(key2); ok {
		t.Error("expected cache cleared")
	}
	ic.Add(key1, 1)
	if _, ok := ic.Get(key1); !ok {
		t.Error("expected reinsert to succeed")
	}
}
