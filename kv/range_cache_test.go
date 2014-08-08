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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"testing"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

type testMetadataDB struct {
	data     llrb.Tree
	cache    *RangeMetadataCache
	hitCount int
}

type testMetadataNode struct {
	*storage.RangeDescriptor
}

func (a testMetadataNode) Compare(b llrb.Comparable) int {
	return bytes.Compare(a.LookupKey(), b.(testMetadataNode).LookupKey())
}

func (db *testMetadataDB) getMetadata(key engine.Key) []*storage.RangeDescriptor {
	response := make([]*storage.RangeDescriptor, 0, 3)
	for i := 0; i < 3; i++ {
		v := db.data.Ceil(testMetadataNode{
			&storage.RangeDescriptor{
				EndKey: engine.NextKey(key),
			},
		})
		if v == nil {
			break
		}
		response = append(response, v.(testMetadataNode).RangeDescriptor)
		key = engine.NextKey(response[i].EndKey)
	}
	return response
}

func (db *testMetadataDB) getRangeMetadata(key engine.Key) ([]*storage.RangeDescriptor, error) {
	db.hitCount++
	metadataKey := engine.RangeMetaKey(key)

	// Recursively call into cache as the real DB would, terminating recursion
	// when a meta1key is encountered.
	if len(metadataKey) > 0 && !bytes.HasPrefix(metadataKey, engine.KeyMeta1Prefix) {
		db.cache.LookupRangeMetadata(metadataKey)
	}
	return db.getMetadata(key), nil
}

func (db *testMetadataDB) splitRange(t *testing.T, key engine.Key) {
	v := db.data.Ceil(testMetadataNode{&storage.RangeDescriptor{EndKey: key}})
	if v == nil {
		t.Fatalf("Error splitting range at key %s, range to split not found", string(key))
	}
	val := v.(testMetadataNode)
	if bytes.Compare(val.EndKey, key) == 0 {
		t.Fatalf("Attempt to split existing range at Endkey: %s", string(key))
	}
	db.data.Insert(testMetadataNode{
		&storage.RangeDescriptor{
			StartKey: val.StartKey,
			EndKey:   key,
		},
	})
	db.data.Insert(testMetadataNode{
		&storage.RangeDescriptor{
			StartKey: key,
			EndKey:   val.EndKey,
		},
	})
}

func newTestMetadataDB() *testMetadataDB {
	db := &testMetadataDB{}
	db.data.Insert(testMetadataNode{
		&storage.RangeDescriptor{
			StartKey: engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMin),
			EndKey:   engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax),
		},
	})
	db.data.Insert(testMetadataNode{
		&storage.RangeDescriptor{
			StartKey: engine.KeyMetaMax,
			EndKey:   engine.KeyMax,
		},
	})
	return db
}

func (db *testMetadataDB) assertHitCount(t *testing.T, expected int) {
	if db.hitCount != expected {
		t.Errorf("Expected hit count to be %d, was %d", expected, db.hitCount)
	}
	db.hitCount = 0
}

func doLookup(t *testing.T, rc *RangeMetadataCache, key string) {
	r, err := rc.LookupRangeMetadata(engine.Key(key))
	if err != nil {
		t.Fatalf("Unexpected error from LookupRangeMetadata: %s", err.Error())
	}
	if !r.ContainsKey(engine.Key(key)) {
		t.Fatalf("Returned range did not contain key: %s-%s, %s", r.StartKey, r.EndKey, key)
	}
}

// TestRangeCache is a simple test which verifies that metadata ranges are being
// cached and retrieved properly.  It sets up a fake backing store for the
// cache, and measures how often that backing store is accessed when looking up
// metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	db := newTestMetadataDB()
	for i, char := range "abcdefghijklmnopqrstuvwx" {
		db.splitRange(t, engine.Key(string(char)))
		if i > 0 && i%6 == 0 {
			db.splitRange(t, engine.RangeMetaKey(engine.Key(string(char))))
		}
	}

	rangeCache := NewRangeMetadataCache(db)
	db.cache = rangeCache

	doLookup(t, rangeCache, "aa")
	db.assertHitCount(t, 2)

	// Metadata for the following ranges should be cached
	doLookup(t, rangeCache, "ab")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "ba")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "cz")
	db.assertHitCount(t, 0)

	// Metadata two ranges weren't cached, same metadata 1 range
	doLookup(t, rangeCache, "d")
	db.assertHitCount(t, 1)
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 0)

	// Metadata two ranges weren't cached, metadata 1 was aggressively cached
	doLookup(t, rangeCache, "ij")
	db.assertHitCount(t, 1)
	doLookup(t, rangeCache, "jk")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "pn")
	db.assertHitCount(t, 1)

	// Totally uncached ranges
	doLookup(t, rangeCache, "vu")
	db.assertHitCount(t, 2)
	doLookup(t, rangeCache, "xx")
	db.assertHitCount(t, 0)

	// Evict clears one level 1 and one level 2 cache
	rangeCache.EvictCachedRangeMetadata(engine.Key("da"))
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "da")
	db.assertHitCount(t, 2)
}
