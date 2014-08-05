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
	"github.com/cockroachdb/cockroach/util"
)

type testMetadataDB struct {
	data     llrb.Tree
	cache    *RangeMetadataCache
	hitCount int
}

type testMetadataNode struct {
	endKey engine.Key
	desc   *storage.RangeDescriptor
}

func (a *testMetadataNode) Compare(b llrb.Comparable) int {
	return bytes.Compare(a.endKey, b.(*testMetadataNode).endKey)
}

func (db *testMetadataDB) getMetadata(key engine.Key) (engine.Key, *storage.RangeDescriptor, error) {
	metadataKey := engine.RangeMetaKey(key)
	v := db.data.Ceil(&testMetadataNode{endKey: metadataKey})
	if v == nil {
		return nil, nil, util.Errorf("Range for key %s not found", key)
	}
	val := v.(*testMetadataNode)
	db.hitCount++
	return val.endKey, val.desc, nil
}

func (db *testMetadataDB) LookupRangeMetadata(key engine.Key) (engine.Key, *storage.RangeDescriptor, error) {
	metadataKey := engine.RangeMetaKey(key)

	// Recursively call into cache as the real DB would, terminating when the
	// initial key is encountered.
	if len(metadataKey) == 0 {
		return nil, nil, nil
	}
	db.cache.LookupRangeMetadata(metadataKey)
	return db.getMetadata(key)
}

func (db *testMetadataDB) splitRange(t *testing.T, key engine.Key) {
	metadataKey := engine.RangeMetaKey(key)
	v := db.data.Ceil(&testMetadataNode{endKey: metadataKey})
	if v == nil {
		t.Fatalf("Error splitting range at key %s, range to split not found", string(key))
	}
	val := v.(*testMetadataNode)
	if bytes.Compare(val.desc.EndKey, key) == 0 {
		t.Fatalf("Attempt to split existing range at Endkey: %s", string(key))
	}
	db.data.Insert(&testMetadataNode{
		endKey: metadataKey,
		desc: &storage.RangeDescriptor{
			StartKey: val.desc.StartKey,
			EndKey:   key,
		},
	})
	db.data.Insert(&testMetadataNode{
		endKey: val.endKey,
		desc: &storage.RangeDescriptor{
			StartKey: key,
			EndKey:   val.desc.EndKey,
		},
	})
}

func newTestMetadataDB() *testMetadataDB {
	db := &testMetadataDB{}
	db.data.Insert(&testMetadataNode{
		endKey: engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax),
		desc: &storage.RangeDescriptor{
			StartKey: engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMin),
			EndKey:   engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax),
		},
	})
	db.data.Insert(&testMetadataNode{
		endKey: engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax),
		desc: &storage.RangeDescriptor{
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
		t.Fatalf("Returned range did not contain key: %v, %s", r, key)
	}
}

// TestRangeCache is a simple test which verifies that metadata ranges are being
// cached and retrieved properly.  It sets up a fake backing store for the
// cache, and measures how often that backing store is accessed when looking up
// metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	db := newTestMetadataDB()
	db.splitRange(t, engine.Key("a"))
	db.splitRange(t, engine.Key("b"))
	db.splitRange(t, engine.Key("c"))
	db.splitRange(t, engine.Key("d"))
	db.splitRange(t, engine.Key("e"))
	db.splitRange(t, engine.Key("f"))
	db.splitRange(t, engine.RangeMetaKey(engine.Key("d")))
	db.hitCount = 0

	rangeCache := NewRangeMetadataCache(db)
	db.cache = rangeCache

	doLookup(t, rangeCache, "ba")
	db.assertHitCount(t, 2)
	doLookup(t, rangeCache, "bb")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "ca")
	db.assertHitCount(t, 1)

	// Different metadata one range
	doLookup(t, rangeCache, "da")
	db.assertHitCount(t, 2)
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 1)

	// Evict clears both level 1 and level 2 cache for a key
	rangeCache.EvictCachedRangeMetadata(engine.Key("da"))
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "da")
	db.assertHitCount(t, 2)
}
