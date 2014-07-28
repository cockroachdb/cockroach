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
	"fmt"
	"testing"

	"code.google.com/p/biogo.store/llrb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

type testMetadataDB struct {
	data     llrb.Tree
	hitCount int
}

type testMetadataNode struct {
	endKey storage.Key
	desc   *storage.RangeDescriptor
}

func (a *testMetadataNode) Compare(b llrb.Comparable) int {
	return bytes.Compare(a.endKey, b.(*testMetadataNode).endKey)
}

func (db *testMetadataDB) getMetadata(prefix storage.Key, key storage.Key) (storage.Key, *storage.RangeDescriptor, error) {
	metadataKey := storage.MakeKey(prefix, key)
	v := db.data.Ceil(&testMetadataNode{endKey: metadataKey})
	if v == nil {
		return nil, nil, util.Errorf("Range for key %s not found", key)
	}
	db.hitCount++
	val := v.(*testMetadataNode)
	return val.endKey, val.desc, nil
}

func (db *testMetadataDB) GetLevelOneMetadata(key storage.Key) (storage.Key, *storage.RangeDescriptor, error) {
	return db.getMetadata(storage.KeyMeta1Prefix, key)
}

func (db *testMetadataDB) GetLevelTwoMetadata(key storage.Key, info *storage.RangeDescriptor) (storage.Key, *storage.RangeDescriptor, error) {
	return db.getMetadata(storage.KeyMeta2Prefix, key)
}

func (db *testMetadataDB) splitRange(t *testing.T, metadataKey storage.Key) {
	v := db.data.Ceil(&testMetadataNode{endKey: metadataKey})
	if v == nil {
		t.Fatalf("Error splitting range at key %s, range to split not found", string(metadataKey))
	}
	val := v.(*testMetadataNode)
	if bytes.Compare(val.endKey, metadataKey) == 0 {
		t.Fatalf("Attempt to split existing range at Endkey: %s", string(metadataKey))
	}
	db.data.Insert(&testMetadataNode{
		endKey: metadataKey,
		desc: &storage.RangeDescriptor{
			StartKey: val.desc.StartKey,
		},
	})
	db.data.Insert(&testMetadataNode{
		endKey: val.endKey,
		desc: &storage.RangeDescriptor{
			StartKey: storage.MakeKey(metadataKey, storage.Key{0}),
		},
	})
}

func (db *testMetadataDB) splitLevelOneAt(t *testing.T, key storage.Key) {
	metadataKey := storage.MakeKey(storage.KeyMeta1Prefix, key)
	db.splitRange(t, metadataKey)
}

func (db *testMetadataDB) splitLevelTwoAt(t *testing.T, key storage.Key) {
	metadataKey := storage.MakeKey(storage.KeyMeta2Prefix, key)
	db.splitRange(t, metadataKey)
}

func newTestMetadataDB() *testMetadataDB {
	db := &testMetadataDB{}
	db.data.Insert(&testMetadataNode{
		endKey: storage.MakeKey(storage.KeyMeta1Prefix, storage.KeyMax),
		desc: &storage.RangeDescriptor{
			StartKey: storage.MakeKey(storage.KeyMeta1Prefix, storage.KeyMin),
		},
	})
	db.data.Insert(&testMetadataNode{
		endKey: storage.MakeKey(storage.KeyMeta2Prefix, storage.KeyMax),
		desc: &storage.RangeDescriptor{
			StartKey: storage.MakeKey(storage.KeyMeta2Prefix, storage.KeyMin),
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
	fmt.Printf("Lookup %s\n", key)
	metadata, err := rc.LookupRangeMetadata(storage.Key(key))
	if err != nil {
		t.Fatalf("Error from lookup range metadata: %s", err.Error())
	}
	fmt.Printf("Metadata: %s\n", metadata.StartKey)
}

// TestRangeCache is a simple test which verifies that metadata ranges are being
// cached and retrieved properly.  It sets up a fake backing store for the
// cache, and measures how often that backing store is accessed when looking up
// metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	db := newTestMetadataDB()
	db.splitLevelTwoAt(t, storage.Key("a"))
	db.splitLevelTwoAt(t, storage.Key("b"))
	db.splitLevelTwoAt(t, storage.Key("c"))
	db.splitLevelTwoAt(t, storage.Key("d"))
	db.splitLevelTwoAt(t, storage.Key("e"))
	db.splitLevelTwoAt(t, storage.Key("f"))
	db.splitLevelOneAt(t, storage.Key("d"))
	db.hitCount = 0

	rangeCache := NewRangeMetadataCache(db)
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
	rangeCache.EvictCachedRangeMetadata(storage.Key("da"))
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "da")
	db.assertHitCount(t, 2)
}
