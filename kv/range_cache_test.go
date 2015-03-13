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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"testing"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

type testDescriptorDB struct {
	data     llrb.Tree
	cache    *RangeDescriptorCache
	hitCount int
}

type testDescriptorNode struct {
	*proto.RangeDescriptor
}

func (a testDescriptorNode) Compare(b llrb.Comparable) int {
	aKey := engine.RangeMetaLookupKey(a.RangeDescriptor)
	bKey := engine.RangeMetaLookupKey(b.(testDescriptorNode).RangeDescriptor)
	return bytes.Compare(aKey, bKey)
}

func (db *testDescriptorDB) getDescriptor(key proto.Key) []proto.RangeDescriptor {
	response := make([]proto.RangeDescriptor, 0, 3)
	for i := 0; i < 3; i++ {
		v := db.data.Ceil(testDescriptorNode{
			&proto.RangeDescriptor{
				EndKey: key.Next(),
			},
		})
		if v == nil {
			break
		}
		response = append(response, *(v.(testDescriptorNode).RangeDescriptor))
		// Break to keep from skidding off the end of the available ranges.
		if response[i].EndKey.Equal(proto.KeyMax) {
			break
		}
		key = proto.Key(response[i].EndKey).Next()
	}
	return response
}

func (db *testDescriptorDB) getRangeDescriptor(key proto.Key) ([]proto.RangeDescriptor, error) {
	db.hitCount++
	metadataKey := engine.RangeMetaKey(key)

	// Recursively call into cache as the real DB would, terminating recursion
	// when a meta1key is encountered.
	if len(metadataKey) > 0 && !bytes.HasPrefix(metadataKey, engine.KeyMeta1Prefix) {
		db.cache.LookupRangeDescriptor(metadataKey)
	}
	return db.getDescriptor(key), nil
}

func (db *testDescriptorDB) splitRange(t *testing.T, key proto.Key) {
	v := db.data.Ceil(testDescriptorNode{&proto.RangeDescriptor{EndKey: key}})
	if v == nil {
		t.Fatalf("Error splitting range at key %s, range to split not found", string(key))
	}
	val := v.(testDescriptorNode)
	if bytes.Compare(val.EndKey, key) == 0 {
		t.Fatalf("Attempt to split existing range at Endkey: %s", string(key))
	}
	db.data.Insert(testDescriptorNode{
		&proto.RangeDescriptor{
			StartKey: val.StartKey,
			EndKey:   key,
		},
	})
	db.data.Insert(testDescriptorNode{
		&proto.RangeDescriptor{
			StartKey: key,
			EndKey:   val.EndKey,
		},
	})
}

func newTestDescriptorDB() *testDescriptorDB {
	db := &testDescriptorDB{}
	db.data.Insert(testDescriptorNode{
		&proto.RangeDescriptor{
			StartKey: engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMin),
			EndKey:   engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax),
		},
	})
	db.data.Insert(testDescriptorNode{
		&proto.RangeDescriptor{
			StartKey: engine.KeyMetaMax,
			EndKey:   engine.KeyMax,
		},
	})
	return db
}

func (db *testDescriptorDB) assertHitCount(t *testing.T, expected int) {
	if db.hitCount != expected {
		t.Errorf("Expected hit count to be %d, was %d", expected, db.hitCount)
	}
	db.hitCount = 0
}

func doLookup(t *testing.T, rc *RangeDescriptorCache, key string) {
	r, err := rc.LookupRangeDescriptor(proto.Key(key))
	if err != nil {
		t.Fatalf("Unexpected error from LookupRangeDescriptor: %s", err.Error())
	}
	if !r.ContainsKey(engine.KeyAddress(proto.Key(key))) {
		t.Fatalf("Returned range did not contain key: %s-%s, %s", r.StartKey, r.EndKey, key)
	}
}

// TestRangeCache is a simple test which verifies that metadata ranges
// are being cached and retrieved properly. It sets up a fake backing
// store for the cache, and measures how often that backing store is
// accessed when looking up metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	db := newTestDescriptorDB()
	for i, char := range "abcdefghijklmnopqrstuvwx" {
		db.splitRange(t, proto.Key(string(char)))
		if i > 0 && i%6 == 0 {
			db.splitRange(t, engine.RangeMetaKey(proto.Key(string(char))))
		}
	}

	rangeCache := NewRangeDescriptorCache(db)
	db.cache = rangeCache

	doLookup(t, rangeCache, "aa")
	db.assertHitCount(t, 2)

	// Descriptors for the following ranges should be cached
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
	rangeCache.EvictCachedRangeDescriptor(proto.Key("da"))
	doLookup(t, rangeCache, "fa")
	db.assertHitCount(t, 0)
	doLookup(t, rangeCache, "da")
	db.assertHitCount(t, 2)
}
