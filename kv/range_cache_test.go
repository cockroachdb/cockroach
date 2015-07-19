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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

type testDescriptorDB struct {
	data        llrb.Tree
	cache       *rangeDescriptorCache
	lookupCount int
}

type testDescriptorNode struct {
	*proto.RangeDescriptor
}

func (a testDescriptorNode) Compare(b llrb.Comparable) int {
	aKey := a.RangeDescriptor.EndKey
	bKey := b.(testDescriptorNode).RangeDescriptor.EndKey
	return bytes.Compare(aKey, bKey)
}

func (db *testDescriptorDB) getDescriptor(key proto.Key) []proto.RangeDescriptor {
	log.Infof("getDescriptor: %s", key)
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
		key = proto.Key(response[i].EndKey)
	}
	return response
}

func (db *testDescriptorDB) getRangeDescriptors(key proto.Key,
	options lookupOptions) ([]proto.RangeDescriptor, error) {
	db.lookupCount++
	metadataKey := keys.RangeMetaKey(key)

	var err error

	// Recursively call into cache as the real DB would, terminating recursion
	// when a meta1key is encountered.
	if len(metadataKey) > 0 && !bytes.HasPrefix(metadataKey, keys.Meta1Prefix) {
		_, err = db.cache.LookupRangeDescriptor(metadataKey, options)
	}
	return db.getDescriptor(key), err
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
			StartKey: keys.MakeKey(keys.Meta2Prefix, proto.KeyMin),
			EndKey:   keys.MakeKey(keys.Meta2Prefix, proto.KeyMax),
		},
	})
	db.data.Insert(testDescriptorNode{
		&proto.RangeDescriptor{
			StartKey: proto.KeyMin,
			EndKey:   proto.KeyMax,
		},
	})
	return db
}

func (db *testDescriptorDB) assertLookupCount(t *testing.T, expected int, key string) {
	if db.lookupCount != expected {
		t.Errorf("Expected lookup count to be %d after %s, was %d", expected, key, db.lookupCount)
	}
	db.lookupCount = 0
}

func doLookup(t *testing.T, rc *rangeDescriptorCache, key string) *proto.RangeDescriptor {
	r, err := rc.LookupRangeDescriptor(proto.Key(key), lookupOptions{})
	if err != nil {
		t.Fatalf("Unexpected error from LookupRangeDescriptor: %s", err)
	}
	if !r.ContainsKey(keys.KeyAddress(proto.Key(key))) {
		t.Fatalf("Returned range did not contain key: %s-%s, %s", r.StartKey, r.EndKey, key)
	}
	log.Infof("doLookup: %s %+v", key, r)
	return r
}

func TestRangeCacheAssumptions(t *testing.T) {
	defer leaktest.AfterTest(t)
	expKeyMin := keys.RangeMetaKey(keys.RangeMetaKey(keys.RangeMetaKey(proto.Key("test"))))
	if !bytes.Equal(expKeyMin, proto.KeyMin) {
		t.Fatalf("RangeCache relies on RangeMetaKey returning KeyMin after two levels, but got %s", expKeyMin)
	}
}

// TestRangeCache is a simple test which verifies that metadata ranges
// are being cached and retrieved properly. It sets up a fake backing
// store for the cache, and measures how often that backing store is
// lookuped when looking up metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	db := newTestDescriptorDB()
	for i, char := range "abcdefghijklmnopqrstuvwx" {
		db.splitRange(t, proto.Key(string(char)))
		if i > 0 && i%6 == 0 {
			db.splitRange(t, keys.RangeMetaKey(proto.Key(string(char))))
		}
	}

	db.cache = newRangeDescriptorCache(db, 2<<10)

	doLookup(t, db.cache, "aa")
	db.assertLookupCount(t, 2, "aa")

	// Descriptors for the following ranges should be cached.
	doLookup(t, db.cache, "ab")
	db.assertLookupCount(t, 0, "ab")
	doLookup(t, db.cache, "ba")
	db.assertLookupCount(t, 0, "ba")
	doLookup(t, db.cache, "cz")
	db.assertLookupCount(t, 0, "cz")

	// Metadata two ranges weren't cached, same metadata 1 range.
	doLookup(t, db.cache, "d")
	db.assertLookupCount(t, 1, "d")
	doLookup(t, db.cache, "fa")
	db.assertLookupCount(t, 0, "fa")

	// Metadata two ranges weren't cached, metadata 1 was aggressively cached
	doLookup(t, db.cache, "ij")
	db.assertLookupCount(t, 1, "ij")
	doLookup(t, db.cache, "jk")
	db.assertLookupCount(t, 0, "jk")
	doLookup(t, db.cache, "pn")
	db.assertLookupCount(t, 1, "pn")

	// Totally uncached ranges
	doLookup(t, db.cache, "vu")
	db.assertLookupCount(t, 2, "vu")
	doLookup(t, db.cache, "xx")
	db.assertLookupCount(t, 0, "xx")

	// Evict clears one level 1 and one level 2 cache
	db.cache.EvictCachedRangeDescriptor(proto.Key("da"), nil)
	doLookup(t, db.cache, "fa")
	db.assertLookupCount(t, 0, "fa")
	doLookup(t, db.cache, "da")
	db.assertLookupCount(t, 2, "da")

	// Looking up a descriptor that lands on an end-key should work
	// without a cache miss.
	doLookup(t, db.cache, "a")
	db.assertLookupCount(t, 0, "a")

	// Attempt to compare-and-evict with a descriptor that is not equal to the
	// cached one; it should not alter the cache.
	db.cache.EvictCachedRangeDescriptor(proto.Key("cz"), &proto.RangeDescriptor{})
	doLookup(t, db.cache, "cz")
	db.assertLookupCount(t, 0, "cz")
	// Now evict with the actual descriptor. The cache should clear the
	// descriptor and the cached meta key.
	db.cache.EvictCachedRangeDescriptor(proto.Key("cz"), doLookup(t, db.cache, "cz"))
	doLookup(t, db.cache, "cz")
	db.assertLookupCount(t, 2, "cz")

}

// TestRangeCacheClearOverlapping verifies that existing, overlapping
// cached entries are cleared when adding a new entry.
func TestRangeCacheClearOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)

	defDesc := &proto.RangeDescriptor{
		StartKey: proto.KeyMin,
		EndKey:   proto.KeyMax,
	}

	cache := newRangeDescriptorCache(nil, 2<<10)
	cache.rangeCache.Add(rangeCacheKey(keys.RangeMetaKey(proto.KeyMax)), defDesc)

	// Now, add a new, overlapping set of descriptors.
	minToADesc := &proto.RangeDescriptor{
		StartKey: proto.KeyMin,
		EndKey:   proto.Key("b"),
	}
	aToMaxDesc := &proto.RangeDescriptor{
		StartKey: proto.Key("b"),
		EndKey:   proto.KeyMax,
	}
	cache.clearOverlappingCachedRangeDescriptors(proto.Key("b"), keys.RangeMetaKey(proto.Key("b")), minToADesc)
	cache.rangeCache.Add(rangeCacheKey(keys.RangeMetaKey(proto.Key("b"))), minToADesc)
	if _, desc := cache.getCachedRangeDescriptor(proto.Key("b")); desc != nil {
		t.Errorf("descriptor unexpectedly non-nil: %s", desc)
	}
	cache.clearOverlappingCachedRangeDescriptors(proto.KeyMax, keys.RangeMetaKey(proto.KeyMax), aToMaxDesc)
	cache.rangeCache.Add(rangeCacheKey(keys.RangeMetaKey(proto.KeyMax)), aToMaxDesc)
	if _, desc := cache.getCachedRangeDescriptor(proto.Key("b")); desc != aToMaxDesc {
		t.Errorf("expected descriptor %s; got %s", aToMaxDesc, desc)
	}

	// Add default descriptor back which should remove two split descriptors.
	cache.clearOverlappingCachedRangeDescriptors(proto.KeyMax, keys.RangeMetaKey(proto.KeyMax), defDesc)
	cache.rangeCache.Add(rangeCacheKey(keys.RangeMetaKey(proto.KeyMax)), defDesc)
	for _, key := range []proto.Key{proto.Key("a"), proto.Key("b")} {
		if _, desc := cache.getCachedRangeDescriptor(key); desc != defDesc {
			t.Errorf("expected descriptor %s for key %s; got %s", defDesc, key, desc)
		}
	}
}
