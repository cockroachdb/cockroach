// Copyright 2015 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

type metaRecord struct {
	key  proto.Key
	desc *proto.RangeDescriptor
}
type metaSlice []metaRecord

// Implementation of sort.Interface.
func (ms metaSlice) Len() int           { return len(ms) }
func (ms metaSlice) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
func (ms metaSlice) Less(i, j int) bool { return ms[i].key.Less(ms[j].key) }

func meta1Key(key proto.Key) proto.Key {
	return keys.MakeKey(keys.Meta1Prefix, key)
}

func meta2Key(key proto.Key) proto.Key {
	return keys.MakeKey(keys.Meta2Prefix, key)
}

// TestUpdateRangeAddressing verifies range addressing records are
// correctly updated on creation of new range descriptors.
func TestUpdateRangeAddressing(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// When split is false, merging treats the right range as the merged
	// range. With merging, expNewLeft indicates the addressing keys we
	// expect to be removed.
	testCases := []struct {
		split                   bool
		leftStart, leftEnd      proto.Key
		rightStart, rightEnd    proto.Key
		leftExpNew, rightExpNew []proto.Key
	}{
		// Start out with whole range.
		{false, proto.KeyMin, proto.KeyMax, proto.KeyMin, proto.KeyMax,
			[]proto.Key{}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.KeyMax)}},
		// Split KeyMin-KeyMax at key "a".
		{true, proto.KeyMin, proto.Key("a"), proto.Key("a"), proto.KeyMax,
			[]proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.Key("a"))}, []proto.Key{meta2Key(proto.KeyMax)}},
		// Split "a"-KeyMax at key "z".
		{true, proto.Key("a"), proto.Key("z"), proto.Key("z"), proto.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(proto.KeyMax)}},
		// Split "a"-"z" at key "m".
		{true, proto.Key("a"), proto.Key("m"), proto.Key("m"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Split KeyMin-"a" at meta2(m).
		{true, proto.KeyMin, keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-"a" at meta2(z).
		{true, keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("z")), keys.RangeMetaKey(proto.Key("z")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-meta2(z) at meta2(r).
		{true, keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("r")), keys.RangeMetaKey(proto.Key("r")), keys.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},

		// Now, merge all of our splits backwards...

		// Merge meta2(m)-meta2(z).
		{false, keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("r")), keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},
		// Merge meta2(m)-"a".
		{false, keys.RangeMetaKey(proto.Key("m")), keys.RangeMetaKey(proto.Key("z")), keys.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge KeyMin-"a".
		{false, proto.KeyMin, keys.RangeMetaKey(proto.Key("m")), proto.KeyMin, proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge "a"-"z".
		{false, proto.Key("a"), proto.Key("m"), proto.Key("a"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Merge "a"-KeyMax.
		{false, proto.Key("a"), proto.Key("z"), proto.Key("a"), proto.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(proto.KeyMax)}},
		// Merge KeyMin-KeyMax.
		{false, proto.KeyMin, proto.Key("a"), proto.KeyMin, proto.KeyMax,
			[]proto.Key{meta2Key(proto.Key("a"))}, []proto.Key{meta1Key(proto.KeyMax), meta2Key(proto.KeyMax)}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		left := &proto.RangeDescriptor{RaftID: int64(i * 2), StartKey: test.leftStart, EndKey: test.leftEnd}
		right := &proto.RangeDescriptor{RaftID: int64(i*2 + 1), StartKey: test.rightStart, EndKey: test.rightEnd}
		b := &client.Batch{}
		if test.split {
			if err := splitRangeAddressing(b, left, right); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := mergeRangeAddressing(b, left, right); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.DB().Run(b); err != nil {
			t.Fatal(err)
		}
		// Scan meta keys directly from engine.
		kvs, err := engine.MVCCScan(store.Engine(), keys.MetaPrefix, keys.MetaMax, 0, proto.MaxTimestamp, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		metas := metaSlice{}
		for _, kv := range kvs {
			scannedDesc := &proto.RangeDescriptor{}
			if err := gogoproto.Unmarshal(kv.Value.Bytes, scannedDesc); err != nil {
				t.Fatal(err)
			}
			metas = append(metas, metaRecord{key: kv.Key, desc: scannedDesc})
		}

		// Continue to build up the expected metas slice, replacing any earlier
		// version of same key.
		addOrRemoveNew := func(keys []proto.Key, desc *proto.RangeDescriptor, add bool) {
			for _, n := range keys {
				found := -1
				for i := range expMetas {
					if expMetas[i].key.Equal(n) {
						found = i
						expMetas[i].desc = desc
						break
					}
				}
				if found == -1 && add {
					expMetas = append(expMetas, metaRecord{key: n, desc: desc})
				} else if found != -1 && !add {
					expMetas = append(expMetas[:found], expMetas[found+1:]...)
				}
			}
		}
		addOrRemoveNew(test.leftExpNew, left, test.split /* on split, add; on merge, remove */)
		addOrRemoveNew(test.rightExpNew, right, true)
		sort.Sort(expMetas)

		if test.split {
			if log.V(1) {
				log.Infof("test case %d: split %q-%q at %q", i, left.StartKey, right.EndKey, left.EndKey)
			}
		} else {
			if log.V(1) {
				log.Infof("test case %d: merge %q-%q + %q-%q", i, left.StartKey, left.EndKey, left.EndKey, right.EndKey)
			}
		}
		for _, meta := range metas {
			if log.V(1) {
				log.Infof("%q", meta.key)
			}
		}

		if !reflect.DeepEqual(expMetas, metas) {
			t.Errorf("expected metas don't match")
			if len(expMetas) != len(metas) {
				t.Errorf("len(expMetas) != len(metas); %d != %d", len(expMetas), len(metas))
			} else {
				for j, meta := range expMetas {
					if !meta.key.Equal(metas[j].key) {
						fmt.Printf("%d: expected %q vs %q\n", j, meta.key, metas[j].key)
					}
					if !reflect.DeepEqual(meta.desc, metas[j].desc) {
						fmt.Printf("%d: expected %q vs %q and %s vs %s\n", j, meta.key, metas[j].key, meta.desc, metas[j].desc)
					}
				}
			}
		}
	}
}

// TestUpdateRangeAddressingSplitMeta1 verifies that it's an error to
// attempt to update range addressing records that would allow a split
// of meta1 records.
func TestUpdateRangeAddressingSplitMeta1(t *testing.T) {
	defer leaktest.AfterTest(t)
	left := &proto.RangeDescriptor{StartKey: proto.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	right := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: proto.KeyMax}
	if err := splitRangeAddressing(&client.Batch{}, left, right); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
