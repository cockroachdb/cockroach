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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/* Package storage_test provides a means of testing store
functionality which depends on a fully-functional KV client. This
cannot be done within the storage package because of circular
dependencies.
*/
package storage_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// createTestStore creates a test store using an in-memory
// engine. Returns the store clock's manual unix nanos time and the
// store. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) *storage.Store {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext)
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	lSender := kv.NewLocalSender()
	sender := kv.NewTxnCoordSender(lSender, clock)
	db := client.NewKV(sender, nil)
	db.User = storage.UserRoot
	store := storage.NewStore(clock, eng, db, g)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	lSender.AddStore(store)
	if err := store.BootstrapRange(); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(); err != nil {
		t.Fatal(err)
	}
	return store
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, raftID int64, storeID int32) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, raftID int64, storeID int32) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
	reply := &proto.PutResponse{}
	return args, reply
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, raftID int64, storeID int32) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Increment: inc,
	}
	reply := &proto.IncrementResponse{}
	return args, reply
}

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
	return engine.MakeKey(engine.KeyMeta1Prefix, key)
}

func meta2Key(key proto.Key) proto.Key {
	return engine.MakeKey(engine.KeyMeta2Prefix, key)
}

// TestUpdateRangeAddressing verifies range addressing records are
// correctly updated on creation of new range descriptors.
func TestUpdateRangeAddressing(t *testing.T) {
	store := createTestStore(t)
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
		{false, engine.KeyMin, engine.KeyMax, engine.KeyMin, engine.KeyMax,
			[]proto.Key{}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
		// Split KeyMin-KeyMax at key "a".
		{true, engine.KeyMin, proto.Key("a"), proto.Key("a"), engine.KeyMax,
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Split "a"-KeyMax at key "z".
		{true, proto.Key("a"), proto.Key("z"), proto.Key("z"), engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Split "a"-"z" at key "m".
		{true, proto.Key("a"), proto.Key("m"), proto.Key("m"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Split KeyMin-"a" at meta2(m).
		{true, engine.KeyMin, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-"a" at meta2(z).
		{true, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")), engine.RangeMetaKey(proto.Key("z")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Split meta2(m)-meta2(z) at meta2(r).
		{true, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},

		// Now, merge all of our splits backwards...

		// Merge meta2(m)-meta2(z).
		{false, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("r"))}, []proto.Key{meta1Key(proto.Key("z"))}},
		// Merge meta2(m)-"a".
		{false, engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")), engine.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("z"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge KeyMin-"a".
		{false, engine.KeyMin, engine.RangeMetaKey(proto.Key("m")), engine.KeyMin, proto.Key("a"),
			[]proto.Key{meta1Key(proto.Key("m"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Merge "a"-"z".
		{false, proto.Key("a"), proto.Key("m"), proto.Key("a"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("m"))}, []proto.Key{meta2Key(proto.Key("z"))}},
		// Merge "a"-KeyMax.
		{false, proto.Key("a"), proto.Key("z"), proto.Key("a"), engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("z"))}, []proto.Key{meta2Key(engine.KeyMax)}},
		// Merge KeyMin-KeyMax.
		{false, engine.KeyMin, proto.Key("a"), engine.KeyMin, engine.KeyMax,
			[]proto.Key{meta2Key(proto.Key("a"))}, []proto.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		left := &proto.RangeDescriptor{RaftID: int64(i * 2), StartKey: test.leftStart, EndKey: test.leftEnd}
		right := &proto.RangeDescriptor{RaftID: int64(i*2 + 1), StartKey: test.rightStart, EndKey: test.rightEnd}
		if test.split {
			if err := storage.SplitRangeAddressing(store.DB(), left, right); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := storage.MergeRangeAddressing(store.DB(), left, right); err != nil {
				t.Fatal(err)
			}
		}
		store.DB().Flush()
		// Scan meta keys directly from engine.
		kvs, err := engine.MVCCScan(store.Engine(), engine.KeyMetaPrefix, engine.KeyMetaMax, 0, proto.MaxTimestamp, nil)
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
			log.V(1).Infof("test case %d: split %q-%q at %q", i, left.StartKey, right.EndKey, left.EndKey)
		} else {
			log.V(1).Infof("test case %d: merge %q-%q + %q-%q", i, left.StartKey, left.EndKey, left.EndKey, right.EndKey)
		}
		for _, meta := range metas {
			log.V(1).Infof("%q", meta.key)
		}
		log.V(1).Infof("")

		if !reflect.DeepEqual(expMetas, metas) {
			t.Errorf("expected metas don't match")
			if len(expMetas) != len(metas) {
				t.Errorf("len(expMetas) != len(metas); %d != %d", len(expMetas), len(metas))
			} else {
				for i, meta := range expMetas {
					if !meta.key.Equal(metas[i].key) {
						fmt.Printf("%d: expected %q vs %q\n", i, meta.key, metas[i].key)
					}
					if !reflect.DeepEqual(meta.desc, metas[i].desc) {
						fmt.Printf("%d: expected %q vs %q and %s vs %s\n", i, meta.key, metas[i].key, meta.desc, metas[i].desc)
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
	store := createTestStore(t)
	left := &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	right := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: engine.KeyMax}
	if err := storage.SplitRangeAddressing(store.DB(), left, right); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
