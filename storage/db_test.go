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
)

// createTestStore creates a test store using an in-memory
// engine. Returns the store clock's manual unix nanos time and the
// store. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) *storage.Store {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext)
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	lSender := kv.NewLocalSender()
	sender := kv.NewCoordinator(lSender, clock)
	db := client.NewKV(sender, nil)
	db.User = storage.UserRoot
	store := storage.NewStore(clock, eng, db, g)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	lSender.AddStore(store)
	if _, err := store.BootstrapRange(); err != nil {
		t.Fatal(err)
	}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	return store
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, rangeID int64) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, rangeID int64) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
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
func incrementArgs(key []byte, inc int64, rangeID int64) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
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
	testCases := []struct {
		start, end proto.Key
		expNew     []proto.Key
	}{
		// Start out with whole range.
		{engine.KeyMin, engine.KeyMax,
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "a".
		{engine.KeyMin, proto.Key("a"),
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// Second half of splitting the range at key "a".
		{proto.Key("a"), engine.KeyMax,
			[]proto.Key{meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "z".
		{proto.Key("a"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("z"))}},
		// Second half of splitting the range at key "z"
		{proto.Key("z"), engine.KeyMax,
			[]proto.Key{meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "m".
		{proto.Key("a"), proto.Key("m"),
			[]proto.Key{meta2Key(proto.Key("m"))}},
		// Second half of splitting the range at key "m"
		{proto.Key("m"), proto.Key("z"),
			[]proto.Key{meta2Key(proto.Key("z"))}},
		// First half of splitting at meta2(m).
		{engine.KeyMin, engine.RangeMetaKey(proto.Key("m")),
			[]proto.Key{meta1Key(proto.Key("m"))}},
		// Second half of splitting at meta2(m).
		{engine.RangeMetaKey(proto.Key("m")), proto.Key("a"),
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// First half of splitting at meta2(z).
		{engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("z"))}},
		// Second half of splitting at meta2(z).
		{engine.RangeMetaKey(proto.Key("z")), proto.Key("a"),
			[]proto.Key{meta1Key(engine.KeyMax), meta2Key(proto.Key("a"))}},
		// First half of splitting at meta2(r).
		{engine.RangeMetaKey(proto.Key("m")), engine.RangeMetaKey(proto.Key("r")),
			[]proto.Key{meta1Key(proto.Key("r"))}},
		// Second half of splitting at meta2(r).
		{engine.RangeMetaKey(proto.Key("r")), engine.RangeMetaKey(proto.Key("z")),
			[]proto.Key{meta1Key(proto.Key("z"))}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		desc := &proto.RangeDescriptor{RaftID: int64(i), StartKey: test.start, EndKey: test.end}
		if err := storage.UpdateRangeAddressing(store.DB(), desc); err != nil {
			t.Fatal(err)
		}
		store.DB().Flush()
		// Scan meta keys directly from engine.
		mvcc := engine.NewMVCC(store.Engine())
		kvs, err := mvcc.Scan(engine.KeyMetaPrefix, engine.KeyMetaMax, 0, proto.MaxTimestamp, nil)
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
		for _, n := range test.expNew {
			found := false
			for i := range expMetas {
				if expMetas[i].key.Equal(n) {
					found = true
					expMetas[i].desc = desc
					break
				}
			}
			if !found {
				expMetas = append(expMetas, metaRecord{key: n, desc: desc})
			}
		}
		sort.Sort(expMetas)
		if !reflect.DeepEqual(expMetas, metas) {
			t.Errorf("expected metas don't match")
			for i, meta := range expMetas {
				fmt.Printf("%d: expected %q vs %q\n", i, meta.key, metas[i].key)
			}
		}
	}
}

// TestUpdateRangeAddressingSplitMeta1 verifies that it's an error to
// attempt to update range addressing records that would allow a split
// of meta1 records.
func TestUpdateRangeAddressingSplitMeta1(t *testing.T) {
	store := createTestStore(t)
	desc := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: engine.KeyMax}
	if err := storage.UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
	desc = &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	if err := storage.UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
