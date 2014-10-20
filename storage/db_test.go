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

package storage

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// testDB is an implementation of the DB interface which
// passes all requests through to a single store.
type testDB struct {
	*BaseDB
	store *Store
	clock *hlc.Clock
}

func newTestDB(store *Store) (*testDB, *hlc.ManualClock) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	db := &testDB{store: store, clock: clock}
	db.BaseDB = NewBaseDB(db.executeCmd)
	return db, &manual
}

func (db *testDB) executeCmd(method string, args proto.Request, replyChan interface{}) {
	reply := reflect.New(reflect.TypeOf(replyChan).Elem().Elem()).Interface().(proto.Response)
	if rng := db.store.LookupRange(args.Header().Key, args.Header().EndKey); rng != nil {
		args.Header().Replica = *rng.GetReplica()
		db.store.ExecuteCmd(method, args, reply)
	} else {
		reply.Header().SetGoError(proto.NewRangeKeyMismatchError(args.Header().Key, args.Header().EndKey, nil))
	}
	reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
}

// InternalEndTxn is a noop as the testDB implementation of DB doesn't
// support transactions. However, in order to enable testing of range
// splitting, it must execute the split trigger if one is specified.
func (db *testDB) InternalEndTxn(args *proto.InternalEndTxnRequest) <-chan *proto.InternalEndTxnResponse {
	replyChan := make(chan *proto.InternalEndTxnResponse, 1)
	reply := &proto.InternalEndTxnResponse{}

	// Lookup range.
	if rng := db.store.LookupRange(args.Key, args.EndKey); rng != nil {
		if args.Commit == true && args.SplitTrigger != nil {
			batch := rng.rm.Engine().NewBatch()
			if err := rng.splitTrigger(batch, args.SplitTrigger); err != nil {
				reply.SetGoError(err)
			} else {
				reply.SetGoError(batch.Commit())
			}
		}
	} else {
		reply.Header().SetGoError(proto.NewRangeKeyMismatchError(args.Header().Key, args.Header().EndKey, nil))
	}
	replyChan <- reply
	return replyChan
}

// RunTransaction is a simple pass through to support testing of range splitting.
func (db *testDB) RunTransaction(opts *TransactionOptions, retryable func(db DB) error) error {
	return retryable(db)
}

type metaRecord struct {
	key  engine.Key
	desc *proto.RangeDescriptor
}
type metaSlice []metaRecord

// Implementation of sort.Interface.
func (ms metaSlice) Len() int           { return len(ms) }
func (ms metaSlice) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
func (ms metaSlice) Less(i, j int) bool { return ms[i].key.Less(ms[j].key) }

func meta1Key(key engine.Key) engine.Key {
	return engine.MakeKey(engine.KeyMeta1Prefix, key)
}

func meta2Key(key engine.Key) engine.Key {
	return engine.MakeKey(engine.KeyMeta2Prefix, key)
}

// TestUpdateRangeAddressing verifies range addressing records are
// correctly updated on creation of new range descriptors.
func TestUpdateRangeAddressing(t *testing.T) {
	store, _ := createTestStore(t)
	testCases := []struct {
		start, end engine.Key
		expNew     []engine.Key
	}{
		// Start out with whole range.
		{engine.KeyMin, engine.KeyMax,
			[]engine.Key{meta1Key(engine.KeyMax), meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "a".
		{engine.KeyMin, engine.Key("a"),
			[]engine.Key{meta1Key(engine.KeyMax), meta2Key(engine.Key("a"))}},
		// Second half of splitting the range at key "a".
		{engine.Key("a"), engine.KeyMax,
			[]engine.Key{meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "z".
		{engine.Key("a"), engine.Key("z"),
			[]engine.Key{meta2Key(engine.Key("z"))}},
		// Second half of splitting the range at key "z"
		{engine.Key("z"), engine.KeyMax,
			[]engine.Key{meta2Key(engine.KeyMax)}},
		// First half of splitting the range at key "m".
		{engine.Key("a"), engine.Key("m"),
			[]engine.Key{meta2Key(engine.Key("m"))}},
		// Second half of splitting the range at key "m"
		{engine.Key("m"), engine.Key("z"),
			[]engine.Key{meta2Key(engine.Key("z"))}},
		// First half of splitting at meta2(m).
		{engine.KeyMin, engine.RangeMetaKey(engine.Key("m")),
			[]engine.Key{meta1Key(engine.Key("m"))}},
		// Second half of splitting at meta2(m).
		{engine.RangeMetaKey(engine.Key("m")), engine.Key("a"),
			[]engine.Key{meta1Key(engine.KeyMax), meta2Key(engine.Key("a"))}},
		// First half of splitting at meta2(z).
		{engine.RangeMetaKey(engine.Key("m")), engine.RangeMetaKey(engine.Key("z")),
			[]engine.Key{meta1Key(engine.Key("z"))}},
		// Second half of splitting at meta2(z).
		{engine.RangeMetaKey(engine.Key("z")), engine.Key("a"),
			[]engine.Key{meta1Key(engine.KeyMax), meta2Key(engine.Key("a"))}},
		// First half of splitting at meta2(r).
		{engine.RangeMetaKey(engine.Key("m")), engine.RangeMetaKey(engine.Key("r")),
			[]engine.Key{meta1Key(engine.Key("r"))}},
		// Second half of splitting at meta2(r).
		{engine.RangeMetaKey(engine.Key("r")), engine.RangeMetaKey(engine.Key("z")),
			[]engine.Key{meta1Key(engine.Key("z"))}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		desc := &proto.RangeDescriptor{RaftID: int64(i), StartKey: test.start, EndKey: test.end}
		if err := UpdateRangeAddressing(store.DB(), desc); err != nil {
			t.Fatal(err)
		}
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
	store, _ := createTestStore(t)
	desc := &proto.RangeDescriptor{StartKey: meta1Key(engine.Key("a")), EndKey: engine.KeyMax}
	if err := UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
	desc = &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(engine.Key("a"))}
	if err := UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
