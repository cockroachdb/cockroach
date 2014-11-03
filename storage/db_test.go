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
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
)

// testSender is an implementation of the client.KVSender interface
// which passes all requests through to a single store. A map keeps
// track of keys with intents for each transaction. Intents are
// resolved on invocation of EndTransaction. NOTE: this does not
// properly handle resolving intents from DeleteRange commands.
type testSender struct {
	store   *Store
	intents map[string][]proto.Key
}

// Send forwards the call to the single store. If the call is part of
// a txn and is read-write, keep track of keys as intent. On txn end,
// resolve all intents. This is a poor man's version of kv/coordinator,
// but it serves the purposes of supporting tests in this package.
// Since kv/ depends on storage/, we can't get access to a coordinator
// sender from here.
func (db *testSender) Send(call *client.Call) {
	// Keep track of all intents.
	// NOTE: this doesn't keep track of range intents.
	txn := call.Args.Header().Txn
	if proto.IsReadWrite(call.Method) && txn != nil && proto.IsTransactional(call.Method) {
		if db.intents == nil {
			db.intents = map[string][]proto.Key{}
		}
		id := string(txn.ID)
		db.intents[id] = append(db.intents[id], call.Args.Header().Key)
	}

	// Handle BeginTransaction separately.
	if call.Method == proto.BeginTransaction {
		btArgs := call.Args.(*proto.BeginTransactionRequest)
		txn = proto.NewTransaction(btArgs.Name, engine.KeyAddress(btArgs.Key),
			btArgs.GetUserPriority(), btArgs.Isolation,
			db.store.Clock().Now(), db.store.Clock().MaxOffset().Nanoseconds())
		call.Reply.(*proto.BeginTransactionResponse).Txn = txn
		return
	}

	// Forward call to range as command.
	if rng := db.store.LookupRange(call.Args.Header().Key, call.Args.Header().EndKey); rng != nil {
		call.Args.Header().Replica = *rng.GetReplica()
		db.store.ExecuteCmd(call.Method, call.Args, call.Reply)
	} else {
		call.Reply.Header().SetGoError(proto.NewRangeKeyMismatchError(call.Args.Header().Key, call.Args.Header().EndKey, nil))
	}

	// Cleanup intents on end transaction.
	if call.Reply.Header().GoError() == nil {
		txn = nil
		if call.Method == proto.EndTransaction {
			txn = call.Reply.(*proto.EndTransactionResponse).Txn
		}
		if txn != nil && txn.Status != proto.PENDING {
			id := string(txn.ID)
			for _, key := range db.intents[id] {
				log.V(1).Infof("cleaning up intent %q for txn %s", key, txn)
				reply := &proto.InternalResolveIntentResponse{}
				db.Send(&client.Call{
					Method: proto.InternalResolveIntent,
					Args: &proto.InternalResolveIntentRequest{
						RequestHeader: proto.RequestHeader{
							Replica:   call.Args.Header().Replica,
							Timestamp: txn.Timestamp,
							Key:       key,
							Txn:       txn,
						},
					},
					Reply: reply,
				})
				if err := reply.GoError(); err != nil {
					log.Warningf("failed to cleanup intent at %q: %s", key, err)
				}
			}
			delete(db.intents, id)
		}
	}
}

// Close implements the client.KVSender interface.
func (db *testSender) Close() {}

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
	store, _ := createTestStore(t)
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
	desc := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: engine.KeyMax}
	if err := UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
	desc = &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	if err := UpdateRangeAddressing(store.DB(), desc); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
