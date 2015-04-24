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

By convention, tests in package storage_test have names of the form
client_*.go.
*/
package storage_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// createTestStore creates a test store using an in-memory
// engine. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) (*storage.Store, *util.Stopper) {
	return createTestStoreWithEngine(t,
		engine.NewInMem(proto.Attributes{}, 10<<20),
		hlc.NewClock(hlc.NewManualClock(0).UnixNano),
		true, nil)
}

// createTestStoreWithEngine creates a test store using the given engine and clock.
// The caller is responsible for closing the store on exit.
func createTestStoreWithEngine(t *testing.T, eng engine.Engine, clock *hlc.Clock,
	bootstrap bool, context *storage.StoreContext) (*storage.Store, *util.Stopper) {
	stopper := util.NewStopper()
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), security.LoadInsecureTLSConfig(), stopper)
	var ctx *storage.StoreContext
	if context == nil {
		ctx = &storage.StoreContext{}
		*ctx = storage.TestStoreContext
	} else {
		ctx = context
	}
	ctx.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	lSender := kv.NewLocalSender()
	sender := kv.NewTxnCoordSender(lSender, clock, false, stopper)
	ctx.Clock = clock
	ctx.DB = client.NewKV(nil, sender)
	ctx.DB.User = storage.UserRoot
	ctx.Transport = multiraft.NewLocalRPCTransport()
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(*ctx, eng)
	if bootstrap {
		if err := store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
			t.Fatal(err)
		}
	}
	lSender.AddStore(store)
	if bootstrap {
		if err := store.BootstrapRange(); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	return store, stopper
}

type multiTestContext struct {
	t           *testing.T
	manualClock *hlc.ManualClock
	clock       *hlc.Clock
	gossip      *gossip.Gossip
	transport   multiraft.Transport
	db          *client.KV
	engines     []engine.Engine
	senders     []*kv.LocalSender
	stores      []*storage.Store
	idents      []proto.StoreIdent
	// We use multiple stoppers so we can restart different parts of the
	// test individually. clientStopper is for 'db', transportStopper is
	// for 'transport', and the 'stoppers' slice corresponds to the
	// 'stores'.
	clientStopper    *util.Stopper
	stoppers         []*util.Stopper
	transportStopper *util.Stopper
}

// startMultiTestContext is a convenience function to create, start, and return
// a multiTestContext.
func startMultiTestContext(t *testing.T, numStores int) *multiTestContext {
	m := &multiTestContext{}
	m.Start(t, numStores)
	return m
}

func (m *multiTestContext) Start(t *testing.T, numStores int) {
	m.t = t
	if m.manualClock == nil {
		m.manualClock = hlc.NewManualClock(0)
	}
	if m.clock == nil {
		m.clock = hlc.NewClock(m.manualClock.UnixNano)
	}
	if m.gossip == nil {
		rpcContext := rpc.NewContext(m.clock, security.LoadInsecureTLSConfig(), nil)
		m.gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	}
	if m.transport == nil {
		m.transport = multiraft.NewLocalRPCTransport()
	}

	if m.clientStopper == nil {
		m.clientStopper = util.NewStopper()
	}

	// Always create the first sender.
	m.senders = append(m.senders, kv.NewLocalSender())

	if m.db == nil {
		txnSender := kv.NewTxnCoordSender(m.senders[0], m.clock, false, m.clientStopper)
		m.db = client.NewKV(nil, txnSender)
		m.db.User = storage.UserRoot
	}

	for i := 0; i < numStores; i++ {
		m.addStore(t)
	}
	if m.transportStopper == nil {
		m.transportStopper = util.NewStopper()
	}
	m.transportStopper.AddCloser(m.transport)
}

func (m *multiTestContext) Stop() {
	m.clientStopper.Stop()
	for _, s := range m.stoppers {
		s.Stop()
	}
	m.transportStopper.Stop()
	// Remove the extra engine refcounts.
	for _, e := range m.engines {
		e.Close()
	}
}

func (m *multiTestContext) makeContext() storage.StoreContext {
	ctx := storage.TestStoreContext
	ctx.Clock = m.clock
	ctx.DB = m.db
	ctx.Gossip = m.gossip
	ctx.Transport = m.transport
	return ctx
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore(t *testing.T) {
	idx := len(m.stores)
	var eng engine.Engine
	var needBootstrap bool
	if len(m.engines) > len(m.stores) {
		eng = m.engines[idx]
	} else {
		eng = engine.NewInMem(proto.Attributes{}, 1<<20)
		m.engines = append(m.engines, eng)
		needBootstrap = true
		// Add an extra refcount to the engine so the underlying rocksdb instances
		// aren't closed when stopping and restarting the stores.
		// These refcounts are removed in Stop().
		if err := eng.Open(); err != nil {
			t.Fatal(err)
		}
	}

	stopper := util.NewStopper()
	ctx := m.makeContext()
	store := storage.NewStore(ctx, eng)
	if needBootstrap {
		err := store.Bootstrap(proto.StoreIdent{
			NodeID:  proto.NodeID(idx + 1),
			StoreID: proto.StoreID(idx + 1),
		}, stopper)
		if err != nil {
			t.Fatal(err)
		}

		// Bootstrap the initial range on the first store
		if idx == 0 {
			if err := store.BootstrapRange(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	m.stores = append(m.stores, store)
	if len(m.senders) == idx {
		m.senders = append(m.senders, kv.NewLocalSender())
	}
	m.senders[idx].AddStore(store)
	// Save the store identities for later so we can use them in
	// replication operations even while the store is stopped.
	m.idents = append(m.idents, store.Ident)
	m.stoppers = append(m.stoppers, stopper)
}

// StopStore stops a store but leaves the engine intact.
// All stopped stores must be restarted before multiTestContext.Stop is called.
func (m *multiTestContext) stopStore(i int) {
	m.senders[i].RemoveStore(m.stores[i])
	m.stoppers[i].Stop()
	m.stoppers[i] = nil
	m.stores[i] = nil
}

// restartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) restartStore(i int) {
	m.stoppers[i] = util.NewStopper()

	ctx := m.makeContext()
	m.stores[i] = storage.NewStore(ctx, m.engines[i])
	if err := m.stores[i].Start(m.stoppers[i]); err != nil {
		m.t.Fatal(err)
	}
	// The sender is assumed to still exist.
	m.senders[i].AddStore(m.stores[i])
}

// restart stops and restarts all stores but leaves the engines intact,
// so the stores should contain the same persistent storage as before.
func (m *multiTestContext) restart() {
	for i := range m.stores {
		m.stopStore(i)
	}
	for i := range m.stores {
		m.restartStore(i)
	}
}

// replicateRange replicates the given range onto the given stores.
func (m *multiTestContext) replicateRange(raftID int64, sourceStoreIndex int, dests ...int) {
	rng, err := m.stores[sourceStoreIndex].GetRange(raftID)
	if err != nil {
		m.t.Fatal(err)
	}

	for _, dest := range dests {
		err = rng.ChangeReplicas(proto.ADD_REPLICA,
			proto.Replica{
				NodeID:  m.stores[dest].Ident.NodeID,
				StoreID: m.stores[dest].Ident.StoreID,
			})
		if err != nil {
			m.t.Fatal(err)
		}
	}

	// Wait for the replication to complete on all destination nodes.
	util.SucceedsWithin(m.t, time.Second, func() error {
		for _, dest := range dests {
			// Use LookupRange(keys) instead of GetRange(raftID) to ensure that the
			// snaphost has been transferred and the descriptor initialized.
			if m.stores[dest].LookupRange(rng.Desc().StartKey, rng.Desc().StartKey) == nil {
				return util.Errorf("range not found on store %d", dest)
			}
		}
		return nil
	})
}

func (m *multiTestContext) unreplicateRange(raftID int64, source, dest int) {
	rng, err := m.stores[source].GetRange(raftID)
	if err != nil {
		m.t.Fatal(err)
	}

	err = rng.ChangeReplicas(proto.REMOVE_REPLICA,
		proto.Replica{
			NodeID:  m.idents[dest].NodeID,
			StoreID: m.idents[dest].StoreID,
		})
	if err != nil {
		m.t.Fatal(err)
	}

	// Removing a range doesn't have any immediately-visible side
	// effects, (and the removed node may be stopped) so return as soon
	// as the removal has committed on the leader.
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, raftID int64, storeID proto.StoreID) (*proto.GetRequest, *proto.GetResponse) {
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
func putArgs(key, value []byte, raftID int64, storeID proto.StoreID) (*proto.PutRequest, *proto.PutResponse) {
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
func incrementArgs(key []byte, inc int64, raftID int64, storeID proto.StoreID) (*proto.IncrementRequest, *proto.IncrementResponse) {
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

func internalTruncateLogArgs(index uint64, raftID int64, storeID proto.StoreID) (
	*proto.InternalTruncateLogRequest, *proto.InternalTruncateLogResponse) {
	args := &proto.InternalTruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Index: index,
	}
	reply := &proto.InternalTruncateLogResponse{}
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
	defer leaktest.AfterTest(t)
	store, stopper := createTestStore(t)
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
		var calls []client.Call
		if test.split {
			var err error
			if calls, err = storage.SplitRangeAddressing(left, right); err != nil {
				t.Fatal(err)
			}
		} else {
			var err error
			if calls, err = storage.MergeRangeAddressing(left, right); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.DB().Run(calls...); err != nil {
			t.Fatal(err)
		}
		// Scan meta keys directly from engine.
		kvs, err := engine.MVCCScan(store.Engine(), engine.KeyMetaPrefix, engine.KeyMetaMax, 0, proto.MaxTimestamp, true, nil)
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
	defer leaktest.AfterTest(t)
	left := &proto.RangeDescriptor{StartKey: engine.KeyMin, EndKey: meta1Key(proto.Key("a"))}
	right := &proto.RangeDescriptor{StartKey: meta1Key(proto.Key("a")), EndKey: engine.KeyMax}
	if _, err := storage.SplitRangeAddressing(left, right); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
