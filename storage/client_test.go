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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

var rootTestBaseContext = testutils.NewRootTestBaseContext()

// createTestStore creates a test store using an in-memory
// engine. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) (*storage.Store, *stop.Stopper) {
	return createTestStoreWithEngine(t,
		engine.NewInMem(proto.Attributes{}, 10<<20),
		hlc.NewClock(hlc.NewManualClock(0).UnixNano),
		true, nil)
}

// createTestStoreWithEngine creates a test store using the given engine and clock.
// The caller is responsible for closing the store on exit.
func createTestStoreWithEngine(t *testing.T, eng engine.Engine, clock *hlc.Clock,
	bootstrap bool, context *storage.StoreContext) (*storage.Store, *stop.Stopper) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(rootTestBaseContext, hlc.NewClock(hlc.UnixNano), stopper)
	if context == nil {
		// make a copy
		ctx := storage.TestStoreContext
		context = &ctx
	}
	context.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	lSender := kv.NewLocalSender()
	sender := kv.NewTxnCoordSender(lSender, clock, false, nil, stopper)
	context.Clock = clock
	var err error
	if context.DB, err = client.Open("//root@", client.SenderOpt(sender)); err != nil {
		t.Fatal(err)
	}
	context.Transport = multiraft.NewLocalRPCTransport(stopper)
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(*context, eng, &proto.NodeDescriptor{NodeID: 1})
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
	t            *testing.T
	storeContext *storage.StoreContext
	manualClock  *hlc.ManualClock
	clock        *hlc.Clock
	gossip       *gossip.Gossip
	transport    multiraft.Transport
	db           *client.DB
	feed         *util.Feed
	// The per-store clocks slice normally contains aliases of
	// multiTestContext.clock, but it may be populated before Start() to
	// use distinct clocks per store.
	clocks  []*hlc.Clock
	engines []engine.Engine
	senders []*kv.LocalSender
	stores  []*storage.Store
	idents  []proto.StoreIdent
	// We use multiple stoppers so we can restart different parts of the
	// test individually. clientStopper is for 'db', transportStopper is
	// for 'transport', and the 'stoppers' slice corresponds to the
	// 'stores'.
	clientStopper    *stop.Stopper
	stoppers         []*stop.Stopper
	transportStopper *stop.Stopper
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
		rpcContext := rpc.NewContext(rootTestBaseContext, m.clock, nil)
		m.gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	}
	if m.clientStopper == nil {
		m.clientStopper = stop.NewStopper()
	}
	if m.transport == nil {
		m.transport = multiraft.NewLocalRPCTransport(m.clientStopper)
	}

	// Always create the first sender.
	m.senders = append(m.senders, kv.NewLocalSender())

	if m.db == nil {
		sender := kv.NewTxnCoordSender(m.senders[0], m.clock, false, nil, m.clientStopper)
		var err error
		if m.db, err = client.Open("//root@", client.SenderOpt(sender)); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < numStores; i++ {
		m.addStore()
	}
	if m.transportStopper == nil {
		m.transportStopper = stop.NewStopper()
	}
	m.transportStopper.AddCloser(m.transport)
}

func (m *multiTestContext) Stop() {
	stoppers := append([]*stop.Stopper{m.clientStopper, m.transportStopper}, m.stoppers...)
	// Quiesce all the stoppers so that we can stop all stoppers in unison.
	for _, s := range stoppers {
		s.Quiesce()
	}
	for _, s := range stoppers {
		s.Stop()
	}
	// Remove the extra engine refcounts.
	for _, e := range m.engines {
		e.Close()
	}
}

func (m *multiTestContext) makeContext(i int) storage.StoreContext {
	var ctx storage.StoreContext
	if m.storeContext != nil {
		ctx = *m.storeContext
	} else {
		ctx = storage.TestStoreContext
	}
	ctx.Clock = m.clocks[i]
	ctx.DB = m.db
	ctx.Gossip = m.gossip
	ctx.Transport = m.transport
	ctx.EventFeed = m.feed
	return ctx
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore() {
	idx := len(m.stores)
	var clock *hlc.Clock
	if len(m.clocks) > idx {
		clock = m.clocks[idx]
	} else {
		clock = m.clock
		m.clocks = append(m.clocks, clock)
	}
	var eng engine.Engine
	var needBootstrap bool
	if len(m.engines) > idx {
		eng = m.engines[idx]
	} else {
		eng = engine.NewInMem(proto.Attributes{}, 1<<20)
		m.engines = append(m.engines, eng)
		needBootstrap = true
		// Add an extra refcount to the engine so the underlying rocksdb instances
		// aren't closed when stopping and restarting the stores.
		// These refcounts are removed in Stop().
		if err := eng.Open(); err != nil {
			m.t.Fatal(err)
		}
	}

	stopper := stop.NewStopper()
	ctx := m.makeContext(idx)
	store := storage.NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: proto.NodeID(idx + 1)})
	if needBootstrap {
		err := store.Bootstrap(proto.StoreIdent{
			NodeID:  proto.NodeID(idx + 1),
			StoreID: proto.StoreID(idx + 1),
		}, stopper)
		if err != nil {
			m.t.Fatal(err)
		}

		// Bootstrap the initial range on the first store
		if idx == 0 {
			if err := store.BootstrapRange(); err != nil {
				m.t.Fatal(err)
			}
		}
	}
	if err := store.Start(stopper); err != nil {
		m.t.Fatal(err)
	}
	store.WaitForInit()
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
	m.stoppers[i] = stop.NewStopper()

	ctx := m.makeContext(i)
	m.stores[i] = storage.NewStore(ctx, m.engines[i], &proto.NodeDescriptor{NodeID: proto.NodeID(i + 1)})
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
func (m *multiTestContext) replicateRange(raftID proto.RangeID, sourceStoreIndex int, dests ...int) {
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
			if m.stores[dest].LookupRange(rng.Desc().StartKey, nil) == nil {
				return util.Errorf("range not found on store %d", dest)
			}
		}
		return nil
	})
}

// unreplicateRange removes a replica of the range in the source store
// from the dest store.
func (m *multiTestContext) unreplicateRange(raftID proto.RangeID, source, dest int) {
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
func getArgs(key []byte, raftID proto.RangeID, storeID proto.StoreID) proto.GetRequest {
	return proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, raftID proto.RangeID, storeID proto.StoreID) proto.PutRequest {
	return proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, raftID proto.RangeID, storeID proto.StoreID) proto.IncrementRequest {
	return proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Increment: inc,
	}
}

func internalTruncateLogArgs(index uint64, raftID proto.RangeID, storeID proto.StoreID) proto.InternalTruncateLogRequest {
	return proto.InternalTruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Index: index,
	}
}
