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
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/batchutil"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"

	gogoproto "github.com/gogo/protobuf/proto"
)

// createTestStore creates a test store using an in-memory
// engine. The caller is responsible for stopping the stopper on exit.
func createTestStore(t *testing.T) (*storage.Store, *stop.Stopper) {
	stopper := stop.NewStopper()
	store := createTestStoreWithEngine(t,
		engine.NewInMem(proto.Attributes{}, 10<<20, stopper),
		hlc.NewClock(hlc.NewManualClock(0).UnixNano),
		true, nil, stopper)
	return store, stopper
}

// createTestStoreWithEngine creates a test store using the given engine and clock.
func createTestStoreWithEngine(t *testing.T, eng engine.Engine, clock *hlc.Clock,
	bootstrap bool, sCtx *storage.StoreContext, stopper *stop.Stopper) *storage.Store {
	rpcContext := rpc.NewContext(&base.Context{}, clock, stopper)
	if sCtx == nil {
		// make a copy
		ctx := storage.TestStoreContext
		sCtx = &ctx
	}
	nodeDesc := &proto.NodeDescriptor{NodeID: 1}
	sCtx.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	localSender := kv.NewLocalSender()
	rpcSend := func(_ rpc.Options, _ string, _ []net.Addr,
		getArgs func(addr net.Addr) gogoproto.Message, getReply func() gogoproto.Message,
		_ *rpc.Context) ([]gogoproto.Message, error) {
		args := getArgs(nil /* net.Addr */).(proto.Request)
		reply, err := batchutil.SendWrapped(localSender, args)
		if reply == nil {
			reply = getReply().(proto.Response)
		}
		reply.Header().SetGoError(err)
		return []gogoproto.Message{reply}, nil
	}

	if err := gossipNodeDesc(sCtx.Gossip, nodeDesc.NodeID); err != nil {
		t.Fatal(err)
	}
	distSender := kv.NewDistSender(&kv.DistSenderContext{
		Clock:             clock,
		RPCSend:           rpcSend,     // defined above
		RangeDescriptorDB: localSender, // for descriptor lookup
	}, sCtx.Gossip)

	sender := kv.NewTxnCoordSender(distSender, clock, false, nil, stopper)
	sCtx.Clock = clock
	sCtx.DB = client.NewDB(sender)
	sCtx.Transport = multiraft.NewLocalRPCTransport(stopper)
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(*sCtx, eng, nodeDesc)
	if bootstrap {
		if err := store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
			t.Fatal(err)
		}
	}
	localSender.AddStore(store)
	if bootstrap {
		if err := store.BootstrapRange(sql.GetInitialSystemValues()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	return store
}

type multiStoreSender struct {
	*multiTestContext
}

type multiTestContext struct {
	t            *testing.T
	storeContext *storage.StoreContext
	manualClock  *hlc.ManualClock
	clock        *hlc.Clock
	gossip       *gossip.Gossip
	storePool    *storage.StorePool
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
	clientStopper      *stop.Stopper
	stoppers           []*stop.Stopper
	transportStopper   *stop.Stopper
	engineStoppers     []*stop.Stopper
	timeUntilStoreDead time.Duration
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
		rpcContext := rpc.NewContext(&base.Context{}, m.clock, nil)
		m.gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	}
	if m.clientStopper == nil {
		m.clientStopper = stop.NewStopper()
	}
	if m.transport == nil {
		m.transport = multiraft.NewLocalRPCTransport(m.clientStopper)
	}
	if m.storePool == nil {
		if m.timeUntilStoreDead == 0 {
			m.timeUntilStoreDead = storage.TestTimeUntilStoreDeadOff
		}
		m.storePool = storage.NewStorePool(m.gossip, m.timeUntilStoreDead, m.clientStopper)
	}

	// Always create the first sender.
	m.senders = append(m.senders, kv.NewLocalSender())

	if m.db == nil {
		distSender := kv.NewDistSender(&kv.DistSenderContext{
			Clock:             m.clock,
			RangeDescriptorDB: m.senders[0],
			RPCSend:           m.rpcSend,
		}, m.gossip)
		sender := kv.NewTxnCoordSender(distSender, m.clock, false, nil, m.clientStopper)
		m.db = client.NewDB(sender)
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
	for _, s := range m.engineStoppers {
		s.Stop()
	}
}

// rpcSend implements the client.rpcSender interface. This implementation of "rpcSend" is
// used to multiplex calls between many local senders in a simple way; It sends
// the request to multiTestContext's localSenders specified in addrs. The request is
// sent in order until no error is returned.
func (m *multiTestContext) rpcSend(_ rpc.Options, _ string, addrs []net.Addr,
	getArgs func(addr net.Addr) gogoproto.Message,
	getReply func() gogoproto.Message, _ *rpc.Context) ([]gogoproto.Message, error) {
	var reply proto.Response
	var err error
	for _, addr := range addrs {
		args := getArgs(nil /* net.Addr */).(proto.Request)
		// Node ID is encoded in the address.
		nodeID, stErr := strconv.Atoi(addr.String())
		if stErr != nil {
			m.t.Fatal(stErr)
		}
		reply, err = batchutil.SendWrapped(m.senders[nodeID-1], args)
		if err == nil {
			return []gogoproto.Message{reply}, nil
		}
		if nlErr, ok := err.(*proto.NotLeaderError); ok && nlErr.Leader == nil {
			// localSender has the range, is *not* the Leader, but the
			// Leader is not known; this can happen if the leader is removed
			// from the group. Move the manual clock forward in an attempt to
			// expire the lease.
			m.expireLeaderLeases()
		}
	}
	if err == nil {
		panic("err must not be nil here")
	}
	return nil, err
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
	ctx.StorePool = m.storePool
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
		engineStopper := stop.NewStopper()
		m.engineStoppers = append(m.engineStoppers, engineStopper)
		eng = engine.NewInMem(proto.Attributes{}, 1<<20, engineStopper)
		m.engines = append(m.engines, eng)
		needBootstrap = true
	}

	stopper := stop.NewStopper()
	ctx := m.makeContext(idx)
	nodeID := proto.NodeID(idx + 1)
	store := storage.NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: nodeID})
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
			if err := store.BootstrapRange(sql.GetInitialSystemValues()); err != nil {
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
	if err := gossipNodeDesc(m.gossip, nodeID); err != nil {
		m.t.Fatal(err)
	}
	// Save the store identities for later so we can use them in
	// replication operations even while the store is stopped.
	m.idents = append(m.idents, store.Ident)
	m.stoppers = append(m.stoppers, stopper)
}

// gossipNodeDesc adds the node descriptor to the gossip network.
// Mostly makes sure that we don't see a warning per request.
func gossipNodeDesc(g *gossip.Gossip, nodeID proto.NodeID) error {
	nodeDesc := &proto.NodeDescriptor{
		NodeID: nodeID,
		// Encode the node ID in the address so that rpcSend
		// can figure out where requests must be sent.
		Address: util.MakeUnresolvedAddr("localhost", fmt.Sprintf("%d", nodeID)),
	}
	if err := g.SetNodeDescriptor(nodeDesc); err != nil {
		return err
	}
	return nil
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
func (m *multiTestContext) replicateRange(rangeID proto.RangeID, sourceStoreIndex int, dests ...int) {
	rng, err := m.stores[sourceStoreIndex].GetReplica(rangeID)
	if err != nil {
		m.t.Fatal(err)
	}

	for _, dest := range dests {
		err = rng.ChangeReplicas(proto.ADD_REPLICA,
			proto.ReplicaDescriptor{
				NodeID:  m.stores[dest].Ident.NodeID,
				StoreID: m.stores[dest].Ident.StoreID,
			}, rng.Desc())
		if err != nil {
			m.t.Fatal(err)
		}
	}

	// Wait for the replication to complete on all destination nodes.
	util.SucceedsWithin(m.t, 3*time.Second, func() error {
		for _, dest := range dests {
			// Use LookupRange(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			if m.stores[dest].LookupReplica(rng.Desc().StartKey, nil) == nil {
				return util.Errorf("range not found on store %d", dest)
			}
		}
		return nil
	})
}

// unreplicateRange removes a replica of the range in the source store
// from the dest store.
func (m *multiTestContext) unreplicateRange(rangeID proto.RangeID, source, dest int) {
	rng, err := m.stores[source].GetReplica(rangeID)
	if err != nil {
		m.t.Fatal(err)
	}

	err = rng.ChangeReplicas(proto.REMOVE_REPLICA,
		proto.ReplicaDescriptor{
			NodeID:  m.idents[dest].NodeID,
			StoreID: m.idents[dest].StoreID,
		}, rng.Desc())
	if err != nil {
		m.t.Fatal(err)
	}

	// Removing a range doesn't have any immediately-visible side
	// effects, (and the removed node may be stopped) so return as soon
	// as the removal has committed on the leader.
}

// expireLeaderLeases increments the context's manual clock far enough into the
// future that current leader leases are expired. Useful for tests which modify
// replica sets.
func (m *multiTestContext) expireLeaderLeases() {
	m.manualClock.Increment(int64(storage.DefaultLeaderLeaseDuration) + 1)
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.GetRequest {
	return proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.ReplicaDescriptor{StoreID: storeID},
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.PutRequest {
	return proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.ReplicaDescriptor{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, rangeID proto.RangeID, storeID proto.StoreID) proto.IncrementRequest {
	return proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.ReplicaDescriptor{StoreID: storeID},
		},
		Increment: inc,
	}
}

func truncateLogArgs(index uint64, rangeID proto.RangeID, storeID proto.StoreID) proto.TruncateLogRequest {
	return proto.TruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RangeID: rangeID,
			Replica: proto.ReplicaDescriptor{StoreID: storeID},
		},
		Index: index,
	}
}
