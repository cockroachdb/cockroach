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
// permissions and limitations under the License.
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
	"math"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/gogo/protobuf/proto"
)

const replicationTimeout = 3 * time.Second

// Check that Stores implements the RangeDescriptorDB interface.
var _ kv.RangeDescriptorDB = &storage.Stores{}

func rg1(s *storage.Store) client.Sender {
	return client.Wrap(s, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID == 0 {
			ba.RangeID = 1
		}
		return ba
	})
}

// createTestStore creates a test store using an in-memory
// engine. The caller is responsible for stopping the stopper on exit.
func createTestStore(t *testing.T) (*storage.Store, *stop.Stopper) {
	stopper := stop.NewStopper()
	store := createTestStoreWithEngine(t,
		engine.NewInMem(roachpb.Attributes{}, 10<<20, stopper),
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
	nodeDesc := &roachpb.NodeDescriptor{NodeID: 1}
	sCtx.Gossip = gossip.New(rpcContext, gossip.TestBootstrap)
	sCtx.Gossip.SetNodeID(nodeDesc.NodeID)
	localSender := storage.NewStores()
	rpcSend := func(_ rpc.Options, _ string, _ []net.Addr,
		getArgs func(addr net.Addr) proto.Message, _ func() proto.Message,
		_ *rpc.Context) ([]proto.Message, error) {
		ba := getArgs(nil /* net.Addr */).(*roachpb.BatchRequest)
		br, pErr := localSender.Send(context.Background(), *ba)
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		br.Error = pErr
		return []proto.Message{br}, nil
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
	sCtx.Transport = storage.NewLocalRPCTransport(stopper)
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(*sCtx, eng, nodeDesc)
	if bootstrap {
		if err := store.Bootstrap(roachpb.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
			t.Fatal(err)
		}
	}
	localSender.AddStore(store)
	if bootstrap {
		if err := store.BootstrapRange(sql.MakeMetadataSchema().GetInitialValues()); err != nil {
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
	transport    storage.RaftTransport
	distSender   *kv.DistSender
	db           *client.DB
	feed         *util.Feed

	// The per-store clocks slice normally contains aliases of
	// multiTestContext.clock, but it may be populated before Start() to
	// use distinct clocks per store.
	clocks  []*hlc.Clock
	engines []engine.Engine
	senders []*storage.Stores
	idents  []roachpb.StoreIdent
	// We use multiple stoppers so we can restart different parts of the
	// test individually. clientStopper is for 'db', transportStopper is
	// for 'transport', and the 'stoppers' slice corresponds to the
	// 'stores'.
	// TODO(bdarnell): scannerStopper is a hack. The scanner is the one
	// source of asynchronous operations that the test has no direct
	// control over, and it is prone to an infinite retry loop during
	// shutdown. When we have fixed our retry configurations (#2500),
	// we should be able to remove scannerStopper.
	scannerStopper     *stop.Stopper
	clientStopper      *stop.Stopper
	transportStopper   *stop.Stopper
	engineStoppers     []*stop.Stopper
	timeUntilStoreDead time.Duration

	// 'stores' and 'stoppers' may change at runtime so the pointers
	// they contain are protected by 'mu'.
	mu       sync.RWMutex
	stores   []*storage.Store
	stoppers []*stop.Stopper
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
		m.gossip = gossip.New(rpcContext, gossip.TestBootstrap)
		m.gossip.SetNodeID(math.MaxInt32)
	}
	if m.scannerStopper == nil {
		m.scannerStopper = stop.NewStopper()
	}
	if m.clientStopper == nil {
		m.clientStopper = stop.NewStopper()
	}
	if m.transport == nil {
		m.transport = storage.NewLocalRPCTransport(m.clientStopper)
	}
	if m.storePool == nil {
		if m.timeUntilStoreDead == 0 {
			m.timeUntilStoreDead = storage.TestTimeUntilStoreDeadOff
		}
		m.storePool = storage.NewStorePool(m.gossip, m.clock, m.timeUntilStoreDead, m.clientStopper)
	}

	// Always create the first sender.
	m.senders = append(m.senders, storage.NewStores())

	if m.db == nil {
		m.distSender = kv.NewDistSender(&kv.DistSenderContext{
			Clock:             m.clock,
			RangeDescriptorDB: m.senders[0],
			RPCSend:           m.rpcSend,
		}, m.gossip)
		sender := kv.NewTxnCoordSender(m.distSender, m.clock, false, nil, m.clientStopper)
		m.db = client.NewDB(sender)
	}

	for i := 0; i < numStores; i++ {
		m.addStore()
	}
	if m.transportStopper == nil {
		m.transportStopper = stop.NewStopper()
	}
	m.transportStopper.AddCloser(m.transport)

	// Wait for gossip to startup.
	util.SucceedsWithin(t, 100*time.Millisecond, func() error {
		if m.gossip.GetSystemConfig() == nil {
			return util.Errorf("system config not available")
		}
		return nil
	})
}

func (m *multiTestContext) Stop() {
	done := make(chan struct{})
	go func() {
		m.mu.RLock()
		defer m.mu.RUnlock()
		stoppers := append([]*stop.Stopper{m.scannerStopper, m.clientStopper, m.transportStopper},
			m.stoppers...)
		// Quiesce all the stoppers so that we can stop all stoppers in unison.
		for _, s := range stoppers {
			// Stoppers may be nil if stopStore has been called without restartStore.
			if s != nil {
				s.Quiesce()
			}
		}
		for _, s := range stoppers {
			if s != nil {
				s.Stop()
			}
		}
		for _, s := range m.engineStoppers {
			s.Stop()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		// If we've already failed, just attach another failure to the
		// test, since a timeout during shutdown after a failure is
		// probably not interesting, and will prevent the display of any
		// pending t.Error. If we're timing out but the test was otherwise
		// a success, panic so we see stack traces from other goroutines.
		if m.t.Failed() {
			m.t.Error("timed out during shutdown")
		} else {
			panic("timed out during shutdown")
		}
	}
}

// rpcSend implements the client.rpcSender interface. This implementation of "rpcSend" is
// used to multiplex calls between many local senders in a simple way; It sends
// the request to multiTestContext's localSenders specified in addrs. The request is
// sent in order until no error is returned.
func (m *multiTestContext) rpcSend(_ rpc.Options, _ string, addrs []net.Addr,
	getArgs func(addr net.Addr) proto.Message,
	getReply func() proto.Message, _ *rpc.Context) ([]proto.Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fail := func(pErr *roachpb.Error) ([]proto.Message, error) {
		br := &roachpb.BatchResponse{}
		br.Error = pErr
		return []proto.Message{br}, nil
	}
	var br *roachpb.BatchResponse
	var pErr *roachpb.Error
	for _, addr := range addrs {
		ba := *getArgs(nil /* net.Addr */).(*roachpb.BatchRequest)
		// Node ID is encoded in the address.
		nodeID, stErr := strconv.Atoi(addr.String())
		if stErr != nil {
			m.t.Fatal(stErr)
		}
		nodeIndex := nodeID - 1
		// The rpcSend method crosses store boundaries: it is possible that the
		// destination store is stopped while the source is still running.
		// Run the send in a Task on the destination store to simulate what
		// would happen with real RPCs.
		if s := m.stoppers[nodeIndex]; s == nil || !s.RunTask(func() {
			br, pErr = m.senders[nodeIndex].Send(context.Background(), ba)
		}) {
			pErr = &roachpb.Error{}
			pErr.SetGoError(rpc.NewSendError("store is stopped", false))
			m.expireLeaderLeases()
			continue
		}
		if pErr == nil {
			return []proto.Message{br}, nil
		}
		switch tErr := pErr.GoError().(type) {
		case *roachpb.RangeKeyMismatchError:
		case *roachpb.NotLeaderError:
			if tErr.Leader == nil {
				// localSender has the range, is *not* the Leader, but the
				// Leader is not known; this can happen if the leader is removed
				// from the group. Move the manual clock forward in an attempt to
				// expire the lease.
				m.expireLeaderLeases()
			} else if m.stores[tErr.Leader.NodeID-1] == nil {
				// The leader is known but down, so expire its lease.
				m.expireLeaderLeases()
			}
		default:
			if testutils.IsError(tErr, `store \d+ not found`) {
				break
			}
			// If any store fails with an error that doesn't indicate we simply
			// sent to the wrong store, it must have been the correct one and
			// the command failed.
			return fail(pErr)
		}
	}
	if pErr == nil {
		panic("err must not be nil here")
	}
	return fail(pErr)
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
	ctx.ScannerStopper = m.scannerStopper
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
		eng = engine.NewInMem(roachpb.Attributes{}, 1<<20, engineStopper)
		m.engines = append(m.engines, eng)
		needBootstrap = true
	}

	stopper := stop.NewStopper()
	ctx := m.makeContext(idx)
	nodeID := roachpb.NodeID(idx + 1)
	store := storage.NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: nodeID})
	if needBootstrap {
		err := store.Bootstrap(roachpb.StoreIdent{
			NodeID:  roachpb.NodeID(idx + 1),
			StoreID: roachpb.StoreID(idx + 1),
		}, stopper)
		if err != nil {
			m.t.Fatal(err)
		}

		// Bootstrap the initial range on the first store
		if idx == 0 {
			if err := store.BootstrapRange(sql.MakeMetadataSchema().GetInitialValues()); err != nil {
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
		m.senders = append(m.senders, storage.NewStores())
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
func gossipNodeDesc(g *gossip.Gossip, nodeID roachpb.NodeID) error {
	nodeDesc := &roachpb.NodeDescriptor{
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.senders[i].RemoveStore(m.stores[i])
	m.stoppers[i].Stop()
	m.stoppers[i] = nil
	m.stores[i] = nil
}

// restartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) restartStore(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stoppers[i] = stop.NewStopper()

	ctx := m.makeContext(i)
	m.stores[i] = storage.NewStore(ctx, m.engines[i], &roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i + 1)})
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
func (m *multiTestContext) replicateRange(rangeID roachpb.RangeID, sourceStoreIndex int, dests ...int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rng, err := m.stores[sourceStoreIndex].GetReplica(rangeID)
	if err != nil {
		m.t.Fatal(err)
	}

	for _, dest := range dests {
		err = rng.ChangeReplicas(roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  m.stores[dest].Ident.NodeID,
				StoreID: m.stores[dest].Ident.StoreID,
			}, rng.Desc())
		if err != nil {
			m.t.Fatal(err)
		}
	}

	// Wait for the replication to complete on all destination nodes.
	util.SucceedsWithin(m.t, replicationTimeout, func() error {
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
func (m *multiTestContext) unreplicateRange(rangeID roachpb.RangeID, source, dest int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rng, err := m.stores[source].GetReplica(rangeID)
	if err != nil {
		m.t.Fatal(err)
	}

	err = rng.ChangeReplicas(roachpb.REMOVE_REPLICA,
		roachpb.ReplicaDescriptor{
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

// readIntFromEngines reads the current integer value at the given key
// from all configured engines, filling in zeros when the value is not
// found. Returns a slice of the same length as mtc.engines.
func (m *multiTestContext) readIntFromEngines(key roachpb.Key) []int64 {
	results := make([]int64, len(m.engines))
	for i, eng := range m.engines {
		val, _, err := engine.MVCCGet(eng, key, m.clock.Now(), true, nil)
		if err != nil {
			log.Errorf("engine %d: error reading from key %s: %s", i, key, err)
		} else if val == nil {
			log.Errorf("engine %d: missing key %s", i, key)
		} else {
			results[i], err = val.GetInt()
			if err != nil {
				log.Errorf("engine %d: error decoding %s from key %s: %s", i, val, key, err)
			}
		}
	}
	return results
}

// waitForValues waits up to the given duration for the integer values
// at the given key to match the expected slice (across all engines).
// Fails the test if they do not match.
func (m *multiTestContext) waitForValues(key roachpb.Key, d time.Duration, expected []int64) {
	util.SucceedsWithinDepth(1, m.t, d, func() error {
		actual := m.readIntFromEngines(key)
		if !reflect.DeepEqual(expected, actual) {
			return util.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}

// expireLeaderLeases increments the context's manual clock far enough into the
// future that current leader leases are expired. Useful for tests which modify
// replica sets.
func (m *multiTestContext) expireLeaderLeases() {
	m.manualClock.Increment(int64(storage.DefaultLeaderLeaseDuration) + 1)
}

// getRaftLeader returns the replica that is the current raft leader for the
// specified rangeID.
func (m *multiTestContext) getRaftLeader(rangeID roachpb.RangeID, d time.Duration) *storage.Replica {
	var raftLeaderRepl *storage.Replica
	util.SucceedsWithinDepth(1, m.t, d, func() error {
		var latestTerm uint64
		for _, store := range m.stores {
			raftStatus := store.RaftStatus(rangeID)
			if raftStatus == nil {
				// Replica does not exist on this store or there is no raft
				// status yet.
				continue
			}
			if raftStatus.Term > latestTerm {
				// If we find any newer term, it means any previous election is
				// invalid.
				raftLeaderRepl = nil
				latestTerm = raftStatus.Term
				if raftStatus.RaftState == raft.StateLeader {
					var err error
					raftLeaderRepl, err = store.GetReplica(rangeID)
					if err != nil {
						return err
					}
				}
			}
		}
		if latestTerm == 0 || raftLeaderRepl == nil {
			return util.Errorf("could not find a raft leader for range %s", rangeID)
		}
		return nil
	})
	return raftLeaderRepl
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key roachpb.Key) roachpb.GetRequest {
	return roachpb.GetRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key roachpb.Key, value []byte) roachpb.PutRequest {
	return roachpb.PutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key roachpb.Key, inc int64) roachpb.IncrementRequest {
	return roachpb.IncrementRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Increment: inc,
	}
}

func truncateLogArgs(index uint64) roachpb.TruncateLogRequest {
	return roachpb.TruncateLogRequest{
		Span:  roachpb.Span{},
		Index: index,
	}
}
