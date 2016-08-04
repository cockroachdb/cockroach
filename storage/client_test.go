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
	"math/rand"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/netutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Check that Stores implements the RangeDescriptorDB interface.
var _ kv.RangeDescriptorDB = &storage.Stores{}

// rg1 returns a wrapping sender that changes all requests to range 0 to
// requests to range 1.
// This function is DEPRECATED. Send your requests to the right range by
// properly initializing the request header.
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
func createTestStore(t testing.TB) (*storage.Store, *stop.Stopper, *hlc.ManualClock) {
	return createTestStoreWithContext(t, storage.TestStoreContext())
}

func createTestStoreWithContext(t testing.TB, sCtx storage.StoreContext) (
	*storage.Store, *stop.Stopper, *hlc.ManualClock) {

	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(123)
	store := createTestStoreWithEngine(t,
		engine.NewInMem(roachpb.Attributes{}, 10<<20, stopper),
		hlc.NewClock(manual.UnixNano),
		true, sCtx, stopper)
	return store, stopper, manual
}

// createTestStoreWithEngine creates a test store using the given engine and clock.
// TestStoreContext() can be used for creating a context suitable for most
// tests.
func createTestStoreWithEngine(t testing.TB, eng engine.Engine, clock *hlc.Clock,
	bootstrap bool, sCtx storage.StoreContext, stopper *stop.Stopper) *storage.Store {
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, clock, stopper)
	nodeDesc := &roachpb.NodeDescriptor{NodeID: 1}
	server := rpc.NewServer(rpcContext) // never started
	sCtx.Gossip = gossip.New(rpcContext, server, nil, stopper, metric.NewRegistry())
	sCtx.Gossip.SetNodeID(nodeDesc.NodeID)
	sCtx.ScanMaxIdleTime = 1 * time.Second
	sCtx.Tracer = tracing.NewTracer()
	stores := storage.NewStores(clock)

	if err := sCtx.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatal(err)
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	distSender := kv.NewDistSender(&kv.DistSenderContext{
		Clock:             clock,
		TransportFactory:  kv.SenderTransportFactory(sCtx.Tracer, stores),
		RPCRetryOptions:   &retryOpts,
		RangeDescriptorDB: stores, // for descriptor lookup
	}, sCtx.Gossip)

	sender := kv.NewTxnCoordSender(distSender, clock, false, tracing.NewTracer(), stopper,
		kv.NewTxnMetrics(metric.NewRegistry()))
	sCtx.Clock = clock
	sCtx.DB = client.NewDB(sender)
	sCtx.StorePool = storage.NewStorePool(
		sCtx.Gossip,
		clock,
		rpcContext,
		/* reservationsEnabled */ true,
		storage.TestTimeUntilStoreDeadOff,
		stopper,
	)
	sCtx.Transport = storage.NewDummyRaftTransport()
	// TODO(bdarnell): arrange to have the transport closed.
	store := storage.NewStore(sCtx, eng, nodeDesc)
	if bootstrap {
		if err := store.Bootstrap(roachpb.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
			t.Fatal(err)
		}
	}
	stores.AddStore(store)
	if bootstrap {
		err := store.BootstrapRange(sqlbase.MakeMetadataSchema().GetInitialValues())
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	return store
}

type multiTestContext struct {
	t            *testing.T
	storeContext *storage.StoreContext
	manualClock  *hlc.ManualClock
	clock        *hlc.Clock
	rpcContext   *rpc.Context
	// This enables the reservation system which by default is disabled.
	reservationsEnabled bool

	nodeIDtoAddr map[roachpb.NodeID]net.Addr

	// The per-store clocks slice normally contains aliases of
	// multiTestContext.clock, but it may be populated before Start() to
	// use distinct clocks per store.
	clocks      []*hlc.Clock
	engines     []engine.Engine
	grpcServers []*grpc.Server
	transports  []*storage.RaftTransport
	distSenders []*kv.DistSender
	dbs         []*client.DB
	gossips     []*gossip.Gossip
	storePools  []*storage.StorePool
	// We use multiple stoppers so we can restart different parts of the
	// test individually. transportStopper is for 'transports', and the
	// 'stoppers' slice corresponds to the 'stores'.
	// TODO(bdarnell): now that there are multiple transports, do we
	// need transportStopper?
	transportStopper   *stop.Stopper
	engineStoppers     []*stop.Stopper
	timeUntilStoreDead time.Duration

	// The fields below may mutate at runtime so the pointers they contain are
	// protected by 'mu'.
	mu       *syncutil.RWMutex
	senders  []*storage.Stores
	stores   []*storage.Store
	stoppers []*stop.Stopper
	idents   []roachpb.StoreIdent
}

func (m *multiTestContext) getNodeIDAddress(nodeID roachpb.NodeID) (net.Addr, error) {
	m.mu.RLock()
	addr, ok := m.nodeIDtoAddr[nodeID]
	m.mu.RUnlock()
	if ok {
		return addr, nil
	}
	return nil, errors.Errorf("unknown peer %d", nodeID)
}

// startMultiTestContext is a convenience function to create, start, and return
// a multiTestContext.
func startMultiTestContext(t *testing.T, numStores int) *multiTestContext {
	m := &multiTestContext{}
	m.Start(t, numStores)
	return m
}

func (m *multiTestContext) Start(t *testing.T, numStores int) {
	{
		// Only the fields we nil out below can be injected into m as it
		// starts up, so fail early if anything else was set (as we'd likely
		// override it and the test wouldn't get what it wanted).
		mCopy := *m
		mCopy.storeContext = nil
		mCopy.clocks = nil
		mCopy.clock = nil
		mCopy.timeUntilStoreDead = 0
		mCopy.reservationsEnabled = false
		var empty multiTestContext
		if !reflect.DeepEqual(empty, mCopy) {
			t.Fatalf("illegal fields set in multiTestContext:\n%s", pretty.Diff(empty, mCopy))
		}
	}
	m.t = t

	m.mu = &syncutil.RWMutex{}
	m.stores = make([]*storage.Store, numStores)
	m.storePools = make([]*storage.StorePool, numStores)
	m.distSenders = make([]*kv.DistSender, numStores)
	m.dbs = make([]*client.DB, numStores)
	m.stoppers = make([]*stop.Stopper, numStores)
	m.senders = make([]*storage.Stores, numStores)
	m.idents = make([]roachpb.StoreIdent, numStores)
	m.grpcServers = make([]*grpc.Server, numStores)
	m.transports = make([]*storage.RaftTransport, numStores)
	m.gossips = make([]*gossip.Gossip, numStores)

	if m.manualClock == nil {
		m.manualClock = hlc.NewManualClock(0)
	}
	if m.clock == nil {
		m.clock = hlc.NewClock(m.manualClock.UnixNano)
	}
	if m.transportStopper == nil {
		m.transportStopper = stop.NewStopper()
	}
	if m.rpcContext == nil {
		m.rpcContext = rpc.NewContext(&base.Context{Insecure: true}, m.clock, m.transportStopper)
	}

	for idx := 0; idx < numStores; idx++ {
		m.addStore(idx)
	}

	// Wait for gossip to startup.
	util.SucceedsSoon(t, func() error {
		for i, g := range m.gossips {
			if _, ok := g.GetSystemConfig(); !ok {
				return errors.Errorf("system config not available at index %d", i)
			}
		}
		return nil
	})
}

func (m *multiTestContext) Stop() {
	done := make(chan struct{})
	go func() {
		m.mu.RLock()
		defer m.mu.RUnlock()

		// Quiesce everyone in parallel (before the transport stopper) to avoid
		// deadlocks.
		var wg sync.WaitGroup
		wg.Add(len(m.stoppers))
		for _, s := range m.stoppers {
			go func(s *stop.Stopper) {
				defer wg.Done()
				// Some Stoppers may be nil if stopStore has been called
				// without restartStore.
				if s != nil {
					// TODO(tschottdorf): seems like it *should* be possible to
					// call .Stop() directly, but then stressing essentially
					// any test (TestRaftAfterRemove is a good example) results
					// in deadlocks where a task can't finish because of
					// getting stuck in addWriteCommand.
					s.Quiesce()
				}
			}(s)
		}
		wg.Wait()

		for _, stopper := range m.stoppers {
			if stopper != nil {
				stopper.Stop()
			}
		}
		m.transportStopper.Stop()

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

type multiTestContextKVTransport struct {
	mtc      *multiTestContext
	ctx      context.Context
	cancel   func()
	replicas kv.ReplicaSlice
	args     roachpb.BatchRequest
}

func (m *multiTestContext) kvTransportFactory(
	_ kv.SendOptions,
	_ *rpc.Context,
	replicas kv.ReplicaSlice,
	args roachpb.BatchRequest,
) (kv.Transport, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &multiTestContextKVTransport{
		mtc:      m,
		ctx:      ctx,
		cancel:   cancel,
		replicas: replicas,
		args:     args,
	}, nil
}

func (t *multiTestContextKVTransport) IsExhausted() bool {
	return len(t.replicas) == 0
}

func (t *multiTestContextKVTransport) SendNext(done chan kv.BatchCall) {
	rep := t.replicas[0]
	t.replicas = t.replicas[1:]

	// Node IDs are assigned in the order the nodes are created by
	// the multi test context, so we can derive the index for stoppers
	// and senders by subtracting 1 from the node ID.
	nodeIndex := int(rep.NodeID) - 1

	// This method crosses store boundaries: it is possible that the
	// destination store is stopped while the source is still running.
	// Run the send in a Task on the destination store to simulate what
	// would happen with real RPCs.
	t.mtc.mu.RLock()
	defer t.mtc.mu.RUnlock()
	if s := t.mtc.stoppers[nodeIndex]; s == nil || s.RunAsyncTask(func() {
		t.mtc.mu.RLock()
		defer t.mtc.mu.RUnlock()
		sender := t.mtc.senders[nodeIndex]
		// Make a copy and clone txn of batch args for sending.
		baCopy := t.args
		if txn := baCopy.Txn; txn != nil {
			txnClone := baCopy.Txn.Clone()
			baCopy.Txn = &txnClone
		}
		br, pErr := sender.Send(t.ctx, baCopy)
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(sender, br))
		}
		br.Error = pErr

		// On certain errors, we must advance our manual clock to ensure
		// that the next attempt has a chance of succeeding.
		switch tErr := pErr.GetDetail().(type) {
		case *roachpb.NotLeaseHolderError:
			if tErr.LeaseHolder == nil {
				// stores has the range, is *not* the lease holder, but the
				// lease holder is not known; this can happen if the lease
				// holder is removed from the group. Move the manual clock
				// forward in an attempt to expire the lease.
				t.mtc.expireLeases()
			} else if t.mtc.stores[tErr.LeaseHolder.NodeID-1] == nil {
				// The lease holder is known but down, so expire its lease.
				t.mtc.expireLeases()
			}
		}
		done <- kv.BatchCall{Reply: br, Err: nil}
	}) != nil {
		done <- kv.BatchCall{Err: roachpb.NewSendError("store is stopped")}
		t.mtc.expireLeases()
	}
}

func (t *multiTestContextKVTransport) Close() {
	t.cancel()
}

// rangeDescByAge implements sort.Interface for RangeDescriptor, sorting by the
// age of the RangeDescriptor. This is intended to find the most recent version
// of the same RangeDescriptor, when multiple versions of it are available.
type rangeDescByAge []*roachpb.RangeDescriptor

func (rd rangeDescByAge) Len() int      { return len(rd) }
func (rd rangeDescByAge) Swap(i, j int) { rd[i], rd[j] = rd[j], rd[i] }
func (rd rangeDescByAge) Less(i, j int) bool {
	// "Less" means "older" according to this sort.
	// A RangeDescriptor version with a higher NextReplicaID is always more recent.
	if rd[i].NextReplicaID != rd[j].NextReplicaID {
		return rd[i].NextReplicaID < rd[j].NextReplicaID
	}
	// If two RangeDescriptor versions have the same NextReplicaID, then the one
	// with the fewest replicas is the newest.
	return len(rd[i].Replicas) > len(rd[j].Replicas)
}

// FirstRange implements the RangeDescriptorDB interface. It returns the range
// descriptor which contains roachpb.KeyMin.
//
// DistSender's implementation of FirstRange() does not work correctly because
// the gossip network used by multiTestContext is only partially operational.
func (m *multiTestContext) FirstRange() (*roachpb.RangeDescriptor, error) {
	var descs []*roachpb.RangeDescriptor
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, str := range m.senders {
		// Find every version of the RangeDescriptor for the first range by
		// querying all stores; it may not be present on all stores, but the
		// current version is guaranteed to be present on one of them.
		if err := str.VisitStores(func(s *storage.Store) error {
			firstRng := s.LookupReplica(roachpb.RKeyMin, nil)
			if firstRng != nil {
				descs = append(descs, firstRng.Desc())
			}
			return nil
		}); err != nil {
			panic(fmt.Sprintf(
				"no error should be possible from this invocation of VisitStores, but found %s", err))
		}
	}
	if len(descs) == 0 {
		// This is a panic because it should currently be impossible in a properly
		// constructed multiTestContext.
		panic("first Range is not present on any store in the multiTestContext.")
	}
	// Sort the RangeDescriptor versions by age and return the most recent
	// version.
	sort.Sort(rangeDescByAge(descs))
	return descs[len(descs)-1], nil
}

// RangeLookup implements the RangeDescriptorDB interface. It looks up the
// descriptors for the given (meta) key.
func (m *multiTestContext) RangeLookup(
	key roachpb.RKey, desc *roachpb.RangeDescriptor, considerIntents, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	// DistSender's RangeLookup function will work correctly, as long as
	// multiTestContext's FirstRange() method returns the correct descriptor for the
	// first range.
	return m.distSenders[0].RangeLookup(key, desc, considerIntents, useReverseScan)
}

func (m *multiTestContext) makeContext(i int) storage.StoreContext {
	var ctx storage.StoreContext
	if m.storeContext != nil {
		ctx = *m.storeContext
	} else {
		ctx = storage.TestStoreContext()
	}
	ctx.Clock = m.clocks[i]
	ctx.Transport = m.transports[i]
	ctx.DB = m.dbs[i]
	ctx.Gossip = m.gossips[i]
	ctx.StorePool = m.storePools[i]
	ctx.TestingKnobs.DisableSplitQueue = true
	return ctx
}

func (m *multiTestContext) populateDB(idx int, stopper *stop.Stopper) {
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	m.distSenders[idx] = kv.NewDistSender(&kv.DistSenderContext{
		Clock:             m.clock,
		RangeDescriptorDB: m,
		TransportFactory:  m.kvTransportFactory,
		RPCRetryOptions:   &retryOpts,
	}, m.gossips[idx])
	sender := kv.NewTxnCoordSender(m.distSenders[idx], m.clock, false, tracing.NewTracer(),
		stopper, kv.NewTxnMetrics(metric.NewRegistry()))
	m.dbs[idx] = client.NewDB(sender)
}

func (m *multiTestContext) populateStorePool(idx int, stopper *stop.Stopper) {
	m.storePools[idx] = storage.NewStorePool(
		m.gossips[idx],
		m.clock,
		m.rpcContext,
		m.reservationsEnabled,
		m.timeUntilStoreDead,
		stopper,
	)
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore(idx int) {
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
	grpcServer := rpc.NewServer(m.rpcContext)
	m.grpcServers[idx] = grpcServer
	m.transports[idx] = storage.NewRaftTransport(m.getNodeIDAddress, grpcServer, m.rpcContext)

	stopper := stop.NewStopper()

	// Give this store all previous stores as gossip bootstraps.
	var resolvers []resolver.Resolver
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, addr := range m.nodeIDtoAddr {
			r, err := resolver.NewResolverFromAddress(addr)
			if err != nil {
				m.t.Fatal(err)
			}
			resolvers = append(resolvers, r)
		}
	}()
	m.gossips[idx] = gossip.New(m.rpcContext, grpcServer, resolvers, m.transportStopper, metric.NewRegistry())
	m.gossips[idx].SetNodeID(roachpb.NodeID(idx + 1))
	if m.timeUntilStoreDead == 0 {
		m.timeUntilStoreDead = storage.TestTimeUntilStoreDeadOff
	}

	m.populateStorePool(idx, stopper)
	m.populateDB(idx, stopper)

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
			err := store.BootstrapRange(sqlbase.MakeMetadataSchema().GetInitialValues())
			if err != nil {
				m.t.Fatal(err)
			}
		}
	}

	if m.nodeIDtoAddr == nil {
		m.nodeIDtoAddr = make(map[roachpb.NodeID]net.Addr)
	}
	ln, err := netutil.ListenAndServeGRPC(m.transportStopper, grpcServer, util.TestAddr)
	if err != nil {
		m.t.Fatal(err)
	}
	m.mu.Lock()
	_, ok := m.nodeIDtoAddr[nodeID]
	if !ok {
		m.nodeIDtoAddr[nodeID] = ln.Addr()
	}
	m.mu.Unlock()
	if ok {
		m.t.Fatalf("node %d already listening", nodeID)
	}
	m.gossips[idx].Start(ln.Addr())

	stores := storage.NewStores(clock)
	stores.AddStore(store)
	roachpb.RegisterInternalStoresServer(grpcServer, storage.MakeInternalStoresServer(m.nodeDesc(nodeID), stores))

	// Add newly created objects to the multiTestContext's collections.
	// (these must be populated before the store is started so that
	// FirstRange() can find the sender)
	m.mu.Lock()
	m.stores[idx] = store
	m.stoppers[idx] = stopper
	sender := storage.NewStores(clock)
	sender.AddStore(store)
	m.senders[idx] = sender
	// Save the store identities for later so we can use them in
	// replication operations even while the store is stopped.
	m.idents[idx] = store.Ident
	m.mu.Unlock()
	if err := store.Start(stopper); err != nil {
		m.t.Fatal(err)
	}
	if err := m.gossipNodeDesc(m.gossips[idx], nodeID); err != nil {
		m.t.Fatal(err)
	}
	store.WaitForInit()
}

func (m *multiTestContext) nodeDesc(nodeID roachpb.NodeID) *roachpb.NodeDescriptor {
	addr := m.nodeIDtoAddr[nodeID]
	return &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
	}
}

// gossipNodeDesc adds the node descriptor to the gossip network.
// Mostly makes sure that we don't see a warning per request.
func (m *multiTestContext) gossipNodeDesc(g *gossip.Gossip, nodeID roachpb.NodeID) error {
	return g.SetNodeDescriptor(m.nodeDesc(nodeID))
}

// StopStore stops a store but leaves the engine intact.
// All stopped stores must be restarted before multiTestContext.Stop is called.
func (m *multiTestContext) stopStore(i int) {
	// Attempting to acquire a write lock here could lead to a situation in
	// which an outstanding Raft proposal would never return due to address
	// resolution calling back into the `multiTestContext` and attempting to
	// acquire a read lock while this write lock is block on another read lock
	// held by `SendNext` which in turn is waiting on that Raft proposal:
	//
	// SendNext[hold RLock] -> Raft[want RLock]
	//             ʌ               /
	//               \            v
	//             stopStore[want Lock]
	//
	// Instead, we only acquire a read lock to fetch the stopper, and are
	// careful not to hold any locks while stopping the stopper.
	m.mu.RLock()
	stopper := m.stoppers[i]
	m.mu.RUnlock()

	stopper.Stop()

	m.mu.Lock()
	m.stoppers[i] = nil
	m.senders[i].RemoveStore(m.stores[i])
	m.stores[i] = nil
	m.mu.Unlock()
}

// restartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) restartStore(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stoppers[i] = stop.NewStopper()
	m.populateDB(i, m.stoppers[i])
	m.populateStorePool(i, m.stoppers[i])

	ctx := m.makeContext(i)
	m.stores[i] = storage.NewStore(ctx, m.engines[i], &roachpb.NodeDescriptor{NodeID: roachpb.NodeID(i + 1)})
	if err := m.stores[i].Start(m.stoppers[i]); err != nil {
		m.t.Fatal(err)
	}
	// The sender is assumed to still exist.
	m.senders[i].AddStore(m.stores[i])
}

// findStartKeyLocked returns the start key of the given range.
func (m *multiTestContext) findStartKeyLocked(rangeID roachpb.RangeID) roachpb.RKey {
	// We can use the first store that returns results because the start
	// key never changes.
	for _, s := range m.stores {
		rep, err := s.GetReplica(rangeID)
		if err == nil && rep.IsInitialized() {
			return rep.Desc().StartKey
		}
	}
	m.t.Fatalf("couldn't find range %s on any store", rangeID)
	return nil // unreached, but the compiler can't tell.
}

// findMemberStoreLocked finds a non-stopped Store which is a member
// of the given range.
func (m *multiTestContext) findMemberStoreLocked(desc roachpb.RangeDescriptor) *storage.Store {
	for _, s := range m.stores {
		if s == nil {
			// Store is stopped.
			continue
		}
		for _, r := range desc.Replicas {
			if s.StoreID() == r.StoreID {
				return s
			}
		}
	}
	m.t.Fatalf("couldn't find a live member of %s", desc)
	return nil // unreached, but the compiler can't tell.
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
func (m *multiTestContext) replicateRange(rangeID roachpb.RangeID, dests ...int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	startKey := m.findStartKeyLocked(rangeID)

	for _, dest := range dests {
		// Perform a consistent read to get the updated range descriptor (as opposed
		// to just going to one of the stores), to make sure we have the effects of
		// the previous ChangeReplicas call. By the time ChangeReplicas returns the
		// raft leader is guaranteed to have the updated version, but followers are
		// not.
		var desc roachpb.RangeDescriptor
		if err := m.dbs[0].GetProto(keys.RangeDescriptorKey(startKey), &desc); err != nil {
			m.t.Fatal(err)
		}

		rep, err := m.findMemberStoreLocked(desc).GetReplica(rangeID)
		if err != nil {
			m.t.Fatal(err)
		}

		if dest >= len(m.stores) {
			m.t.Fatalf("store index %d out of range; there's only %d of them", dest, len(m.stores))
		}

		if err := rep.ChangeReplicas(
			context.Background(),
			roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  m.stores[dest].Ident.NodeID,
				StoreID: m.stores[dest].Ident.StoreID,
			},
			&desc,
		); err != nil {
			m.t.Fatal(err)
		}
	}

	// Wait for the replication to complete on all destination nodes.
	util.SucceedsSoon(m.t, func() error {
		for _, dest := range dests {
			// Use LookupRange(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			if store := m.stores[dest]; store.LookupReplica(startKey, nil) == nil {
				return errors.Errorf("range %d not found on store %+v", rangeID, store.Ident)
			}
		}
		return nil
	})
}

// unreplicateRange removes a replica of the range from the dest store.
func (m *multiTestContext) unreplicateRange(rangeID roachpb.RangeID, dest int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	startKey := m.findStartKeyLocked(rangeID)

	var desc roachpb.RangeDescriptor
	if err := m.dbs[0].GetProto(keys.RangeDescriptorKey(startKey), &desc); err != nil {
		m.t.Fatal(err)
	}

	rep, err := m.findMemberStoreLocked(desc).GetReplica(rangeID)
	if err != nil {
		m.t.Fatal(err)
	}

	if err := rep.ChangeReplicas(
		context.Background(),
		roachpb.REMOVE_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  m.idents[dest].NodeID,
			StoreID: m.idents[dest].StoreID,
		},
		&desc,
	); err != nil {
		m.t.Fatal(err)
	}
}

// readIntFromEngines reads the current integer value at the given key
// from all configured engines, filling in zeros when the value is not
// found. Returns a slice of the same length as mtc.engines.
func (m *multiTestContext) readIntFromEngines(key roachpb.Key) []int64 {
	results := make([]int64, len(m.engines))
	for i, eng := range m.engines {
		val, _, err := engine.MVCCGet(context.Background(), eng, key, m.clock.Now(), true, nil)
		if err != nil {
			log.Errorf(context.TODO(), "engine %d: error reading from key %s: %s", i, key, err)
		} else if val == nil {
			log.Errorf(context.TODO(), "engine %d: missing key %s", i, key)
		} else {
			results[i], err = val.GetInt()
			if err != nil {
				log.Errorf(context.TODO(), "engine %d: error decoding %s from key %s: %s", i, val, key, err)
			}
		}
	}
	return results
}

// waitForValues waits up to the given duration for the integer values
// at the given key to match the expected slice (across all engines).
// Fails the test if they do not match.
func (m *multiTestContext) waitForValues(key roachpb.Key, expected []int64) {
	util.SucceedsSoonDepth(1, m.t, func() error {
		actual := m.readIntFromEngines(key)
		if !reflect.DeepEqual(expected, actual) {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})
}

// expireLeases increments the context's manual clock far enough into the
// future that current range leases are expired. Useful for tests which modify
// replica sets.
func (m *multiTestContext) expireLeases() {
	m.manualClock.Increment(storage.LeaseExpiration(m.stores[0], m.clock))
}

// getRaftLeader returns the replica that is the current raft leader for the
// specified rangeID.
func (m *multiTestContext) getRaftLeader(rangeID roachpb.RangeID) *storage.Replica {
	var raftLeaderRepl *storage.Replica
	util.SucceedsSoonDepth(1, m.t, func() error {
		m.mu.RLock()
		defer m.mu.RUnlock()
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
			return errors.Errorf("could not find a raft leader for range %s", rangeID)
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

func TestSortRangeDescByAge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var replicaDescs []roachpb.ReplicaDescriptor
	var rangeDescs []*roachpb.RangeDescriptor

	expectedReplicas := 0
	nextReplID := 1

	// Cut a new range version with the current replica set.
	newRangeVersion := func(marker string) {
		currentRepls := append([]roachpb.ReplicaDescriptor(nil), replicaDescs...)
		rangeDescs = append(rangeDescs, &roachpb.RangeDescriptor{
			RangeID:       roachpb.RangeID(1),
			Replicas:      currentRepls,
			NextReplicaID: roachpb.ReplicaID(nextReplID),
			EndKey:        roachpb.RKey(marker),
		})
	}

	// function to add a replica.
	addReplica := func(marker string) {
		replicaDescs = append(replicaDescs, roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(nextReplID),
			StoreID:   roachpb.StoreID(nextReplID),
			ReplicaID: roachpb.ReplicaID(nextReplID),
		})
		nextReplID++
		newRangeVersion(marker)
		expectedReplicas++
	}

	// function to remove a replica.
	removeReplica := func(marker string) {
		remove := rand.Intn(len(replicaDescs))
		replicaDescs = append(replicaDescs[:remove], replicaDescs[remove+1:]...)
		newRangeVersion(marker)
		expectedReplicas--
	}

	for i := 0; i < 10; i++ {
		addReplica(fmt.Sprint("added", i))
	}
	for i := 0; i < 3; i++ {
		removeReplica(fmt.Sprint("removed", i))
	}
	addReplica("final-add")

	// randomize array
	sortedRangeDescs := make([]*roachpb.RangeDescriptor, len(rangeDescs))
	for i, r := range rand.Perm(len(rangeDescs)) {
		sortedRangeDescs[i] = rangeDescs[r]
	}
	// Sort array by age.
	sort.Sort(rangeDescByAge(sortedRangeDescs))
	// Make sure both arrays are equal.
	if !reflect.DeepEqual(sortedRangeDescs, rangeDescs) {
		t.Fatalf("RangeDescriptor sort by age was not correct. Diff: %s", pretty.Diff(sortedRangeDescs, rangeDescs))
	}
}

func verifyRangeStats(eng engine.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats) error {
	var ms enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), eng, rangeID, &ms); err != nil {
		return err
	}
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount = 0, 0
	if expMS != ms {
		return fmt.Errorf("expected stats %+v; got %+v", expMS, ms)
	}
	return nil
}

func verifyRecomputedStats(
	eng engine.Reader, d *roachpb.RangeDescriptor, expMS enginepb.MVCCStats, nowNanos int64,
) error {
	if ms, err := storage.ComputeStatsForRange(d, eng, nowNanos); err != nil {
		return err
	} else if expMS != ms {
		return fmt.Errorf("expected range's stats to agree with recomputation: got\n%+v\nrecomputed\n%+v", expMS, ms)
	}
	return nil
}
