// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	aload "github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
)

var testIdent = roachpb.StoreIdent{
	ClusterID: uuid.MakeV4(),
	NodeID:    1,
	StoreID:   1,
}

func (s *Store) TestSender() kv.Sender {
	return kv.Wrap(s, func(ba *kvpb.BatchRequest) *kvpb.BatchRequest {
		if ba.RangeID != 0 {
			return ba
		}

		// If the client hasn't set ba.Range, we do it a favor and figure out the
		// range to which the request needs to go.
		//
		// NOTE: We don't use keys.Range(ba.Requests) here because that does some
		// validation on the batch, and some tests using this sender don't like
		// that.
		key, err := keys.Addr(ba.Requests[0].GetInner().Header().Key)
		if err != nil {
			log.Fatalf(context.Background(), "%v", err)
		}

		ba.RangeID = roachpb.RangeID(1)
		if repl := s.LookupReplica(key); repl != nil {
			ba.RangeID = repl.RangeID

			// Attempt to assign a Replica descriptor to the batch if
			// necessary, but don't throw an error if this fails.
			if ba.Replica == (roachpb.ReplicaDescriptor{}) {
				if desc, err := repl.GetReplicaDescriptor(); err == nil {
					ba.Replica = desc
				}
			}
		}
		return ba
	})
}

// testStoreOpts affords control over aspects of store creation.
type testStoreOpts struct {
	// If createSystemRanges is not set, the store will have a single range. If
	// set, the store will have all the system ranges that are generally created
	// for a cluster at boostrap.
	createSystemRanges bool
	bootstrapVersion   roachpb.Version // defaults to TestingClusterVersion
}

func (opts *testStoreOpts) splits() (_kvs []roachpb.KeyValue, _splits []roachpb.RKey) {
	kvs, splits := bootstrap.MakeMetadataSchema(
		keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	).GetInitialValues()
	if !opts.createSystemRanges {
		return kvs, nil
	}
	splits = append(config.StaticSplits(), splits...)
	sort.Slice(splits, func(i, j int) bool {
		return splits[i].Less(splits[j])
	})
	return kvs, splits
}

type mockNodeStore struct {
	desc *roachpb.NodeDescriptor
}

func (m mockNodeStore) GetNodeDescriptor(id roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	return m.desc, nil
}

func (m mockNodeStore) GetNodeDescriptorCount() int {
	return 1
}

func (m mockNodeStore) GetStoreDescriptor(id roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	return nil, errorutil.NewStoreNotFoundError(id)
}

type dummyFirstRangeProvider struct {
	store *Store
}

func (d dummyFirstRangeProvider) GetFirstRangeDescriptor() (*roachpb.RangeDescriptor, error) {
	return d.store.GetReplicaIfExists(1).Desc(), nil
}

func (d dummyFirstRangeProvider) OnFirstRangeChanged(f func(*roachpb.RangeDescriptor)) {}

// createTestStoreWithoutStart creates a test store using an in-memory
// engine without starting the store. It returns the store, the store
// clock's manual unix nanos time and a stopper. The caller is
// responsible for stopping the stopper upon completion.
// Some fields of cfg are populated by this function.
func createTestStoreWithoutStart(
	ctx context.Context, t testing.TB, stopper *stop.Stopper, opts testStoreOpts, cfg *StoreConfig,
) *Store {
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)

	rpcOpts := rpc.DefaultContextOptions()
	rpcOpts.Insecure = true
	rpcOpts.Clock = cfg.Clock.WallClock()
	rpcOpts.ToleratedOffset = cfg.Clock.ToleratedOffset()
	rpcOpts.Stopper = stopper
	rpcOpts.Settings = cfg.Settings
	rpcOpts.TenantRPCAuthorizer = tenantcapabilitiesauthorizer.NewAllowEverythingAuthorizer()
	rpcContext := rpc.NewContext(ctx, rpcOpts)
	stopper.SetTracer(cfg.AmbientCtx.Tracer)
	server, err := rpc.NewServer(rpcContext) // never started
	require.NoError(t, err)

	// Some tests inject their own Gossip and StorePool, via
	// createTestAllocatorWithKnobs, at the time of writing
	// TestChooseLeaseToTransfer and TestNoLeaseTransferToBehindReplicas. This is
	// generally considered bad and should eventually be refactored away.
	if cfg.Gossip == nil {
		cfg.Gossip = gossip.NewTest(1, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	}
	if cfg.StorePool == nil {
		cfg.StorePool = NewTestStorePool(*cfg)
	}
	if cfg.RPCContext == nil {
		cfg.RPCContext = rpcContext
	}
	// Many tests using this test harness (as opposed to higher-level
	// ones like multiTestContext or TestServer) want to micro-manage
	// replicas and the background queues just get in the way. The
	// scanner doesn't run frequently enough to expose races reliably,
	// so just disable the scanner for all tests that use this function
	// instead of figuring out exactly which tests need it.
	cfg.TestingKnobs.DisableScanner = true
	// The scanner affects background operations; we must also disable the split
	// and merge queues separately to cover event-driven splits and merges.
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.DisableMergeQueue = true
	// When using the span configs infrastructure, we initialize dependencies
	// (spanconfig.KVSubscriber) outside of pkg/kv/kvserver due to circular
	// dependency reasons. Tests using this harness can probably be refactored
	// to do the same (with some effort). That's unlikely to happen soon, so
	// let's continue to use the system config span.
	cfg.SystemConfigProvider = config.NewConstantSystemConfigProvider(
		config.NewSystemConfig(zonepb.DefaultZoneConfigRef()),
	)
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)
	require.Nil(t, cfg.Transport)

	require.NotNil(t, cfg.Gossip) // was set above already
	// Even though testContext is fundamentally a single-store test, some tests
	// will try config changes, etc, so we will see some use of the transport
	// and it's important that this doesn't cause crashes. Just set up the
	// "real thing" since it's straightforward enough.
	cfg.NodeDialer = nodedialer.New(rpcContext, gossip.AddressResolver(cfg.Gossip))
	cfg.Transport = NewRaftTransport(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Tracer(),
		cfg.NodeDialer,
		server,
		stopper,
		kvflowdispatch.NewDummyDispatch(),
		NoopStoresFlowControlIntegration{},
		NoopRaftTransportDisconnectListener{},
		nil, /* knobs */
	)

	stores := NewStores(cfg.AmbientCtx, cfg.Clock)
	nodeDesc := &roachpb.NodeDescriptor{NodeID: 1}

	rangeProv := &dummyFirstRangeProvider{}
	var storeSender struct{ kv.Sender }
	ds := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx:         cfg.AmbientCtx,
		Settings:           cfg.Settings,
		Clock:              cfg.Clock,
		NodeDescs:          mockNodeStore{desc: nodeDesc},
		RPCContext:         rpcContext,
		RPCRetryOptions:    &retry.Options{},
		NodeDialer:         cfg.NodeDialer,
		FirstRangeProvider: rangeProv,
		TestingKnobs: kvcoord.ClientTestingKnobs{
			TransportFactory: kvcoord.SenderTransportFactory(cfg.AmbientCtx.Tracer, &storeSender),
		},
	})

	txnCoordSenderFactory := kvcoord.NewTxnCoordSenderFactory(kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx:        cfg.AmbientCtx,
		Settings:          cfg.Settings,
		Clock:             cfg.Clock,
		Stopper:           stopper,
		HeartbeatInterval: -1,
	}, ds)
	require.Nil(t, cfg.DB)
	cfg.DB = kv.NewDB(cfg.AmbientCtx, txnCoordSenderFactory, cfg.Clock, stopper)
	store := NewStore(ctx, *cfg, eng, nodeDesc)
	storeSender.Sender = store

	storeIdent := roachpb.StoreIdent{NodeID: 1, StoreID: 1}
	cv := clusterversion.TestingClusterVersion
	if opts.bootstrapVersion != (roachpb.Version{}) {
		cv = clusterversion.ClusterVersion{Version: opts.bootstrapVersion}
	}
	require.NoError(t, kvstorage.WriteClusterVersion(ctx, eng, cv))
	if err := kvstorage.InitEngine(
		ctx, eng, storeIdent,
	); err != nil {
		t.Fatal(err)
	}
	rangeProv.store = store
	store.Ident = &storeIdent // would usually be set during Store.Start, but can't call that yet
	stores.AddStore(store)

	kvs, splits := opts.splits()
	if err := WriteInitialClusterData(
		ctx, eng, kvs /* initialValues */, cv.Version,
		1 /* numStores */, splits, cfg.Clock.PhysicalNow(), cfg.TestingKnobs,
	); err != nil {
		t.Fatal(err)
	}
	return store
}

func createTestStore(
	ctx context.Context, t testing.TB, opts testStoreOpts, stopper *stop.Stopper,
) (*Store, *timeutil.ManualTime) {
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	store := createTestStoreWithConfig(ctx, t, stopper, opts, &cfg)
	return store, manual
}

// createTestStore creates a test store using an in-memory
// engine. It returns the store, the store clock's manual unix nanos time
// and a stopper. The caller is responsible for stopping the stopper
// upon completion.
func createTestStoreWithConfig(
	ctx context.Context, t testing.TB, stopper *stop.Stopper, opts testStoreOpts, cfg *StoreConfig,
) *Store {
	store := createTestStoreWithoutStart(ctx, t, stopper, opts, cfg)
	// Put an empty system config into gossip.
	//
	// TODO(ajwerner): Remove this in 22.2. It's possible it can be removed
	// already.
	if err := store.Gossip().AddInfoProto(gossip.KeyDeprecatedSystemConfig,
		&config.SystemConfigEntries{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(ctx, stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()
	return store
}

// TestIterateIDPrefixKeys lays down a number of tombstones (at keys.RangeTombstoneKey) interspersed
// with other irrelevant keys (both chosen randomly). It then verifies that IterateIDPrefixKeys
// correctly returns only the relevant keys and values.
func TestIterateIDPrefixKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	seed := randutil.NewPseudoSeed()
	// const seed = -1666367124291055473
	t.Logf("seed is %d", seed)
	rng := rand.New(rand.NewSource(seed))

	ops := []func(rangeID roachpb.RangeID) roachpb.Key{
		keys.RaftHardStateKey, // unreplicated; sorts after tombstone
		// Replicated key-anchored local key (i.e. not one we should care about).
		// Will be written at zero timestamp, but that's ok.
		func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RangeDescriptorKey([]byte(fmt.Sprintf("fakerange%d", rangeID)))
		},
		func(rangeID roachpb.RangeID) roachpb.Key {
			return roachpb.Key(fmt.Sprintf("fakeuserkey%d", rangeID))
		},
	}

	const rangeCount = 10
	rangeIDFn := func() roachpb.RangeID {
		return 1 + roachpb.RangeID(rng.Intn(10*rangeCount)) // spread rangeIDs out
	}

	// Write a number of keys that should be irrelevant to the iteration in this test.
	for i := 0; i < rangeCount; i++ {
		rangeID := rangeIDFn()

		// Grab between one and all ops, randomly.
		for _, opIdx := range rng.Perm(len(ops))[:rng.Intn(1+len(ops))] {
			key := ops[opIdx](rangeID)
			t.Logf("writing op=%d rangeID=%d", opIdx, rangeID)
			if err := storage.MVCCPut(
				ctx,
				eng,
				key,
				hlc.Timestamp{},
				roachpb.MakeValueFromString("fake value for "+key.String()),
				storage.MVCCWriteOptions{},
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	type seenT struct {
		rangeID   roachpb.RangeID
		tombstone kvserverpb.RangeTombstone
	}

	// Next, write the keys we're planning to see again.
	var wanted []seenT
	{
		used := make(map[roachpb.RangeID]struct{})
		for {
			rangeID := rangeIDFn()
			if _, ok := used[rangeID]; ok {
				// We already wrote this key, so roll the dice again.
				continue
			}

			tombstone := kvserverpb.RangeTombstone{
				NextReplicaID: roachpb.ReplicaID(rng.Int31n(100)),
			}

			used[rangeID] = struct{}{}
			wanted = append(wanted, seenT{rangeID: rangeID, tombstone: tombstone})

			t.Logf("writing tombstone at rangeID=%d", rangeID)
			if err := storage.MVCCPutProto(
				ctx, eng, keys.RangeTombstoneKey(rangeID), hlc.Timestamp{}, &tombstone, storage.MVCCWriteOptions{},
			); err != nil {
				t.Fatal(err)
			}

			if len(wanted) >= rangeCount {
				break
			}
		}
	}

	sort.Slice(wanted, func(i, j int) bool {
		return wanted[i].rangeID < wanted[j].rangeID
	})

	var seen []seenT
	var tombstone kvserverpb.RangeTombstone

	handleTombstone := func(rangeID roachpb.RangeID) error {
		seen = append(seen, seenT{rangeID: rangeID, tombstone: tombstone})
		return nil
	}

	if err := kvstorage.IterateIDPrefixKeys(ctx, eng, keys.RangeTombstoneKey, &tombstone, handleTombstone); err != nil {
		t.Fatal(err)
	}
	placeholder := seenT{
		rangeID: roachpb.RangeID(9999),
	}

	if len(wanted) != len(seen) {
		t.Errorf("wanted %d results, got %d", len(wanted), len(seen))
	}

	for len(wanted) < len(seen) {
		wanted = append(wanted, placeholder)
	}
	for len(seen) < len(wanted) {
		seen = append(seen, placeholder)
	}

	if diff := pretty.Diff(wanted, seen); len(diff) > 0 {
		pretty.Ldiff(t, wanted, seen)
		t.Fatal("diff(wanted, seen) is nonempty")
	}
}

// TestStoreConfigSetDefaults checks that StoreConfig.SetDefaults() sets proper
// defaults based on numStores.
func TestStoreConfigSetDefaultsNumStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := map[string]struct {
		conc        int
		defaultConc int
		numStores   int
		expectConc  int
	}{
		"zero default is retained":         {defaultConc: 0, numStores: 4, expectConc: 0},
		"negative default is retained":     {defaultConc: -1, numStores: 4, expectConc: -1},
		"zero stores retains default":      {defaultConc: 4, expectConc: 4},
		"explicit value not distributed":   {conc: 4, numStores: 2, expectConc: 4},
		"explicit value overrides default": {conc: 4, defaultConc: 16, numStores: 2, expectConc: 4},
		"default value is distributed":     {defaultConc: 16, numStores: 4, expectConc: 4},
		"default value uses ceil division": {defaultConc: 16, numStores: 5, expectConc: 4},
		"all stores have at least 1":       {defaultConc: 4, numStores: 10, expectConc: 1},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			defer func(original int) {
				defaultRaftSchedulerConcurrency = original // restore global default
			}(defaultRaftSchedulerConcurrency)
			defaultRaftSchedulerConcurrency = tc.defaultConc
			cfg := StoreConfig{RaftSchedulerConcurrency: tc.conc}
			cfg.SetDefaults(tc.numStores)
			require.Equal(t, tc.expectConc, cfg.RaftSchedulerConcurrency)
		})
	}
}

// TestStoreInitAndBootstrap verifies store initialization and bootstrap.
func TestStoreInitAndBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	cfg := TestStoreConfig(nil)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{}, &cfg)
	defer stopper.Stop(ctx)

	if _, err := kvstorage.ReadStoreIdent(ctx, store.TODOEngine()); err != nil {
		t.Fatalf("unable to read store ident: %+v", err)
	}

	store.VisitReplicas(func(repl *Replica) (more bool) {
		// Stats should agree with recomputation. Hold raftMu to avoid
		// background activity from creating discrepancies between engine
		// and in-mem stats.
		repl.raftMu.Lock()
		defer repl.raftMu.Unlock()
		memMS := repl.GetMVCCStats()
		// Stats should agree with a recomputation.
		now := store.Clock().Now()
		diskMS, err := rditer.ComputeStatsForRange(repl.Desc(), store.TODOEngine(), now.WallTime)
		require.NoError(t, err)
		memMS.AgeTo(diskMS.LastUpdateNanos)
		require.Equal(t, memMS, diskMS)
		return true // more
	})
}

// TestInitializeEngineErrors verifies bootstrap failure if engine
// is not empty.
func TestInitializeEngineErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	// Put some random garbage into the engine.
	require.NoError(t, eng.PutUnversioned(roachpb.Key("foo"), []byte("bar")))

	cfg := TestStoreConfig(nil)
	cfg.Transport = NewDummyRaftTransport(cfg.Settings, cfg.AmbientCtx.Tracer)
	store := NewStore(ctx, cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})

	// Can't init as haven't bootstrapped.
	err := store.Start(ctx, stopper)
	require.ErrorIs(t, err, &kvstorage.NotBootstrappedError{})

	// Bootstrap should fail on non-empty engine.
	err = kvstorage.InitEngine(ctx, eng, testIdent)
	require.ErrorContains(t, err, "cannot be bootstrapped")

	// Bootstrap should fail on MVCC range key in engine.
	require.NoError(t, eng.PutMVCCRangeKey(storage.MVCCRangeKey{
		StartKey:  roachpb.Key("a"),
		EndKey:    roachpb.Key("b"),
		Timestamp: hlc.MinTimestamp,
	}, storage.MVCCValue{}))

	err = kvstorage.InitEngine(ctx, eng, testIdent)
	require.ErrorContains(t, err, "found mvcc range key")
}

// create a Replica and add it to the store. Note that replicas
// created in this way do not have their raft groups fully initialized
// so most KV operations will not work on them. This function is
// deprecated; new tests should create replicas by splitting from a
// properly-bootstrapped initial range.
func createReplica(s *Store, rangeID roachpb.RangeID, start, end roachpb.RKey) *Replica {
	ctx := context.Background()
	desc := &roachpb.RangeDescriptor{
		RangeID:  rangeID,
		StartKey: start,
		EndKey:   end,
		InternalReplicas: []roachpb.ReplicaDescriptor{{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}},
		NextReplicaID: 2,
	}
	const replicaID = 1
	if err := stateloader.WriteInitialRangeState(
		ctx, s.TODOEngine(), *desc, replicaID, clusterversion.TestingClusterVersion.Version,
	); err != nil {
		panic(err)
	}
	r, err := loadInitializedReplicaForTesting(ctx, s, desc, replicaID)
	if err != nil {
		panic(err)
	}
	return r
}

func TestStoreAddRemoveRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)
	if _, err := store.GetReplica(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}
	// Create a new range (id=2).
	repl2 := createReplica(store, 2, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplica(repl2); err != nil {
		t.Fatal(err)
	}
	// Try to add the same range twice
	err = store.AddReplica(repl2)
	if err == nil {
		t.Fatal("expected error re-adding same range")
	}
	// Try to remove range 1 again.
	if err := store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Fatalf("didn't expect error re-removing same range: %v", err)
	}
	// Try to add a range with previously-used (but now removed) ID.
	repl2Dup := createReplica(store, 1, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplica(repl2Dup); err == nil {
		t.Fatal("expected error inserting a duplicated range")
	}
	// Add another range with different key range and then test lookup.
	repl3 := createReplica(store, 3, roachpb.RKey("c"), roachpb.RKey("d"))
	if err := store.AddReplica(repl3); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		key    roachpb.RKey
		expRng *Replica
	}{
		{roachpb.RKey("a"), repl2},
		{roachpb.RKey("a\xff\xff"), repl2},
		{roachpb.RKey("c"), repl3},
		{roachpb.RKey("c\xff\xff"), repl3},
		{roachpb.RKey("x60\xff\xff"), nil},
		{roachpb.RKey("x60\xff\xff"), nil},
		{roachpb.RKey("d"), nil},
	}

	for i, test := range testCases {
		if r := store.LookupReplica(test.key); r != test.expRng {
			t.Errorf("%d: expected range %v; got %v", i, test.expRng, r)
		}
	}
}

// TestReplicasByKey tests that operations that depend on the
// store.replicasByKey map function correctly when the underlying replicas'
// start and end keys are manipulated in place. This mutation happens when a
// snapshot is applied that advances a replica past a split.
func TestReplicasByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	// Shrink the main replica.
	rep, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	rep.mu.Lock()
	desc := *rep.mu.state.Desc // shallow copy to replace desc wholesale
	desc.EndKey = roachpb.RKey("e")
	rep.mu.state.Desc = &desc
	rep.mu.Unlock()

	// Ensure that this shrinkage is recognized by future additions to replicasByKey.
	reps := []*struct {
		replica            *Replica
		id                 int
		start, end         roachpb.RKey
		expectedErrorOnAdd string
	}{
		// [a,c) is contained in [KeyMin, e)
		{nil, 2, roachpb.RKey("a"), roachpb.RKey("c"), "overlaps with"},
		// [c,f) partially overlaps with [KeyMin, e)
		{nil, 3, roachpb.RKey("c"), roachpb.RKey("f"), "overlaps with"},
		// [e, f) is disjoint from [KeyMin, e)
		{nil, 4, roachpb.RKey("e"), roachpb.RKey("f"), ""},
	}

	for i, desc := range reps {
		desc.replica = createReplica(store, roachpb.RangeID(desc.id), desc.start, desc.end)
		err := store.AddReplica(desc.replica)
		if !testutils.IsError(err, desc.expectedErrorOnAdd) {
			t.Fatalf("adding replica %d: expected err %q, but encountered %v", i, desc.expectedErrorOnAdd, err)
		}
	}
}

func TestStoreRemoveReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// Can't remove Replica with DestroyData false because this requires the destroyStatus
	// to already have been set by the caller (but we didn't).
	require.ErrorContains(t, store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: false,
	}), `replica not marked as destroyed`)

	// Remove the Replica twice, as this should be idempotent.
	// NB: we rely on this idempotency today (as @tbg found out when he accidentally
	// removed it).
	for i := 0; i < 2; i++ {
		require.NoError(t, store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
			DestroyData: true,
		}), "%d", i)
	}

	// However, if we have DestroyData=false, caller is expected to be the unique first "destroyer"
	// of the Replica.
	require.ErrorContains(t, store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: false,
	}), `does not exist`)

	// Verify that removal of a replica marks it as destroyed so that future raft
	// commands on the Replica will silently be dropped.
	err = repl1.withRaftGroup(func(r *raft.RawNode) (bool, error) {
		return true, errors.Errorf("unexpectedly created a raft group")
	})
	require.Equal(t, errRemoved, err)

	repl1.mu.RLock()
	expErr := repl1.mu.destroyStatus.err
	repl1.mu.RUnlock()

	if expErr == nil {
		t.Fatal("replica was not marked as destroyed")
	}

	if _, err = repl1.checkExecutionCanProceedBeforeStorageSnapshot(ctx, &kvpb.BatchRequest{}, nil /* g */); !errors.Is(err, expErr) {
		t.Fatalf("expected error %s, but got %v", expErr, err)
	}
}

func TestStoreReplicaVisitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	// Remove range 1.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}

	// Add 10 new ranges.
	const newCount = 10
	for i := 0; i < newCount; i++ {
		repl := createReplica(store, roachpb.RangeID(i+1), roachpb.RKey(fmt.Sprintf("a%02d", i)), roachpb.RKey(fmt.Sprintf("a%02d", i+1)))
		if err := store.AddReplica(repl); err != nil {
			t.Fatal(err)
		}
	}

	// Verify two passes of the visit, the second one in-order.
	visitor := newStoreReplicaVisitor(store)
	exp := make(map[roachpb.RangeID]struct{})
	for i := 0; i < newCount; i++ {
		exp[roachpb.RangeID(i+1)] = struct{}{}
	}

	for pass := 0; pass < 2; pass++ {
		if ec := visitor.EstimatedCount(); ec != 10 {
			t.Fatalf("expected 10 remaining; got %d", ec)
		}
		i := 1
		seen := make(map[roachpb.RangeID]struct{})

		// Ensure that our next pass is done in-order.
		if pass == 1 {
			_ = visitor.InOrder()
		}
		var lastRangeID roachpb.RangeID
		visitor.Visit(func(repl *Replica) bool {
			if pass == 1 {
				if repl.RangeID <= lastRangeID {
					t.Fatalf("on second pass, expect ranges to be visited in ascending range ID order; %d !> %d", repl.RangeID, lastRangeID)
				}
				lastRangeID = repl.RangeID
			}
			_, ok := seen[repl.RangeID]
			if ok {
				t.Fatalf("already saw %d", repl.RangeID)
			}

			seen[repl.RangeID] = struct{}{}
			if ec := visitor.EstimatedCount(); ec != 10-i {
				t.Fatalf(
					"expected %d remaining; got %d after seeing %+v",
					10-i, ec, seen,
				)
			}
			i++
			return true
		})
		if ec := visitor.EstimatedCount(); ec != 10 {
			t.Fatalf("expected 10 remaining; got %d", ec)
		}
		if !reflect.DeepEqual(exp, seen) {
			t.Fatalf("got %v, expected %v", seen, exp)
		}
	}
}

func TestMarkReplicaInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	// Clobber the existing range so we can test overlaps that aren't KeyMin or KeyMax.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}

	repl := createReplica(store, roachpb.RangeID(2), roachpb.RKey("a"), roachpb.RKey("c"))
	if err := store.AddReplica(repl); err != nil {
		t.Fatal(err)
	}

	newRangeID := roachpb.RangeID(3)
	const replicaID = 1
	require.NoError(t,
		logstore.NewStateLoader(newRangeID).SetRaftReplicaID(ctx, store.TODOEngine(), replicaID))

	r, err := newUninitializedReplica(store, newRangeID, replicaID)
	require.NoError(t, err)

	store.mu.Lock()
	defer store.mu.Unlock()

	expectedResult := "attempted to process uninitialized replica"
	ctx = r.AnnotateCtx(ctx)
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err := store.markReplicaInitializedLockedReplLocked(ctx, r); !testutils.IsError(err, expectedResult) {
			t.Errorf("expected markReplicaInitializedLocked with uninitialized replica to fail, got %v", err)
		}
	}()

	// Initialize the range with start and end keys.
	desc := protoutil.Clone(r.Desc()).(*roachpb.RangeDescriptor)
	desc.StartKey = roachpb.RKey("b")
	desc.EndKey = roachpb.RKey("d")
	desc.InternalReplicas = []roachpb.ReplicaDescriptor{{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}}
	desc.NextReplicaID = 2
	r.setDescRaftMuLocked(ctx, desc)
	expectedResult = "not in uninitReplicas"
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err := store.markReplicaInitializedLockedReplLocked(ctx, r); !testutils.IsError(err, expectedResult) {
			t.Errorf("expected markReplicaInitializedLocked on a replica that's not in the uninit map to fail, got %v", err)
		}
	}()

	store.mu.uninitReplicas[newRangeID] = r
	require.NoError(t, store.addToReplicasByRangeIDLocked(r))

	expectedResult = "overlaps with"
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if err := store.markReplicaInitializedLockedReplLocked(ctx, r); !testutils.IsError(err, expectedResult) {
			t.Errorf("expected markReplicaInitializedLocked with overlapping keys to fail, got %v", err)
		}
	}()
}

// TestStoreSend verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)
	gArgs := getArgs([]byte("a"))

	// Try a successful get request.
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &gArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("aaa"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestStoreObservedTimestamp verifies that execution of a transactional
// command on a Store always returns a timestamp observation, either per the
// error's or the response's transaction, as well as an originating NodeID.
func TestStoreObservedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	badKey := []byte("a")
	goodKey := []byte("b")

	testCases := []struct {
		key   roachpb.Key
		check func(int64, roachpb.NodeID, kvpb.Response, *kvpb.Error)
	}{
		{badKey,
			func(wallNanos int64, nodeID roachpb.NodeID, _ kvpb.Response, pErr *kvpb.Error) {
				if pErr == nil {
					t.Fatal("expected an error")
				}
				txn := pErr.GetTxn()
				if txn == nil || txn.ID == (uuid.UUID{}) {
					t.Fatalf("expected nontrivial transaction in %s", pErr)
				}
				if ts, _ := txn.GetObservedTimestamp(nodeID); ts.WallTime != wallNanos {
					t.Fatalf("unexpected observed timestamps, expected %d->%d but got map %+v",
						nodeID, wallNanos, txn.ObservedTimestamps)
				}
				if pErr.OriginNode != nodeID {
					t.Fatalf("unexpected OriginNode %d, expected %d",
						pErr.OriginNode, nodeID)
				}

			}},
		{goodKey,
			func(wallNanos int64, nodeID roachpb.NodeID, pReply kvpb.Response, pErr *kvpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				txn := pReply.Header().Txn
				if txn == nil || txn.ID == (uuid.UUID{}) {
					t.Fatal("expected transactional response")
				}
				obs, _ := txn.GetObservedTimestamp(nodeID)
				if act, exp := obs.WallTime, wallNanos; exp != act {
					t.Fatalf("unexpected observed wall time: %d, wanted %d", act, exp)
				}
			}},
	}

	for _, test := range testCases {
		func() {
			manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
			cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
			cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
				func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
					if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
						return kvpb.NewError(errors.Errorf("boom"))
					}
					return nil
				}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
			txn := newTransaction("test", test.key, 1, store.cfg.Clock)
			txn.GlobalUncertaintyLimit = hlc.MaxTimestamp
			h := kvpb.Header{Txn: txn}
			pArgs := putArgs(test.key, []byte("value"))
			assignSeqNumsForReqs(txn, &pArgs)
			pReply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs)
			test.check(manual.Now().UnixNano(), store.NodeID(), pReply, pErr)
		}()
	}
}

// TestStoreAnnotateNow verifies that the Store sets Now on the batch responses.
func TestStoreAnnotateNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	badKey := []byte("a")
	goodKey := []byte("b")
	desc := roachpb.ReplicaDescriptor{
		NodeID: 5,
		// not relevant
		StoreID:   1,
		ReplicaID: 2,
	}

	testCases := []struct {
		key   roachpb.Key
		check func(*kvpb.BatchResponse, *kvpb.Error)
	}{
		{badKey,
			func(_ *kvpb.BatchResponse, pErr *kvpb.Error) {
				if pErr == nil {
					t.Fatal("expected an error")
				}
				if pErr.Now.IsEmpty() {
					t.Fatal("timestamp not annotated on error")
				}
			}},
		{goodKey,
			func(pReply *kvpb.BatchResponse, pErr *kvpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				if pReply.Now.IsEmpty() {
					t.Fatal("timestamp not annotated on batch response")
				}
			}},
	}

	testutils.RunTrueAndFalse(t, "useTxn", func(t *testing.T, useTxn bool) {
		for _, test := range testCases {
			t.Run(test.key.String(), func(t *testing.T) {
				cfg := TestStoreConfig(nil)
				cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
					func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
						if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
							return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
						}
						return nil
					}
				stopper := stop.NewStopper()
				defer stopper.Stop(ctx)
				store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
				var txn *roachpb.Transaction
				pArgs := putArgs(test.key, []byte("value"))
				if useTxn {
					txn = newTransaction("test", test.key, 1, store.cfg.Clock)
					txn.GlobalUncertaintyLimit = hlc.MaxTimestamp
					assignSeqNumsForReqs(txn, &pArgs)
				}
				ba := &kvpb.BatchRequest{
					Header: kvpb.Header{
						Txn:     txn,
						Replica: desc,
					},
				}
				ba.Add(&pArgs)

				test.check(store.TestSender().Send(ctx, ba))
			})
		}
	})
}

// TestStoreVerifyKeys checks that key length is enforced and
// that end keys must sort >= start.
func TestStoreVerifyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)
	// Try a start key == KeyMax.
	gArgs := getArgs(roachpb.KeyMax)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("expected error for start key == KeyMax: %v", pErr)
	}
	// Try a get with an end key specified (get requires only a start key and should fail).
	gArgs.EndKey = roachpb.KeyMax
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("unexpected error for end key specified on a non-range-based operation: %v", pErr)
	}
	// Try a scan with end key < start key.
	sArgs := scanArgs([]byte("b"), []byte("a"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for end key < start: %v", pErr)
	}
	// Try a scan with start key == end key.
	sArgs.Key = []byte("a")
	sArgs.EndKey = sArgs.Key
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for start == end key: %v", pErr)
	}
	// Try a scan with range-local start key, but "regular" end key.
	sArgs.Key = keys.MakeRangeKey([]byte("test"), []byte("sffx"), nil)
	sArgs.EndKey = []byte("z")
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), sArgs); !testutils.IsPError(pErr, "range-local") {
		t.Fatalf("unexpected error for local start, non-local end key: %v", pErr)
	}

	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	meta2KeyMax := testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax)
	pArgs := putArgs(meta2KeyMax, []byte("value"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", pErr)
	}
	// Try to put a range descriptor record for a start key which is
	// maximum length.
	key := append([]byte{}, roachpb.RKeyMax...)
	key[len(key)-1] = 0x01
	pArgs = putArgs(keys.RangeDescriptorKey(key), []byte("value"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to range descriptor for KeyMax value: %s", pErr)
	}
	// Try a put to txn record for a meta2 key (note that this doesn't
	// actually happen in practice, as txn records are not put directly,
	// but are instead manipulated only through txn methods).
	pArgs = putArgs(keys.TransactionKey(meta2KeyMax, uuid.MakeV4()), []byte("value"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to txn meta2 value: %s", pErr)
	}
}

// TestStoreSendUpdateTime verifies that the node clock is updated.
func TestStoreSendUpdateTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)
	args := getArgs([]byte("a"))
	now := hlc.ClockTimestamp(store.cfg.Clock.Now().Add(store.cfg.Clock.MaxOffset().Nanoseconds(), 0))
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Now: now}, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	ts := store.cfg.Clock.Now()
	if ts.WallTime != now.WallTime || ts.Logical <= now.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", now, ts)
	}
}

// TestStoreSendWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreSendWithZeroTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)
	args := getArgs([]byte("a"))

	ba := &kvpb.BatchRequest{}
	ba.Add(&args)
	br, pErr := store.TestSender().Send(ctx, ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if br.Timestamp.WallTime != store.cfg.Clock.Now().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.cfg.Clock.Now(), br.Timestamp)
	}
}

// TestStoreSendWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails.
func TestStoreSendWithClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClock(timeutil.NewManualTime(timeutil.Unix(0, 123)),
		time.Millisecond /* maxOffset */, time.Millisecond /* toleratedOffset */))
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	args := getArgs([]byte("a"))
	// Set args timestamp to exceed max offset.
	now := hlc.ClockTimestamp(store.cfg.Clock.Now().Add(store.cfg.Clock.MaxOffset().Nanoseconds()+1, 0))
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Now: now}, &args)
	if !testutils.IsPError(pErr, "remote wall time is too far ahead") {
		t.Errorf("unexpected error: %v", pErr)
	}
}

// splitTestRange splits a range. This does *not* fully emulate a real split
// and should not be used in new tests. Tests that need splits should either live in
// client_split_test.go and use AdminSplit instead of this function or use the
// TestServerInterface.
// See #702
// TODO(bdarnell): convert tests that use this function to use AdminSplit instead.
func splitTestRange(store *Store, splitKey roachpb.RKey, t *testing.T) *Replica {
	ctx := context.Background()
	repl := store.LookupReplica(splitKey)
	_, err := repl.AdminSplit(ctx, kvpb.AdminSplitRequest{
		RequestHeader:  kvpb.RequestHeader{Key: splitKey.AsRawKey()},
		SplitKey:       splitKey.AsRawKey(),
		ExpirationTime: store.Clock().Now().Add(24*time.Hour.Nanoseconds(), 0),
	}, "splitTestRange")
	require.NoError(t, err.GoError())
	return store.LookupReplica(splitKey)
}

// TestStoreSendOutOfRange passes a key not contained
// within the range's key range and not present in any
// adjacent ranges on a store.
func TestStoreSendOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	// key 'a' isn't in Range 1000 and Range 1000 doesn't exist
	// adjacent on this store
	args := getArgs([]byte("a"))
	if _, err := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
		RangeID: 1000, // doesn't exist
	}, &args); err == nil {
		t.Error("expected key to be out of range")
	}

	splitKey := roachpb.RKey("b")
	repl2 := splitTestRange(store, splitKey, t)

	// Range 2 is from "b" to KeyMax, so reading "a"-"c" from range 2 should
	// fail because it's before the start of the range and straddles multiple ranges
	// so it cannot be server side retried.
	scanArgs := scanArgs([]byte("a"), []byte("c"))
	if _, err := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
		RangeID: repl2.RangeID,
	}, scanArgs); err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRangeIDAllocation verifies that  range IDs are
// allocated in successive blocks.
func TestStoreRangeIDAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	// Range IDs should be allocated from ID 2 (first allocated range)
	// to rangeIDAllocCount * 3 + 1.
	for i := 0; i < rangeIDAllocCount*3; i++ {
		rangeID, err := store.AllocateRangeID(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2+i, rangeID)
	}
}

// TestStoreReplicasByKey verifies we can lookup ranges by key using
// the sorted replicasByKey slice.
func TestStoreReplicasByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)

	r0 := store.LookupReplica(roachpb.RKeyMin)
	r1 := splitTestRange(store, roachpb.RKey("A"), t)
	r2 := splitTestRange(store, roachpb.RKey("C"), t)
	r3 := splitTestRange(store, roachpb.RKey("X"), t)
	r4 := splitTestRange(store, roachpb.RKey("ZZ"), t)

	if r := store.LookupReplica(roachpb.RKey("0")); r != r0 {
		t.Errorf("mismatched replica %s != %s", r, r0)
	}
	if r := store.LookupReplica(roachpb.RKey("B")); r != r1 {
		t.Errorf("mismatched replica %s != %s", r, r1)
	}
	if r := store.LookupReplica(roachpb.RKey("C")); r != r2 {
		t.Errorf("mismatched replica %s != %s", r, r2)
	}
	if r := store.LookupReplica(roachpb.RKey("M")); r != r2 {
		t.Errorf("mismatched replica %s != %s", r, r2)
	}
	if r := store.LookupReplica(roachpb.RKey("X")); r != r3 {
		t.Errorf("mismatched replica %s != %s", r, r3)
	}
	if r := store.LookupReplica(roachpb.RKey("Z")); r != r3 {
		t.Errorf("mismatched replica %s != %s", r, r3)
	}
	if r := store.LookupReplica(roachpb.RKey("ZZ")); r != r4 {
		t.Errorf("mismatched replica %s != %s", r, r4)
	}
	if r := store.LookupReplica(roachpb.RKey("\xff\x00")); r != r4 {
		t.Errorf("mismatched replica %s != %s", r, r4)
	}
	if store.LookupReplica(roachpb.RKeyMax) != nil {
		t.Errorf("expected roachpb.KeyMax to not have an associated replica")
	}
}

// TestStoreResolveWriteIntent adds a write intent and then verifies
// that a put returns success and aborts intent's txn in the event the
// pushee has lower priority. Otherwise, verifies that the put blocks
// until the original txn is ended.
func TestStoreResolveWriteIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClock(
		manual, 1000*time.Nanosecond /* maxOffset */, 1000*time.Nanosecond /* toleratedOffset */))
	cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			pr, ok := filterArgs.Req.(*kvpb.PushTxnRequest)
			if !ok || pr.PusherTxn.Name != "test" {
				return nil
			}
			if exp, act := manual.Now().UnixNano(), pr.PushTo.WallTime; exp > act {
				return kvpb.NewError(fmt.Errorf("expected PushTo > WallTime, but got %d < %d:\n%+v", act, exp, pr))
			}
			return nil
		}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	for i, resolvable := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, store.cfg.Clock)
		pushee := newTransaction("test", key, 1, store.cfg.Clock)
		if resolvable {
			pushee.Priority = enginepb.MinTxnPriority
			pusher.Priority = enginepb.MaxTxnPriority // Pusher will win.
		} else {
			pushee.Priority = enginepb.MaxTxnPriority
			pusher.Priority = enginepb.MinTxnPriority // Pusher will lose.
		}

		// First lay down intent using the pushee's txn.
		pArgs := putArgs(key, []byte("value"))
		h := kvpb.Header{Txn: pushee}
		assignSeqNumsForReqs(pushee, &pArgs)
		if _, err := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs); err != nil {
			t.Fatal(err)
		}

		manual.Advance(100)
		// Now, try a put using the pusher's txn.
		h.Txn = pusher
		resultCh := make(chan *kvpb.Error, 1)
		go func() {
			_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &pArgs)
			resultCh <- pErr
		}()

		if resolvable {
			if pErr := <-resultCh; pErr != nil {
				t.Fatalf("expected intent resolved; got unexpected error: %s", pErr)
			}
			txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
			var txn roachpb.Transaction
			if ok, err := storage.MVCCGetProto(
				ctx, store.TODOEngine(), txnKey, hlc.Timestamp{}, &txn, storage.MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatalf("expected transaction record; got %s", txn)
			}
		} else {
			select {
			case pErr := <-resultCh:
				t.Fatalf("did not expect put to complete with lower priority: %s", pErr)
			case <-time.After(10 * time.Millisecond):
				// Send an end transaction to allow the original push to complete.
				etArgs, h := endTxnArgs(pushee, true)
				assignSeqNumsForReqs(pushee, &etArgs)
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &etArgs); pErr != nil {
					t.Fatal(pErr)
				}
				if pErr := <-resultCh; pErr != nil {
					t.Fatalf("expected successful put after pushee txn ended; got %s", pErr)
				}
			}
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	key := roachpb.Key("a")
	pusher := newTransaction("test", key, 1, store.cfg.Clock)
	pushee := newTransaction("test", key, 1, store.cfg.Clock)
	pushee.Priority = enginepb.MinTxnPriority
	pusher.Priority = enginepb.MaxTxnPriority // Pusher will win.

	// First lay down intent using the pushee's txn.
	args := incrementArgs(key, 1)
	h := kvpb.Header{Txn: pushee}
	assignSeqNumsForReqs(pushee, args)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try a put using the pusher's txn.
	h.Txn = pusher
	args.Increment = 2
	assignSeqNumsForReqs(pusher, args)
	if resp, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, args); pErr != nil {
		t.Errorf("expected increment to succeed: %s", pErr)
	} else if reply := resp.(*kvpb.IncrementResponse); reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a write intent
// for a read will push the timestamp. It tests this along a few dimensions:
// - high-priority pushes       vs. low-priority pushes
// - already pushed pushee txns vs. not already pushed pushee txns
// - PENDING pushee txn records vs. STAGING pushee txn records
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	storeCfg := TestStoreConfig(nil)
	storeCfg.TestingKnobs.DontRetryPushTxnFailures = true
	storeCfg.TestingKnobs.DontRecoverIndeterminateCommits = true
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &storeCfg)

	testCases := []struct {
		pusherWillWin       bool   // if true, pusher will have a high enough priority to push the pushee
		pusheeAlreadyPushed bool   // if true, pushee's timestamp will be set above pusher's target timestamp
		pusheeStagingRecord bool   // if true, pushee's record is STAGING, otherwise PENDING
		expPushError        string // regexp pattern to match on run error, if not empty
		expPusheeRetry      bool   // do we expect the pushee to hit a retry error when committing?
	}{
		{
			// Insufficient priority to push.
			pusherWillWin:       false,
			pusheeAlreadyPushed: false,
			pusheeStagingRecord: false,
			expPushError:        "failed to push",
			expPusheeRetry:      false,
		},
		{
			// Successful push.
			pusherWillWin:       true,
			pusheeAlreadyPushed: false,
			pusheeStagingRecord: false,
			expPushError:        "",
			expPusheeRetry:      true,
		},
		{
			// Already pushed, no-op.
			pusherWillWin:       false,
			pusheeAlreadyPushed: true,
			pusheeStagingRecord: false,
			expPushError:        "",
			expPusheeRetry:      false,
		},
		{
			// Already pushed, no-op.
			pusherWillWin:       true,
			pusheeAlreadyPushed: true,
			pusheeStagingRecord: false,
			expPushError:        "",
			expPusheeRetry:      false,
		},
		{
			// Insufficient priority to push.
			pusherWillWin:       false,
			pusheeAlreadyPushed: false,
			pusheeStagingRecord: true,
			expPushError:        "failed to push",
			expPusheeRetry:      false,
		},
		{
			// Cannot push STAGING txn record.
			pusherWillWin:       true,
			pusheeAlreadyPushed: false,
			pusheeStagingRecord: true,
			expPushError:        "found txn in indeterminate STAGING state",
			expPusheeRetry:      false,
		},
		{
			// Already pushed the STAGING record, no-op.
			pusherWillWin:       false,
			pusheeAlreadyPushed: true,
			pusheeStagingRecord: true,
			expPushError:        "",
			expPusheeRetry:      false,
		},
		{
			// Already pushed the STAGING record, no-op.
			pusherWillWin:       true,
			pusheeAlreadyPushed: true,
			pusheeStagingRecord: true,
			expPushError:        "",
			expPusheeRetry:      false,
		},
	}
	for i, tc := range testCases {
		name := fmt.Sprintf("%d-pusherWillWin=%t,pusheePushed=%t,pusheeStaging=%t",
			i, tc.pusherWillWin, tc.pusheeAlreadyPushed, tc.pusheeStagingRecord)
		t.Run(name, func(t *testing.T) {
			key := roachpb.Key(fmt.Sprintf("key-%s", name))

			// First, write original value. We use this value as a sentinel; we'll
			// check that we can read it later.
			{
				args := putArgs(key, []byte("value1"))
				if _, pErr := kv.SendWrapped(ctx, store.TestSender(), &args); pErr != nil {
					t.Fatal(pErr)
				}
			}

			pusher := newTransaction("pusher", key, 1, store.cfg.Clock)
			pushee := newTransaction("pushee", key, 1, store.cfg.Clock)

			// Set transaction priorities.
			if tc.pusherWillWin {
				pushee.Priority = enginepb.MinTxnPriority
				pusher.Priority = enginepb.MaxTxnPriority // Pusher will win.
			} else {
				pushee.Priority = enginepb.MaxTxnPriority
				pusher.Priority = enginepb.MinTxnPriority // Pusher will lose.
			}

			// Second, lay down intent using the pushee's txn.
			{
				args := putArgs(key, []byte("value2"))
				assignSeqNumsForReqs(pushee, &args)
				h := kvpb.Header{Txn: pushee}
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &args); pErr != nil {
					t.Fatal(pErr)
				}
			}

			// Determine the timestamp to read at.
			clockTs := store.cfg.Clock.NowAsClockTimestamp()
			readTs := clockTs.ToTimestamp()
			// Give the pusher a previous observed timestamp equal to this read
			// timestamp. This ensures that the pusher doesn't need to push the
			// intent any higher just to push it out of its uncertainty window.
			pusher.UpdateObservedTimestamp(store.Ident.NodeID, clockTs)

			// If the pushee is already pushed, update the transaction record.
			if tc.pusheeAlreadyPushed {
				pushedTs := store.cfg.Clock.Now()
				pushee.WriteTimestamp.Forward(pushedTs)
				pushee.ReadTimestamp.Forward(pushedTs)
				hb, hbH := heartbeatArgs(pushee, store.cfg.Clock.Now())
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), hbH, &hb); pErr != nil {
					t.Fatal(pErr)
				}
			}

			// If the pushee is staging, update the transaction record.
			if tc.pusheeStagingRecord {
				et, etH := endTxnArgs(pushee, true)
				et.InFlightWrites = []roachpb.SequencedWrite{{Key: []byte("keyA"), Sequence: 1}}
				etReply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), etH, &et)
				if pErr != nil {
					t.Fatal(pErr)
				}
				if replyTxn := etReply.Header().Txn; replyTxn.Status != roachpb.STAGING {
					t.Fatalf("expected STAGING txn, found %v", replyTxn)
				}
			}

			// Now, try to read value using the pusher's txn.
			pusher.ReadTimestamp.Forward(readTs)
			pusher.WriteTimestamp.Forward(readTs)
			gArgs := getArgs(key)
			assignSeqNumsForReqs(pusher, &gArgs)
			repl, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: pusher}, &gArgs)
			if tc.expPushError == "" {
				if pErr != nil {
					t.Errorf("expected read to succeed: %s", pErr)
				} else if replyBytes, err := repl.(*kvpb.GetResponse).Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(replyBytes, []byte("value1")) {
					t.Errorf("expected bytes to be %q, got %q", "value1", replyBytes)
				}
			} else {
				if !testutils.IsPError(pErr, tc.expPushError) {
					t.Fatalf("expected error %q, found %v", tc.expPushError, pErr)
				}
			}

			// Finally, try to end the pushee's transaction. Check whether
			// the commit succeeds or fails.
			etArgs, etH := endTxnArgs(pushee, true)
			assignSeqNumsForReqs(pushee, &etArgs)
			_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), etH, &etArgs)
			if tc.expPusheeRetry {
				if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
					t.Errorf("expected transaction retry error; got %s", pErr)
				}
			} else {
				if pErr != nil {
					t.Fatalf("expected no commit error; got %s", pErr)
				}
			}
		})
	}
}

// TestStoreResolveWriteIntentNoTxn verifies that reads and writes
// which are not part of a transaction can push intents.
func TestStoreResolveWriteIntentNoTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, store.cfg.Clock)

	// First, write the pushee's txn via HeartbeatTxn request.
	hb, hbH := heartbeatArgs(pushee, pushee.WriteTimestamp)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), hbH, &hb); pErr != nil {
		t.Fatal(pErr)
	}

	// Next, lay down intent from pushee.
	args := putArgs(key, []byte("value1"))
	assignSeqNumsForReqs(pushee, &args)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), hbH, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try to read outside a transaction.
	getTS := store.cfg.Clock.Now() // accessed later
	{
		gArgs := getArgs(key)
		if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
			Timestamp:    getTS,
			UserPriority: roachpb.MaxUserPriority,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if gReply := reply.(*kvpb.GetResponse); gReply.Value != nil {
			t.Errorf("expected value to be nil, got %+v", gReply.Value)
		}
	}

	{
		// Next, try to write outside of a transaction. We will succeed in pushing txn.
		putTS := store.cfg.Clock.Now()
		args.Value.SetBytes([]byte("value2"))
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
			Timestamp:    putTS,
			UserPriority: roachpb.MaxUserPriority,
		}, &args); pErr != nil {
			t.Errorf("expected success aborting pushee's txn; got %s", pErr)
		}
	}

	// Read pushee's txn.
	txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
	var txn roachpb.Transaction
	if ok, err := storage.MVCCGetProto(
		ctx, store.TODOEngine(), txnKey, hlc.Timestamp{}, &txn, storage.MVCCGetOptions{},
	); !ok || err != nil {
		t.Fatalf("not found or err: %+v", err)
	}
	if txn.Status != roachpb.ABORTED {
		t.Errorf("expected pushee to be aborted; got %s", txn.Status)
	}

	// Verify that the pushee's timestamp was moved forward on
	// former read, since we have it available in write intent error.
	minExpTS := getTS
	minExpTS.Logical++
	if txn.WriteTimestamp.Less(minExpTS) {
		t.Errorf("expected pushee timestamp pushed to %s; got %s", minExpTS, txn.WriteTimestamp)
	}
	// Similarly, verify that pushee's priority was moved from 0
	// to MaxTxnPriority-1 during push.
	if txn.Priority != enginepb.MaxTxnPriority-1 {
		t.Errorf("expected pushee priority to be pushed to %d; got %d", enginepb.MaxTxnPriority-1, txn.Priority)
	}

	// Finally, try to end the pushee's transaction; it should have
	// been aborted.
	etArgs, h := endTxnArgs(pushee, true)
	assignSeqNumsForReqs(pushee, &etArgs)
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &etArgs)
	if pErr == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", pErr)
	}
}

// TestStoreReadInconsistent verifies that gets and scans with read
// consistency set to INCONSISTENT or READ_UNCOMMITTED either push or
// simply ignore extant intents (if they cannot be pushed), depending
// on the intent priority. READ_UNCOMMITTED requests will also return
// the intents that they run into.
func TestStoreReadInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, rc := range []kvpb.ReadConsistencyType{
		kvpb.READ_UNCOMMITTED,
		kvpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			cfg := TestStoreConfig(nil)
			// The test relies on being able to commit a Txn without specifying the
			// intent, while preserving the Txn record. Turn off
			// automatic cleanup for this to work.
			cfg.TestingKnobs.EvalKnobs.DisableTxnAutoGC = true
			store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

			for _, canPush := range []bool{true, false} {
				keyA := roachpb.Key(fmt.Sprintf("%t-a", canPush))
				keyB := roachpb.Key(fmt.Sprintf("%t-b", canPush))

				// First, write keyA.
				args := putArgs(keyA, []byte("value1"))
				if _, pErr := kv.SendWrapped(ctx, store.TestSender(), &args); pErr != nil {
					t.Fatal(pErr)
				}

				// Next, write intents for keyA and keyB. Note that the
				// transactions have unpushable priorities if canPush is true and
				// very pushable ones otherwise.
				priority := roachpb.UserPriority(-math.MaxInt32)
				if canPush {
					priority = -1
				}
				args.Value.SetBytes([]byte("value2"))
				txnA := newTransaction("testA", keyA, priority, store.cfg.Clock)
				txnB := newTransaction("testB", keyB, priority, store.cfg.Clock)
				for _, txn := range []*roachpb.Transaction{txnA, txnB} {
					args.Key = txn.Key
					assignSeqNumsForReqs(txn, &args)
					if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: txn}, &args); pErr != nil {
						t.Fatal(pErr)
					}
				}
				// End txn B, but without resolving the intent.
				etArgs, h := endTxnArgs(txnB, true)
				assignSeqNumsForReqs(txnB, &etArgs)
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &etArgs); pErr != nil {
					t.Fatal(pErr)
				}

				// Now, get from both keys and verify. Whether we can push or not, we
				// will be able to read with both INCONSISTENT and READ_UNCOMMITTED.
				// With READ_UNCOMMITTED, we'll also be able to see the intent's value.
				gArgs := getArgs(keyA)
				if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
					ReadConsistency: rc,
				}, &gArgs); pErr != nil {
					t.Errorf("expected read to succeed: %s", pErr)
				} else {
					gReply := reply.(*kvpb.GetResponse)
					if replyBytes, err := gReply.Value.GetBytes(); err != nil {
						t.Fatal(err)
					} else if !bytes.Equal(replyBytes, []byte("value1")) {
						t.Errorf("expected value %q, got %+v", []byte("value1"), reply)
					} else if rc == kvpb.READ_UNCOMMITTED {
						// READ_UNCOMMITTED will also return the intent.
						if replyIntentBytes, err := gReply.IntentValue.GetBytes(); err != nil {
							t.Fatal(err)
						} else if !bytes.Equal(replyIntentBytes, []byte("value2")) {
							t.Errorf("expected value %q, got %+v", []byte("value2"), reply)
						}
					} else if rc == kvpb.INCONSISTENT {
						if gReply.IntentValue != nil {
							t.Errorf("expected value nil, got %+v", gReply.IntentValue)
						}
					}
				}

				gArgs.Key = keyB
				if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
					ReadConsistency: rc,
				}, &gArgs); pErr != nil {
					t.Errorf("expected read to succeed: %s", pErr)
				} else {
					gReply := reply.(*kvpb.GetResponse)
					if gReply.Value != nil {
						// The new value of B will not be read at first.
						t.Errorf("expected value nil, got %+v", gReply.Value)
					} else if rc == kvpb.READ_UNCOMMITTED {
						// READ_UNCOMMITTED will also return the intent.
						if replyIntentBytes, err := gReply.IntentValue.GetBytes(); err != nil {
							t.Fatal(err)
						} else if !bytes.Equal(replyIntentBytes, []byte("value2")) {
							t.Errorf("expected value %q, got %+v", []byte("value2"), reply)
						}
					} else if rc == kvpb.INCONSISTENT {
						if gReply.IntentValue != nil {
							t.Errorf("expected value nil, got %+v", gReply.IntentValue)
						}
					}
				}
				// However, it will be read eventually, as B's intent can be
				// resolved asynchronously as txn B is committed.
				testutils.SucceedsSoon(t, func() error {
					if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
						ReadConsistency: rc,
					}, &gArgs); pErr != nil {
						return errors.Errorf("expected read to succeed: %s", pErr)
					} else if gReply := reply.(*kvpb.GetResponse).Value; gReply == nil {
						return errors.Errorf("value is nil")
					} else if replyBytes, err := gReply.GetBytes(); err != nil {
						return err
					} else if !bytes.Equal(replyBytes, []byte("value2")) {
						return errors.Errorf("expected value %q, got %+v", []byte("value2"), reply)
					}
					return nil
				})

				// Scan keys and verify results.
				sArgs := scanArgs(keyA, keyB.Next())
				reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
					ReadConsistency: rc,
				}, sArgs)
				if pErr != nil {
					t.Errorf("expected scan to succeed: %s", pErr)
				}
				sReply := reply.(*kvpb.ScanResponse)
				if l := len(sReply.Rows); l != 2 {
					t.Errorf("expected 2 results; got %d", l)
				} else if key := sReply.Rows[0].Key; !key.Equal(keyA) {
					t.Errorf("expected key %q; got %q", keyA, key)
				} else if key := sReply.Rows[1].Key; !key.Equal(keyB) {
					t.Errorf("expected key %q; got %q", keyB, key)
				} else if val1, err := sReply.Rows[0].Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(val1, []byte("value1")) {
					t.Errorf("expected value %q, got %q", []byte("value1"), val1)
				} else if val2, err := sReply.Rows[1].Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(val2, []byte("value2")) {
					t.Errorf("expected value %q, got %q", []byte("value2"), val2)
				} else if rc == kvpb.READ_UNCOMMITTED {
					if l := len(sReply.IntentRows); l != 1 {
						t.Errorf("expected 1 intent result; got %d", l)
					} else if intentKey := sReply.IntentRows[0].Key; !intentKey.Equal(keyA) {
						t.Errorf("expected intent key %q; got %q", keyA, intentKey)
					} else if intentVal1, err := sReply.IntentRows[0].Value.GetBytes(); err != nil {
						t.Fatal(err)
					} else if !bytes.Equal(intentVal1, []byte("value2")) {
						t.Errorf("expected intent value %q, got %q", []byte("value2"), intentVal1)
					}
				} else if rc == kvpb.INCONSISTENT {
					if l := len(sReply.IntentRows); l != 0 {
						t.Errorf("expected 0 intent result; got %d", l)
					}
				}

				// Reverse scan keys and verify results.
				rsArgs := revScanArgs(keyA, keyB.Next())
				reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
					ReadConsistency: rc,
				}, rsArgs)
				if pErr != nil {
					t.Errorf("expected scan to succeed: %s", pErr)
				}
				rsReply := reply.(*kvpb.ReverseScanResponse)
				if l := len(rsReply.Rows); l != 2 {
					t.Errorf("expected 2 results; got %d", l)
				} else if key := rsReply.Rows[0].Key; !key.Equal(keyB) {
					t.Errorf("expected key %q; got %q", keyA, key)
				} else if key := rsReply.Rows[1].Key; !key.Equal(keyA) {
					t.Errorf("expected key %q; got %q", keyB, key)
				} else if val1, err := rsReply.Rows[0].Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(val1, []byte("value2")) {
					t.Errorf("expected value %q, got %q", []byte("value2"), val1)
				} else if val2, err := rsReply.Rows[1].Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(val2, []byte("value1")) {
					t.Errorf("expected value %q, got %q", []byte("value1"), val2)
				} else if rc == kvpb.READ_UNCOMMITTED {
					if l := len(rsReply.IntentRows); l != 1 {
						t.Errorf("expected 1 intent result; got %d", l)
					} else if intentKey := rsReply.IntentRows[0].Key; !intentKey.Equal(keyA) {
						t.Errorf("expected intent key %q; got %q", keyA, intentKey)
					} else if intentVal1, err := rsReply.IntentRows[0].Value.GetBytes(); err != nil {
						t.Fatal(err)
					} else if !bytes.Equal(intentVal1, []byte("value2")) {
						t.Errorf("expected intent value %q, got %q", []byte("value2"), intentVal1)
					}
				} else if rc == kvpb.INCONSISTENT {
					if l := len(rsReply.IntentRows); l != 0 {
						t.Errorf("expected 0 intent result; got %d", l)
					}
				}
			}
		})
	}
}

// TestStoreScanResumeTSCache verifies that the timestamp cache is properly
// updated when scans, reverse scan, and get requests return partial results
// and a resume span.
func TestStoreScanResumeTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	manualClock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manualClock))
	cfg.Settings = cluster.MakeTestingClusterSettings()
	allowDroppingLatchesBeforeEval.Override(ctx, &cfg.Settings.SV, false)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	// Write three keys at time t0.
	t0 := timeutil.Unix(1, 0)
	manualClock.MustAdvanceTo(t0)
	h := kvpb.Header{Timestamp: makeTS(t0.UnixNano(), 0)}
	for _, keyStr := range []string{"a", "b", "c"} {
		key := roachpb.Key(keyStr)
		putArgs := putArgs(key, []byte("value"))
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &putArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Scan the span at t1 with max keys and verify the expected resume span.
	span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}
	sArgs := scanArgs(span.Key, span.EndKey)
	t1 := timeutil.Unix(2, 0)
	manualClock.MustAdvanceTo(t1)
	h.Timestamp = makeTS(t1.UnixNano(), 0)
	h.MaxSpanRequestKeys = 2
	reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, sArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	sReply := reply.(*kvpb.ScanResponse)
	if a, e := len(sReply.Rows), 2; a != e {
		t.Errorf("expected %d rows; got %d", e, a)
	}
	expResumeSpan := &roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
	if a, e := sReply.ResumeSpan, expResumeSpan; !reflect.DeepEqual(a, e) {
		t.Errorf("expected resume span %s; got %s", e, a)
	}

	// Verify the timestamp cache has been set for "b".Next(), but not for "c".
	rTS, _ := store.tsCache.GetMax(roachpb.Key("b").Next(), nil)
	if a, e := rTS, makeTS(t1.UnixNano(), 0); a != e {
		t.Errorf("expected timestamp cache for \"b\".Next() set to %s; got %s", e, a)
	}
	rTS, _ = store.tsCache.GetMax(roachpb.Key("c"), nil)
	if a, lt := rTS, makeTS(t1.UnixNano(), 0); lt.LessEq(a) {
		t.Errorf("expected timestamp cache for \"c\" set less than %s; got %s", lt, a)
	}

	// Reverse scan the span at t1 with max keys and verify the expected resume span.
	t2 := timeutil.Unix(3, 0)
	manualClock.MustAdvanceTo(t2)
	h.Timestamp = makeTS(t2.UnixNano(), 0)
	rsArgs := revScanArgs(span.Key, span.EndKey)
	reply, pErr = kv.SendWrappedWith(ctx, store.TestSender(), h, rsArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	rsReply := reply.(*kvpb.ReverseScanResponse)
	if a, e := len(rsReply.Rows), 2; a != e {
		t.Errorf("expected %d rows; got %d", e, a)
	}
	expResumeSpan = &roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("a").Next()}
	if a, e := rsReply.ResumeSpan, expResumeSpan; !reflect.DeepEqual(a, e) {
		t.Errorf("expected resume span %s; got %s", e, a)
	}

	// Verify the timestamp cache has been set for "a".Next(), but not for "a".
	rTS, _ = store.tsCache.GetMax(roachpb.Key("a").Next(), nil)
	if a, e := rTS, makeTS(t2.UnixNano(), 0); a != e {
		t.Errorf("expected timestamp cache for \"a\".Next() set to %s; got %s", e, a)
	}
	rTS, _ = store.tsCache.GetMax(roachpb.Key("a"), nil)
	if a, lt := rTS, makeTS(t2.UnixNano(), 0); lt.LessEq(a) {
		t.Errorf("expected timestamp cache for \"a\" set less than %s; got %s", lt, a)
	}

	// Scan the span using 3 Get requests at t3 with max keys and verify the
	// expected resume spans. The two Get requests that are evaluated should be
	// accounted for in the timestamp cache, but the third request which is not
	// evaluated due to the key limit should not be accounted for.
	t3 := timeutil.Unix(4, 0)
	manualClock.MustAdvanceTo(t3)
	h.Timestamp = makeTS(t3.UnixNano(), 0)
	ba := &kvpb.BatchRequest{}
	ba.Header = h
	ba.Add(getArgsString("a"), getArgsString("b"), getArgsString("c"))
	br, pErr := store.TestSender().Send(ctx, ba)
	require.Nil(t, pErr)
	require.Len(t, br.Responses, 3)
	for i, ru := range br.Responses {
		resp := ru.GetGet()
		if i < 2 {
			require.NotNil(t, resp.Value)
			require.Nil(t, resp.ResumeSpan)
		} else {
			require.Nil(t, resp.Value)
			require.NotNil(t, resp.ResumeSpan)
		}
	}

	// Verify the timestamp cache has been set for "a" and "b", but not for "c".
	rTS, _ = store.tsCache.GetMax(roachpb.Key("a"), nil)
	require.Equal(t, makeTS(t3.UnixNano(), 0), rTS)
	rTS, _ = store.tsCache.GetMax(roachpb.Key("b"), nil)
	require.Equal(t, makeTS(t3.UnixNano(), 0), rTS)
	rTS, _ = store.tsCache.GetMax(roachpb.Key("c"), nil)
	require.Equal(t, makeTS(t2.UnixNano(), 0), rTS)
}

// TestStoreSkipLockedTSCache verifies that the timestamp cache is properly
// updated when get, scan, and reverse scan requests use the SkipLocked wait
// policy.
func TestStoreSkipLockedTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name string
		reqs []kvpb.Request
	}{
		{"get", []kvpb.Request{getArgsString("a"), getArgsString("b"), getArgsString("c")}},
		{"scan", []kvpb.Request{scanArgsString("a", "d")}},
		{"revscan", []kvpb.Request{revScanArgsString("a", "d")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store, manualClock := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

			// Write three keys at time t0.
			t0 := timeutil.Unix(1, 0)
			manualClock.MustAdvanceTo(t0)
			h := kvpb.Header{Timestamp: makeTS(t0.UnixNano(), 0)}
			for _, keyStr := range []string{"a", "b", "c"} {
				putArgs := putArgs(roachpb.Key(keyStr), []byte("value"))
				_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &putArgs)
				require.Nil(t, pErr)
			}

			// Lock the middle key at t1.
			t1 := timeutil.Unix(2, 0)
			manualClock.MustAdvanceTo(t1)
			lockedKey := roachpb.Key("b")
			txn := roachpb.MakeTransaction("locker", lockedKey, 0, 0, makeTS(t1.UnixNano(), 0), 0, 0)
			txnH := kvpb.Header{Txn: &txn}
			putArgs := putArgs(lockedKey, []byte("newval"))
			_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), txnH, &putArgs)
			require.Nil(t, pErr)

			// Read the span at t2 using a SkipLocked wait policy.
			t2 := timeutil.Unix(3, 0)
			manualClock.MustAdvanceTo(t2)
			ba := &kvpb.BatchRequest{}
			ba.Timestamp = makeTS(t2.UnixNano(), 0)
			ba.WaitPolicy = lock.WaitPolicy_SkipLocked
			ba.Add(tc.reqs...)
			br, pErr := store.TestSender().Send(ctx, ba)
			require.Nil(t, pErr)

			// Validate the response keys. The locked key should not be included.
			var respKeys []string
			for i, ru := range br.Responses {
				req, resp := ba.Requests[i].GetInner(), ru.GetInner()
				require.NoError(t, kvpb.ResponseKeyIterate(req, resp, func(k roachpb.Key) {
					respKeys = append(respKeys, string(k))
				}))
			}
			sort.Strings(respKeys) // normalize reverse scan
			require.Equal(t, []string{"a", "c"}, respKeys)

			// Verify the timestamp cache has been set for "a" and "c", but not for "b".
			t2TS := makeTS(t2.UnixNano(), 0)
			rTS, _ := store.tsCache.GetMax(roachpb.Key("a"), nil)
			require.True(t, rTS.EqOrdering(t2TS))
			rTS, _ = store.tsCache.GetMax(roachpb.Key("b"), nil)
			require.True(t, rTS.Less(t2TS))
			rTS, _ = store.tsCache.GetMax(roachpb.Key("c"), nil)
			require.True(t, rTS.EqOrdering(t2TS))
		})
	}
}

// TestStoreScanIntents verifies that a scan across 10 intents resolves
// them in one fell swoop using both consistent and inconsistent reads.
func TestStoreScanIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfg := TestStoreConfig(nil)
	var count int32
	countPtr := &count

	cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if req, ok := filterArgs.Req.(*kvpb.ScanRequest); ok {
				// Avoid counting scan requests not generated by this test, e.g. those
				// generated by periodic gossips.
				if bytes.HasPrefix(req.Key, []byte(t.Name())) {
					atomic.AddInt32(countPtr, 1)
				}
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	testCases := []struct {
		consistent bool
		canPush    bool  // can the txn be pushed?
		expFinish  bool  // do we expect the scan to finish?
		expCount   int32 // how many times do we expect to scan?
	}{
		// Consistent which can push will detect conflicts and resolve them.
		{true, true, true, 1},
		// Consistent but can't push will backoff and retry and not finish.
		{true, false, false, -1},
		// Inconsistent and can push will make one loop, with async resolves.
		{false, true, true, 1},
		// Inconsistent and can't push will just read inconsistent (will read nils).
		{false, false, true, 1},
	}
	for i, test := range testCases {
		// The command filter just counts the number of scan requests which are
		// submitted to the range.
		atomic.StoreInt32(countPtr, 0)

		// Lay down 10 intents to scan over.
		var txn *roachpb.Transaction
		keys := []roachpb.Key{}
		for j := 0; j < 10; j++ {
			key := roachpb.Key(fmt.Sprintf("%s%d-%02d", t.Name(), i, j))
			keys = append(keys, key)
			if txn == nil {
				priority := roachpb.UserPriority(1)
				if test.canPush {
					priority = roachpb.MinUserPriority
				}
				txn = newTransaction(fmt.Sprintf("test-%d", i), key, priority, store.cfg.Clock)
			}
			args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
			assignSeqNumsForReqs(txn, &args)
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: txn}, &args); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Scan the range and verify count. Do this in a goroutine in case
		// it isn't expected to finish.
		sArgs := scanArgs(keys[0], keys[9].Next())
		var sReply *kvpb.ScanResponse
		ts := store.Clock().Now()
		consistency := kvpb.CONSISTENT
		if !test.consistent {
			consistency = kvpb.INCONSISTENT
		}
		errChan := make(chan *kvpb.Error, 1)
		go func() {
			reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
				Timestamp:       ts,
				ReadConsistency: consistency,
			}, sArgs)
			if pErr == nil {
				sReply = reply.(*kvpb.ScanResponse)
			}
			errChan <- pErr
		}()

		wait := testutils.DefaultSucceedsSoonDuration
		if !test.expFinish {
			wait = 10 * time.Millisecond
		}
		select {
		case pErr := <-errChan:
			if pErr != nil {
				t.Fatal(pErr)
			}
			if len(sReply.Rows) != 0 {
				t.Errorf("expected empty scan result; got %+v", sReply.Rows)
			}
			if countVal := atomic.LoadInt32(countPtr); countVal != test.expCount {
				t.Errorf("%d: expected scan count %d; got %d", i, test.expCount, countVal)
			}
		case <-time.After(wait):
			if test.expFinish {
				t.Errorf("%d: scan failed to finish after %s", i, wait)
			} else {
				// Commit the unpushable txn so the read can finish.
				etArgs, h := endTxnArgs(txn, true)
				for _, key := range keys {
					etArgs.LockSpans = append(etArgs.LockSpans, roachpb.Span{Key: key})
				}
				assignSeqNumsForReqs(txn, &etArgs)
				if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &etArgs); pErr != nil {
					t.Fatal(pErr)
				}
				<-errChan
			}
		}
	}
}

// TestStoreScanIntentsRespectsLimit verifies that when reads are allowed to
// resolve their conflicts before eval (i.e. when they are allowed to drop their
// latches early), the scan for conflicting intents respects the max intent
// limits.
//
// The test proceeds as follows: a writer lays down more than
// `MaxIntentsPerWriteIntentErrorDefault` intents, and a reader is expected to
// encounter these intents and raise a `WriteIntentError` with exactly
// `MaxIntentsPerWriteIntentErrorDefault` intents in the error.
func TestStoreScanIntentsRespectsLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	skip.UnderRace(
		t, "this test writes a ton of intents and tries to clean them up, too slow under race",
	)

	var interceptWriteIntentErrors atomic.Value
	// `commitCh` is used to block the writer from committing until the reader has
	// encountered the intents laid down by the writer.
	commitCh := make(chan struct{})
	// intentsLaidDownCh is signalled when the writer is done laying down intents.
	intentsLaidDownCh := make(chan struct{})
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &StoreTestingKnobs{
					TestingConcurrencyRetryFilter: func(
						ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error,
					) {
						if errors.HasType(pErr.GoError(), (*kvpb.WriteIntentError)(nil)) {
							// Assert that the WriteIntentError has MaxIntentsPerWriteIntentErrorIntents.
							if trap := interceptWriteIntentErrors.Load(); trap != nil && trap.(bool) {
								require.Equal(
									t, storage.MaxIntentsPerWriteIntentErrorDefault,
									len(pErr.GetDetail().(*kvpb.WriteIntentError).Intents),
								)
								interceptWriteIntentErrors.Store(false)
								// Allow the writer to commit.
								t.Logf("allowing writer to commit")
								close(commitCh)
							}
						}
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	store, err := tc.Server(0).GetStores().(*Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)
	var intentKeys []roachpb.Key
	var wg sync.WaitGroup
	wg.Add(2)

	// Lay down more than `MaxIntentsPerWriteIntentErrorDefault` intents.
	go func() {
		defer wg.Done()
		txn := newTransaction(
			"test", roachpb.Key("test-key"), roachpb.NormalUserPriority, tc.Server(0).Clock(),
		)
		for j := 0; j < storage.MaxIntentsPerWriteIntentErrorDefault+10; j++ {
			var key roachpb.Key
			key = append(key, keys.ScratchRangeMin...)
			key = append(key, []byte(fmt.Sprintf("%d", j))...)
			intentKeys = append(intentKeys, key)
			args := putArgs(key, []byte(fmt.Sprintf("value%07d", j)))
			_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: txn}, &args)
			require.Nil(t, pErr)
		}
		intentsLaidDownCh <- struct{}{}
		<-commitCh // Wait for the test to tell us to commit the txn.
		args, header := endTxnArgs(txn, true /* commit */)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), header, &args)
		require.Nil(t, pErr)
	}()

	select {
	case <-intentsLaidDownCh:
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatal("timed out waiting for intents to be laid down")
	}

	// Now, expect a conflicting reader to encounter the intents and raise a
	// WriteIntentError with exactly `MaxIntentsPerWriteIntentErrorDefault`
	// intents. See the TestingConcurrencyRetryFilter above.
	var ba kv.Batch
	for i := 0; i < storage.MaxIntentsPerWriteIntentErrorDefault+10; i += 10 {
		for _, key := range intentKeys[i : i+10] {
			args := getArgs(key)
			ba.AddRawRequest(&args)
		}
	}
	t.Logf("issuing gets while intercepting WriteIntentErrors")
	interceptWriteIntentErrors.Store(true)
	go func() {
		defer wg.Done()
		err := store.DB().Run(ctx, &ba)
		require.NoError(t, err)
	}()

	wg.Wait()
}

// TestStoreScanInconsistentResolvesIntents lays down 10 intents,
// commits the txn without resolving intents, then does repeated
// inconsistent reads until the data shows up, showing that the
// inconsistent reads are triggering intent resolution.
func TestStoreScanInconsistentResolvesIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var intercept atomic.Value
	intercept.Store(uuid.Nil)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			req, ok := filterArgs.Req.(*kvpb.ResolveIntentRequest)
			if ok && intercept.Load().(uuid.UUID).Equal(req.IntentTxn.ID) {
				return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)
	splitTestRange(store, keys.MustAddr(keys.ScratchRangeMin), t)

	var sl []roachpb.Key
	require.NoError(t, store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		sl = nil                  // handle retries
		intercept.Store(txn.ID()) // prevent async intent resolution for this txn
		// Anchor the txn on TableDataMin-range. This prevents a fast-path in which
		// the txn removes its intents atomically with the commit, which we want to
		// avoid for the purpose of this test.
		if err := txn.Put(ctx, keys.TableDataMin, "hello"); err != nil {
			return err
		}
		for j := 0; j < 10; j++ {
			var key roachpb.Key
			key = append(key, keys.ScratchRangeMin...)
			key = append(key, byte(j))
			sl = append(sl, key)
			if err := txn.Put(ctx, key, fmt.Sprintf("value%02d", j)); err != nil {
				return err
			}
		}
		return nil
	}))

	intercept.Store(uuid.Nil) // allow async intent resolution

	// Scan the range repeatedly until we've verified count.
	testutils.SucceedsSoon(t, func() error {
		var b kv.Batch
		b.Scan(sl[0], sl[9].Next())
		b.Header.ReadConsistency = kvpb.INCONSISTENT
		require.NoError(t, store.DB().Run(ctx, &b))
		if exp, act := len(sl), len(b.Results[0].Rows); exp != act {
			return errors.Errorf("expected %d keys, scanned %d", exp, act)
		}
		return nil
	})
}

// TestStoreScanIntentsFromTwoTxns lays down two intents from two
// different transactions. The clock is next moved forward, causing
// the transaction to expire. The intents are then scanned
// consistently, which triggers a push of both transactions and then
// resolution of the intents, allowing the scan to complete.
func TestStoreScanIntentsFromTwoTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, manualClock := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	// Lay down two intents from two txns to scan over.
	key1 := roachpb.Key("bar")
	txn1 := newTransaction("test1", key1, 1, store.cfg.Clock)
	args := putArgs(key1, []byte("value1"))
	assignSeqNumsForReqs(txn1, &args)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: txn1}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	key2 := roachpb.Key("foo")
	txn2 := newTransaction("test2", key2, 1, store.cfg.Clock)
	args = putArgs(key2, []byte("value2"))
	assignSeqNumsForReqs(txn2, &args)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: txn2}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, expire the transactions by moving the clock forward. This will
	// result in the subsequent scan operation pushing both transactions
	// in a single batch.
	manualClock.Advance(time.Duration(txnwait.TxnLivenessThreshold.Load()) + 1)

	// Scan the range and verify empty result (expired txn is aborted,
	// cleaning up intents).
	sArgs := scanArgs(key1, key2.Next())
	if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{}, sArgs); pErr != nil {
		t.Fatal(pErr)
	} else if sReply := reply.(*kvpb.ScanResponse); len(sReply.Rows) != 0 {
		t.Errorf("expected empty result; got %+v", sReply.Rows)
	}
}

// TestStoreScanMultipleIntents lays down ten intents from a single
// transaction. The clock is then moved forward such that the txn is
// expired and the intents are scanned INCONSISTENTly. Verify that all
// ten intents are resolved from a single INCONSISTENT scan.
func TestStoreScanMultipleIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var resolveCount int32
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if _, ok := filterArgs.Req.(*kvpb.ResolveIntentRequest); ok {
				atomic.AddInt32(&resolveCount, 1)
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	// Lay down ten intents from a single txn.
	key1 := roachpb.Key("key00")
	key10 := roachpb.Key("key09")
	txn := newTransaction("test", key1, 1, store.cfg.Clock)
	ba := &kvpb.BatchRequest{}
	for i := 0; i < 10; i++ {
		pArgs := putArgs(roachpb.Key(fmt.Sprintf("key%02d", i)), []byte("value"))
		ba.Add(&pArgs)
		assignSeqNumsForReqs(txn, &pArgs)
	}
	ba.Header = kvpb.Header{Txn: txn}
	if _, pErr := store.TestSender().Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, expire the transactions by moving the clock forward. This will
	// result in the subsequent scan operation pushing both transactions
	// in a single batch.
	manual.Advance(time.Duration(txnwait.TxnLivenessThreshold.Load()) + 1)

	// Query the range with a single scan, which should cause all intents
	// to be resolved.
	sArgs := scanArgs(key1, key10.Next())
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), sArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify all ten intents are resolved from the single inconsistent scan.
	testutils.SucceedsSoon(t, func() error {
		if a, e := atomic.LoadInt32(&resolveCount), int32(10); a != e {
			return fmt.Errorf("expected %d; got %d resolves", e, a)
		}
		return nil
	})
}

// TestStoreBadRequests verifies that Send returns errors for
// bad requests that do not pass key verification.
func TestStoreBadRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	txn := newTransaction("test", roachpb.Key("a"), 1 /* priority */, store.cfg.Clock)

	args1 := getArgs(roachpb.Key("a"))
	args1.EndKey = roachpb.Key("b")

	args2 := getArgs(roachpb.RKeyMax)

	args3 := scanArgs(roachpb.Key("a"), roachpb.Key("a"))
	args4 := scanArgs(roachpb.Key("b"), roachpb.Key("a"))

	args5 := scanArgs(roachpb.RKeyMin, roachpb.Key("a"))
	args6 := scanArgs(keys.RangeDescriptorKey(roachpb.RKey(keys.MinKey)), roachpb.Key("a"))

	tArgs0, _ := heartbeatArgs(txn, hlc.Timestamp{})

	tArgs2, tHeader2 := endTxnArgs(txn, false /* commit */)
	tHeader2.Txn.Key = roachpb.Key(tHeader2.Txn.Key).Next()

	tArgs3, tHeader3 := heartbeatArgs(txn, hlc.Timestamp{})
	tHeader3.Txn.Key = roachpb.Key(tHeader3.Txn.Key).Next()

	tArgs4 := pushTxnArgs(txn, txn, kvpb.PUSH_ABORT)
	tArgs4.PusheeTxn.Key = roachpb.Key(txn.Key).Next()

	testCases := []struct {
		args   kvpb.Request
		header *kvpb.Header
		err    string
	}{
		// EndKey for non-Range is invalid.
		{&args1, nil, "should not be specified"},
		// Start key must be less than KeyMax.
		{&args2, nil, "must be less than"},
		// End key must be greater than start.
		{args3, nil, "must be greater than"},
		{args4, nil, "must be greater than"},
		// Can't range from local to global.
		{args5, nil, "must be greater than LocalMax"},
		{args6, nil, "is range-local, but"},
		// Txn must be specified in Header.
		{&tArgs0, nil, "no transaction specified"},
		// Txn key must be same as the request key.
		{&tArgs2, &tHeader2, "request key .* should match txn key .*"},
		{&tArgs3, &tHeader3, "request key .* should match txn key .*"},
		{&tArgs4, nil, "request key .* should match pushee"},
	}
	for i, test := range testCases {
		t.Run("", func(t *testing.T) {
			if test.header == nil {
				test.header = &kvpb.Header{}
			}
			if test.header.Txn != nil {
				assignSeqNumsForReqs(test.header.Txn, test.args)
			}
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), *test.header, test.args); !testutils.IsPError(pErr, test.err) {
				t.Errorf("%d expected error %q, got error %v", i, test.err, pErr)
			}
		})
	}
}

func TestStore_HottestReplicasByTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t, testStoreOpts{createSystemRanges: true}, stopper)

	type testData struct {
		tenantID uint64
		qps      float64
	}

	td := []testData{{1, 2}, {1, 3}, {1, 4}, {1, 5},
		{2, 1}, {2, 2}, {2, 3}, {2, 4}}

	acc := NewTenantReplicaAccumulator(aload.Queries)

	for i, c := range td {
		cr := candidateReplica{
			Replica: &Replica{RangeID: roachpb.RangeID(i)},
			usage:   allocator.RangeUsageInfo{QueriesPerSecond: c.qps},
		}
		cr.mu.tenantID = roachpb.MustMakeTenantID(c.tenantID)
		acc.AddReplica(cr)
	}

	store.replRankingsByTenant.Update(acc)

	hotReplicas := store.HottestReplicasByTenant(roachpb.MustMakeTenantID(1))
	require.NotNil(t, hotReplicas)
	require.Equal(t, 4, len(hotReplicas))

	// Test requesting hottest replicas asynchronously to detect data race.
	iterationsNum := 100
	wg := sync.WaitGroup{}
	wg.Add(iterationsNum)
	for i := 0; i < iterationsNum; i++ {
		go func() {
			require.NotNil(t, store.HottestReplicasByTenant(roachpb.MustMakeTenantID(1)))
			require.NotNil(t, store.HottestReplicasByTenant(roachpb.MustMakeTenantID(2)))
			wg.Done()
		}()
	}
	wg.Wait()
}

// fakeRangeQueue implements the rangeQueue interface and
// records which range is passed to MaybeRemove.
type fakeRangeQueue struct {
	maybeRemovedRngs chan roachpb.RangeID
}

func (fq *fakeRangeQueue) Start(_ *stop.Stopper) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeAddAsync(context.Context, replicaInQueue, hlc.ClockTimestamp) {
	// Do nothing
}

func (fq *fakeRangeQueue) AddAsync(context.Context, replicaInQueue, float64) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeRemove(rangeID roachpb.RangeID) {
	fq.maybeRemovedRngs <- rangeID
}

func (fq *fakeRangeQueue) Name() string {
	return "fakeRangeQueue"
}

func (fq *fakeRangeQueue) NeedsLease() bool {
	return false
}

func (fq *fakeRangeQueue) SetDisabled(disabled bool) {
	// Do nothing.
}

// TestMaybeRemove tests that MaybeRemove is called when a range is removed.
func TestMaybeRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := TestStoreConfig(nil)
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithoutStart(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

	// Add a queue to the scanner before starting the store and running the scanner.
	// This is necessary to avoid data race.
	fq := &fakeRangeQueue{
		maybeRemovedRngs: make(chan roachpb.RangeID),
	}
	store.scanner.AddQueues(fq)

	if err := store.Start(ctx, stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()

	repl, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(ctx, repl, repl.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}
	// MaybeRemove is called.
	removedRng := <-fq.maybeRemovedRngs
	if removedRng != repl.RangeID {
		t.Errorf("Unexpected removed range %v", removedRng)
	}
}

func TestStoreGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	store := tc.store

	assertThreshold := func(ts hlc.Timestamp) {
		repl, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		repl.mu.Lock()
		gcThreshold := *repl.mu.state.GCThreshold
		pgcThreshold, err := repl.mu.stateLoader.LoadGCThreshold(context.Background(), store.TODOEngine())
		repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if gcThreshold != *pgcThreshold {
			t.Fatalf("persisted != in-memory threshold: %s vs %s", pgcThreshold, gcThreshold)
		}
		if *pgcThreshold != ts {
			t.Fatalf("expected timestamp %s, got %s", ts, pgcThreshold)
		}
	}

	// Threshold should start at zero.
	assertThreshold(hlc.Timestamp{})

	threshold := hlc.Timestamp{
		WallTime: 2e9,
	}

	gcr := kvpb.GCRequest{
		// Bogus span to make it a valid request.
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		},
		Threshold: threshold,
	}
	if _, pErr := tc.SendWrappedWith(kvpb.Header{RangeID: 1}, &gcr); pErr != nil {
		t.Fatal(pErr)
	}

	assertThreshold(threshold)
}

// TestRaceOnTryGetOrCreateReplicas exercises a case where a race between
// different raft messages addressed to different replica IDs could lead to
// a nil pointer panic.
func TestRaceOnTryGetOrCreateReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	s := tc.store
	var wg sync.WaitGroup
	for i := 3; i < 100; i++ {
		wg.Add(1)
		go func(rid roachpb.ReplicaID) {
			defer wg.Done()
			r, _, _ := s.getOrCreateReplica(ctx, 42, rid, &roachpb.ReplicaDescriptor{
				NodeID:    2,
				StoreID:   2,
				ReplicaID: 2,
			})
			if r != nil {
				r.raftMu.Unlock()
			}
		}(roachpb.ReplicaID(i))
	}
	wg.Wait()
}

func TestStoreRangePlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	s := tc.store

	s.mu.Lock()
	numPlaceholders := len(s.mu.replicaPlaceholders)
	s.mu.Unlock()

	if numPlaceholders != 0 {
		t.Fatal("new store should have zero replica placeholders")
	}

	// Clobber the existing range so we can test non-overlapping placeholders.
	repl1, err := s.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := s.RemoveReplica(ctx, repl1, repl1.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Error(err)
	}

	repID := roachpb.RangeID(2)
	rep := createReplica(s, repID, roachpb.RKeyMin, roachpb.RKey("c"))
	if err := s.AddReplica(rep); err != nil {
		t.Fatal(err)
	}

	placeholder1 := &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(7),
			StartKey: roachpb.RKey("c"),
			EndKey:   roachpb.RKey("d"),
		},
	}
	placeholder2 := &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(8),
			StartKey: roachpb.RKey("d"),
			EndKey:   roachpb.RKeyMax,
		},
	}

	check := func(exp string) {
		exp = strings.TrimSpace(exp)
		t.Helper()
		act := strings.TrimSpace(s.mu.replicasByKey.String())
		require.Equal(t, exp, act)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Test that simple insertion works.
	require.NoError(t, s.addPlaceholderLocked(placeholder1))
	require.NoError(t, s.addPlaceholderLocked(placeholder2))

	check(`
[] - [99] (/Min - "c")
[99] - [100] ("c" - "d")
[100] - [255 255] ("d" - /Max)`)

	checkErr := func(re string) func(bool, error) {
		t.Helper()
		return func(removed bool, err error) {
			require.True(t, testutils.IsError(err, re), "expected to match %s: %v", re, err)
			require.False(t, removed)
		}
	}
	checkRemoved := func(removed bool, nilErr error) {
		t.Helper()
		require.NoError(t, nilErr)
		require.True(t, removed)
	}
	checkNotRemoved := func(removed bool, nilErr error) {
		t.Helper()
		require.NoError(t, nilErr)
		require.False(t, removed)
	}

	// Test that simple deletion works.
	checkRemoved(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderFailed))

	check(`
[] - [99] (/Min - "c")
[100] - [255 255] ("d" - /Max)`)

	// Test cannot double insert the same placeholder.
	placeholder1.tainted = 0 // reset for re-use
	require.NoError(t, s.addPlaceholderLocked(placeholder1))
	if err := s.addPlaceholderLocked(placeholder1); !testutils.IsError(err, ".*overlaps with existing") {
		t.Fatalf("should not be able to add ReplicaPlaceholder for the same key twice, got: %+v", err)
	}

	// Test double deletion of a placeholder.
	checkRemoved(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderFilled))
	checkNotRemoved(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderFailed))
	checkNotRemoved(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderDropped))
	checkErr(`attempting to fill tainted placeholder`)(s.removePlaceholderLocked(
		ctx, placeholder1, removePlaceholderFilled,
	))
	placeholder1.tainted = 0 // pretend it wasn't already deleted
	checkErr(`expected placeholder .* to exist`)(s.removePlaceholderLocked(
		ctx, placeholder1, removePlaceholderDropped,
	))

	check(`
[] - [99] (/Min - "c")
[100] - [255 255] ("d" - /Max)`)

	// This placeholder overlaps with an existing replica.
	placeholder1 = &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  repID,
			StartKey: roachpb.RKeyMin,
			EndKey:   roachpb.RKey("c"),
		},
	}

	addPH := func(ph *ReplicaPlaceholder) (bool, error) {
		return false, s.addPlaceholderLocked(ph)
	}

	// Test that placeholder cannot clobber existing replica.
	checkErr(`.*overlaps with existing`)(addPH(placeholder1))

	// Test that Placeholder deletion doesn't delete replicas.
	placeholder1.tainted = 0
	checkErr(`expected placeholder .* to exist`)(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderFilled))
	checkNotRemoved(s.removePlaceholderLocked(ctx, placeholder1, removePlaceholderFailed))
	check(`
[] - [99] (/Min - "c")
[100] - [255 255] ("d" - /Max)`)
}

// Test that we remove snapshot placeholders when raft ignores the
// snapshot. This is testing the removal of placeholder after handleRaftReady
// processing for an uninitialized Replica.
func TestStoreRemovePlaceholderOnRaftIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	s := tc.store

	// Remove the existing replica so we can insert a placeholder.
	repl1, err := s.GetReplica(1)
	desc := repl1.Desc()
	require.NoError(t, err)
	require.NoError(t, s.RemoveReplica(ctx, repl1, desc.NextReplicaID, RemoveOptions{
		DestroyData: true,
	}))

	// Wrap the snapshot in a minimal header. The request will be dropped because
	// replica 2 is not in the ConfState.
	req := &kvserverpb.SnapshotRequest_Header{
		State: kvserverpb.ReplicaState{Desc: desc},
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			RangeID: 1,
			ToReplica: roachpb.ReplicaDescriptor{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 2,
			},
			FromReplica: roachpb.ReplicaDescriptor{
				NodeID:    2,
				StoreID:   2,
				ReplicaID: 3,
			},
			Message: raftpb.Message{
				Type: raftpb.MsgSnap,
				Snapshot: &raftpb.Snapshot{
					Data: []byte{},
					Metadata: raftpb.SnapshotMetadata{
						Index: 1,
						Term:  1,
					},
				},
			},
		},
	}

	placeholder := &ReplicaPlaceholder{rangeDesc: *desc}

	{
		s.mu.Lock()
		err := s.addPlaceholderLocked(placeholder)
		s.mu.Unlock()
		require.NoError(t, err)
	}

	_, pErr := s.processRaftSnapshotRequest(ctx, req,
		IncomingSnapshot{
			SnapUUID:    uuid.MakeV4(),
			Desc:        desc,
			placeholder: placeholder,
		},
	)
	require.NoError(t, pErr.GoError())

	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		numPlaceholders := len(s.mu.replicaPlaceholders)
		s.mu.Unlock()

		if numPlaceholders != 0 {
			return errors.Errorf("expected 0 placeholders, but found %d", numPlaceholders)
		}
		// The count of dropped placeholders is incremented after the placeholder
		// is removed (and while not holding Store.mu), so we need to perform the
		// check of the number of dropped placeholders in this retry loop.
		if n := atomic.LoadInt32(&s.counts.droppedPlaceholders); n != 1 {
			return errors.Errorf("expected 1 dropped placeholder, but found %d", n)
		}
		return nil
	})
}

type fakeSnapshotStream struct {
	nextResp *kvserverpb.SnapshotResponse
	nextErr  error
}

func (c fakeSnapshotStream) Recv() (*kvserverpb.SnapshotResponse, error) {
	return c.nextResp, c.nextErr
}

func (c fakeSnapshotStream) Send(request *kvserverpb.SnapshotRequest) error {
	return nil
}

type fakeStorePool struct {
	failedThrottles int
}

func (sp *fakeStorePool) Throttle(
	reason storepool.ThrottleReason, why string, toStoreID roachpb.StoreID,
) {
	switch reason {
	case storepool.ThrottleFailed:
		sp.failedThrottles++
	default:
		panic("unknown reason")
	}
}

// TestSendSnapshotThrottling tests the store pool throttling behavior of
// store.sendSnapshotUsingDelegate, ensuring that it properly updates the StorePool on
// various exceptional conditions and new capacity estimates.
func TestSendSnapshotThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	e := storage.NewDefaultInMemForTesting()
	defer e.Close()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tr := tracing.NewTracer()

	header := kvserverpb.SnapshotRequest_Header{
		State: kvserverpb.ReplicaState{
			Desc: &roachpb.RangeDescriptor{RangeID: 1},
		},
	}
	newBatch := e.NewWriteBatch

	// Test that a failed Recv() causes a fail throttle
	{
		sp := &fakeStorePool{}
		expectedErr := errors.New("")
		c := fakeSnapshotStream{nil, expectedErr}
		_, err := sendSnapshot(
			ctx, st, tr, c, sp, header, nil /* snap */, newBatch, nil /* sent */, nil, /* recordBytesSent */
		)
		if sp.failedThrottles != 1 {
			t.Fatalf("expected 1 failed throttle, but found %d", sp.failedThrottles)
		}
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %s, but found %s", err, expectedErr)
		}
	}

	// Test that an errored snapshot causes a fail throttle.
	{
		sp := &fakeStorePool{}
		resp := &kvserverpb.SnapshotResponse{
			Status:       kvserverpb.SnapshotResponse_ERROR,
			EncodedError: errors.EncodeError(ctx, errors.New("boom")),
		}
		c := fakeSnapshotStream{resp, nil}
		_, err := sendSnapshot(
			ctx, st, tr, c, sp, header, nil /* snap */, newBatch, nil /* sent */, nil, /* recordBytesSent */
		)
		if sp.failedThrottles != 1 {
			t.Fatalf("expected 1 failed throttle, but found %d", sp.failedThrottles)
		}
		if err == nil {
			t.Fatalf("expected error, found nil")
		}
	}
}

// TestSendSnapshotConcurrency tests the sending of concurrent snapshots and
// verifies they are only sent "2 at a time". This is not intended to test the
// prioritization of the snapshots as that is covered by the multi-queue
// testing.
func TestSendSnapshotConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)
	s := tc.store

	// Checking this now makes sure that if the defaults change this test will also.
	require.Equal(t, 2, s.snapshotSendQueue.AvailableLen())
	require.Equal(t, 0, s.snapshotSendQueue.QueueLen())
	cleanup1, err := s.reserveSendSnapshot(ctx, &kvserverpb.DelegateSendSnapshotRequest{
		SenderQueueName:     kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
		SenderQueuePriority: 1,
	}, 1)
	require.NoError(t, err)
	cleanup2, err := s.reserveSendSnapshot(ctx, &kvserverpb.DelegateSendSnapshotRequest{
		SenderQueueName:     kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
		SenderQueuePriority: 1,
	}, 1)
	require.NoError(t, err)
	require.Equal(t, 0, s.snapshotSendQueue.AvailableLen())
	require.Equal(t, 1, s.snapshotSendQueue.QueueLen())

	// At this point both the first two tasks will be holding reservations and
	// waiting for cleanup, a third task will block or fail First send one with
	// the queue length set to 0 - this will fail since the first tasks are still
	// running.
	_, err = s.reserveSendSnapshot(ctx, &kvserverpb.DelegateSendSnapshotRequest{
		SenderQueueName:     kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
		SenderQueuePriority: 1,
		QueueOnDelegateLen:  0,
	}, 1)
	require.Error(t, err)
	require.Equal(t, 0, s.snapshotSendQueue.AvailableLen())
	require.Equal(t, 1, s.snapshotSendQueue.QueueLen())

	// Now add a task that will wait indefinitely for another task to finish.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		before := timeutil.Now()
		cleanup3, err := s.reserveSendSnapshot(ctx, &kvserverpb.DelegateSendSnapshotRequest{
			SenderQueueName:     kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
			SenderQueuePriority: 1,
			QueueOnDelegateLen:  -1,
		}, 1)
		after := timeutil.Now()
		require.NoError(t, err)
		require.GreaterOrEqual(t, after.Sub(before), 10*time.Millisecond)
		cleanup3()
		wg.Done()
	}()

	// Now add one that will queue up and wait for another task to finish. This
	// task will not block for more than 8ms, but we want to wait for it to
	// complete to make sure it frees the permit.
	go func() {
		deadlineCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()

		// This will time out since the deadline is set artificially low. Make sure
		// the permit is released.
		_, err := s.reserveSendSnapshot(deadlineCtx, &kvserverpb.DelegateSendSnapshotRequest{
			SenderQueueName:     kvserverpb.SnapshotRequest_REPLICATE_QUEUE,
			SenderQueuePriority: 1,
			QueueOnDelegateLen:  -1,
		}, 1)
		require.Error(t, err)
		wg.Done()
	}()

	// Wait a little time before calling signaling the first two as complete.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, s.snapshotSendQueue.AvailableLen())
	// One remaining task are queued at this point.
	require.Equal(t, 2, s.snapshotSendQueue.QueueLen())

	cleanup1()
	cleanup2()

	// Wait until all cleanup run before checking the number of permits.
	wg.Wait()
	require.Equal(t, 2, s.snapshotSendQueue.AvailableLen())
	require.Equal(t, 0, s.snapshotSendQueue.QueueLen())
}

func TestReserveSnapshotThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)
	s := tc.store

	cleanupNonEmpty1, err := s.reserveReceiveSnapshot(ctx, &kvserverpb.SnapshotRequest_Header{
		RangeSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := s.ReservationCount(); n != 1 {
		t.Fatalf("expected 1 reservation, but found %d", n)
	}
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueLength.Value(),
		"unexpected snapshot queue length")
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueSize.Value(),
		"unexpected snapshot queue size")
	require.Equal(t, int64(1), s.Metrics().RangeSnapshotRecvInProgress.Value(),
		"unexpected snapshots in progress")
	require.Equal(t, int64(1), s.Metrics().RangeSnapshotRecvTotalInProgress.Value(),
		"unexpected snapshots in progress")

	// Ensure we allow a concurrent empty snapshot.
	cleanupEmpty, err := s.reserveReceiveSnapshot(ctx, &kvserverpb.SnapshotRequest_Header{})
	if err != nil {
		t.Fatal(err)
	}
	// Empty snapshots are not throttled and so do not increase the reservation
	// count.
	if n := s.ReservationCount(); n != 1 {
		t.Fatalf("expected 1 reservation, but found %d", n)
	}
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueLength.Value(),
		"unexpected snapshot queue length")
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueSize.Value(),
		"unexpected snapshot queue size")
	require.Equal(t, int64(1), s.Metrics().RangeSnapshotRecvInProgress.Value(),
		"unexpected snapshots in progress")
	require.Equal(t, int64(2), s.Metrics().RangeSnapshotRecvTotalInProgress.Value(),
		"unexpected snapshots in progress")
	cleanupEmpty()

	if n := s.ReservationCount(); n != 1 {
		t.Fatalf("expected 1 reservation, but found %d", n)
	}

	// Verify we block concurrent snapshots by spawning a goroutine which will
	// execute the cleanup after a short delay but only if another snapshot was
	// not allowed through.
	var boom int32
	go func() {
		time.Sleep(20 * time.Millisecond)
		if atomic.LoadInt32(&boom) == 0 {
			if s.Metrics().RangeSnapshotRecvQueueLength.Value() != int64(1) {
				t.Errorf("unexpected snapshot queue length; expected: %d, got: %d", 1,
					s.Metrics().RangeSnapshotRecvQueueLength.Value())
			}
			if s.Metrics().RangeSnapshotRecvQueueSize.Value() != int64(10) {
				t.Errorf("unexplected snapshot queue size; expected: %d, got: %d", 1,
					s.Metrics().RangeSnapshotRecvQueueSize.Value())
			}
			if s.Metrics().RangeSnapshotRecvInProgress.Value() != int64(1) {
				t.Errorf("unexpected snapshots in progress; expected: %d, got: %d", 1,
					s.Metrics().RangeSnapshotRecvInProgress.Value())
			}
			cleanupNonEmpty1()
		} else {
			t.Errorf("next snapshot acquired reservation before previous called cleanup()")
		}
	}()

	cleanupNonEmpty3, err := s.reserveReceiveSnapshot(ctx, &kvserverpb.SnapshotRequest_Header{
		RangeSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&boom, 1)
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueLength.Value(),
		"unexpected snapshot queue length")
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvQueueSize.Value(),
		"unexpected snapshot queue size")
	require.Equal(t, int64(1), s.Metrics().RangeSnapshotRecvInProgress.Value(),
		"unexpected snapshots in progress")
	require.Equal(t, int64(1), s.Metrics().RangeSnapshotRecvTotalInProgress.Value(),
		"unexpected snapshots in progress")
	cleanupNonEmpty3()
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvInProgress.Value(),
		"unexpected snapshots in progress")
	require.Equal(t, int64(0), s.Metrics().RangeSnapshotRecvTotalInProgress.Value(),
		"unexpected snapshots in progress")

	if n := s.ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}
}

// TestReserveSnapshotFullnessLimit documents that we have no mechanism to
// decline snapshots based on the remaining capacity of the target store.
func TestReserveSnapshotFullnessLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)
	s := tc.store

	desc, err := s.Descriptor(ctx, false /* useCached */)
	if err != nil {
		t.Fatal(err)
	}
	desc.Capacity.Available = 1
	desc.Capacity.Used = desc.Capacity.Capacity - desc.Capacity.Available

	s.cfg.StorePool.DetailsMu.Lock()
	s.cfg.StorePool.GetStoreDetailLocked(desc.StoreID).Desc = desc
	s.cfg.StorePool.DetailsMu.Unlock()

	if n := s.ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}

	// A snapshot should be allowed.
	cleanupAccepted, err := s.reserveReceiveSnapshot(ctx, &kvserverpb.SnapshotRequest_Header{
		RangeSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := s.ReservationCount(); n != 1 {
		t.Fatalf("expected 1 reservation, but found %d", n)
	}
	cleanupAccepted()

	// Even if the store isn't mostly full, a range that's larger than the
	// available disk space should be rejected.
	desc.Capacity.Available = desc.Capacity.Capacity / 2
	desc.Capacity.Used = desc.Capacity.Capacity - desc.Capacity.Available
	s.cfg.StorePool.DetailsMu.Lock()
	s.cfg.StorePool.GetStoreDetailLocked(desc.StoreID).Desc = desc
	s.cfg.StorePool.DetailsMu.Unlock()

	if n := s.ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}
}

// TestSnapshotReservationQueueTimeoutAvoidsStarvation verifies that the
// snapshot reservation queue applies a tighter queueing timeout than overall
// operation timeout to incoming snapshot requests, which enables it to avoid
// starvation even with its FIFO queueing policy under high concurrency.
func TestReserveSnapshotQueueTimeoutAvoidsStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderShort(t)

	// Run each snapshot with a 100-millisecond timeout, a 50-millisecond queue
	// timeout, and a 15-millisecond process time.
	const timeout = 100 * time.Millisecond
	const maxQueueTimeout = 50 * time.Millisecond
	const timeoutFrac = float64(maxQueueTimeout) / float64(timeout)
	const processTime = 15 * time.Millisecond
	// Run 8 workers that are each trying to perform snapshots for 3 seconds.
	const workers = 8
	const duration = 3 * time.Second
	// We expect that roughly duration / processTime snapshots "succeed". To avoid
	// flakiness, we assert half of this.
	const expSuccesses = int(duration / processTime)
	const assertSuccesses = expSuccesses / 2
	// Sanity check config. If workers*processTime < timeout, then the queue time
	// will never be large enough to create starvation. If timeout-maxQueueTimeout
	// < processTime then most snapshots won't have enough time to complete.
	require.Greater(t, workers*processTime, timeout)
	require.Greater(t, timeout-maxQueueTimeout, processTime)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tsc := TestStoreConfig(nil)
	// Set the concurrency to 1 explicitly, in case the default ever changes.
	tsc.SnapshotApplyLimit = 1
	tc := testContext{}
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	s := tc.store
	snapshotReservationQueueTimeoutFraction.Override(ctx, &s.ClusterSettings().SV, timeoutFrac)

	var done int64
	var successes int64
	var g errgroup.Group
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for atomic.LoadInt64(&done) == 0 {
				if err := func() error {
					snapCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					cleanup, err := s.reserveReceiveSnapshot(snapCtx, &kvserverpb.SnapshotRequest_Header{RangeSize: 1})
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							return nil
						}
						return err
					}
					defer cleanup()
					if atomic.LoadInt64(&done) != 0 {
						// If the test has ended, don't process.
						return nil
					}
					// Process...
					time.Sleep(processTime)
					// Check for sufficient processing time. If we hit a timeout, don't
					// count the process attempt as a success. We could make this more
					// reactive and terminate the sleep as soon as the ctx is canceled,
					// but let's assume the worst case.
					if err := snapCtx.Err(); err != nil {
						t.Logf("hit %v while processing", err)
					} else {
						atomic.AddInt64(&successes, 1)
					}
					return nil
				}(); err != nil {
					return err
				}
			}
			return nil
		})
	}

	time.Sleep(duration)
	atomic.StoreInt64(&done, 1)
	require.NoError(t, g.Wait())
	require.GreaterOrEqual(t, int(atomic.LoadInt64(&successes)), assertSuccesses)
}

func TestSnapshotRateLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()
	limit := rebalanceSnapshotRate.Get(&st.SV)
	require.Equal(t, int64(32<<20), limit)
}

type mockSpanConfigReader struct {
	real      spanconfig.StoreReader
	overrides map[string]roachpb.SpanConfig
}

func (m *mockSpanConfigReader) NeedsSplit(
	ctx context.Context, start, end roachpb.RKey,
) (bool, error) {
	panic("unimplemented")
}

func (m *mockSpanConfigReader) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	panic("unimplemented")
}

func (m *mockSpanConfigReader) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	if conf, ok := m.overrides[string(key)]; ok {
		return conf, nil
	}
	return m.GetSpanConfigForKey(ctx, key)
}

var _ spanconfig.StoreReader = &mockSpanConfigReader{}

// TestAllocatorCheckRangeUnconfigured tests evaluating the allocation decisions
// for a range with a single replica using the default system configuration and
// no other available allocation targets.
func TestAllocatorCheckRangeUnconfigured(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "confAvailable", func(t *testing.T, confAvailable bool) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		tc := testContext{}
		tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
		cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
		if !confAvailable {
			cfg.TestingKnobs.MakeSystemConfigSpanUnavailableToQueues = true
		}

		tc.StartWithStoreConfig(ctx, t, stopper, cfg)
		s := tc.store

		action, _, _, err := s.AllocatorCheckRange(ctx, tc.repl.Desc(),
			false /* collectTraces */, nil, /* overrideStorePool */
		)
		require.Error(t, err)

		if confAvailable {
			// Expect allocator error if range has nowhere to upreplicate.
			var allocatorError allocator.AllocationError
			require.ErrorAs(t, err, &allocatorError)
			require.Equal(t, allocatorimpl.AllocatorAddVoter, action)
		} else {
			// Expect error looking up spanConfig if we can't use the system config span,
			// as the spanconfig.KVSubscriber infrastructure is not initialized.
			require.ErrorIs(t, err, errSpanConfigsUnavailable)
			require.Equal(t, allocatorimpl.AllocatorNoop, action)
		}
	})
}

// TestAllocatorCheckRange runs a number of tests to check the allocator's
// range repair action and target based on a number of different configured
// stores.
func TestAllocatorCheckRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	defaultSystemSpanConfig := zonepb.DefaultSystemZoneConfigRef().AsSpanConfig()

	for _, tc := range []struct {
		name               string
		stores             []*roachpb.StoreDescriptor
		existingReplicas   []roachpb.ReplicaDescriptor
		spanConfig         *roachpb.SpanConfig
		livenessOverrides  map[roachpb.NodeID]livenesspb.NodeLivenessStatus
		expectedAction     allocatorimpl.AllocatorAction
		expectValidTarget  bool
		expectedTarget     roachpb.ReplicationTarget
		expectedLogMessage string
		expectErr          bool
		expectAllocatorErr bool
		expectedErr        error
		expectedErrStr     string
	}{
		{
			name:   "overreplicated",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
				{NodeID: 4, StoreID: 4, ReplicaID: 4},
			},
			livenessOverrides: nil,
			expectedAction:    allocatorimpl.AllocatorRemoveVoter,
			expectErr:         false,
		},
		{
			name:   "overreplicated but store dead",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
				{NodeID: 4, StoreID: 4, ReplicaID: 4},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DEAD,
			},
			expectedAction: allocatorimpl.AllocatorRemoveDeadVoter,
			expectErr:      false,
		},
		{
			name:   "decommissioning but underreplicated",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				2: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:    allocatorimpl.AllocatorAddVoter,
			expectErr:         false,
			expectValidTarget: true,
		},
		{
			name:   "decommissioning with replacement",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:    allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectErr:         false,
			expectValidTarget: true,
		},
		{
			name:   "decommissioning without valid replacement",
			stores: threeStores,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:     allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectAllocatorErr: true,
			expectedErrStr:     "likely not enough nodes in cluster",
		},
		{
			name:   "five to four nodes at RF five",
			stores: noLocalityStores, // Five stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
				{NodeID: 4, StoreID: 4, ReplicaID: 4},
				{NodeID: 5, StoreID: 5, ReplicaID: 5},
			},
			spanConfig: &defaultSystemSpanConfig,
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:    allocatorimpl.AllocatorRemoveDecommissioningVoter,
			expectErr:         false,
			expectValidTarget: false,
		},
		{
			name:   "five to two nodes at RF five replaces first",
			stores: noLocalityStores, // Five stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
				{NodeID: 4, StoreID: 4, ReplicaID: 4},
				{NodeID: 5, StoreID: 5, ReplicaID: 5},
			},
			spanConfig: &defaultSystemSpanConfig,
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				5: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:     allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectValidTarget:  false,
			expectAllocatorErr: true,
			expectedErrStr:     "likely not enough nodes in cluster",
		},
		{
			name:   "replace first when lowering effective RF",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				// Region "a"
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				// Region "b"
				{NodeID: 4, StoreID: 4, ReplicaID: 3},
				{NodeID: 5, StoreID: 5, ReplicaID: 4},
				// Region "c"
				{NodeID: 7, StoreID: 7, ReplicaID: 5},
			},
			spanConfig: &defaultSystemSpanConfig,
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				// Downsize to one node per region: 3,6,9.
				1: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				2: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				5: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				7: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				8: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:    allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectErr:         false,
			expectValidTarget: true,
		},
		{
			name:   "replace dead first when lowering effective RF",
			stores: multiRegionStores, // Nine stores
			existingReplicas: []roachpb.ReplicaDescriptor{
				// Region "a"
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				// Region "b"
				{NodeID: 4, StoreID: 4, ReplicaID: 3},
				{NodeID: 5, StoreID: 5, ReplicaID: 4},
				// Region "c"
				{NodeID: 7, StoreID: 7, ReplicaID: 5},
			},
			spanConfig: &defaultSystemSpanConfig,
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				// Downsize to: 1,4,7,9 but 7 is dead.
				// Replica on n7 should be replaced first.
				2: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				3: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				5: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				6: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				7: livenesspb.NodeLivenessStatus_DEAD,
				8: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			expectedAction:    allocatorimpl.AllocatorReplaceDeadVoter,
			expectErr:         false,
			expectValidTarget: true,
		},
		{
			name:   "decommissioning without satisfying partially constrained locality",
			stores: fourSingleStoreRacks,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 4, StoreID: 4, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			spanConfig: &roachpb.SpanConfig{
				NumReplicas: 3,
				Constraints: []roachpb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "4",
							},
						},
					},
				},
			},
			expectedAction:     allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectAllocatorErr: true,
			expectedErrStr:     "replicas must match constraints",
			expectedLogMessage: "cannot allocate necessary voter on s3",
		},
		{
			name:   "decommissioning without satisfying multiple partial constraints",
			stores: fourSingleStoreRacks,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 4, StoreID: 4, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			spanConfig: &roachpb.SpanConfig{
				NumReplicas: 3,
				Constraints: []roachpb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Value: "black",
							},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "4",
							},
						},
					},
				},
			},
			expectedAction:     allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectAllocatorErr: true,
			expectedErrStr:     "replicas must match constraints",
			expectedLogMessage: "cannot allocate necessary voter on s3",
		},
		{
			name:   "decommissioning during upreplication with partial constraints",
			stores: fourSingleStoreRacks,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			spanConfig: &roachpb.SpanConfig{
				NumReplicas: 3,
				Constraints: []roachpb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "4",
							},
						},
					},
				},
			},
			expectedAction:    allocatorimpl.AllocatorAddVoter,
			expectValidTarget: true,
		},
		{
			name:   "decommissioning with replacement satisfying locality",
			stores: multiRegionStores,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 4, StoreID: 4, ReplicaID: 2},
				{NodeID: 7, StoreID: 7, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				1: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				3: livenesspb.NodeLivenessStatus_DEAD,
			},
			spanConfig: &roachpb.SpanConfig{
				NumReplicas: 3,
				Constraints: []roachpb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "region",
								Value: "a",
							},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "region",
								Value: "b",
							},
						},
					},
				},
			},
			expectedAction:    allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectValidTarget: true,
			expectedTarget:    roachpb.ReplicationTarget{NodeID: 2, StoreID: 2},
			expectErr:         false,
		},
		{
			name:   "decommissioning without satisfying fully constrained locality",
			stores: fourSingleStoreRacks,
			existingReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 4, StoreID: 4, ReplicaID: 3},
			},
			livenessOverrides: map[roachpb.NodeID]livenesspb.NodeLivenessStatus{
				4: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
			},
			spanConfig: &roachpb.SpanConfig{
				NumReplicas: 3,
				Constraints: []roachpb.ConstraintsConjunction{
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "1",
							},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "2",
							},
						},
					},
					{
						NumReplicas: 1,
						Constraints: []roachpb.Constraint{
							{
								Type:  roachpb.Constraint_REQUIRED,
								Key:   "rack",
								Value: "4",
							},
						},
					},
				},
			},
			expectedAction:     allocatorimpl.AllocatorReplaceDecommissioningVoter,
			expectAllocatorErr: true,
			expectedErrStr:     "replicas must match constraints",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setup store pool based on store descriptors and configure test store.
			nodesSeen := make(map[roachpb.NodeID]struct{})
			for _, storeDesc := range tc.stores {
				nodesSeen[storeDesc.Node.NodeID] = struct{}{}
			}
			numNodes := len(nodesSeen)

			// Create a test store simulating n1s1, where we have other nodes/stores as
			// determined by the test configuration. As we do not start the test store,
			// queues will not be running.
			stopper, g, sp, _, manual := allocatorimpl.CreateTestAllocator(ctx, numNodes, false /* deterministic */)
			defer stopper.Stop(context.Background())
			gossiputil.NewStoreGossiper(g).GossipStores(tc.stores, t)

			clock := hlc.NewClockForTesting(manual)
			cfg := TestStoreConfig(clock)
			cfg.Gossip = g
			cfg.StorePool = sp
			if tc.spanConfig != nil {
				mockSr := &mockSpanConfigReader{
					real: cfg.SystemConfigProvider.GetSystemConfig(),
					overrides: map[string]roachpb.SpanConfig{
						"a": *tc.spanConfig,
					},
				}

				cfg.TestingKnobs.ConfReaderInterceptor = func() spanconfig.StoreReader {
					return mockSr
				}
			}

			s := createTestStoreWithoutStart(ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg)

			desc := &roachpb.RangeDescriptor{
				RangeID:          789,
				StartKey:         roachpb.RKey("a"),
				EndKey:           roachpb.RKey("b"),
				InternalReplicas: tc.existingReplicas,
			}

			var storePoolOverride storepool.AllocatorStorePool
			if len(tc.livenessOverrides) > 0 {
				livenessOverride := storepool.OverrideNodeLivenessFunc(tc.livenessOverrides, sp.NodeLivenessFn)
				nodeCountOverride := func() int {
					numDecomOverrides := 0
					for _, livenessStatus := range tc.livenessOverrides {
						if livenessStatus == livenesspb.NodeLivenessStatus_DECOMMISSIONING ||
							livenessStatus == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
							numDecomOverrides++
						}
					}
					return numNodes - numDecomOverrides
				}
				storePoolOverride = storepool.NewOverrideStorePool(sp, livenessOverride, nodeCountOverride)
			}

			// Execute actual allocator range repair check.
			action, target, recording, err := s.AllocatorCheckRange(ctx, desc,
				true /* collectTraces */, storePoolOverride,
			)

			require.Equalf(t, tc.expectedAction, action,
				"expected action \"%s\", got action \"%s\"", tc.expectedAction, action,
			)

			// Validate expectations from test case.
			if tc.expectErr || tc.expectAllocatorErr {
				require.Error(t, err)

				if tc.expectedErr != nil {
					require.ErrorIs(t, err, tc.expectedErr)
				}
				if tc.expectAllocatorErr {
					var allocatorError allocator.AllocationError
					require.ErrorAs(t, err, &allocatorError)
				}
				if tc.expectedErrStr != "" {
					require.ErrorContains(t, err, tc.expectedErrStr)
				}
			} else {
				require.NoError(t, err)
			}

			if tc.expectValidTarget {
				require.Falsef(t, roachpb.Empty(target), "expected valid target")
			}

			if !roachpb.Empty(tc.expectedTarget) {
				require.Equalf(t, tc.expectedTarget, target, "expected target %s, got %s",
					tc.expectedTarget, target,
				)
			}

			if tc.expectedLogMessage != "" {
				_, ok := recording.FindLogMessage(tc.expectedLogMessage)
				require.Truef(t, ok, "expected to find \"%s\" in trace:\n%s", tc.expectedLogMessage, recording)
			}
		})
	}
}

// TestManuallyEnqueueUninitializedReplica makes sure that uninitialized
// replicas cannot be enqueued.
func TestManuallyEnqueueUninitializedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)

	repl, _, _ := tc.store.getOrCreateReplica(ctx, 42, 7, &roachpb.ReplicaDescriptor{
		NodeID:    tc.store.NodeID(),
		StoreID:   tc.store.StoreID(),
		ReplicaID: 7,
	})
	_, _, err := tc.store.Enqueue(
		ctx, "replicaGC", repl, true /* skipShouldQueue */, false, /* async */
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enqueueing uninitialized replica")
}

// TestStoreGetOrCreateReplicaWritesRaftReplicaID tests that an uninitialized
// replica has a RaftReplicaID.
func TestStoreGetOrCreateReplicaWritesRaftReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)

	repl, created, err := tc.store.getOrCreateReplica(
		ctx, 42, 7, &roachpb.ReplicaDescriptor{
			NodeID:    tc.store.NodeID(),
			StoreID:   tc.store.StoreID(),
			ReplicaID: 7,
		})
	require.NoError(t, err)
	require.True(t, created)
	replicaID, err := repl.mu.stateLoader.LoadRaftReplicaID(ctx, tc.store.TODOEngine())
	require.NoError(t, err)
	require.Equal(t, &kvserverpb.RaftReplicaID{ReplicaID: 7}, replicaID)
}

func BenchmarkStoreGetReplica(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, b, testStoreOpts{createSystemRanges: true}, stopper)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := store.GetReplica(1)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
