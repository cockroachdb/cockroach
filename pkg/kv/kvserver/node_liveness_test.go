// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func verifyLiveness(t *testing.T, tc *testcluster.TestCluster) {
	testutils.SucceedsSoon(t, func() error {
		for _, s := range tc.Servers {
			return verifyLivenessServer(s, int64(len(tc.Servers)))
		}
		return nil
	})
}

func verifyLivenessServer(s serverutils.TestServerInterface, numServers int64) error {
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	if !nl.GetNodeVitalityFromCache(s.NodeID()).IsLive(livenesspb.TestingIsAliveAndHasHeartbeated) {
		return errors.Errorf("node %d not live", s.NodeID())
	}
	if a, e := nl.Metrics().LiveNodes.Value(), numServers; a != e {
		return errors.Errorf("expected node %d's LiveNodes metric to be %d; got %d",
			s.NodeID(), e, a)
	}
	return nil
}

func pauseNodeLivenessHeartbeatLoops(tc *testcluster.TestCluster) func() {
	var enableFns []func()
	for _, s := range tc.Servers {
		enableFns = append(enableFns, s.NodeLiveness().(*liveness.NodeLiveness).PauseHeartbeatLoopForTest())
	}
	return func() {
		for _, fn := range enableFns {
			fn()
		}
	}
}
func TestNodeLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	// Advance clock past the liveness threshold to verify IsLive becomes false.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).TestingGetLivenessThreshold().Nanoseconds() + 1)

	for _, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		nodeID := s.NodeID()
		if nl.GetNodeVitalityFromCache(nodeID).IsLive(livenesspb.IsAliveNotification) {
			t.Errorf("expected node %d to be considered not-live after advancing node clock", nodeID)
		}
		testutils.SucceedsSoon(t, func() error {
			if a, e := nl.Metrics().LiveNodes.Value(), int64(0); a != e {
				return errors.Errorf("expected node %d's LiveNodes metric to be %d; got %d",
					nodeID, e, a)
			}
			return nil
		})
	}
	// Trigger a manual heartbeat and verify liveness is reestablished.
	for _, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		l, ok := nl.Self()
		assert.True(t, ok)
		for {
			err := nl.Heartbeat(context.Background(), l)
			if err == nil {
				break
			}
			if errors.Is(err, liveness.ErrEpochIncremented) {
				log.Warningf(context.Background(), "retrying after %s", err)
				continue
			}

			t.Fatal(err)
		}
	}
	verifyLiveness(t, tc)

	// Verify metrics counts.
	for i, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		if c := nl.Metrics().HeartbeatSuccesses.Count(); c < 2 {
			t.Errorf("node %d: expected metrics count >= 2; got %d", (i + 1), c)
		}
	}
}

func TestNodeLivenessInitialIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgs: base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:    true,
						StickyVFSID: "1",
					},
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: stickyVFSRegistry,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	nl, ok := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(tc.Servers[0].NodeID())
	assert.True(t, ok)
	if nl.Epoch != 1 {
		t.Fatalf("expected epoch to be set to 1 initially; got %d; liveness %s", nl.Epoch, nl)
	}

	// Restart the node and verify the epoch is incremented with initial heartbeat.
	require.NoError(t, tc.Restart())
	verifyEpochIncremented(t, tc, 0)
}

func verifyEpochIncremented(t *testing.T, tc *testcluster.TestCluster, nodeIdx int) {
	testutils.SucceedsSoon(t, func() error {
		liv, ok := tc.Servers[nodeIdx].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(tc.Servers[nodeIdx].NodeID())
		if !ok {
			return errors.New("liveness not found")
		}
		if liv.Epoch < 2 {
			return errors.Errorf("expected epoch to be >=2 on restart but was %d", liv.Epoch)
		}
		return nil
	})
}

// TestRedundantNodeLivenessHeartbeatsAvoided tests that in a thundering herd
// scenario with many goroutines rush to synchronously heartbeat a node's
// liveness record, redundant heartbeats are detected and avoided.
func TestRedundantNodeLivenessHeartbeatsAvoided(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	defer s.Stopper().Stop(ctx)

	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	nlActive, _ := store.GetStoreConfig().NodeLivenessDurations()

	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})
	nl.PauseHeartbeatLoopForTest()
	enableSync := nl.PauseSynchronousHeartbeatsForTest()

	nlSelf, ok := nl.Self()
	assert.True(t, ok)
	hbBefore := nl.Metrics().HeartbeatSuccesses.Count()
	require.Equal(t, int64(0), nl.Metrics().HeartbeatsInFlight.Value())

	// Issue a set of synchronous node liveness heartbeats. Mimic the kind of
	// thundering herd we see due to lease acquisitions when a node's liveness
	// epoch is incremented.
	var g errgroup.Group
	const herdSize = 30
	for i := 0; i < herdSize; i++ {
		g.Go(func() error {
			before := s.Clock().Now()
			if err := nl.Heartbeat(ctx, nlSelf); err != nil {
				return err
			}
			livenessAfter, found := nl.Self()
			assert.True(t, found)
			exp := livenessAfter.Expiration
			minExp := before.Add(nlActive.Nanoseconds(), 0).ToLegacyTimestamp()
			if exp.Less(minExp) {
				return errors.Errorf("expected min expiration %v, found %v", minExp, exp)
			}
			return nil
		})
	}

	// Wait for all heartbeats to be in-flight, at which point they will have
	// already computed their minimum expiration time.
	testutils.SucceedsSoon(t, func() error {
		inFlight := nl.Metrics().HeartbeatsInFlight.Value()
		if inFlight < herdSize {
			return errors.Errorf("not all heartbeats in-flight, want %d, got %d", herdSize, inFlight)
		} else if inFlight > herdSize {
			t.Fatalf("unexpected in-flight heartbeat count: %d", inFlight)
		}
		return nil
	})

	// Allow the heartbeats to proceed. Only a single one should end up touching
	// the liveness record. The rest should be considered redundant.
	enableSync()
	require.NoError(t, g.Wait())
	require.Equal(t, hbBefore+1, nl.Metrics().HeartbeatSuccesses.Count())
	require.Equal(t, int64(0), nl.Metrics().HeartbeatsInFlight.Value())

	// Send one more heartbeat. Should update liveness record.
	nlSelf, ok = nl.Self()
	require.True(t, ok)
	require.NoError(t, nl.Heartbeat(ctx, nlSelf))
	require.Equal(t, hbBefore+2, nl.Metrics().HeartbeatSuccesses.Count())
	require.Equal(t, int64(0), nl.Metrics().HeartbeatsInFlight.Value())
}

// TestNodeIsLiveCallback verifies that the liveness callback for a
// node is invoked when it changes from state false to true.
func TestNodeIsLiveCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	var started atomic.Bool
	var cbMu syncutil.Mutex
	cbs := map[roachpb.NodeID]struct{}{}
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,

			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
					NodeLiveness: kvserver.NodeLivenessTestingKnobs{
						IsLiveCallback: func(l livenesspb.Liveness) {
							if started.Load() {
								cbMu.Lock()
								defer cbMu.Unlock()
								cbs[l.NodeID] = struct{}{}
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)
	// Only record entires after we have paused the normal heartbeat loop to make
	// sure they come from the Heartbeat below.
	started.Store(true)

	// Advance clock past the liveness threshold.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).TestingGetLivenessThreshold().Nanoseconds() + 1)

	// Trigger a manual heartbeat and verify callbacks for each node ID are invoked.
	for _, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		l, ok := nl.Self()
		assert.True(t, ok)
		if err := nl.Heartbeat(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}

	testutils.SucceedsSoon(t, func() error {
		cbMu.Lock()
		defer cbMu.Unlock()
		for _, s := range tc.Servers {
			nodeID := s.NodeID()
			if _, ok := cbs[nodeID]; !ok {
				return errors.Errorf("expected IsLive callback for node %d", nodeID)
			}
		}
		return nil
	})
}

// TestNodeHeartbeatCallback verifies that HeartbeatCallback is invoked whenever
// this node updates its own liveness status.
func TestNodeHeartbeatCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	// Verify that last update time has been set for all nodes.
	verifyUptimes := func() {
		for i := range tc.Servers {
			s := tc.GetFirstStoreFromServer(t, i)
			uptm, err := s.ReadLastUpTimestamp(context.Background())
			require.NoError(t, err)
			require.NotZero(t, uptm)
		}
	}

	verifyUptimes()
}

// TestNodeLivenessEpochIncrement verifies that incrementing the epoch
// of a node requires the node to be considered not-live and that on
// increment, no other nodes believe the epoch-incremented node to be
// live.
func TestNodeLivenessEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	// First try to increment the epoch of a known-live node.
	deadNodeID := tc.Servers[1].NodeID()
	oldLiveness, ok := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(deadNodeID)
	assert.True(t, ok)
	if err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !testutils.IsError(err, "cannot increment epoch on live node") {
		t.Fatalf("expected error incrementing a live node: %+v", err)
	}

	// Advance clock past liveness threshold & increment epoch.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).TestingGetLivenessThreshold().Nanoseconds() + 1)
	if err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(ctx, oldLiveness.Liveness); err != nil {
		t.Fatalf("unexpected error incrementing a non-live node: %+v", err)
	}

	// Verify that the epoch has been advanced.
	testutils.SucceedsSoon(t, func() error {
		newLiveness, ok := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(deadNodeID)
		if !ok {
			return errors.New("liveness not found")
		}
		if newLiveness.Epoch != oldLiveness.Epoch+1 {
			return errors.Errorf("expected epoch to increment")
		}
		if newLiveness.Expiration != oldLiveness.Expiration {
			return errors.Errorf("expected expiration to remain unchanged")
		}
		if tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetNodeVitalityFromCache(deadNodeID).IsLive(livenesspb.IsAliveNotification) {
			return errors.Errorf("expected dead node to remain dead after epoch increment")
		}
		return nil
	})

	// Verify epoch increment metric count.
	if c := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).Metrics().EpochIncrements.Count(); c != 1 {
		t.Errorf("expected epoch increment == 1; got %d", c)
	}

	// Verify error on incrementing an already-incremented epoch.
	if err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !errors.Is(err, liveness.ErrEpochAlreadyIncremented) {
		t.Fatalf("unexpected error incrementing a non-live node: %+v", err)
	}

	// Verify error incrementing with a too-high expectation for liveness epoch.
	oldLiveness.Epoch = 3
	if err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !testutils.IsError(err, "unexpected liveness epoch 2; expected >= 3") {
		t.Fatalf("expected error incrementing with a too-high expected epoch: %+v", err)
	}
}

// TestNodeLivenessRestart verifies that if nodes are shutdown and
// restarted, the node liveness records are re-gossiped immediately.
func TestNodeLivenessRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()

	const numServers int = 2
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// After verifying node is in liveness table, stop store.
	verifyLiveness(t, tc)
	tc.StopServer(1)

	// Clear the liveness records in store 1's gossip to make sure we're
	// seeing the liveness record properly gossiped at store startup.
	var expKeys []string
	for _, s := range tc.Servers {
		nodeID := s.NodeID()
		key := gossip.MakeNodeLivenessKey(nodeID)
		expKeys = append(expKeys, key)
		if err := s.GossipI().(*gossip.Gossip).
			AddInfoProto(key, &livenesspb.Liveness{NodeID: nodeID}, 0); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(expKeys)

	// Register a callback to gossip in order to verify liveness records
	// are re-gossiped.
	var keysMu struct {
		syncutil.Mutex
		keys []string
	}

	// Restart store and verify gossip contains liveness record for nodes 1&2.
	require.NoError(t, tc.RestartServerWithInspect(1, func(s serverutils.TestServerInterface) {
		livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
		s.GossipI().(*gossip.Gossip).
			RegisterCallback(livenessRegex, func(key string, _ roachpb.Value) {
				keysMu.Lock()
				defer keysMu.Unlock()
				for _, k := range keysMu.keys {
					if k == key {
						return
					}
				}
				keysMu.keys = append(keysMu.keys, key)
			})
	}))
	testutils.SucceedsSoon(t, func() error {
		keysMu.Lock()
		defer keysMu.Unlock()
		sort.Strings(keysMu.keys)
		if !reflect.DeepEqual(keysMu.keys, expKeys) {
			return errors.Errorf("expected keys %+v != keys %+v", expKeys, keysMu.keys)
		}
		return nil
	})
}

// TestNodeLivenessSelf verifies that a node keeps its own most recent liveness
// heartbeat info in preference to anything which might be received belatedly
// through gossip.
//
// Note that this test originally injected a Gossip update with a higher Epoch
// and semantics have since changed to make the "self" record less special. It
// is updated like any other node's record, with appropriate safeguards against
// clobbering in place.
func TestNodeLivenessSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	g := s.GossipI().(*gossip.Gossip)
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	nl.PauseHeartbeatLoopForTest()

	// Verify liveness is properly initialized. This needs to be wrapped in a
	// SucceedsSoon because node liveness gets initialized via an async gossip
	// callback.
	var livenessRecord liveness.Record
	testutils.SucceedsSoon(t, func() error {
		l, ok := nl.GetLiveness(g.NodeID.Get())
		if !ok {
			return errors.New("liveness not found")
		}
		livenessRecord = l
		return nil
	})
	if err := nl.Heartbeat(context.Background(), livenessRecord.Liveness); err != nil {
		t.Fatal(err)
	}

	// Gossip random nonsense for liveness and verify that asking for
	// the node's own node ID returns the "correct" value.
	key := gossip.MakeNodeLivenessKey(g.NodeID.Get())
	var count int32
	g.RegisterCallback(key, func(_ string, val roachpb.Value) {
		atomic.AddInt32(&count, 1)
	})
	testutils.SucceedsSoon(t, func() error {
		fakeBehindLiveness := livenessRecord
		fakeBehindLiveness.Epoch-- // almost certainly results in zero

		if err := g.AddInfoProto(key, &fakeBehindLiveness, 0); err != nil {
			t.Fatal(err)
		}
		if atomic.LoadInt32(&count) < 2 {
			return errors.New("expected count >= 2")
		}
		return nil
	})

	// Self should not see the fake liveness, but have kept the real one.
	lGetRec, ok := nl.GetLiveness(g.NodeID.Get())
	require.True(t, ok)
	lGet := lGetRec.Liveness
	lSelf, ok := nl.Self()
	assert.True(t, ok)
	if !reflect.DeepEqual(lGet, lSelf) {
		t.Errorf("expected GetLiveness() to return same value as Self(): %+v != %+v", lGet, lSelf)
	}
	if lGet.Epoch == 2 || lSelf.NodeID == 2 {
		t.Errorf("expected GetLiveness() and Self() not to return artificially gossiped liveness: %+v, %+v", lGet, lSelf)
	}
}

func TestNodeLivenessGetIsLiveMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)
	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	require.True(t, nl.GetNodeVitalityFromCache(1).IsLive(livenesspb.IsAliveNotification))
	require.True(t, nl.GetNodeVitalityFromCache(2).IsLive(livenesspb.IsAliveNotification))
	require.True(t, nl.GetNodeVitalityFromCache(3).IsLive(livenesspb.IsAliveNotification))

	// Advance the clock but only heartbeat node 0.
	manualClock.Increment(nl.TestingGetLivenessThreshold().Nanoseconds() + 1)
	var livenessRec liveness.Record
	testutils.SucceedsSoon(t, func() error {
		lr, ok := nl.GetLiveness(tc.Servers[0].NodeID())
		if !ok {
			return errors.New("liveness not found")
		}
		livenessRec = lr
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		if err := nl.Heartbeat(context.Background(), livenessRec.Liveness); err != nil {
			if errors.Is(err, liveness.ErrEpochIncremented) {
				return err
			}
			t.Fatal(err)
		}
		return nil
	})

	// Now verify only node 0 is live.
	require.True(t, nl.GetNodeVitalityFromCache(1).IsLive(livenesspb.IsAliveNotification))
	require.False(t, nl.GetNodeVitalityFromCache(2).IsLive(livenesspb.IsAliveNotification))
	require.False(t, nl.GetNodeVitalityFromCache(3).IsLive(livenesspb.IsAliveNotification))
}

func TestNodeLivenessGetLivenesses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	testStartTime := manualClock.UnixNano()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	actualLMapNodes := make(map[roachpb.NodeID]struct{})
	originalExpiration := testStartTime + nl.TestingGetLivenessThreshold().Nanoseconds()
	for id, v := range nl.ScanNodeVitalityFromCache() {
		if a, e := v.GetInternalLiveness().Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		if a, e := v.GetInternalLiveness().Expiration.WallTime, originalExpiration; a < e {
			t.Errorf("liveness record had expiration %d, wanted %d", a, e)
		}
		actualLMapNodes[id] = struct{}{}
	}
	expectedLMapNodes := map[roachpb.NodeID]struct{}{1: {}, 2: {}, 3: {}}
	if !reflect.DeepEqual(actualLMapNodes, expectedLMapNodes) {
		t.Errorf("got liveness map nodes %+v; wanted %+v", actualLMapNodes, expectedLMapNodes)
	}

	// Advance the clock but only heartbeat node 0.
	manualClock.Increment(nl.TestingGetLivenessThreshold().Nanoseconds() + 1)
	var livenessRecord liveness.Record
	testutils.SucceedsSoon(t, func() error {
		livenessRec, ok := nl.GetLiveness(tc.Servers[0].NodeID())
		if !ok {
			return errors.New("liveness not found")
		}
		livenessRecord = livenessRec
		return nil
	})
	if err := nl.Heartbeat(context.Background(), livenessRecord.Liveness); err != nil {
		t.Fatal(err)
	}

	// Verify that node liveness receives the change.
	actualLMapNodes = make(map[roachpb.NodeID]struct{})
	for id, v := range nl.ScanNodeVitalityFromCache() {
		if a, e := v.GetInternalLiveness().Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		expectedExpiration := originalExpiration
		if id == 1 {
			expectedExpiration += nl.TestingGetLivenessThreshold().Nanoseconds() + 1
		}
		if a, e := v.GetInternalLiveness().Expiration.WallTime, expectedExpiration; a < e {
			t.Errorf("liveness record had expiration %d, wanted %d", a, e)
		}
		actualLMapNodes[id] = struct{}{}
	}
	if !reflect.DeepEqual(actualLMapNodes, expectedLMapNodes) {
		t.Errorf("got liveness map nodes %+v; wanted %+v", actualLMapNodes, expectedLMapNodes)
	}
}

// TestNodeLivenessConcurrentHeartbeats verifies that concurrent attempts
// to heartbeat all succeed.
func TestNodeLivenessConcurrentHeartbeats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manualClock,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	nl.PauseHeartbeatLoopForTest()

	const concurrency = 10

	// Advance clock past the liveness threshold & concurrently heartbeat node.
	manualClock.Increment(nl.TestingGetLivenessThreshold().Nanoseconds() + 1)
	l, ok := nl.Self()
	assert.True(t, ok)
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			errCh <- nl.Heartbeat(context.Background(), l)
		}()
	}
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent heartbeat %d failed: %+v", i, err)
		}
	}
}

// TestNodeLivenessConcurrentIncrementEpochs verifies concurrent
// attempts to increment liveness of another node all succeed.
func TestNodeLivenessConcurrentIncrementEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	const concurrency = 10

	// Advance the clock and this time increment epoch concurrently for node 1.
	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	manualClock.Increment(nl.TestingGetLivenessThreshold().Nanoseconds() + 1)
	l, ok := nl.GetLiveness(tc.Servers[1].NodeID())
	assert.True(t, ok)
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			errCh <- nl.IncrementEpoch(context.Background(), l.Liveness)
		}()
	}
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, liveness.ErrEpochAlreadyIncremented) {
			t.Fatalf("concurrent increment epoch %d failed: %+v", i, err)
		}
	}
}

// TestNodeLivenessSetDraining verifies that when draining, a node's liveness
// record is updated and the node will not be present in the store list of other
// nodes once they are aware of its draining state.
func TestNodeLivenessSetDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	drainingNodeIdx := 0
	drainingNodeID := tc.Servers[0].NodeID()

	nodeIDAppearsInStoreList := func(id roachpb.NodeID, sl storepool.StoreList) bool {
		for _, store := range sl.TestingStores() {
			if store.Node.NodeID == id {
				return true
			}
		}
		return false
	}

	// Verify success on failed update of a liveness record that already has the
	// given draining setting.
	if err := tc.Servers[drainingNodeIdx].NodeLiveness().(*liveness.NodeLiveness).TestingSetDrainingInternal(
		ctx, liveness.Record{Liveness: livenesspb.Liveness{
			NodeID: drainingNodeID,
		}}, false,
	); err != nil {
		t.Fatal(err)
	}

	if err := tc.Servers[drainingNodeIdx].NodeLiveness().(*liveness.NodeLiveness).SetDraining(ctx, true /* drain */, nil /* reporter */); err != nil {
		t.Fatal(err)
	}

	// Draining node disappears from store lists.
	{
		const expectedLive = 2
		// Executed in a retry loop to wait until the new liveness record has
		// been gossiped to the rest of the cluster.
		testutils.SucceedsSoon(t, func() error {
			for i, s := range tc.Servers {
				curNodeID := s.NodeID()
				sl, alive, _ := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.TestingGetStoreList()
				if alive != expectedLive {
					return errors.Errorf(
						"expected %d live stores but got %d from node %d",
						expectedLive,
						alive,
						curNodeID,
					)
				}
				if nodeIDAppearsInStoreList(drainingNodeID, sl) {
					return errors.Errorf(
						"expected node %d not to appear in node %d's store list",
						drainingNodeID,
						curNodeID,
					)
				}
			}
			return nil
		})
	}

	// Stop and restart the store to verify that a restarted server clears the
	// draining field on the liveness record.
	tc.StopServer(drainingNodeIdx)
	require.NoError(t, tc.RestartServer(drainingNodeIdx))

	// Restarted node appears once again in the store list.
	{
		const expectedLive = 3
		// Executed in a retry loop to wait until the new liveness record has
		// been gossiped to the rest of the cluster.
		testutils.SucceedsSoon(t, func() error {
			for i, s := range tc.Servers {
				curNodeID := s.NodeID()
				sl, alive, _ := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.TestingGetStoreList()
				if alive != expectedLive {
					return errors.Errorf(
						"expected %d live stores but got %d from node %d",
						expectedLive,
						alive,
						curNodeID,
					)
				}
				if !nodeIDAppearsInStoreList(drainingNodeID, sl) {
					return errors.Errorf(
						"expected node %d to appear in node %d's store list: %+v",
						drainingNodeID,
						curNodeID,
						sl.TestingStores(),
					)
				}
			}
			return nil
		})
	}
}

func TestNodeLivenessRetryAmbiguousResultError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var injectError atomic.Bool
	var injectedErrorCount atomic.Int32

	injectError.Store(true)
	testingEvalFilter := func(args kvserverbase.FilterArgs) *kvpb.Error {
		if _, ok := args.Req.(*kvpb.ConditionalPutRequest); !ok {
			return nil
		}
		if injectError.Swap(false) {
			injectedErrorCount.Add(1)
			return kvpb.NewError(kvpb.NewAmbiguousResultErrorf("test"))
		}
		return nil
	}
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: testingEvalFilter,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Verify retry of the ambiguous result for heartbeat loop.
	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})

	// And again on manual heartbeat.
	// NOTE: we make sure to set pause the heartbeat loop before we grab the
	// liveness record to ensure that we don't race with the heartbeat loop in
	// a way that allows our manual heartbeat to short-circuit.
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	defer nl.PauseHeartbeatLoopForTest()

	injectError.Store(true)
	l, ok := nl.Self()
	assert.True(t, ok)
	require.NoError(t, nl.Heartbeat(context.Background(), l))

	// TODO(baptist): Once we remove epoch leases, we should remove the manual
	// Heartbeat call on node liveness and remove the manual heartbeat from this
	// test.
	// Verify that the error was injected twice (or fewer times).
	// We mostly expect exactly twice but it's been tricky to actually make this
	// be true in all cases (see #126040, which didn't manage).
	require.LessOrEqual(t, injectedErrorCount.Load(), int32(2))
}

// This tests the create code path for node liveness, for that we need to create
// a cluster of nodes, because we need to exercise to join RPC codepath.
func TestNodeLivenessRetryAmbiguousResultOnCreateError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := []struct {
		err error
	}{
		{kvpb.NewAmbiguousResultErrorf("test")},
		{kvpb.NewTransactionStatusError(kvpb.TransactionStatusError_REASON_UNKNOWN, "foo")},
		{kv.OnePCNotAllowedError{}},
	}
	for _, tc := range testcases {
		t.Run(tc.err.Error(), func(t *testing.T) {

			// Keeps track of node IDs that have errored.
			var errored syncutil.Set[roachpb.NodeID]

			testingEvalFilter := func(args kvserverbase.FilterArgs) *kvpb.Error {
				if req, ok := args.Req.(*kvpb.ConditionalPutRequest); ok {
					if !keys.NodeLivenessSpan.ContainsKey(req.Key) {
						return nil
					}
					var liveness livenesspb.Liveness
					assert.NoError(t, req.Value.GetProto(&liveness))
					if errored.Add(liveness.NodeID) {
						if liveness.NodeID != 1 {
							// We expect this to come from the create code path on all nodes
							// except the first. Make sure that is actually true.
							assert.Zero(t, liveness.Epoch)
						}
						return kvpb.NewError(tc.err)
					}
				}
				return nil
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, 3,
				base.TestClusterArgs{
					ReplicationMode: base.ReplicationManual,
					ServerArgs: base.TestServerArgs{
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
									TestingEvalFilter: testingEvalFilter,
								},
							},
						},
					},
				})
			defer tc.Stopper().Stop(ctx)

			for _, s := range tc.Servers {
				// Verify retry of the ambiguous result for heartbeat loop.
				testutils.SucceedsSoon(t, func() error {
					return verifyLivenessServer(s, 3)
				})
				_, ok := s.NodeLiveness().(*liveness.NodeLiveness).Self()
				require.True(t, ok)
				ok = errored.Contains(s.NodeID())
				require.True(t, ok)
			}
		})
	}
}

// Test that, although a liveness heartbeat is generally retried on
// AmbiguousResultError (see test above), it is not retried when the error is
// caused by a canceled context.
func TestNodeLivenessNoRetryOnAmbiguousResultCausedByCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var sem chan struct{}
	testingEvalFilter := func(args kvserverbase.FilterArgs) *kvpb.Error {
		// Maybe trap a liveness heartbeat.
		_, ok := args.Req.(*kvpb.ConditionalPutRequest)
		if !ok {
			return nil
		}
		if !bytes.HasPrefix(args.Req.Header().Key, keys.NodeLivenessPrefix) {
			return nil
		}

		if sem == nil {
			return nil
		}

		// Block the request.
		sem <- struct{}{}
		<-sem
		return nil
	}
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: testingEvalFilter,
				},
			},
			DialerKnobs: nodedialer.DialerTestingKnobs{
				// We're going to cancel a client RPC context and we want that
				// cancellation to disconnect the client from the server. That only
				// happens when going through gRPC, not when optimizing gRPC away.
				TestingNoLocalClientOptimization: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	nl := s.NodeLiveness().(*liveness.NodeLiveness)

	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})

	// We want to control the heartbeats.
	nl.PauseHeartbeatLoopForTest()

	sem = make(chan struct{})

	l, ok := nl.Self()
	require.True(t, ok)

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		// Wait for a trapped heartbeat.
		<-sem
		// Cancel the RPC. This should cause the DistSender to return an AmbiguousResultError.
		cancel()
	}()

	err := nl.Heartbeat(hbCtx, l)

	// Now that the client has gotten a response, unblock the evaluation on the
	// server.
	sem <- struct{}{}

	// Check that Heartbeat() returned an ambiguous error, and take that as proof
	// that the heartbeat wasn't retried.
	require.True(t, errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)), "%+v", err)
}

func verifyNodeIsDecommissioning(t *testing.T, tc *testcluster.TestCluster, nodeID roachpb.NodeID) {
	testutils.SucceedsSoon(t, func() error {
		for _, s := range tc.Servers {
			liv, _ := s.NodeLiveness().(*liveness.NodeLiveness).GetLiveness(nodeID)
			if !liv.Membership.Decommissioning() {
				return errors.Errorf("unexpected Membership value of %v for node %v", liv.Membership, liv.NodeID)
			}
		}
		return nil
	})
}

func testNodeLivenessSetDecommissioning(t *testing.T, decommissionNodeIdx int) {
	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	callerNodeLiveness := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	nodeID := tc.Servers[decommissionNodeIdx].NodeID()

	// Verify success on failed update of a liveness record that already has the
	// given decommissioning setting.
	oldLivenessRec, ok := callerNodeLiveness.GetLiveness(nodeID)
	assert.True(t, ok)
	if _, err := callerNodeLiveness.TestingSetDecommissioningInternal(
		ctx, oldLivenessRec, livenesspb.MembershipStatus_ACTIVE,
	); err != nil {
		t.Fatal(err)
	}

	// Set a node to decommissioning state.
	if _, err := callerNodeLiveness.SetMembershipStatus(
		ctx, nodeID, livenesspb.MembershipStatus_DECOMMISSIONING); err != nil {
		t.Fatal(err)
	}
	verifyNodeIsDecommissioning(t, tc, nodeID)

	// Stop and restart the store to verify that a restarted server retains the
	// decommissioning field on the liveness record.
	tc.StopServer(decommissionNodeIdx)
	require.NoError(t, tc.RestartServer(decommissionNodeIdx))

	// Wait until store has restarted and published a new heartbeat to ensure not
	// looking at pre-restart state. Want to be sure test fails if node wiped the
	// decommission flag.
	verifyEpochIncremented(t, tc, decommissionNodeIdx)
	verifyNodeIsDecommissioning(t, tc, nodeID)
}

// TestNodeLivenessSetDecommissioning verifies that when decommissioning, a
// node's liveness record is updated and remains after restart.
func TestNodeLivenessSetDecommissioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Sets itself to decommissioning.
	testNodeLivenessSetDecommissioning(t, 0)
	// Set another node to decommissioning.
	testNodeLivenessSetDecommissioning(t, 1)
}

// TestNodeLivenessDecommissionAbsent exercises a scenario in which a node is
// asked to decommission another node whose liveness record is not gossiped any
// more.
//
// See (*NodeLiveness).SetDecommissioning for details.
func TestNodeLivenessDecommissionAbsent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	const goneNodeID = roachpb.NodeID(10000)
	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	nl1 := tc.Servers[1].NodeLiveness().(*liveness.NodeLiveness)
	nl2 := tc.Servers[1].NodeLiveness().(*liveness.NodeLiveness)

	// When the node simply never existed, expect an error.
	if _, err := nl.SetMembershipStatus(
		ctx, goneNodeID, livenesspb.MembershipStatus_DECOMMISSIONING,
	); !errors.Is(err, liveness.ErrMissingRecord) {
		t.Fatal(err)
	}

	// Pretend the node was once there but isn't gossiped anywhere.
	if err := tc.Servers[0].DB().CPut(ctx, keys.NodeLivenessKey(goneNodeID), &livenesspb.Liveness{
		NodeID:     goneNodeID,
		Epoch:      1,
		Expiration: tc.Servers[0].Clock().Now().ToLegacyTimestamp(),
		Membership: livenesspb.MembershipStatus_ACTIVE,
	}, nil); err != nil {
		t.Fatal(err)
	}

	setMembershipStatus := func(nodeLiveness *liveness.NodeLiveness,
		status livenesspb.MembershipStatus, shouldCommit bool) {
		if committed, err := nodeLiveness.SetMembershipStatus(
			ctx, goneNodeID, status); err != nil {
			t.Fatal(err)
		} else {
			require.Equal(t, committed, shouldCommit)
		}
	}

	// Decommission from second node.
	setMembershipStatus(nl1, livenesspb.MembershipStatus_DECOMMISSIONING, true)
	// Re-decommission from first node.
	setMembershipStatus(nl, livenesspb.MembershipStatus_DECOMMISSIONING, false)
	// Recommission from first node.
	setMembershipStatus(nl, livenesspb.MembershipStatus_ACTIVE, true)
	// Decommission from second node (a second time).
	setMembershipStatus(nl1, livenesspb.MembershipStatus_DECOMMISSIONING, true)
	// Recommission from third node.
	setMembershipStatus(nl2, livenesspb.MembershipStatus_ACTIVE, true)
}

func BenchmarkNodeLivenessScanStorage(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	const numNodes = 100
	setupEng := func(b *testing.B, numLiveVersions int, haveFullyDeadKeys bool) storage.Engine {
		eng := storage.NewDefaultInMemForTesting(storage.DisableAutomaticCompactions)
		// 20 per minute, so 1000 represents 50 min of liveness writes in a level.
		// This is unusual, but we can have such accumulation if flushes and
		// compactions are rare.
		const numVersionsPerLevel = 1000
		// All versions in level l will be deleted in level l+1. The versions
		// written at the highest level are not deleted and the number of these
		// per node is controlled by numLiveVersions. Additionally, if
		// haveFullyDeadKeys is true, the highest level only has live versions for
		// alternating nodes.
		//
		// NB: haveFullyDeadKeys=true is not representative of NodeLiveness, since
		// we don't remove decommissioned entries. It is included here just to
		// understand the effect on the NextPrefix optimization.
		const numLevels = 7
		tsFunc := func(l int, v int) int64 {
			return int64(l*numVersionsPerLevel + v + 10)
		}
		for l := 0; l < numLevels; l++ {
			for v := 0; v < numVersionsPerLevel; v++ {
				ts := tsFunc(l, v)
				for n := roachpb.NodeID(0); n < numNodes; n++ {
					lKey := keys.NodeLivenessKey(n)
					// Always write a version if not at the highest level. If at the
					// highest level, only write if v < numLiveVersions. Additionally,
					// either haveFullyDeadKeys must be false or this node is one that
					// has live versions.
					if l < numLevels-1 || (v < numLiveVersions && (!haveFullyDeadKeys || n%2 == 0)) {
						liveness := livenesspb.Liveness{
							NodeID:     n,
							Epoch:      100,
							Expiration: hlc.LegacyTimestamp{WallTime: ts},
							Draining:   false,
							Membership: livenesspb.MembershipStatus_ACTIVE,
						}

						require.NoError(b, storage.MVCCPutProto(
							ctx, eng, lKey, hlc.Timestamp{WallTime: ts}, &liveness,
							storage.MVCCWriteOptions{}))
					}
					// Else most recent level and the other conditions for writing a
					// version are not satisfied.

					if l != 0 {
						// Clear the key from the next older level.
						require.NoError(b, eng.ClearMVCC(storage.MVCCKey{
							Key:       lKey,
							Timestamp: hlc.Timestamp{WallTime: tsFunc(l-1, v)},
						}, storage.ClearOptions{}))
					}
				}
				if l == 0 && v < 10 {
					// Flush to grow the memtable size.
					require.NoError(b, eng.Flush())
				}
			}
			if l == 0 {
				// Since did many flushes, compact everything down.
				require.NoError(b, eng.Compact())
			} else {
				// Flush the next level. This will become a L0 sub-level.
				require.NoError(b, eng.Flush())
			}
		}
		return eng
	}
	scanLiveness := func(b *testing.B, eng storage.Engine, expectedCount int) (blockBytes uint64) {
		ss := &kvpb.ScanStats{}
		opts := storage.MVCCScanOptions{
			ScanStats: ss,
		}
		scanRes, err := storage.MVCCScan(
			ctx, eng.NewReader(storage.StandardDurability), keys.NodeLivenessPrefix,
			keys.NodeLivenessKeyMax, hlc.MaxTimestamp, opts)
		if err != nil {
			b.Fatal(err.Error())
		}
		if expectedCount != len(scanRes.KVs) {
			b.Fatalf("expected %d != actual %d", expectedCount, len(scanRes.KVs))
		}
		return ss.BlockBytes
	}

	// We expect active nodes to have 100s of live versions since liveness is
	// written every 3s, and GC is configured to happen after 10min. But GC can
	// be delayed, and decommissioned nodes will only have 1 version, so we
	// explore those extremes.
	//
	// Results on M1 macbook with dead-keys=false and compacted=true:
	// NodeLivenessScanStorage/num-live=2/compacted=true-10   26.80µ ± 9%
	// NodeLivenessScanStorage/num-live=5/compacted=true-10   30.34µ ± 3%
	// NodeLivenessScanStorage/num-live=10/compacted=true-10   38.88µ ± 8%
	// NodeLivenessScanStorage/num-live=1000/compacted=true-10 861.5µ ± 3%
	//
	// When compacted=false the scan takes ~10ms, which is > 100x slower, but
	// probably acceptable for this workload.
	// NodeLivenessScanStorage/num-live=2/compacted=false-10     9.430m ± 5%
	// NodeLivenessScanStorage/num-live=5/compacted=false-10     9.534m ± 4%
	// NodeLivenessScanStorage/num-live=10/compacted=false-10    9.456m ± 2%
	// NodeLivenessScanStorage/num-live=1000/compacted=false-10 10.34m ± 7%
	//
	// dead-keys=true (and compacted=false) defeats the NextPrefix optimization,
	// since the next prefix can have all its keys deleted and the iterator has
	// to step through all of them (it can't be sure that all the keys for that
	// next prefix are deleted). This case should not occur in the liveness
	// range, as discussed earlier.
	//
	// NodeLivenessScanStorage/num-live=2/dead-keys=true/compacted=false-10 58.33m
	for _, numLiveVersions := range []int{2, 5, 10, 1000} {
		b.Run(fmt.Sprintf("num-live=%d", numLiveVersions), func(b *testing.B) {
			for _, haveDeadKeys := range []bool{false, true} {
				b.Run(fmt.Sprintf("dead-keys=%t", haveDeadKeys), func(b *testing.B) {
					for _, compacted := range []bool{false, true} {
						b.Run(fmt.Sprintf("compacted=%t", compacted), func(b *testing.B) {
							eng := setupEng(b, numLiveVersions, haveDeadKeys)
							defer eng.Close()
							if compacted {
								require.NoError(b, eng.Compact())
							}
							b.ResetTimer()
							blockBytes := uint64(0)
							for i := 0; i < b.N; i++ {
								expectedCount := numNodes
								if haveDeadKeys {
									expectedCount /= 2
								}
								blockBytes += scanLiveness(b, eng, expectedCount)
							}
							b.ReportMetric(float64(blockBytes)/float64(b.N), "block-bytes/op")
						})
					}
				})
			}
		})
	}
}
