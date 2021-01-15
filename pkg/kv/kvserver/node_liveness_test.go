// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
func verifyLivenessServer(s *server.TestServer, numServers int64) error {
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	live, err := nl.IsLive(s.Gossip().NodeID.Get())
	if err != nil {
		return err
	} else if !live {
		return errors.Errorf("node %d not live", s.Gossip().NodeID.Get())
	}
	if a, e := nl.Metrics().LiveNodes.Value(), numServers; a != e {
		return errors.Errorf("expected node %d's LiveNodes metric to be %d; got %d",
			s.Gossip().NodeID.Get(), e, a)
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
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	// Advance clock past the liveness threshold to verify IsLive becomes false.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLivenessThreshold().Nanoseconds() + 1)

	for _, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		nodeID := s.Gossip().NodeID.Get()
		live, err := nl.IsLive(nodeID)
		if err != nil {
			t.Error(err)
		} else if live {
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

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:               true,
						StickyInMemoryEngineID: "1",
					},
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyEngineRegistry: stickyEngineRegistry,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	nl, ok := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(tc.Servers[0].Gossip().NodeID.Get())
	assert.True(t, ok)
	if nl.Epoch != 1 {
		t.Errorf("expected epoch to be set to 1 initially; got %d", nl.Epoch)
	}

	// Restart the node and verify the epoch is incremented with initial heartbeat.
	require.NoError(t, tc.Restart())
	verifyEpochIncremented(t, tc, 0)
}

func verifyEpochIncremented(t *testing.T, tc *testcluster.TestCluster, nodeIdx int) {
	testutils.SucceedsSoon(t, func() error {
		liv, ok := tc.Servers[nodeIdx].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(tc.Servers[nodeIdx].Gossip().NodeID.Get())
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
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s := serv.(*server.TestServer)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
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
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	var cbMu syncutil.Mutex
	cbs := map[roachpb.NodeID]struct{}{}
	tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).RegisterCallback(func(l livenesspb.Liveness) {
		cbMu.Lock()
		defer cbMu.Unlock()
		cbs[l.NodeID] = struct{}{}
	})

	// Advance clock past the liveness threshold.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLivenessThreshold().Nanoseconds() + 1)

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
			nodeID := s.Gossip().NodeID.Get()
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
	manualClock := hlc.NewHybridManualClock()
	expected := manualClock.UnixNano()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	// Verify that last update time has been set for all nodes.
	verifyUptimes := func() error {
		for i := range tc.Servers {
			s := tc.GetFirstStoreFromServer(t, i)
			uptm, err := s.ReadLastUpTimestamp(context.Background())
			if err != nil {
				return errors.Wrapf(err, "error reading last up time from store %d", i)
			}
			if a, e := uptm.WallTime, expected; a < e {
				return errors.Errorf("store %d last uptime = %d; wanted %d", i, a, e)
			}
		}
		return nil
	}

	if err := verifyUptimes(); err != nil {
		t.Fatal(err)
	}

	// Advance clock past the liveness threshold and force a manual heartbeat on
	// all node liveness objects, which should update the last up time for each
	// store.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLivenessThreshold().Nanoseconds() + 1)
	expected = manualClock.UnixNano()
	for _, s := range tc.Servers {
		nl := s.NodeLiveness().(*liveness.NodeLiveness)
		l, ok := nl.Self()
		assert.True(t, ok)
		if err := nl.Heartbeat(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}
	// NB: since the heartbeat callback is invoked synchronously in
	// `Heartbeat()` which this goroutine invoked, we don't need to wrap this in
	// a retry.
	if err := verifyUptimes(); err != nil {
		t.Fatal(err)
	}
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
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	// First try to increment the epoch of a known-live node.
	deadNodeID := tc.Servers[1].Gossip().NodeID.Get()
	oldLiveness, ok := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLiveness(deadNodeID)
	assert.True(t, ok)
	if err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !testutils.IsError(err, "cannot increment epoch on live node") {
		t.Fatalf("expected error incrementing a live node: %+v", err)
	}

	// Advance clock past liveness threshold & increment epoch.
	manualClock.Increment(tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).GetLivenessThreshold().Nanoseconds() + 1)
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
		if live, err := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness).IsLive(deadNodeID); live || err != nil {
			return errors.Errorf("expected dead node to remain dead after epoch increment %t: %v", live, err)
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

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	const numServers int = 2
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
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
		nodeID := s.Gossip().NodeID.Get()
		key := gossip.MakeNodeLivenessKey(nodeID)
		expKeys = append(expKeys, key)
		if err := s.Gossip().AddInfoProto(key, &livenesspb.Liveness{NodeID: nodeID}, 0); err != nil {
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
	require.NoError(t, tc.RestartServerWithInspect(1, func(s *server.TestServer) {
		livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
		s.Gossip().RegisterCallback(livenessRegex, func(key string, _ roachpb.Value) {
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
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	g := s.Gossip()
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
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)
	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	lMap := nl.GetIsLiveMap()
	l1, _ := nl.GetLiveness(1)
	l2, _ := nl.GetLiveness(2)
	l3, _ := nl.GetLiveness(3)
	expectedLMap := liveness.IsLiveMap{
		1: {Liveness: l1.Liveness, IsLive: true},
		2: {Liveness: l2.Liveness, IsLive: true},
		3: {Liveness: l3.Liveness, IsLive: true},
	}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}

	// Advance the clock but only heartbeat node 0.
	manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
	var livenessRec liveness.Record
	testutils.SucceedsSoon(t, func() error {
		lr, ok := nl.GetLiveness(tc.Servers[0].Gossip().NodeID.Get())
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
	lMap = nl.GetIsLiveMap()
	l1, _ = nl.GetLiveness(1)
	l2, _ = nl.GetLiveness(2)
	l3, _ = nl.GetLiveness(3)
	expectedLMap = liveness.IsLiveMap{
		1: {Liveness: l1.Liveness, IsLive: true},
		2: {Liveness: l2.Liveness, IsLive: false},
		3: {Liveness: l3.Liveness, IsLive: false},
	}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}
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
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	verifyLiveness(t, tc)
	pauseNodeLivenessHeartbeatLoops(tc)

	nl := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	actualLMapNodes := make(map[roachpb.NodeID]struct{})
	originalExpiration := testStartTime + nl.GetLivenessThreshold().Nanoseconds()
	for _, l := range nl.GetLivenesses() {
		if a, e := l.Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		if a, e := l.Expiration.WallTime, originalExpiration; a < e {
			t.Errorf("liveness record had expiration %d, wanted %d", a, e)
		}
		actualLMapNodes[l.NodeID] = struct{}{}
	}
	expectedLMapNodes := map[roachpb.NodeID]struct{}{1: {}, 2: {}, 3: {}}
	if !reflect.DeepEqual(actualLMapNodes, expectedLMapNodes) {
		t.Errorf("got liveness map nodes %+v; wanted %+v", actualLMapNodes, expectedLMapNodes)
	}

	// Advance the clock but only heartbeat node 0.
	manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
	var livenessRecord liveness.Record
	testutils.SucceedsSoon(t, func() error {
		livenessRec, ok := nl.GetLiveness(tc.Servers[0].Gossip().NodeID.Get())
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
	for _, l := range nl.GetLivenesses() {
		if a, e := l.Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		expectedExpiration := originalExpiration
		if l.NodeID == 1 {
			expectedExpiration += nl.GetLivenessThreshold().Nanoseconds() + 1
		}
		if a, e := l.Expiration.WallTime, expectedExpiration; a < e {
			t.Errorf("liveness record had expiration %d, wanted %d", a, e)
		}
		actualLMapNodes[l.NodeID] = struct{}{}
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
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})
	nl := s.NodeLiveness().(*liveness.NodeLiveness)
	nl.PauseHeartbeatLoopForTest()

	const concurrency = 10

	// Advance clock past the liveness threshold & concurrently heartbeat node.
	manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
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
						ClockSource: manualClock.UnixNano,
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
	manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
	l, ok := nl.GetLiveness(tc.Servers[1].Gossip().NodeID.Get())
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

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	drainingNodeIdx := 0
	drainingNodeID := tc.Servers[0].Gossip().NodeID.Get()

	nodeIDAppearsInStoreList := func(id roachpb.NodeID, sl kvserver.StoreList) bool {
		for _, store := range sl.Stores() {
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
				curNodeID := s.Gossip().NodeID.Get()
				sl, alive, _ := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.GetStoreList()
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
				curNodeID := s.Gossip().NodeID.Get()
				sl, alive, _ := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().StorePool.GetStoreList()
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
						sl.Stores(),
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

	var injectError atomic.Value
	var injectedErrorCount int32

	injectError.Store(true)
	testingEvalFilter := func(args kvserverbase.FilterArgs) *roachpb.Error {
		if _, ok := args.Req.(*roachpb.ConditionalPutRequest); !ok {
			return nil
		}
		if val := injectError.Load(); val != nil && val.(bool) {
			atomic.AddInt32(&injectedErrorCount, 1)
			injectError.Store(false)
			return roachpb.NewError(roachpb.NewAmbiguousResultError("test"))
		}
		return nil
	}
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
			Store: &kvserver.StoreTestingKnobs{
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: testingEvalFilter,
				},
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		return verifyLivenessServer(s, 1)
	})
	nl := s.NodeLiveness().(*liveness.NodeLiveness)

	l, ok := nl.Self()
	assert.True(t, ok)

	// And again on manual heartbeat.
	injectError.Store(true)
	if err := nl.Heartbeat(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if count := atomic.LoadInt32(&injectedErrorCount); count != 2 {
		t.Errorf("expected injected error count of 2; got %d", count)
	}
}

func verifyNodeIsDecommissioning(t *testing.T, tc *testcluster.TestCluster, nodeID roachpb.NodeID) {
	testutils.SucceedsSoon(t, func() error {
		for _, s := range tc.Servers {
			for _, liv := range s.NodeLiveness().(*liveness.NodeLiveness).GetLivenesses() {
				if liv.NodeID != nodeID {
					continue
				}
				if !liv.Membership.Decommissioning() {
					return errors.Errorf("unexpected Membership value of %v for node %v", liv.Membership, liv.NodeID)
				}
			}
		}
		return nil
	})
}

func testNodeLivenessSetDecommissioning(t *testing.T, decommissionNodeIdx int) {
	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, tc)

	callerNodeLiveness := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	nodeID := tc.Servers[decommissionNodeIdx].Gossip().NodeID.Get()

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
