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
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func verifyLiveness(t *testing.T, mtc *multiTestContext) {
	testutils.SucceedsSoon(t, func() error {
		for i, nl := range mtc.nodeLivenesses {
			for _, g := range mtc.gossips {
				live, err := nl.IsLive(g.NodeID.Get())
				if err != nil {
					return err
				} else if !live {
					return errors.Errorf("node %d not live", g.NodeID.Get())
				}
			}
			if a, e := nl.Metrics().LiveNodes.Value(), int64(len(mtc.nodeLivenesses)); a != e {
				return errors.Errorf("expected node %d's LiveNodes metric to be %d; got %d",
					mtc.gossips[i].NodeID.Get(), e, a)
			}
		}
		return nil
	})
}

func pauseNodeLivenessHeartbeats(mtc *multiTestContext, pause bool) {
	for _, nl := range mtc.nodeLivenesses {
		nl.PauseHeartbeat(pause)
	}
}

func TestNodeLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	// Advance clock past the liveness threshold to verify IsLive becomes false.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	for idx, nl := range mtc.nodeLivenesses {
		nodeID := mtc.gossips[idx].NodeID.Get()
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
	for _, nl := range mtc.nodeLivenesses {
		l, err := nl.Self()
		if err != nil {
			t.Fatal(err)
		}
		for {
			err := nl.Heartbeat(context.Background(), l)
			if err == nil {
				break
			}
			if errors.Is(err, kvserver.ErrEpochIncremented) {
				log.Warningf(context.Background(), "retrying after %s", err)
				continue
			}

			t.Fatal(err)
		}
	}
	verifyLiveness(t, mtc)

	// Verify metrics counts.
	for i, nl := range mtc.nodeLivenesses {
		if c := nl.Metrics().HeartbeatSuccesses.Count(); c < 2 {
			t.Errorf("node %d: expected metrics count >= 2; got %d", (i + 1), c)
		}
	}
}

func TestNodeLivenessInitialIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 1)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)

	liveness, err := mtc.nodeLivenesses[0].GetLiveness(mtc.gossips[0].NodeID.Get())
	if err != nil {
		t.Fatal(err)
	}
	if liveness.Epoch != 1 {
		t.Errorf("expected epoch to be set to 1 initially; got %d", liveness.Epoch)
	}

	// Restart the node and verify the epoch is incremented with initial heartbeat.
	mtc.stopStore(0)
	mtc.restartStore(0)
	verifyEpochIncremented(t, mtc, 0)
}

func verifyEpochIncremented(t *testing.T, mtc *multiTestContext, nodeIdx int) {
	testutils.SucceedsSoon(t, func() error {
		liveness, err := mtc.nodeLivenesses[nodeIdx].GetLiveness(mtc.gossips[nodeIdx].NodeID.Get())
		if err != nil {
			return err
		}
		if liveness.Epoch < 2 {
			return errors.Errorf("expected epoch to be >=2 on restart but was %d", liveness.Epoch)
		}
		return nil
	})

}

// TestNodeIsLiveCallback verifies that the liveness callback for a
// node is invoked when it changes from state false to true.
func TestNodeIsLiveCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	var cbMu syncutil.Mutex
	cbs := map[roachpb.NodeID]struct{}{}
	mtc.nodeLivenesses[0].RegisterCallback(func(nodeID roachpb.NodeID) {
		cbMu.Lock()
		defer cbMu.Unlock()
		cbs[nodeID] = struct{}{}
	})

	// Advance clock past the liveness threshold.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)

	// Trigger a manual heartbeat and verify callbacks for each node ID are invoked.
	for _, nl := range mtc.nodeLivenesses {
		l, err := nl.Self()
		if err != nil {
			t.Fatal(err)
		}
		if err := nl.Heartbeat(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}

	testutils.SucceedsSoon(t, func() error {
		cbMu.Lock()
		defer cbMu.Unlock()
		for _, g := range mtc.gossips {
			nodeID := g.NodeID.Get()
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
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	// Verify that last update time has been set for all nodes.
	verifyUptimes := func() error {
		expected := mtc.clock().Now()
		for i, s := range mtc.stores {
			uptm, err := s.ReadLastUpTimestamp(context.Background())
			if err != nil {
				return errors.Wrapf(err, "error reading last up time from store %d", i)
			}
			if a, e := uptm.WallTime, expected.WallTime; a != e {
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
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	for _, nl := range mtc.nodeLivenesses {
		l, err := nl.Self()
		if err != nil {
			t.Fatal(err)
		}
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
	ctx := context.Background()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	// First try to increment the epoch of a known-live node.
	deadNodeID := mtc.gossips[1].NodeID.Get()
	oldLiveness, err := mtc.nodeLivenesses[0].GetLiveness(deadNodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := mtc.nodeLivenesses[0].IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !testutils.IsError(err, "cannot increment epoch on live node") {
		t.Fatalf("expected error incrementing a live node: %+v", err)
	}

	// Advance clock past liveness threshold & increment epoch.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	if err := mtc.nodeLivenesses[0].IncrementEpoch(ctx, oldLiveness.Liveness); err != nil {
		t.Fatalf("unexpected error incrementing a non-live node: %+v", err)
	}

	// Verify that the epoch has been advanced.
	testutils.SucceedsSoon(t, func() error {
		newLiveness, err := mtc.nodeLivenesses[0].GetLiveness(deadNodeID)
		if err != nil {
			return err
		}
		if newLiveness.Epoch != oldLiveness.Epoch+1 {
			return errors.Errorf("expected epoch to increment")
		}
		if newLiveness.Expiration != oldLiveness.Expiration {
			return errors.Errorf("expected expiration to remain unchanged")
		}
		if live, err := mtc.nodeLivenesses[0].IsLive(deadNodeID); live || err != nil {
			return errors.Errorf("expected dead node to remain dead after epoch increment %t: %v", live, err)
		}
		return nil
	})

	// Verify epoch increment metric count.
	if c := mtc.nodeLivenesses[0].Metrics().EpochIncrements.Count(); c != 1 {
		t.Errorf("expected epoch increment == 1; got %d", c)
	}

	// Verify error on incrementing an already-incremented epoch.
	if err := mtc.nodeLivenesses[0].IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !errors.Is(err, kvserver.ErrEpochAlreadyIncremented) {
		t.Fatalf("unexpected error incrementing a non-live node: %+v", err)
	}

	// Verify error incrementing with a too-high expectation for liveness epoch.
	oldLiveness.Epoch = 3
	if err := mtc.nodeLivenesses[0].IncrementEpoch(
		ctx, oldLiveness.Liveness,
	); !testutils.IsError(err, "unexpected liveness epoch 2; expected >= 3") {
		t.Fatalf("expected error incrementing with a too-high expected epoch: %+v", err)
	}
}

// TestNodeLivenessRestart verifies that if nodes are shutdown and
// restarted, the node liveness records are re-gossiped immediately.
func TestNodeLivenessRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// After verifying node is in liveness table, stop store.
	verifyLiveness(t, mtc)
	mtc.stopStore(0)

	// Clear the liveness records in store 1's gossip to make sure we're
	// seeing the liveness record properly gossiped at store startup.
	var expKeys []string
	for _, g := range mtc.gossips {
		key := gossip.MakeNodeLivenessKey(g.NodeID.Get())
		expKeys = append(expKeys, key)
		if err := g.AddInfoProto(key, &kvserverpb.Liveness{}, 0); err != nil {
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
	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	mtc.gossips[0].RegisterCallback(livenessRegex, func(key string, _ roachpb.Value) {
		keysMu.Lock()
		defer keysMu.Unlock()
		for _, k := range keysMu.keys {
			if k == key {
				return
			}
		}
		keysMu.keys = append(keysMu.keys, key)
	})

	// Restart store and verify gossip contains liveness record for nodes 1&2.
	mtc.restartStore(0)
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
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 1)
	g := mtc.gossips[0]

	pauseNodeLivenessHeartbeats(mtc, true)

	// Verify liveness is properly initialized. This needs to be wrapped in a
	// SucceedsSoon because node liveness gets initialized via an async gossip
	// callback.
	var liveness kvserver.LivenessRecord
	testutils.SucceedsSoon(t, func() error {
		var err error
		liveness, err = mtc.nodeLivenesses[0].GetLiveness(g.NodeID.Get())
		return err
	})
	if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness.Liveness); err != nil {
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
		fakeBehindLiveness := liveness
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
	l := mtc.nodeLivenesses[0]
	lGetRec, err := l.GetLiveness(g.NodeID.Get())
	require.NoError(t, err)
	lGet := lGetRec.Liveness
	lSelf, err := l.Self()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(lGet, lSelf) {
		t.Errorf("expected GetLiveness() to return same value as Self(): %+v != %+v", lGet, lSelf)
	}
	if lGet.Epoch == 2 || lSelf.NodeID == 2 {
		t.Errorf("expected GetLiveness() and Self() not to return artificially gossiped liveness: %+v, %+v", lGet, lSelf)
	}
}

func TestNodeLivenessGetIsLiveMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)
	lMap := mtc.nodeLivenesses[0].GetIsLiveMap()
	expectedLMap := kvserver.IsLiveMap{
		1: {IsLive: true, Epoch: 1},
		2: {IsLive: true, Epoch: 1},
		3: {IsLive: true, Epoch: 1},
	}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}

	// Advance the clock but only heartbeat node 0.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	liveness, _ := mtc.nodeLivenesses[0].GetLiveness(mtc.gossips[0].NodeID.Get())

	testutils.SucceedsSoon(t, func() error {
		if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness.Liveness); err != nil {
			if errors.Is(err, kvserver.ErrEpochIncremented) {
				return err
			}
			t.Fatal(err)
		}
		return nil
	})

	// Now verify only node 0 is live.
	lMap = mtc.nodeLivenesses[0].GetIsLiveMap()
	expectedLMap = kvserver.IsLiveMap{
		1: {IsLive: true, Epoch: 1},
		2: {IsLive: false, Epoch: 1},
		3: {IsLive: false, Epoch: 1},
	}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}
}

func TestNodeLivenessGetLivenesses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	livenesses := mtc.nodeLivenesses[0].GetLivenesses()
	actualLMapNodes := make(map[roachpb.NodeID]struct{})
	originalExpiration := mtc.clock().PhysicalNow() + mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds()
	for _, l := range livenesses {
		if a, e := l.Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		if a, e := l.Expiration.WallTime, originalExpiration; a != e {
			t.Errorf("liveness record had expiration %d, wanted %d", a, e)
		}
		actualLMapNodes[l.NodeID] = struct{}{}
	}
	expectedLMapNodes := map[roachpb.NodeID]struct{}{1: {}, 2: {}, 3: {}}
	if !reflect.DeepEqual(actualLMapNodes, expectedLMapNodes) {
		t.Errorf("got liveness map nodes %+v; wanted %+v", actualLMapNodes, expectedLMapNodes)
	}

	// Advance the clock but only heartbeat node 0.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	liveness, _ := mtc.nodeLivenesses[0].GetLiveness(mtc.gossips[0].NodeID.Get())
	if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness.Liveness); err != nil {
		t.Fatal(err)
	}

	// Verify that node liveness receives the change.
	livenesses = mtc.nodeLivenesses[0].GetLivenesses()
	actualLMapNodes = make(map[roachpb.NodeID]struct{})
	for _, l := range livenesses {
		if a, e := l.Epoch, int64(1); a != e {
			t.Errorf("liveness record had epoch %d, wanted %d", a, e)
		}
		expectedExpiration := originalExpiration
		if l.NodeID == 1 {
			expectedExpiration += mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1
		}
		if a, e := l.Expiration.WallTime, expectedExpiration; a != e {
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
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 1)

	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	const concurrency = 10

	// Advance clock past the liveness threshold & concurrently heartbeat node.
	nl := mtc.nodeLivenesses[0]
	mtc.manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
	l, err := nl.Self()
	if err != nil {
		t.Fatal(err)
	}
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
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	verifyLiveness(t, mtc)
	pauseNodeLivenessHeartbeats(mtc, true)

	const concurrency = 10

	// Advance the clock and this time increment epoch concurrently for node 1.
	nl := mtc.nodeLivenesses[0]
	mtc.manualClock.Increment(nl.GetLivenessThreshold().Nanoseconds() + 1)
	l, err := nl.GetLiveness(mtc.gossips[1].NodeID.Get())
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			errCh <- nl.IncrementEpoch(context.Background(), l.Liveness)
		}()
	}
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, kvserver.ErrEpochAlreadyIncremented) {
			t.Fatalf("concurrent increment epoch %d failed: %+v", i, err)
		}
	}
}

// TestNodeLivenessSetDraining verifies that when draining, a node's liveness
// record is updated and the node will not be present in the store list of other
// nodes once they are aware of its draining state.
func TestNodeLivenessSetDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)
	mtc.initGossipNetwork()

	verifyLiveness(t, mtc)

	ctx := context.Background()
	drainingNodeIdx := 0
	drainingNodeID := mtc.gossips[drainingNodeIdx].NodeID.Get()

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
	if err := mtc.nodeLivenesses[drainingNodeIdx].SetDrainingInternal(
		ctx, kvserver.LivenessRecord{}, false,
	); err != nil {
		t.Fatal(err)
	}

	mtc.nodeLivenesses[drainingNodeIdx].SetDraining(ctx, true /* drain */, nil /* reporter */)

	// Draining node disappears from store lists.
	{
		const expectedLive = 2
		// Executed in a retry loop to wait until the new liveness record has
		// been gossiped to the rest of the cluster.
		testutils.SucceedsSoon(t, func() error {
			for i, sp := range mtc.storePools {
				curNodeID := mtc.gossips[i].NodeID.Get()
				sl, alive, _ := sp.GetStoreList()
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
	mtc.stopStore(drainingNodeIdx)
	mtc.restartStore(drainingNodeIdx)

	// Restarted node appears once again in the store list.
	{
		const expectedLive = 3
		// Executed in a retry loop to wait until the new liveness record has
		// been gossiped to the rest of the cluster.
		testutils.SucceedsSoon(t, func() error {
			for i, sp := range mtc.storePools {
				curNodeID := mtc.gossips[i].NodeID.Get()
				sl, alive, _ := sp.GetStoreList()
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

	var injectError atomic.Value
	var injectedErrorCount int32

	injectError.Store(true)
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.EvalKnobs.TestingEvalFilter = func(args kvserverbase.FilterArgs) *roachpb.Error {
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
	mtc := &multiTestContext{
		storeConfig: &storeCfg,
	}
	mtc.Start(t, 1)
	defer mtc.Stop()

	// Verify retry of the ambiguous result for heartbeat loop.
	verifyLiveness(t, mtc)

	nl := mtc.nodeLivenesses[0]
	l, err := nl.Self()
	if err != nil {
		t.Fatal(err)
	}

	// And again on manual heartbeat.
	injectError.Store(true)
	if err := nl.Heartbeat(context.Background(), l); err != nil {
		t.Fatal(err)
	}
	if count := atomic.LoadInt32(&injectedErrorCount); count != 2 {
		t.Errorf("expected injected error count of 2; got %d", count)
	}
}

func verifyNodeIsDecommissioning(t *testing.T, mtc *multiTestContext, nodeID roachpb.NodeID) {
	testutils.SucceedsSoon(t, func() error {
		for _, nl := range mtc.nodeLivenesses {
			livenesses := nl.GetLivenesses()
			for _, liveness := range livenesses {
				if liveness.Decommissioning != (liveness.NodeID == nodeID) {
					return errors.Errorf("unexpected Decommissioning value of %v for node %v", liveness.Decommissioning, liveness.NodeID)
				}
			}
		}
		return nil
	})
}

func TestNodeLivenessStatusMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short")
	}

	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Disable replica rebalancing to ensure that the liveness range
				// does not get out of the first node (we'll be shutting down nodes).
				DisableReplicaRebalancing: true,
				// Disable LBS because when the scan is happening at the rate it's happening
				// below, it's possible that one of the system ranges trigger a split.
				DisableLoadBasedSplitting: true,
			},
		},
		RaftConfig: base.RaftConfig{
			// Make everything tick faster to ensure dead nodes are
			// recognized dead faster.
			RaftTickInterval: 100 * time.Millisecond,
		},
		// Scan like a bat out of hell to ensure replication and replica GC
		// happen in a timely manner.
		ScanInterval: 50 * time.Millisecond,
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: serverArgs,
		// Disable full replication otherwise StartTestCluster with just 1
		// node will wait forever.
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	ctx = logtags.AddTag(ctx, "in test", nil)

	log.Infof(ctx, "setting zone config to disable replication")
	// Allow for inserting zone configs without having to go through (or
	// duplicate the logic from) the CLI.
	config.TestingSetupZoneConfigHook(tc.Stopper())
	zoneConfig := zonepb.DefaultZoneConfig()
	// Force just one replica per range to ensure that we can shut down
	// nodes without endangering the liveness range.
	zoneConfig.NumReplicas = proto.Int32(1)
	config.TestingSetZoneConfig(keys.MetaRangesID, zoneConfig)

	log.Infof(ctx, "starting 3 more nodes")
	tc.AddServer(t, serverArgs)
	tc.AddServer(t, serverArgs)
	tc.AddServer(t, serverArgs)

	log.Infof(ctx, "waiting for node statuses")
	tc.WaitForNodeStatuses(t)
	tc.WaitForNodeLiveness(t)
	log.Infof(ctx, "waiting done")

	firstServer := tc.Server(0).(*server.TestServer)

	liveNodeID := firstServer.NodeID()

	deadNodeID := tc.Server(1).NodeID()
	log.Infof(ctx, "shutting down node %d", deadNodeID)
	tc.StopServer(1)
	log.Infof(ctx, "done shutting down node %d", deadNodeID)

	decommissioningNodeID := tc.Server(2).NodeID()
	log.Infof(ctx, "decommissioning node %d", decommissioningNodeID)
	if err := firstServer.Decommission(ctx, true, []roachpb.NodeID{decommissioningNodeID}); err != nil {
		t.Fatal(err)
	}
	log.Infof(ctx, "done decommissioning node %d", decommissioningNodeID)

	removedNodeID := tc.Server(3).NodeID()
	log.Infof(ctx, "decommissioning and shutting down node %d", removedNodeID)
	if err := firstServer.Decommission(ctx, true, []roachpb.NodeID{removedNodeID}); err != nil {
		t.Fatal(err)
	}
	tc.StopServer(3)
	log.Infof(ctx, "done removing node %d", removedNodeID)

	log.Infof(ctx, "checking status map")

	// See what comes up in the status.

	cc, err := tc.Server(0).RPCContext().GRPCDialNode(
		firstServer.RPCAddr(), firstServer.NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err)
	admin := serverpb.NewAdminClient(cc)

	type testCase struct {
		nodeID         roachpb.NodeID
		expectedStatus kvserverpb.NodeLivenessStatus
	}

	// Below we're going to check that all statuses converge and stabilize
	// to a known situation.
	testData := []testCase{
		{liveNodeID, kvserverpb.NodeLivenessStatus_LIVE},
		{deadNodeID, kvserverpb.NodeLivenessStatus_DEAD},
		{decommissioningNodeID, kvserverpb.NodeLivenessStatus_DECOMMISSIONING},
		{removedNodeID, kvserverpb.NodeLivenessStatus_DECOMMISSIONED},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("n%d->%s", test.nodeID, test.expectedStatus), func(t *testing.T) {
			nodeID, expectedStatus := test.nodeID, test.expectedStatus

			testutils.SucceedsSoon(t, func() error {
				// Ensure that dead nodes are quickly recognized as dead by
				// gossip. Overriding cluster settings is generally a really bad
				// idea as they are also populated via Gossip and so our update
				// is possibly going to be wiped out. But going through SQL
				// doesn't allow durations below 1m15s, which is much too long
				// for a test.
				// We do this in every SucceedsSoon attempt, so we'll be good.
				kvserver.TimeUntilStoreDead.Override(&firstServer.ClusterSettings().SV,
					kvserver.TestTimeUntilStoreDead)

				log.Infof(ctx, "checking expected status (%s) for node %d", expectedStatus, nodeID)
				resp, err := admin.Liveness(ctx, &serverpb.LivenessRequest{})
				require.NoError(t, err)
				nodeStatuses := resp.Statuses

				st, ok := nodeStatuses[nodeID]
				if !ok {
					return errors.Errorf("node %d: not in statuses\n", nodeID)
				}
				if st != expectedStatus {
					return errors.Errorf("node %d: unexpected status: got %s, expected %s\n",
						nodeID, st, expectedStatus,
					)
				}
				return nil
			})
		})
	}
}

func testNodeLivenessSetDecommissioning(t *testing.T, decommissionNodeIdx int) {
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)
	mtc.initGossipNetwork()

	verifyLiveness(t, mtc)

	ctx := context.Background()
	callerNodeLiveness := mtc.nodeLivenesses[0]
	nodeID := mtc.gossips[decommissionNodeIdx].NodeID.Get()

	// Verify success on failed update of a liveness record that already has the
	// given decommissioning setting.
	if _, err := callerNodeLiveness.SetDecommissioningInternal(
		ctx, nodeID, kvserver.LivenessRecord{}, false,
	); err != nil {
		t.Fatal(err)
	}

	// Set a node to decommissioning state.
	if _, err := callerNodeLiveness.SetDecommissioning(ctx, nodeID, true); err != nil {
		t.Fatal(err)
	}
	verifyNodeIsDecommissioning(t, mtc, nodeID)

	// Stop and restart the store to verify that a restarted server retains the
	// decommissioning field on the liveness record.
	mtc.stopStore(decommissionNodeIdx)
	mtc.restartStore(decommissionNodeIdx)

	// Wait until store has restarted and published a new heartbeat to ensure not
	// looking at pre-restart state. Want to be sure test fails if node wiped the
	// decommission flag.
	verifyEpochIncremented(t, mtc, decommissionNodeIdx)
	verifyNodeIsDecommissioning(t, mtc, nodeID)
}

// TestNodeLivenessSetDecommissioning verifies that when decommissioning, a
// node's liveness record is updated and remains after restart.
func TestNodeLivenessSetDecommissioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)
	mtc.initGossipNetwork()

	verifyLiveness(t, mtc)

	ctx := context.Background()
	const goneNodeID = roachpb.NodeID(10000)

	// When the node simply never existed, expect an error.
	if _, err := mtc.nodeLivenesses[0].SetDecommissioning(
		ctx, goneNodeID, true,
	); !errors.Is(err, kvserver.ErrNoLivenessRecord) {
		t.Fatal(err)
	}

	// Pretend the node was once there but isn't gossiped anywhere.
	if err := mtc.dbs[0].CPut(ctx, keys.NodeLivenessKey(goneNodeID), &kvserverpb.Liveness{
		NodeID:     goneNodeID,
		Epoch:      1,
		Expiration: hlc.LegacyTimestamp(mtc.clock().Now()),
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Decommission from second node.
	if committed, err := mtc.nodeLivenesses[1].SetDecommissioning(ctx, goneNodeID, true); err != nil {
		t.Fatal(err)
	} else if !committed {
		t.Fatal("no change committed")
	}
	// Re-decommission from first node.
	if committed, err := mtc.nodeLivenesses[0].SetDecommissioning(ctx, goneNodeID, true); err != nil {
		t.Fatal(err)
	} else if committed {
		t.Fatal("spurious change committed")
	}
	// Recommission from first node.
	if committed, err := mtc.nodeLivenesses[0].SetDecommissioning(ctx, goneNodeID, false); err != nil {
		t.Fatal(err)
	} else if !committed {
		t.Fatal("no change committed")
	}
	// Decommission from second node (a second time).
	if committed, err := mtc.nodeLivenesses[1].SetDecommissioning(ctx, goneNodeID, true); err != nil {
		t.Fatal(err)
	} else if !committed {
		t.Fatal("no change committed")
	}
	// Recommission from third node.
	if committed, err := mtc.nodeLivenesses[2].SetDecommissioning(ctx, goneNodeID, false); err != nil {
		t.Fatal(err)
	} else if !committed {
		t.Fatal("no change committed")
	}
}
