// Copyright 2016 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage_test

import (
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
		if err := nl.Heartbeat(context.Background(), l); err != nil {
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
	testutils.SucceedsSoon(t, func() error {
		liveness, err := mtc.nodeLivenesses[0].GetLiveness(mtc.gossips[0].NodeID.Get())
		if err != nil {
			return err
		}
		if liveness.Epoch != 2 {
			return errors.Errorf("expected epoch to be incremented to 2 on restart; got %d", liveness.Epoch)
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
		expected := mtc.clock.Now()
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
		context.Background(), oldLiveness); !testutils.IsError(err, "cannot increment epoch on live node") {
		t.Fatalf("expected error incrementing a live node: %v", err)
	}

	// Advance clock past liveness threshold & increment epoch.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	if err := mtc.nodeLivenesses[0].IncrementEpoch(context.Background(), oldLiveness); err != nil {
		t.Fatalf("unexpected error incrementing a non-live node: %s", err)
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

	// Verify noop on incrementing an already-incremented epoch.
	if err := mtc.nodeLivenesses[0].IncrementEpoch(context.Background(), oldLiveness); err != nil {
		t.Fatalf("unexpected error incrementing a non-live node: %s", err)
	}

	// Verify error incrementing with a too-high expectation for liveness epoch.
	oldLiveness.Epoch = 3
	if err := mtc.nodeLivenesses[0].IncrementEpoch(
		context.Background(), oldLiveness); !testutils.IsError(err, "unexpected liveness epoch 2; expected >= 3") {
		t.Fatalf("expected error incrementing with a too-high expected epoch: %v", err)
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
		if err := g.AddInfoProto(key, &storage.Liveness{}, 0); err != nil {
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

// TestNodeLivenessSelf verifies that a node keeps its own most
// recent liveness heartbeat info in preference to anything which
// might be received belatedly through gossip.
func TestNodeLivenessSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 1)

	// Verify liveness of all nodes for all nodes.
	pauseNodeLivenessHeartbeats(mtc, true)
	g := mtc.gossips[0]
	liveness, _ := mtc.nodeLivenesses[0].GetLiveness(g.NodeID.Get())
	if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness); err != nil {
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
		if err := g.AddInfoProto(key, &storage.Liveness{
			NodeID: 1,
			Epoch:  2,
		}, 0); err != nil {
			t.Fatal(err)
		}
		if atomic.LoadInt32(&count) < 2 {
			return errors.New("expected count >= 2")
		}
		return nil
	})

	// Self should not see new epoch.
	l := mtc.nodeLivenesses[0]
	lGet, err := l.GetLiveness(g.NodeID.Get())
	if err != nil {
		t.Fatal(err)
	}
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
	expectedLMap := map[roachpb.NodeID]bool{1: true, 2: true, 3: true}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}

	// Advance the clock but only heartbeat node 0.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	liveness, _ := mtc.nodeLivenesses[0].GetLiveness(mtc.gossips[0].NodeID.Get())
	if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness); err != nil {
		t.Fatal(err)
	}

	// Now verify only node 0 is live.
	lMap = mtc.nodeLivenesses[0].GetIsLiveMap()
	expectedLMap = map[roachpb.NodeID]bool{1: true, 2: false, 3: false}
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
	originalExpiration := mtc.clock.PhysicalNow() + mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1
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
	if err := mtc.nodeLivenesses[0].Heartbeat(context.Background(), liveness); err != nil {
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
			t.Fatalf("concurrent heartbeat %d failed: %s", i, err)
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
			errCh <- nl.IncrementEpoch(context.Background(), l)
		}()
	}
	for i := 0; i < concurrency; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent increment epoch %d failed: %s", i, err)
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

	nodeIDAppearsInStoreList := func(id roachpb.NodeID, sl storage.StoreList) bool {
		for _, store := range sl.Stores() {
			if store.Node.NodeID == id {
				return true
			}
		}
		return false
	}

	// Verify success on failed update of a liveness record that already has the
	// given draining setting.
	if err := mtc.nodeLivenesses[drainingNodeIdx].SetDrainingInternal(ctx, &storage.Liveness{}, false); err != nil {
		t.Fatal(err)
	}

	mtc.nodeLivenesses[drainingNodeIdx].SetDraining(ctx, true)

	// Draining node disappears from store lists.
	{
		const expectedLive = 2
		// Executed in a retry loop to wait until the new liveness record has
		// been gossiped to the rest of the cluster.
		testutils.SucceedsSoon(t, func() error {
			for i, sp := range mtc.storePools {
				curNodeID := mtc.gossips[i].NodeID.Get()
				sl, alive, _ := sp.GetStoreList(0)
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
				sl, alive, _ := sp.GetStoreList(0)
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.TestingEvalFilter = func(args storagebase.FilterArgs) *roachpb.Error {
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
