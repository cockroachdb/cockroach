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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func verifyLiveness(t *testing.T, mtc *multiTestContext) {
	util.SucceedsSoon(t, func() error {
		for _, nl := range mtc.nodeLivenesses {
			for _, g := range mtc.gossips {
				live, err := nl.IsLive(g.NodeID.Get())
				if err != nil {
					return err
				} else if !live {
					return errors.Errorf("node %d not live", g.NodeID.Get())
				}
			}
		}
		return nil
	})
}

func stopNodeLivenessHeartbeats(mtc *multiTestContext) {
	for _, nl := range mtc.nodeLivenesses {
		nl.StopHeartbeat()
	}
}

func TestNodeLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#9973")
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Verify liveness of all nodes for all nodes.
	verifyLiveness(t, mtc)
	stopNodeLivenessHeartbeats(mtc)

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
	}
	// Trigger a manual heartbeat and verify liveness is reestablished.
	for _, nl := range mtc.nodeLivenesses {
		if err := nl.ManualHeartbeat(); err != nil {
			t.Fatal(err)
		}
	}
	verifyLiveness(t, mtc)

	// Verify metrics counts.
	for _, nl := range mtc.nodeLivenesses {
		if c := nl.Metrics().HeartbeatSuccesses.Count(); c != 2 {
			t.Errorf("expected metrics count == 2; got %d", c)
		}
	}
}

// TestNodeLivenessEpochIncrement verifies that incrementing the epoch
// of a node requires the node to be considered not-live and that on
// increment, no other nodes believe the epoch-incremented node to be
// live.
func TestNodeLivenessEpochIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

	verifyLiveness(t, mtc)
	stopNodeLivenessHeartbeats(mtc)

	// First try to increment the epoch of a known-live node.
	deadNodeID := mtc.gossips[1].NodeID.Get()
	if err := mtc.nodeLivenesses[0].IncrementEpoch(
		context.Background(), deadNodeID); !testutils.IsError(err, "cannot increment epoch on live node") {
		t.Fatalf("expected error incrementing a live node: %v", err)
	}

	// Advance clock past liveness threshold & increment epoch.
	oldLiveness, err := mtc.nodeLivenesses[0].GetLiveness(deadNodeID)
	if err != nil {
		t.Fatal(err)
	}
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	if err := mtc.nodeLivenesses[0].IncrementEpoch(context.Background(), deadNodeID); err != nil {
		t.Fatalf("unexpected error incrementing a live node: %s", err)
	}

	// Verify that the epoch has been advanced.
	util.SucceedsSoon(t, func() error {
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
}

// TestNodeLivenessRestart verifies that if nodes are shutdown and
// restarted, the node liveness records are re-gossiped immediately.
func TestNodeLivenessRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 2)
	defer mtc.Stop()

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
	util.SucceedsSoon(t, func() error {
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
	mtc := startMultiTestContext(t, 1)
	defer mtc.Stop()

	// Verify liveness of all nodes for all nodes.
	stopNodeLivenessHeartbeats(mtc)
	if err := mtc.nodeLivenesses[0].ManualHeartbeat(); err != nil {
		t.Fatal(err)
	}

	// Gossip random nonsense for liveness and verify that asking for
	// the node's own node ID returns the "correct" value.
	g := mtc.gossips[0]
	key := gossip.MakeNodeLivenessKey(g.NodeID.Get())
	var count int32
	g.RegisterCallback(key, func(_ string, val roachpb.Value) {
		atomic.AddInt32(&count, 1)
	})
	util.SucceedsSoon(t, func() error {
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
	if lGet != lSelf {
		t.Errorf("expected GetLiveness() to return same value as Self(): %+v != %+v", lGet, lSelf)
	}
	if lGet.Epoch == 2 || lSelf.NodeID == 2 {
		t.Errorf("expected GetLiveness() and Self() not to return artificially gossiped liveness: %+v, %+v", lGet, lSelf)
	}
}

func TestNodeLivenessGetLivenessMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	verifyLiveness(t, mtc)
	stopNodeLivenessHeartbeats(mtc)
	lMap := mtc.nodeLivenesses[0].GetLivenessMap()
	expectedLMap := map[roachpb.NodeID]bool{1: true, 2: true, 3: true}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}

	// Advance the clock but only heartbeat node 0.
	mtc.manualClock.Increment(mtc.nodeLivenesses[0].GetLivenessThreshold().Nanoseconds() + 1)
	if err := mtc.nodeLivenesses[0].ManualHeartbeat(); err != nil {
		t.Fatal(err)
	}

	// Now verify only node 0 is live.
	lMap = mtc.nodeLivenesses[0].GetLivenessMap()
	expectedLMap = map[roachpb.NodeID]bool{1: true, 2: false, 3: false}
	if !reflect.DeepEqual(expectedLMap, lMap) {
		t.Errorf("expected liveness map %+v; got %+v", expectedLMap, lMap)
	}
}
