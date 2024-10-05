// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package livenesspb

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TestNodeVitalityEntry is here to minimize the impact on tests of changing to
// the new interface for tests that previously used IsLiveMap. It doesn't
// directly look at timestamps, so the status must be manually updated.
type TestNodeVitalityEntry struct {
	Liveness Liveness
	Alive    bool
}

// TestNodeVitality is a test class for simulating and modifying NodeLiveness
// directly. The map is intended to be manually created and modified prior to
// running a test.
type TestNodeVitality map[roachpb.NodeID]TestNodeVitalityEntry

// TestCreateNodeVitality creates a test instance of node vitality which is easy
// to simulate different health conditions without requiring the need to take
// nodes down or publish anything through gossip.  This method takes an optional
// list of ides which are all marked as healthy when created.
func TestCreateNodeVitality(ids ...roachpb.NodeID) TestNodeVitality {
	m := TestNodeVitality{}
	for _, id := range ids {
		m.AddNode(id)
	}
	return m
}

func (e TestNodeVitalityEntry) Convert() NodeVitality {
	clock := hlc.NewClockForTesting(hlc.NewHybridManualClock())
	now := clock.Now()
	if e.Alive {
		return e.Liveness.CreateNodeVitality(now, now, hlc.Timestamp{}, true, time.Second, time.Second)
	} else {
		return e.Liveness.CreateNodeVitality(now, now.AddDuration(-time.Hour), hlc.Timestamp{}, true, time.Second, time.Second)
	}
}

func (tnv TestNodeVitality) GetNodeVitalityFromCache(id roachpb.NodeID) NodeVitality {
	val, found := tnv[id]
	if !found {
		return NodeVitality{}
	}
	return val.Convert()
}

// ScanNodeVitalityFromKV is only for testing so doesn't actually scan KV,
// instead it returns the cached values.
func (tnv TestNodeVitality) ScanNodeVitalityFromKV(_ context.Context) (NodeVitalityMap, error) {
	return tnv.ScanNodeVitalityFromCache(), nil
}

func (tnv TestNodeVitality) ScanNodeVitalityFromCache() NodeVitalityMap {
	nvm := make(NodeVitalityMap, len(tnv))
	for key, entry := range tnv {
		nvm[key] = entry.Convert()
	}
	return nvm
}

func (tnv TestNodeVitality) AddNextNode() {
	maxNodeID := roachpb.NodeID(0)
	for id := range tnv {
		if id > maxNodeID {
			maxNodeID = id
		}
	}
	tnv.AddNode(maxNodeID + 1)
}

func (tnv TestNodeVitality) AddNode(id roachpb.NodeID) {
	now := hlc.NewClockForTesting(hlc.NewHybridManualClock()).Now()
	tnv[id] = TestNodeVitalityEntry{
		Liveness: Liveness{
			NodeID:     id,
			Epoch:      1,
			Expiration: now.AddDuration(time.Minute).ToLegacyTimestamp(),
			Draining:   false,
			Membership: MembershipStatus_ACTIVE,
		},
		Alive: true,
	}
}

// Draining marks a given node as draining.
func (tnv TestNodeVitality) Draining(id roachpb.NodeID, drain bool) {
	entry := tnv[id]
	entry.Liveness.Draining = drain
	tnv[id] = entry
}

// Decommissioning marks a given node as decommissioning.
func (tnv TestNodeVitality) Decommissioning(id roachpb.NodeID, alive bool) {
	entry := tnv[id]
	entry.Liveness.Membership = MembershipStatus_DECOMMISSIONING
	entry.Alive = alive
	tnv[id] = entry
}

// Decommissioned marks a given node as decommissioned.
func (tnv TestNodeVitality) Decommissioned(id roachpb.NodeID, alive bool) {
	now := hlc.NewClockForTesting(hlc.NewHybridManualClock()).Now()
	entry := tnv[id]
	entry.Liveness.Membership = MembershipStatus_DECOMMISSIONED
	// Mark the liveness as expired if not alive.
	if !alive {
		entry.Liveness.Expiration = now.AddDuration(-1).ToLegacyTimestamp()
	}
	entry.Alive = alive
	tnv[id] = entry
}

// DownNode marks a node as expired.
func (tnv TestNodeVitality) DownNode(id roachpb.NodeID) {
	entry := tnv[id]
	entry.Alive = false
	tnv[id] = entry
}

// RestartNode marks a node as alive by setting the expiration in the future.
func (tnv TestNodeVitality) RestartNode(id roachpb.NodeID) {
	entry := tnv[id]
	entry.Alive = true
	entry.Liveness.Epoch++
	tnv[id] = entry
}

// FakeNodeVitality creates a node vitality record that is either dead or alive
// by all accounts.
func FakeNodeVitality(alive bool) NodeVitality {
	if alive {
		return NodeVitality{
			nodeID:               1,
			connected:            true,
			now:                  hlc.Timestamp{}.AddDuration(time.Nanosecond),
			timeUntilNodeDead:    time.Second,
			timeAfterNodeSuspect: time.Second,
			livenessExpiration:   hlc.Timestamp{}.AddDuration(2 * time.Nanosecond),
			livenessEpoch:        1,
		}
	} else {
		return NodeVitality{
			nodeID:        1,
			connected:     false,
			livenessEpoch: 1,
		}
	}
}
