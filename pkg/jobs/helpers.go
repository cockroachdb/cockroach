// Copyright 2018 The Cockroach Authors.
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

package jobs

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// FakeNodeID is a dummy node ID for use in tests. It always stores 1.
var FakeNodeID = func() *base.NodeIDContainer {
	nodeID := base.NodeIDContainer{}
	nodeID.Reset(1)
	return &nodeID
}()

// FakeNodeLiveness allows simulating liveness failures without the full
// storage.NodeLiveness machinery.
type FakeNodeLiveness struct {
	mu struct {
		syncutil.Mutex
		livenessMap map[roachpb.NodeID]*storage.Liveness
	}

	// A non-blocking send is performed over these channels when the corresponding
	// method is called.
	SelfCalledCh          chan struct{}
	GetLivenessesCalledCh chan struct{}
}

// NewFakeNodeLiveness initializes a new NodeLiveness with nodeCount live nodes.
func NewFakeNodeLiveness(nodeCount int) *FakeNodeLiveness {
	nl := &FakeNodeLiveness{
		SelfCalledCh:          make(chan struct{}),
		GetLivenessesCalledCh: make(chan struct{}),
	}
	nl.mu.livenessMap = make(map[roachpb.NodeID]*storage.Liveness)
	for i := 0; i < nodeCount; i++ {
		nodeID := roachpb.NodeID(i + 1)
		nl.mu.livenessMap[nodeID] = &storage.Liveness{
			Epoch:      1,
			Expiration: hlc.LegacyTimestamp(hlc.MaxTimestamp),
			NodeID:     nodeID,
		}
	}
	return nl
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (*FakeNodeLiveness) ModuleTestingKnobs() {}

// Self implements the implicit storage.NodeLiveness interface. It uses NodeID
// as the node ID. On every call, a nonblocking send is performed over nl.ch to
// allow tests to execute a callback.
func (nl *FakeNodeLiveness) Self() (*storage.Liveness, error) {
	select {
	case nl.SelfCalledCh <- struct{}{}:
	default:
	}
	nl.mu.Lock()
	defer nl.mu.Unlock()
	selfCopy := *nl.mu.livenessMap[FakeNodeID.Get()]
	return &selfCopy, nil
}

// GetLivenesses implements the implicit storage.NodeLiveness interface.
func (nl *FakeNodeLiveness) GetLivenesses() (out []storage.Liveness) {
	select {
	case nl.GetLivenessesCalledCh <- struct{}{}:
	default:
	}
	nl.mu.Lock()
	defer nl.mu.Unlock()
	for _, liveness := range nl.mu.livenessMap {
		out = append(out, *liveness)
	}
	return out
}

// FakeIncrementEpoch increments the epoch for the node with the specified ID.
func (nl *FakeNodeLiveness) FakeIncrementEpoch(id roachpb.NodeID) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.livenessMap[id].Epoch++
}

// FakeSetExpiration sets the expiration time of the liveness for the node with
// the specified ID to ts.
func (nl *FakeNodeLiveness) FakeSetExpiration(id roachpb.NodeID, ts hlc.Timestamp) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.livenessMap[id].Expiration = hlc.LegacyTimestamp(ts)
}
