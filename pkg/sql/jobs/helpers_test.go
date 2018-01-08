// Copyright 2017 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func ResetResumeHooks() func() {
	oldResumeHooks := resumeHooks
	return func() { resumeHooks = oldResumeHooks }
}

// FakeNodeID is a dummy node ID for use in tests. It always stores 1.
var FakeNodeID = func() *base.NodeIDContainer {
	nodeID := base.NodeIDContainer{}
	nodeID.Reset(1)
	return &nodeID
}()

// FakeClusterID is a dummy cluster ID for use in tests. It always returns
// "deadbeef-dead-beef-dead-beefdeadbeef" as a uuid.UUID.
var FakeClusterID = func() func() uuid.UUID {
	clusterID := uuid.FromUint128(uint128.FromInts(0xdeadbeefdeadbeef, 0xdeadbeefdeadbeef))
	return func() uuid.UUID { return clusterID }
}()

// FakeNodeLiveness allows simulating liveness failures without the full
// storage.NodeLiveness machinery.
type FakeNodeLiveness struct {
	clock *hlc.Clock
	mu    struct {
		syncutil.Mutex
		livenessMap map[roachpb.NodeID]*storage.Liveness
	}

	// A non-blocking send is performed over these channels when the corresponding
	// method is called.
	SelfCalledCh          chan struct{}
	GetLivenessesCalledCh chan struct{}
}

// NewFakeNodeLiveness initializes a new NodeLiveness with nodeCount live nodes.
func NewFakeNodeLiveness(clock *hlc.Clock, nodeCount int) *FakeNodeLiveness {
	nl := &FakeNodeLiveness{
		clock:                 clock,
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

// FakeResumer calls optional callbacks during the job lifecycle.
type FakeResumer struct {
	OnResume func(job *Job) error
	Fail     func(job *Job) error
	Success  func(job *Job) error
	Terminal func(job *Job)
}

func (d FakeResumer) Resume(_ context.Context, job *Job, _ chan<- tree.Datums) error {
	if d.OnResume != nil {
		return d.OnResume(job)
	}
	return nil
}

func (d FakeResumer) OnFailOrCancel(_ context.Context, _ *client.Txn, job *Job) error {
	if d.Fail != nil {
		return d.Fail(job)
	}
	return nil
}

func (d FakeResumer) OnSuccess(_ context.Context, _ *client.Txn, job *Job) error {
	if d.Success != nil {
		return d.Success(job)
	}
	return nil
}

func (d FakeResumer) OnTerminal(_ context.Context, job *Job, _ Status, _ chan<- tree.Datums) {
	if d.Terminal != nil {
		d.Terminal(job)
	}
}

var _ Resumer = FakeResumer{}
