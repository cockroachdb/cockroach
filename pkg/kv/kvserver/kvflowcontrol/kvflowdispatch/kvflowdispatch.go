// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowdispatch

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// AdmittedRaftLogEntriesBytes is an estimate that comes from
// kvflowdispatch.TestDispatchSize().
const AdmittedRaftLogEntriesBytes = 50

// Dispatch is a concrete implementation of the kvflowcontrol.Dispatch
// interface. It's used to (i) dispatch information about admitted raft log
// entries to specific nodes, and (ii) to read pending dispatches.
type Dispatch struct {
	mu struct {
		// TODO(irfansharif,aaditya): On kv0/enc=false/nodes=3/cpu=96 this mutex
		// is responsible for ~3.7% of the mutex contention. Look to address it
		// as part of #104154. Perhaps shard this mutex by node ID? Or use a
		// syncutil.Map instead?
		syncutil.Mutex
		// outbox maintains pending dispatches on a per-node basis.
		outbox map[roachpb.NodeID]dispatches
	}
	metrics *metrics
	// handles is used to dispatch tokens locally. Remote token dispatches are
	// driven by the RaftTransport.
	handles kvflowcontrol.Handles
	nodeID  *base.NodeIDContainer
}

// dispatchKey is used to coalesce dispatches bound for a given node. If
// transmitting two kvflowcontrolpb.AdmittedRaftLogEntries with the same
// <RangeID,StoreID,WorkPriority> triple, with UpToRaftLogPositions L1 and L2
// where L1 < L2, we can simply dispatch the one with L2.
type dispatchKey struct {
	roachpb.RangeID
	roachpb.StoreID
	admissionpb.WorkPriority
}

type dispatches map[dispatchKey]kvflowcontrolpb.RaftLogPosition

var _ kvflowcontrol.Dispatch = &Dispatch{}

// New constructs a new Dispatch.
func New(
	registry *metric.Registry, handles kvflowcontrol.Handles, nodeID *base.NodeIDContainer,
) *Dispatch {
	d := &Dispatch{
		handles: handles,
		nodeID:  nodeID,
	}
	d.mu.outbox = make(map[roachpb.NodeID]dispatches)
	d.metrics = newMetrics()
	registry.AddMetricStruct(d.metrics)
	return d
}

// Dispatch is part of the kvflowcontrol.Dispatch interface.
func (d *Dispatch) Dispatch(
	ctx context.Context, nodeID roachpb.NodeID, entries kvflowcontrolpb.AdmittedRaftLogEntries,
) {
	if log.V(1) {
		log.Infof(ctx, "dispatching %s to n%s", entries, nodeID)
	}
	pri := admissionpb.WorkPriority(entries.AdmissionPriority)
	wc := admissionpb.WorkClassFromPri(pri)
	if nodeID == d.nodeID.Get() { // local fast-path
		handle, found := d.handles.Lookup(entries.RangeID)
		if found {
			handle.ReturnTokensUpto(
				ctx,
				admissionpb.WorkPriority(entries.AdmissionPriority),
				entries.UpToRaftLogPosition, kvflowcontrol.Stream{
					StoreID: entries.StoreID,
				})
		}
		// If we've not found the local kvflowcontrol.Handle, it's because the
		// range leaseholder/leader has recently been moved elsewhere. It's ok
		// to drop these tokens on the floor since we already returned it when
		// moving the leaseholder/leader.
		d.metrics.LocalDispatch[wc].Inc(1)
		return
	}
	d.metrics.RemoteDispatch[wc].Inc(1)

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.outbox[nodeID]; !ok {
		d.mu.outbox[nodeID] = dispatches{}
		d.metrics.PendingNodes.Inc(1)
	}

	dk := dispatchKey{
		entries.RangeID,
		entries.StoreID,
		pri,
	}

	existing, found := d.mu.outbox[nodeID][dk]
	if !found || existing.Less(entries.UpToRaftLogPosition) {
		d.mu.outbox[nodeID][dk] = entries.UpToRaftLogPosition

		if !found {
			d.metrics.PendingDispatches[wc].Inc(1)
		} else {
			// We're replacing an existing dispatch with one with a higher log
			// position. Increment the coalesced metric.
			d.metrics.CoalescedDispatches[wc].Inc(1)
		}
	}
	if found && !existing.Less(entries.UpToRaftLogPosition) {
		// We're dropping a dispatch given we already have a pending one with a
		// higher log position. Increment the coalesced metric.
		d.metrics.CoalescedDispatches[wc].Inc(1)
	}
}

// PendingDispatch is part of the kvflowcontrol.Dispatch interface.
func (d *Dispatch) PendingDispatch() []roachpb.NodeID {
	d.mu.Lock()
	defer d.mu.Unlock()

	nodes := make([]roachpb.NodeID, 0, len(d.mu.outbox))
	for node := range d.mu.outbox {
		nodes = append(nodes, node)
	}
	return nodes
}

// PendingDispatchFor is part of the kvflowcontrol.Dispatch interface.
func (d *Dispatch) PendingDispatchFor(
	nodeID roachpb.NodeID, maxBytes int64,
) ([]kvflowcontrolpb.AdmittedRaftLogEntries, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.outbox[nodeID]; !ok {
		return nil, 0
	}

	var entries []kvflowcontrolpb.AdmittedRaftLogEntries
	maxEntries := maxBytes / AdmittedRaftLogEntriesBytes
	for key, dispatch := range d.mu.outbox[nodeID] {
		if maxEntries == 0 {
			break
		}
		// TODO(irfansharif,aaditya): This contributes to 0.5% of alloc_objects
		// under kv0/enc=false/nodes=3/cpu=96. Maybe address it as part of
		// #104154; we're simply copying things over. Maybe use a sync.Pool here
		// and around the outbox map?
		entries = append(entries, kvflowcontrolpb.AdmittedRaftLogEntries{
			RangeID:             key.RangeID,
			StoreID:             key.StoreID,
			AdmissionPriority:   int32(key.WorkPriority),
			UpToRaftLogPosition: dispatch,
		})
		wc := admissionpb.WorkClassFromPri(key.WorkPriority)
		d.metrics.PendingDispatches[wc].Dec(1)
		maxEntries -= 1
		delete(d.mu.outbox[nodeID], key)
	}

	remainingDispatches := len(d.mu.outbox[nodeID])
	if remainingDispatches == 0 {
		delete(d.mu.outbox, nodeID)
		d.metrics.PendingNodes.Dec(1)
	}

	return entries, remainingDispatches
}

// testingMetrics returns the underlying metrics struct for testing purposes.
func (d *Dispatch) testingMetrics() *metrics {
	return d.metrics
}
