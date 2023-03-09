// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowdispatch

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Dispatch is a concrete implementation of the kvflowcontrol.Dispatch
// interface. It's used to (i) dispatch information about admitted raft log
// entries to specific nodes, and (ii) to read pending dispatches.
type Dispatch struct {
	mu struct {
		syncutil.Mutex
		// outbox maintains pending dispatches on a per-node basis.
		outbox map[roachpb.NodeID]dispatches
	}
	metrics *metrics
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
func New(registry *metric.Registry) *Dispatch {
	d := &Dispatch{}
	d.mu.outbox = make(map[roachpb.NodeID]dispatches)
	d.metrics = newMetrics()
	registry.AddMetricStruct(d.metrics)
	return d
}

// Dispatch is part of the kvflowcontrol.Dispatch interface.
func (d *Dispatch) Dispatch(nodeID roachpb.NodeID, entries kvflowcontrolpb.AdmittedRaftLogEntries) {
	// XXX: Implement local fast path. If local node ID, find the relevant
	// kvflowcontrol.Handle and invoke ReturnTokensUpto.
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.outbox[nodeID]; !ok {
		d.mu.outbox[nodeID] = dispatches{}
		d.metrics.PendingNodes.Inc(1)
	}

	dk := dispatchKey{
		entries.RangeID,
		entries.StoreID,
		admissionpb.WorkPriority(entries.AdmissionPriority),
	}

	existing, found := d.mu.outbox[nodeID][dk]
	wc := admissionpb.WorkClassFromPri(dk.WorkPriority)
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
	nodeID roachpb.NodeID,
) []kvflowcontrolpb.AdmittedRaftLogEntries {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.outbox[nodeID]; !ok {
		return nil
	}

	var entries []kvflowcontrolpb.AdmittedRaftLogEntries
	for key, dispatch := range d.mu.outbox[nodeID] {
		entries = append(entries, kvflowcontrolpb.AdmittedRaftLogEntries{
			RangeID:             key.RangeID,
			StoreID:             key.StoreID,
			AdmissionPriority:   int32(key.WorkPriority),
			UpToRaftLogPosition: dispatch,
		})
		wc := admissionpb.WorkClassFromPri(key.WorkPriority)
		d.metrics.PendingDispatches[wc].Dec(1)
	}

	delete(d.mu.outbox, nodeID)
	d.metrics.PendingNodes.Dec(1)
	return entries
}

// testingMetrics returns the underlying metrics struct for testing purposes.
func (d *Dispatch) testingMetrics() *metrics {
	return d.metrics
}
