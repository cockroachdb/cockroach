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
func New() *Dispatch {
	d := &Dispatch{}
	d.mu.outbox = make(map[roachpb.NodeID]dispatches)
	return d
}

// Dispatch is part of the kvflowcontrol.Dispatch interface.
func (d *Dispatch) Dispatch(nodeID roachpb.NodeID, entries kvflowcontrolpb.AdmittedRaftLogEntries) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.mu.outbox[nodeID]; !ok {
		d.mu.outbox[nodeID] = dispatches{}
	}

	dk := dispatchKey{
		entries.RangeID,
		entries.StoreID,
		admissionpb.WorkPriority(entries.AdmissionPriority),
	}
	existing, found := d.mu.outbox[nodeID][dk]
	if !found || existing.Less(entries.UpToRaftLogPosition) {
		d.mu.outbox[nodeID][dk] = entries.UpToRaftLogPosition
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
	}
	delete(d.mu.outbox, nodeID)
	return entries
}
