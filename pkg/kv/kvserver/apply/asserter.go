// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO: put in a test package
package apply

import (
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/raft/v3/raftpb"
)

type appliedCmd struct {
	cmdID    kvserverbase.CmdIDKey
	rangeID  roachpb.RangeID
	logIndex kvpb.RaftIndex
	replicas map[roachpb.ReplicaID]bool
}

// Asserter is a test utility that tracks application of Raft commands, and
// asserts that a command is only applied once per replica, and never applied at
// different log indexes (i.e. a double-apply or replay).
//
// It accounts for snapshots, but not splits/merges which are considered
// separate Raft groups, as the risk of proposals leaking across Raft groups
// appears negligible.
type Asserter struct {
	mu syncutil.Mutex
	// appliedIndex tracks the applied index of each replica.
	appliedIndex map[roachpb.RangeID]map[roachpb.ReplicaID]kvpb.RaftIndex
	// appliedCmds tracks applied commands by range and command ID.
	appliedCmds map[roachpb.RangeID]map[kvserverbase.CmdIDKey]*appliedCmd
}

// NewAsserter creates a new asserter.
func NewAsserter() *Asserter {
	return &Asserter{
		appliedIndex: map[roachpb.RangeID]map[roachpb.ReplicaID]kvpb.RaftIndex{},
		appliedCmds:  map[roachpb.RangeID]map[kvserverbase.CmdIDKey]*appliedCmd{},
	}
}

// ensureRangeLocked ensures that data structures exist for the given range.
// Asserter.mu must be held.
func (a *Asserter) ensureRangeLocked(rangeID roachpb.RangeID) {
	if _, ok := a.appliedCmds[rangeID]; !ok {
		a.appliedCmds[rangeID] = map[kvserverbase.CmdIDKey]*appliedCmd{}
	}
	if _, ok := a.appliedIndex[rangeID]; !ok {
		a.appliedIndex[rangeID] = map[roachpb.ReplicaID]kvpb.RaftIndex{}
	}
}

// Apply records a command being applied, and asserts validity.
//
// It assumes that entry application is durable, i.e. that nodes won't lose
// applied state. This is not true for real CRDB nodes which can lose applied
// state on restart, but is generally true in tests. If needed, we can loosen
// this requirement by signalling truncation of applied state on node restart.
func (a *Asserter) Apply(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID kvserverbase.CmdIDKey,
	entry raftpb.Entry,
) {
	if len(cmdID) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.ensureRangeLocked(rangeID)

	// Assert and record the applied index.
	if appliedIndex := a.appliedIndex[rangeID][replicaID]; kvpb.RaftIndex(entry.Index) <= appliedIndex {
		panic(fmt.Sprintf("applied index regression for r%d/%d: %d -> %d", rangeID, replicaID, appliedIndex, entry.Index))
	}
	a.appliedIndex[rangeID][replicaID] = kvpb.RaftIndex(entry.Index)

	if ac, ok := a.appliedCmds[rangeID][cmdID]; !ok {
		// New command was applied, record it.
		a.appliedCmds[rangeID][cmdID] = &appliedCmd{
			cmdID:    cmdID,
			rangeID:  rangeID,
			logIndex: kvpb.RaftIndex(entry.Index),
			replicas: map[roachpb.ReplicaID]bool{replicaID: true},
		}
	} else if ac.logIndex == kvpb.RaftIndex(entry.Index) {
		// Command applying at the expected index, record the replica.
		ac.replicas[replicaID] = true
	} else {
		// Applied command at unexpected log index, bail out.
		var replicas []roachpb.ReplicaID
		for id := range ac.replicas {
			replicas = append(replicas, id)
		}
		msg := fmt.Sprintf("command %s re-applied at index %d on r%d/%d\n", cmdID, entry.Index, rangeID, replicaID)
		msg += fmt.Sprintf("previously applied at index %d on replicas %s\n", ac.logIndex, replicas)
		msg += fmt.Sprintf("entry: %+v", entry) // TODO: this formatting is not helpful
		panic(msg)
	}
}

// ApplySnapshot records snapshot application at the given applied index. The
// replica is considered to have applied all commands up to and including the
// applied index.
func (a *Asserter) ApplySnapshot(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	index kvpb.RaftIndex,
) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.ensureRangeLocked(rangeID)

	// Record the applied index.
	a.appliedIndex[rangeID][replicaID] = index

	// Record the applied commands for this replica. A command must have applied
	// on at least one replica before it can be included in a snapshot, so all
	// relevant commands are guaranteed to be present in the map.
	for _, cmd := range a.appliedCmds[rangeID] {
		if cmd.logIndex <= index {
			cmd.replicas[replicaID] = true
		} else {
			delete(cmd.replicas, replicaID)
		}
	}
}
