// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO: put in a test package
package kvserver

import (
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/raft/v3/raftpb"
)

// RaftApplyAsserter is a test utility that tracks application of Raft
// commands, and asserts that a command is never applied at different log
// indexes (i.e. a double-apply or replay).
type RaftApplyAsserter struct {
	mu syncutil.Mutex
	// appliedCmds is a map of applied command IDs to the range ID and Raft log
	// index at which it was applied.
	appliedCmds map[kvserverbase.CmdIDKey]*appliedCmd
}

type appliedCmd struct {
	cmdID    kvserverbase.CmdIDKey
	rangeID  roachpb.RangeID
	logIndex kvpb.RaftIndex
	replicas map[roachpb.ReplicaID]bool
}

func NewRaftApplyAsserter() *RaftApplyAsserter {
	return &RaftApplyAsserter{
		appliedCmds: map[kvserverbase.CmdIDKey]*appliedCmd{},
	}
}

// Applied handles a command being applied, recording it and asserting that it
// was not already applied at a different log index.
//
// TODO: we need to handle snapshots too, by treating all commands up to the
// snapshot index as applied on that replica.
func (a *RaftApplyAsserter) Applied(
	cmdID kvserverbase.CmdIDKey,
	entry raftpb.Entry,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
) {
	if len(cmdID) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if ac, ok := a.appliedCmds[cmdID]; !ok {
		// New command was applied, record it.
		a.appliedCmds[cmdID] = &appliedCmd{
			cmdID:    cmdID,
			rangeID:  rangeID,
			logIndex: kvpb.RaftIndex(entry.Index),
			replicas: map[roachpb.ReplicaID]bool{replicaID: true},
		}
	} else if ac.rangeID != rangeID {
		// Command ID was used for a different range, ignore it.
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
