// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/raft/v3/raftpb"
)

// Asserter is a test utility that tracks application of Raft commands, and
// primarily asserts that:
//
//   - A request is only applied once, at a single log index and lease applied index
//     across all replica (i.e. no double-applies or replays).
//
//   - A replica's applied index, applied term, lease applied index, and closed timestamp
//     do not regress (assuming no node restarts).
type Asserter struct {
	mu syncutil.Mutex
	// proposedCmds tracks proposed commands by ID, along with the proposer.
	// Reproposals under new command IDs are considered separate proposals here.
	proposedCmds map[roachpb.RangeID]map[kvserverbase.CmdIDKey]roachpb.ReplicaID
	// reproposedCmds keeps track of LAI reproposals under different command IDs
	// (when the LAI changes), keyed by the original seed proposal's command ID
	// and the reproposal's command ID.
	reproposedCmds map[roachpb.RangeID]map[kvserverbase.CmdIDKey]map[kvserverbase.CmdIDKey]bool
	// seedCmds maps a LAI reproposal command ID back to its original seed proposal.
	// It is the reverse index of reproposedCmds.
	seedCmds map[roachpb.RangeID]map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey
	// appliedCmds tracks the log index at which a command is applied. Reproposals
	// under new command IDs are tracked as separate commands here (only one
	// should apply).
	appliedCmds map[roachpb.RangeID]map[kvserverbase.CmdIDKey]appliedPosition
	// replicaApplied tracks the applied position of each replica.
	replicaApplied map[roachpb.RangeID]map[roachpb.ReplicaID]appliedPosition
}

type appliedPosition struct {
	raftIndex         kvpb.RaftIndex
	raftTerm          kvpb.RaftTerm
	leaseAppliedIndex kvpb.LeaseAppliedIndex
	closedTS          hlc.Timestamp
}

func (a appliedPosition) String() string {
	if a == (appliedPosition{}) {
		return "<empty>"
	}
	return fmt.Sprintf("index %d term %d (LAI=%d CTS=%s)",
		a.raftIndex, a.raftTerm, a.leaseAppliedIndex, a.closedTS)
}

// NewAsserter creates a new asserter.
func NewAsserter() *Asserter {
	return &Asserter{
		proposedCmds:   map[roachpb.RangeID]map[kvserverbase.CmdIDKey]roachpb.ReplicaID{},
		reproposedCmds: map[roachpb.RangeID]map[kvserverbase.CmdIDKey]map[kvserverbase.CmdIDKey]bool{},
		seedCmds:       map[roachpb.RangeID]map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{},
		appliedCmds:    map[roachpb.RangeID]map[kvserverbase.CmdIDKey]appliedPosition{},
		replicaApplied: map[roachpb.RangeID]map[roachpb.ReplicaID]appliedPosition{},
	}
}

// ensureRangeLocked ensures that data structures exist for the given range.
// Asserter.mu must be held.
func (a *Asserter) ensureRangeLocked(rangeID roachpb.RangeID) {
	if _, ok := a.proposedCmds[rangeID]; !ok {
		a.proposedCmds[rangeID] = map[kvserverbase.CmdIDKey]roachpb.ReplicaID{}
	}
	if _, ok := a.reproposedCmds[rangeID]; !ok {
		a.reproposedCmds[rangeID] = map[kvserverbase.CmdIDKey]map[kvserverbase.CmdIDKey]bool{}
	}
	if _, ok := a.seedCmds[rangeID]; !ok {
		a.seedCmds[rangeID] = map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{}
	}
	if _, ok := a.appliedCmds[rangeID]; !ok {
		a.appliedCmds[rangeID] = map[kvserverbase.CmdIDKey]appliedPosition{}
	}
	if _, ok := a.replicaApplied[rangeID]; !ok {
		a.replicaApplied[rangeID] = map[roachpb.ReplicaID]appliedPosition{}
	}
}

// Propose tracks and asserts command proposals.
func (a *Asserter) Propose(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID, seedID kvserverbase.CmdIDKey,
	cmd *kvserverpb.RaftCommand,
	req kvpb.BatchRequest,
) {
	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d cmd %s: %s (%s)",
			rangeID, replicaID, cmdID, fmt.Sprintf(msg, args...), req))
	}

	// INVARIANT: all proposals have a command ID.
	if len(cmdID) == 0 {
		fail("proposed command without ID")
	}

	if req.IsSingleRequestLeaseRequest() {
		// INVARIANT: lease requests never set a LAI or closed timestamp. These have
		// their own replay protection.
		if cmd.MaxLeaseIndex != 0 || cmd.ClosedTimestamp != nil {
			fail("lease request proposal with LAI=%s CTS=%s", cmd.MaxLeaseIndex, cmd.ClosedTimestamp)
		}

	} else {
		// INVARIANT: all non-lease-request proposals have a LAI.
		if cmd.MaxLeaseIndex == 0 {
			fail("proposal without LAI")
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureRangeLocked(rangeID)

	// INVARIANT: a command can only be proposed by a single replica. The same
	// command may be reproposed under the same ID by the same replica.
	if proposedBy, ok := a.proposedCmds[rangeID][cmdID]; ok && replicaID != proposedBy {
		fail("originally proposed by different replica %d", proposedBy)
	}
	a.proposedCmds[rangeID][cmdID] = replicaID

	// Check and track LAI reproposals. These use a different command ID than the
	// original seed proposal.
	if seedID != "" {
		if _, ok := a.reproposedCmds[rangeID][seedID]; !ok {
			a.reproposedCmds[rangeID][seedID] = map[kvserverbase.CmdIDKey]bool{}
		}

		if seedProposedBy, ok := a.proposedCmds[rangeID][seedID]; !ok {
			// INVARIANT: a LAI reproposal must reference a previous seed proposal.
			fail("unknown seed proposal %s", seedID)
		} else if seedProposedBy != replicaID {
			// INVARIANT: a LAI reproposal must be made by the seed replica.
			fail("seed proposal %s by different replica %d", seedID, seedProposedBy)
		}
		a.reproposedCmds[rangeID][seedID][cmdID] = true

		if s, ok := a.seedCmds[rangeID][cmdID]; ok && s != seedID {
			// INVARIANT: a reproposal of a LAI reproposal must always reference the
			// same seed proposal.
			fail("expected seed proposal %s, got %s", s, seedID)
		}
		a.seedCmds[rangeID][cmdID] = seedID
	}
}

// Apply tracks and asserts command application. The command must be reported
// via Propose() first.
func (a *Asserter) Apply(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID kvserverbase.CmdIDKey,
	entry raftpb.Entry,
	leaseAppliedIndex kvpb.LeaseAppliedIndex,
	closedTS hlc.Timestamp,
) {
	pos := appliedPosition{
		raftIndex:         kvpb.RaftIndex(entry.Index),
		raftTerm:          kvpb.RaftTerm(entry.Term),
		leaseAppliedIndex: leaseAppliedIndex,
		closedTS:          closedTS,
	}

	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d cmd %s: %s (%s)\ndata: %x",
			rangeID, replicaID, cmdID, fmt.Sprintf(msg, args...), pos, entry.Data))
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureRangeLocked(rangeID)

	// INVARIANT: the replica's applied position can't regress.
	newReplicaApplied := pos
	if applied, ok := a.replicaApplied[rangeID][replicaID]; ok {
		// INVARIANT: the applied index must progress.
		if pos.raftIndex <= applied.raftIndex {
			fail("applied index regression %d -> %d", applied.raftIndex, pos.raftIndex)
		}
		// INVARIANT: the Raft term must not regress.
		if pos.raftTerm < applied.raftTerm {
			fail("applied term regression %d -> %d", applied.raftTerm, pos.raftTerm)
		}
		// Lease requests or non-CRDB proposals don't carry a LAI and closed
		// timestamp, keep the replica's current values. Propose() asserts that only
		// lease requests do this.
		if leaseAppliedIndex == 0 {
			newReplicaApplied.leaseAppliedIndex = applied.leaseAppliedIndex
			newReplicaApplied.closedTS = applied.closedTS
		} else {
			// INVARIANT: the lease applied index must progress.
			if pos.leaseAppliedIndex <= applied.leaseAppliedIndex {
				fail("lease applied index regression %d -> %d",
					applied.leaseAppliedIndex, pos.leaseAppliedIndex)
			}
			// INVARIANT: the closed timestamp must not regress.
			if pos.closedTS.Less(applied.closedTS) {
				fail("closed timestamp regression %s -> %s", applied.closedTS, pos.closedTS)
			}
		}
	}
	a.replicaApplied[rangeID][replicaID] = newReplicaApplied

	// etcd/raft may submit entries without a command ID, typically noop commands
	// on leader changes. Ignore these after updating the replica position. In
	// Propose() we assert that commands we propose ourselves have a command ID.
	if len(cmdID) == 0 {
		return
	}

	// INVARIANT: a command must be proposed before it can apply.
	if _, ok := a.proposedCmds[rangeID][cmdID]; !ok {
		fail("command was not proposed")
	}

	// INVARIANT: a given command must always apply at the same position
	// across all replicas.
	if appliedPos, ok := a.appliedCmds[rangeID][cmdID]; ok && appliedPos != pos {
		fail("command already applied at %s", appliedPos)
	}
	a.appliedCmds[rangeID][cmdID] = pos

	// INVARIANT: a request is only ever applied once via a single command, across
	// the original seed proposal and all LAI reproposals.
	seedID, ok := a.seedCmds[rangeID][cmdID]
	if !ok {
		seedID = cmdID
	}
	otherCmds := map[kvserverbase.CmdIDKey]bool{seedID: true}
	for id := range a.reproposedCmds[rangeID][seedID] {
		otherCmds[id] = true
	}
	delete(otherCmds, cmdID) // this command applied
	for id := range otherCmds {
		if appliedPos, ok := a.appliedCmds[rangeID][id]; ok {
			fail("request already applied by different command %s at %s", id, appliedPos)
		}
	}
}

// ApplySnapshot tracks and asserts snapshot application.
func (a *Asserter) ApplySnapshot(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	sender roachpb.ReplicaID,
	index kvpb.RaftIndex,
	term kvpb.RaftTerm,
	leaseAppliedIndex kvpb.LeaseAppliedIndex,
	closedTS hlc.Timestamp,
) {
	pos := appliedPosition{
		raftIndex:         index,
		raftTerm:          term,
		leaseAppliedIndex: leaseAppliedIndex,
		closedTS:          closedTS,
	}

	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d snapshot from %d at index %d: %s (%s)",
			rangeID, replicaID, sender, pos.raftIndex, fmt.Sprintf(msg, args...), pos))
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.ensureRangeLocked(rangeID)

	// INVARIANT: a snapshot must not regress the replica's applied position.
	//
	// NB: this is different from the assertions in Apply(), where the indices
	// must progress. Here, it's ok for indices to remain the same, although we
	// don't expect this to happen in practice.
	if applied, ok := a.replicaApplied[rangeID][replicaID]; ok {
		if pos.raftIndex < applied.raftIndex {
			fail("applied index regression %d -> %d", applied.raftIndex, pos.raftIndex)
		}
		if pos.raftTerm < applied.raftTerm {
			fail("applied term regression %d -> %d", applied.raftTerm, pos.raftTerm)
		}
		if pos.leaseAppliedIndex < applied.leaseAppliedIndex {
			fail("lease applied index regression %d -> %d",
				applied.leaseAppliedIndex, pos.leaseAppliedIndex)
		}
		if pos.closedTS.Less(applied.closedTS) {
			fail("closed timestamp regression %s -> %s", applied.closedTS, closedTS)
		}
	}
	a.replicaApplied[rangeID][replicaID] = pos
}
