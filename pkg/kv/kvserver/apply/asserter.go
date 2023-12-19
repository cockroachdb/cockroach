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
	syncutil.Mutex
	ranges map[roachpb.RangeID]*rangeAsserter
}

// rangeAsserter tracks and asserts application for an individual range. We
// don't make cross-range assertions.
type rangeAsserter struct {
	syncutil.Mutex
	rangeID roachpb.RangeID
	// proposedCmds tracks proposed commands by ID, along with the proposer. LAI
	// reproposals under new command IDs are considered separate proposals here.
	proposedCmds map[kvserverbase.CmdIDKey]roachpb.ReplicaID
	// seedCmds maps a LAI reproposal command ID back to its original seed proposal.
	seedCmds map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey
	// seedAppliedAs tracks which command actually applied an original seed
	// command, for LAI reproposals which use separate command IDs. It is tracked
	// for all seed proposals, regardless of whether they are reproposed.
	seedAppliedAs map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey
	// appliedCmds tracks the position at which a command is applied. LAI
	// reproposals under new command IDs are tracked as separate commands here
	// (only one should apply).
	appliedCmds map[kvserverbase.CmdIDKey]appliedPosition
	// replicaApplied tracks the applied position of each replica.
	replicaApplied map[roachpb.ReplicaID]appliedPosition
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
		ranges: map[roachpb.RangeID]*rangeAsserter{},
	}
}

// forRange retrieves or creates a rangeAsserter for the given range.
func (a *Asserter) forRange(rangeID roachpb.RangeID) *rangeAsserter {
	a.Lock()
	defer a.Unlock()

	if r := a.ranges[rangeID]; r != nil {
		return r
	}
	r := &rangeAsserter{
		rangeID:        rangeID,
		proposedCmds:   map[kvserverbase.CmdIDKey]roachpb.ReplicaID{},
		seedCmds:       map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{},
		seedAppliedAs:  map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{},
		appliedCmds:    map[kvserverbase.CmdIDKey]appliedPosition{},
		replicaApplied: map[roachpb.ReplicaID]appliedPosition{},
	}
	a.ranges[rangeID] = r
	return r
}

// Propose tracks and asserts command proposals.
func (a *Asserter) Propose(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID, seedID kvserverbase.CmdIDKey,
	cmd *kvserverpb.RaftCommand,
	req kvpb.BatchRequest,
) {
	a.forRange(rangeID).propose(replicaID, cmdID, seedID, cmd, req)
}

func (r *rangeAsserter) propose(
	replicaID roachpb.ReplicaID,
	cmdID, seedID kvserverbase.CmdIDKey,
	cmd *kvserverpb.RaftCommand,
	req kvpb.BatchRequest,
) {
	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d cmd %s: %s (%s)",
			r.rangeID, replicaID, cmdID, fmt.Sprintf(msg, args...), req))
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

	r.Lock()
	defer r.Unlock()

	// INVARIANT: a command can only be proposed by a single replica. The same
	// command may be reproposed under the same ID by the same replica.
	if proposedBy, ok := r.proposedCmds[cmdID]; ok && replicaID != proposedBy {
		fail("originally proposed by different replica %d", proposedBy)
	}
	r.proposedCmds[cmdID] = replicaID

	// Check and track LAI reproposals. These use a different command ID than the
	// original seed proposal.
	if seedID != "" {
		if seedProposedBy, ok := r.proposedCmds[seedID]; !ok {
			// INVARIANT: a LAI reproposal must reference a previous seed proposal.
			fail("unknown seed proposal %s", seedID)
		} else if seedProposedBy != replicaID {
			// INVARIANT: a LAI reproposal must be made by the seed replica.
			fail("seed proposal %s by different replica %d", seedID, seedProposedBy)
		}

		if s, ok := r.seedCmds[cmdID]; ok && s != seedID {
			// INVARIANT: a reproposal of a LAI reproposal must always reference the
			// same seed proposal.
			fail("expected seed proposal %s, got %s", s, seedID)
		}
		r.seedCmds[cmdID] = seedID
	}
}

// Apply tracks and asserts command application. Rejected commands (e.g. via LAI
// checks) are not considered applied. The command must be reported via
// Propose() first.
func (a *Asserter) Apply(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID kvserverbase.CmdIDKey,
	entry raftpb.Entry,
	leaseAppliedIndex kvpb.LeaseAppliedIndex,
	closedTS hlc.Timestamp,
) {
	a.forRange(rangeID).apply(replicaID, cmdID, entry, leaseAppliedIndex, closedTS)
}

func (r *rangeAsserter) apply(
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
			r.rangeID, replicaID, cmdID, fmt.Sprintf(msg, args...), pos, entry.Data))
	}

	// INVARIANT: all commands have a command ID. etcd/raft may commit noop
	// proposals on leader changes that do not have a command ID, but we skip
	// these during application.
	if len(cmdID) == 0 {
		fail("applied command without ID")
	}

	r.Lock()
	defer r.Unlock()

	// INVARIANT: the replica's applied position can't regress. There may be gaps
	// in the apply sequence, e.g. due to rejected commands (e.g. LAI violations)
	// or etcd/raft noop commands on leader changes.
	newReplicaApplied := pos
	if applied, ok := r.replicaApplied[replicaID]; ok {
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
	r.replicaApplied[replicaID] = newReplicaApplied

	// etcd/raft may submit entries without a command ID, typically noop commands
	// on leader changes. Ignore these after updating the replica position. In
	// Propose() we assert that commands we propose ourselves have a command ID.
	if len(cmdID) == 0 {
		return
	}

	// INVARIANT: a command must be proposed before it can apply.
	if _, ok := r.proposedCmds[cmdID]; !ok {
		fail("command was not proposed")
	}

	// INVARIANT: a given command must always apply at the same position
	// across all replicas.
	if appliedPos, ok := r.appliedCmds[cmdID]; ok && appliedPos != pos {
		fail("command already applied at %s", appliedPos)
	}
	r.appliedCmds[cmdID] = pos

	// INVARIANT: a command is only applied under a single command ID, even across
	// multiple LAI reproposals using different command IDs.
	seedID, ok := r.seedCmds[cmdID]
	if !ok {
		// This is not a LAI reproposal, so it's a seed proposal. It may or may not
		// have seen LAI reproposals.
		seedID = cmdID
	}
	if appliedAs, ok := r.seedAppliedAs[seedID]; ok && appliedAs != cmdID {
		fail("command already applied as %s at %s", appliedAs, r.appliedCmds[appliedAs])
	}
	r.seedAppliedAs[seedID] = cmdID
}

// ApplySnapshot tracks and asserts snapshot applieetion.
func (a *Asserter) ApplySnapshot(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	sender roachpb.ReplicaID,
	index kvpb.RaftIndex,
	term kvpb.RaftTerm,
	leaseAppliedIndex kvpb.LeaseAppliedIndex,
	closedTS hlc.Timestamp,
) {
	a.forRange(rangeID).applySnapshot(replicaID, sender, index, term, leaseAppliedIndex, closedTS)
}

func (r *rangeAsserter) applySnapshot(
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
			r.rangeID, replicaID, sender, pos.raftIndex, fmt.Sprintf(msg, args...), pos))
	}

	r.Lock()
	defer r.Unlock()

	// INVARIANT: a snapshot must not regress the replica's applied position.
	//
	// NB: this is different from the assertions in Apply(), where the indices
	// must progress. Here, it's ok for indices to remain the same, although we
	// don't expect this to happen in practice.
	if applied, ok := r.replicaApplied[replicaID]; ok {
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
	r.replicaApplied[replicaID] = pos
}
