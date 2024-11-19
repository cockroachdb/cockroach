// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apply

import (
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Asserter is a test utility that tracks application of Raft commands, and
// primarily asserts that:
//
//   - A request is only applied once, at a single log index and lease applied
//     index across all replicas (i.e. no double-applies or replays).
//
//   - Commands do not regress the Raft index/term, lease applied index, and
//     closed timestamp (assuming no node restarts).
//
//   - All replicas apply the same commands in the same order at the same
//     positions.
type Asserter struct {
	syncutil.Mutex
	ranges map[roachpb.RangeID]*rangeAsserter
}

// rangeAsserter tracks and asserts application for an individual range. We
// don't make cross-range assertions.
type rangeAsserter struct {
	syncutil.Mutex
	rangeID roachpb.RangeID
	// log tracks applied commands by their Raft log index. It may have gaps,
	// represented by empty entries -- typically rejected commands that don't
	// apply, or noop commands on Raft leader changes.
	log []applyState
	// proposedCmds tracks proposed commands by ID, along with the proposer. LAI
	// reproposals under new command IDs are considered separate proposals here.
	proposedCmds map[kvserverbase.CmdIDKey]roachpb.ReplicaID
	// seedCmds maps a LAI reproposal command ID back to its original seed proposal.
	seedCmds map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey
	// seedAppliedAs tracks which command actually applied an original seed
	// command, for LAI reproposals which use separate command IDs. It is tracked
	// for all seed proposals, regardless of whether they are reproposed.
	seedAppliedAs map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey
	// appliedCmds tracks the log index at which a command is applied. LAI
	// reproposals under new command IDs are tracked as separate commands here
	// (only one should apply).
	appliedCmds map[kvserverbase.CmdIDKey]kvpb.RaftIndex
	// replicaAppliedIndex tracks the applied index of each replica.
	replicaAppliedIndex map[roachpb.ReplicaID]kvpb.RaftIndex
}

type applyState struct {
	cmdID             kvserverbase.CmdIDKey
	raftIndex         kvpb.RaftIndex
	raftTerm          kvpb.RaftTerm
	leaseAppliedIndex kvpb.LeaseAppliedIndex
	closedTS          hlc.Timestamp
}

var gap = applyState{}

func (l applyState) String() string {
	if l == (applyState{}) {
		return "<empty>"
	}
	return fmt.Sprintf("cmd %q index %d term %d (LAI=%d CTS=%s)",
		l.cmdID, l.raftIndex, l.raftTerm, l.leaseAppliedIndex, l.closedTS)
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
		rangeID:             rangeID,
		proposedCmds:        map[kvserverbase.CmdIDKey]roachpb.ReplicaID{},
		seedCmds:            map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{},
		seedAppliedAs:       map[kvserverbase.CmdIDKey]kvserverbase.CmdIDKey{},
		appliedCmds:         map[kvserverbase.CmdIDKey]kvpb.RaftIndex{},
		replicaAppliedIndex: map[roachpb.ReplicaID]kvpb.RaftIndex{},
	}
	a.ranges[rangeID] = r
	return r
}

// stateAt reconstructs the applied state at the given index, by finding the
// latest non-empty value of each field at the given index. In particular, it
// finds the latest LAI and closed timestamp, which may be omitted by individual
// entries (e.g. lease requests). Gaps are skipped, so the returned raftIndex
// may be lower than the given index.
//
// In the common case, this only needs to look at the previous entry.
func (r *rangeAsserter) stateAt(index kvpb.RaftIndex) applyState {
	if int(index) >= len(r.log) {
		panic(fmt.Sprintf("index %d is beyond end of log at %d", index, len(r.log)-1))
	}
	// All entries (except gaps) have cmdID, raftIndex, and raftTerm, so we're
	// done once we also find a LAI and closed timestamp.
	var s applyState
	for i := int(index); i >= 0 && (s.leaseAppliedIndex == 0 || s.closedTS.IsEmpty()); i-- {
		e := r.log[i]
		if s.cmdID == "" {
			s.cmdID = e.cmdID
		}
		if s.raftIndex == 0 {
			s.raftIndex = e.raftIndex
		}
		if s.raftTerm == 0 {
			s.raftTerm = e.raftTerm
		}
		if s.leaseAppliedIndex == 0 {
			s.leaseAppliedIndex = e.leaseAppliedIndex
		}
		if s.closedTS.IsEmpty() {
			s.closedTS = e.closedTS
		}
	}
	return s
}

// Propose tracks and asserts command proposals.
func (a *Asserter) Propose(
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	cmdID, seedID kvserverbase.CmdIDKey,
	cmd *kvserverpb.RaftCommand,
	req *kvpb.BatchRequest,
) {
	a.forRange(rangeID).propose(replicaID, cmdID, seedID, cmd, req)
}

func (r *rangeAsserter) propose(
	replicaID roachpb.ReplicaID,
	cmdID, seedID kvserverbase.CmdIDKey,
	cmd *kvserverpb.RaftCommand,
	req *kvpb.BatchRequest,
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
		// their own replay protection (using a conditional write).
		//
		// TODO(erikgrinaker): consider assertions around lease request replays.
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
	raftEntry raftpb.Entry,
	leaseAppliedIndex kvpb.LeaseAppliedIndex,
	closedTS hlc.Timestamp,
) {
	entry := applyState{
		cmdID:             cmdID,
		raftIndex:         kvpb.RaftIndex(raftEntry.Index),
		raftTerm:          kvpb.RaftTerm(raftEntry.Term),
		leaseAppliedIndex: leaseAppliedIndex,
		closedTS:          closedTS,
	}

	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d: %s (%s)\ndata: %x",
			r.rangeID, replicaID, fmt.Sprintf(msg, args...), entry, raftEntry.Data))
	}

	// INVARIANT: all commands have a command ID. etcd/raft may commit noop
	// proposals on leader changes that do not have a command ID, but we skip
	// these during application.
	if len(cmdID) == 0 {
		fail("applied command without ID")
	}

	r.Lock()
	defer r.Unlock()

	// INVARIANT: a command must be proposed before it can apply.
	if _, ok := r.proposedCmds[cmdID]; !ok {
		fail("command was not proposed")
	}

	if int(entry.raftIndex) < len(r.log) {
		// INVARIANT: all replicas must apply the same log entry at the same index.
		if e := r.log[int(entry.raftIndex)]; e != entry {
			fail("applied entry differs from existing log entry: %s", e)
		}

	} else {
		// The entry is not yet tracked in the log.

		// INVARIANT: entries may not regress the applied state.
		if len(r.log) > 0 {
			s := r.stateAt(kvpb.RaftIndex(len(r.log) - 1))
			// INVARIANT: the Raft index must progress.
			//
			// This is trivially true since we're appending to the log, which is
			// indexed by the Raft log index. We assert it anyway, as documentation.
			if entry.raftIndex <= s.raftIndex {
				fail("Raft index regression %d -> %d", s.raftIndex, entry.raftIndex)
			}
			// INVARIANT: the Raft term must not regress.
			if entry.raftTerm < s.raftTerm {
				fail("Raft term regression %d -> %d", s.raftTerm, entry.raftTerm)
			}
			// INVARIANT: the lease applied index must progress. Lease requests don't
			// carry a LAI (asserted in Propose).
			if entry.leaseAppliedIndex > 0 && entry.leaseAppliedIndex <= s.leaseAppliedIndex {
				fail("lease applied index regression %d -> %d",
					s.leaseAppliedIndex, entry.leaseAppliedIndex)
			}
			// INVARIANT: the closed timestamp must not regress. Lease requests don't
			// carry a closed timestamp (asserted in Propose).
			if entry.closedTS.IsSet() && entry.closedTS.Less(s.closedTS) {
				fail("closed timestamp regression %s -> %s", s.closedTS, entry.closedTS)
			}
		}

		// Append the entry, and insert gaps as necessary -- e.g. due to rejected
		// commands or etcd/raft noop commands on leader changes.
		for i := len(r.log); i < int(entry.raftIndex); i++ {
			r.log = append(r.log, applyState{}) // insert gap
		}
		r.log = append(r.log, entry)
	}

	// INVARIANT: the replica's applied index must progress.
	if i := r.replicaAppliedIndex[replicaID]; entry.raftIndex < i {
		fail("applied index regression %d -> %d", i, entry.raftIndex)
	}

	// INVARIANT: the replica must apply all commands, sequentially (except
	// when applying a snapshot).
	for i := r.replicaAppliedIndex[replicaID] + 1; i < entry.raftIndex; i++ {
		if e := r.log[i]; e != gap { // ignore gaps
			fail("replica skipped log entry: %s", e)
		}
	}
	r.replicaAppliedIndex[replicaID] = entry.raftIndex

	// INVARIANT: a given command must at most apply at a single index.
	if appliedIndex, ok := r.appliedCmds[cmdID]; ok && appliedIndex != entry.raftIndex {
		fail("command already applied at %s", r.log[appliedIndex])
	}
	r.appliedCmds[cmdID] = entry.raftIndex

	// INVARIANT: a command is only applied under a single command ID, even across
	// multiple LAI reproposals using different command IDs.
	seedID, ok := r.seedCmds[cmdID]
	if !ok {
		// This is not a LAI reproposal, so it's a seed proposal. It may or may not
		// have seen LAI reproposals.
		seedID = cmdID
	}
	if appliedAs, ok := r.seedAppliedAs[seedID]; ok && appliedAs != cmdID {
		fail("command already applied as %s at %s", appliedAs, r.log[r.appliedCmds[appliedAs]])
	}
	r.seedAppliedAs[seedID] = cmdID
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
	state := applyState{
		raftIndex:         index,
		raftTerm:          term,
		leaseAppliedIndex: leaseAppliedIndex,
		closedTS:          closedTS,
	}

	fail := func(msg string, args ...interface{}) {
		panic(fmt.Sprintf("r%d/%d snapshot from %d at index %d: %s (%s)",
			r.rangeID, replicaID, sender, index, fmt.Sprintf(msg, args...), state))
	}

	r.Lock()
	defer r.Unlock()

	// INVARIANT: a snapshot must progress the replica's applied index.
	if ri := r.replicaAppliedIndex[replicaID]; index <= ri {
		fail("replica applied index regression: %d -> %d", ri, index)
	}
	r.replicaAppliedIndex[replicaID] = index

	// We can't have a snapshot without any applied log entries, except when this
	// is an initial snapshot. It's possible that the initial snapshot follows an
	// empty entry appended by the raft leader at the start of this term. Since we
	// don't register this entry as applied, r.log can be empty here.
	//
	// See the comment in r.apply() method, around the empty cmdID check, and the
	// comment for r.log saying that there can be gaps in the observed applies.
	if len(r.log) == 0 {
		return
	}

	// INVARIANT: a snapshot must match the applied state at the given Raft index.
	//
	// The snapshot may point beyond the log or to a gap in our log because of
	// rejected or noop commands, in which case we match it against the state of
	// the latest applied command before the snapshot index.
	logIndex := index
	if lastIndex := kvpb.RaftIndex(len(r.log) - 1); logIndex > lastIndex {
		logIndex = lastIndex
	}
	logState := r.stateAt(logIndex)
	logState.cmdID = "" // not known for snapshot, ignore it
	if state.raftIndex > logState.raftIndex && state.raftTerm >= logState.raftTerm {
		// Snapshot pointed to an unknown entry, most likely a rejected command.
		// Compare it with the latest applied command.
		state.raftIndex = logState.raftIndex
		state.raftTerm = logState.raftTerm
	}
	if state != logState {
		fail("state differs from log state: %s", state)
	}
}
