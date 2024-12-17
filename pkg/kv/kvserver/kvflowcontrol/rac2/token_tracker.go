// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/container/ring"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// entryID is the ID of a raft log entry. Should not be confused with LogMark.
// TODO(pav-kv): export raft.entryID and use it here.
type entryID struct {
	index uint64
	term  uint64
}

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry identified by its raft entryID, with
// a given priority.
type Tracker struct {
	// term is the raft leader term owning this tracker. Does not change during
	// the lifetime of this tracker, after Init is called.
	term uint64
	// tracked contains the per-priority tracked log entries ordered by log index.
	// All the tracked entries are in the term's leader log.
	tracked [raftpb.NumPriorities]ring.Buffer[tracked]
	// deducted contains the per-priority token deduction totals.
	// Invariant: deducted[pri] = sum(tracked[pri][i].tokens)
	deducted [raftpb.NumPriorities]kvflowcontrol.Tokens

	stream kvflowcontrol.Stream // used for logging only
}

// tracked represents a flow token deduction, identified by the raft entry ID.
type tracked struct {
	id     entryID
	tokens kvflowcontrol.Tokens
}

func (t *Tracker) Init(term uint64, stream kvflowcontrol.Stream) {
	*t = Tracker{
		term:   term,
		stream: stream,
	}
}

func (t *Tracker) Empty() bool {
	// TODO(pav-kv): can optimize this loop out if needed. We can maintain the
	// total number of tokens held, and return whether it's zero. It's also
	// possible to make it atomic and avoid locking the mutex in replicaSendStream
	// when calling this.
	for pri := range t.tracked {
		if t.tracked[pri].Length() != 0 {
			return false
		}
	}
	return true
}

// Track registers a token deduction of the given priority for the given raft
// entry. Entries must be registered in the increasing order of log index.
func (t *Tracker) Track(
	ctx context.Context, id entryID, pri raftpb.Priority, tokens kvflowcontrol.Tokens,
) bool {
	// TODO(pav-kv): pass in the leader term, and check that it matches the term
	// of the leader whose Tracker this is.
	if length := t.tracked[pri].Length(); length >= 1 {
		last := t.tracked[pri].At(length - 1).id
		// Tracker exists in the context of a single replicaSendStream, which cannot
		// span the leader losing leadership and regaining it. So the entry IDs must
		// advance.
		if id.index <= last.index || id.term < last.term {
			log.Fatalf(ctx, "expected in order tracked log entries: last=%+v, entry=%+v", last, id)
			return false
		}
	}
	t.tracked[pri].Push(tracked{id: id, tokens: tokens})
	t.deducted[pri] += tokens

	if log.V(1) {
		log.Infof(ctx, "tracking %v flow control tokens for pri=%s stream=%s log-position=%d/%d",
			tokens, pri, t.stream, id.term, id.index)
	}
	return true
}

// Untrack all token deductions of the given priority that have indexes less
// than or equal to the one provided, per priority, and terms less than or
// equal to the leader term. evalTokensGEIndex is used to separately count the
// untracked (eval) tokens that are for indices >= evalTokensGEIndex.
func (t *Tracker) Untrack(
	av AdmittedVector, evalTokensGEIndex uint64,
) (returnedSend, returnedEval [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	if av.Term != t.term {
		// If av.Term < t.term, this is an admission vector for a stale leader, and
		// we should ignore it. The current-term log is not guaranteed to be
		// consistent with the av.Term log.
		//
		// TODO(pav-kv): if av.Term > t.term, we should return all tokens instead of
		// returning here. However, raft will know about the term bump soon enough,
		// exit StateLeader, and the tokens will be returned anyway.
		return
	}

	for pri, uptoIndex := range av.Admitted {
		var untracked int
		for n := t.tracked[pri].Length(); untracked < n; untracked++ {
			deduction := t.tracked[pri].At(untracked)
			if deduction.id.index > uptoIndex {
				break
			}
			returnedSend[pri] += deduction.tokens
			t.deducted[pri] -= deduction.tokens
			if deduction.id.index >= evalTokensGEIndex {
				returnedEval[pri] += deduction.tokens
			}
		}
		if untracked > 0 {
			t.tracked[pri].Pop(untracked)
		}
	}

	return returnedSend, returnedEval
}

// UntrackAll removes all tracked deductions, and returns the total amount of
// previously tracked tokens for each priority.
func (t *Tracker) UntrackAll() (returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	returned = t.deducted
	t.deducted = [raftpb.NumPriorities]kvflowcontrol.Tokens{}
	t.tracked = [raftpb.NumPriorities]ring.Buffer[tracked]{}
	return returned
}

// tokensGE returns the sum of the current tokens in the tracker with index >=
// index.
func (t *Tracker) tokensGE(index uint64) [raftpb.NumPriorities]kvflowcontrol.Tokens {
	var tokens [raftpb.NumPriorities]kvflowcontrol.Tokens
	for pri := range t.tracked {
		n := t.tracked[pri].Length()
		for i := n - 1; i >= 0; i-- {
			tracked := t.tracked[pri].At(i)
			if tracked.id.index >= index {
				tokens[pri] += tracked.tokens
			} else {
				// NB: the tracked indices are in ascending order, we are iterating in
				// reverse order, so no more indices will satisfy the condition to be
				// counted in tokens.
				break
			}
		}
	}
	return tokens
}

// Inspect returns a snapshot of all tracked token deductions. It's used to
// power /inspectz-style debugging pages.
func (t *Tracker) Inspect() ([]kvflowinspectpb.TrackedDeduction, kvflowcontrol.Tokens) {
	var res []kvflowinspectpb.TrackedDeduction
	var totalTokens kvflowcontrol.Tokens
	for pri := range t.tracked {
		n := t.tracked[pri].Length()
		for i := 0; i < n; i++ {
			deduction := t.tracked[pri].At(i)
			res = append(res, kvflowinspectpb.TrackedDeduction{
				Tokens: int64(deduction.tokens),
				RaftLogPosition: kvflowcontrolpb.RaftLogPosition{
					Index: deduction.id.index,
					Term:  deduction.id.term,
				},
				Priority: int32(RaftToAdmissionPriority(raftpb.Priority(pri))),
			})
		}
		// TODO(pav-kv): consider returning per-priority deductions instead of total.
		totalTokens += t.deducted[pri]
	}
	return res, totalTokens
}
