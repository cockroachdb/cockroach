// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry identified by its raft log index and
// term with a given RaftPriority.
type Tracker struct {
	// tracked entries are stored in increasing order of (term, index), per
	// priority.
	tracked [raftpb.NumPriorities][]tracked

	stream kvflowcontrol.Stream // used for logging only
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log index and term.
type tracked struct {
	tokens      kvflowcontrol.Tokens
	index, term uint64
}

func (dt *Tracker) Init(stream kvflowcontrol.Stream) {
	*dt = Tracker{
		tracked: [raftpb.NumPriorities][]tracked{},
		stream:  stream,
	}
}

// Track token deductions of the given priority with the given raft log index and term.
func (t *Tracker) Track(
	ctx context.Context, term uint64, index uint64, pri raftpb.Priority, tokens kvflowcontrol.Tokens,
) bool {
	if len(t.tracked[pri]) >= 1 {
		last := t.tracked[pri][len(t.tracked[pri])-1]
		// Tracker exists in the context of a single replicaSendStream, which cannot
		// span the leader losing leadership and regaining it. So the indices must
		// advance.
		if last.index >= index {
			log.Fatalf(ctx, "expected in order tracked log indexes (%d < %d)",
				last.index, index)
			return false
		}
		if last.term > term {
			log.Fatalf(ctx, "expected in order tracked leader terms (%d < %d)",
				last.term, term)
			return false
		}
	}

	t.tracked[pri] = append(t.tracked[pri], tracked{
		tokens: tokens,
		index:  index,
		term:   term,
	})

	return true
}

// Untrack all token deductions of the given priority that have indexes less
// than or equal to the one provided, per priority, and terms less than or
// equal to the leader term.
func (t *Tracker) Untrack(
	term uint64, admitted [raftpb.NumPriorities]uint64,
) (returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	for pri := range admitted {
		uptoIndex := admitted[pri]
		var untracked int
		for n := len(t.tracked[pri]); untracked < n; untracked++ {
			deduction := t.tracked[pri][untracked]
			if deduction.term > term || (deduction.term == term && deduction.index > uptoIndex) {
				break
			}
			returned[pri] += deduction.tokens
		}
		t.tracked[pri] = t.tracked[pri][untracked:]
	}

	return returned
}

// UntrackGE untracks all token deductions of the given priority that have
// indexes greater than or equal to the one provided.
func (t *Tracker) UntrackGE(index uint64) (returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	for pri := range t.tracked {
		j := len(t.tracked[pri]) - 1
		for j >= 0 {
			tr := t.tracked[pri][j]
			if tr.index >= index {
				returned[pri] += tr.tokens
				j--
			} else {
				break
			}
		}
		t.tracked[pri] = t.tracked[pri][:j+1]
	}

	return returned
}

// UntrackAll iterates through all tracked token deductions, untracking all of them
// and returning the sum of tokens for each priority.
func (t *Tracker) UntrackAll() (returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	for pri, deductions := range t.tracked {
		for _, deduction := range deductions {
			returned[pri] += deduction.tokens
		}
	}
	t.tracked = [raftpb.NumPriorities][]tracked{}

	return returned
}

// Inspect returns a snapshot of all tracked token deductions. It's used to
// power /inspectz-style debugging pages.
func (t *Tracker) Inspect() []kvflowinspectpb.TrackedDeduction {
	var res []kvflowinspectpb.TrackedDeduction
	for pri, deductions := range t.tracked {
		for _, deduction := range deductions {
			res = append(res, kvflowinspectpb.TrackedDeduction{
				Tokens: int64(deduction.tokens),
				RaftLogPosition: kvflowcontrolpb.RaftLogPosition{
					Index: deduction.index,
					Term:  deduction.term,
				},
				Priority: int32(RaftToAdmissionPriority(raftpb.Priority(pri))),
			})
		}
	}
	return res
}
