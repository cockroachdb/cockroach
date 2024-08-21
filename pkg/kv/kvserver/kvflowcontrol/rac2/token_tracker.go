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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry (identified by raft index position),
// with a given RaftPriority.
type Tracker struct {
	tracked [raftpb.NumPriorities][]tracked

	stream kvflowcontrol.Stream // used for logging only
}

func (t *Tracker) String() string {
	return redact.StringWithoutMarkers(t)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (t *Tracker) SafeFormat(w redact.SafePrinter, _ rune) {
	var buf redact.StringBuilder
	fmt.Fprintf(&buf, "Tracked(%v)=[", t.stream)
	i := 0
	for pri, tracked := range t.tracked {
		if len(tracked) == 0 {
			continue
		}
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%v=%v", raftpb.Priority(pri), tracked)
		i++
	}
	buf.WriteString("]")
	w.Print(buf)
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log position (typically where the proposed command is expected to end
// up).
type tracked struct {
	tokens                    kvflowcontrol.Tokens
	originalPri, inheritedPri raftpb.Priority
	index                     uint64
}

// SafeFormat implements the redact.SafeFormatter interface.
func (tr tracked) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("{tokens=%s ogPri=%s inPri=%s index=%d}",
		tr.tokens, tr.originalPri, tr.inheritedPri, tr.index)
}

// Init constructs a new Tracker with the given lower bound raft log position
// (below which we're not allowed to deduct tokens).
func (t *Tracker) Init(stream kvflowcontrol.Stream) {
	*t = Tracker{
		tracked: [int(raftpb.NumPriorities)][]tracked{},
		stream:  stream,
	}
}

// Track token deductions of the given priority with the given raft log index.
// originalPri is used to return eval tokens, while inheritedPri is used to
// return send tokens.
func (t *Tracker) Track(
	ctx context.Context,
	index uint64,
	inheritedPri raftpb.Priority,
	originalPri raftpb.Priority,
	tokens kvflowcontrol.Tokens,
) bool {
	if len(t.tracked[inheritedPri]) >= 1 {
		last := t.tracked[inheritedPri][len(t.tracked[inheritedPri])-1]
		if last.index >= index {
			log.Fatalf(ctx, "expected in order tracked log indexes (%d < %d)",
				last.index, index)
			return false
		}
	}

	t.tracked[inheritedPri] = append(t.tracked[inheritedPri], tracked{
		tokens:       tokens,
		originalPri:  originalPri,
		inheritedPri: inheritedPri,
		index:        index,
	})

	return true
}

// Untrack all token deductions of the given priority that have indexes less
// than or equal to the one provided.
func (t *Tracker) Untrack(
	inheritedPri raftpb.Priority,
	uptoIndex uint64,
	f func(index uint64, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens),
) {
	if t == nil {
		return
	}

	var untracked int
	for {
		if untracked == len(t.tracked[inheritedPri]) {
			break
		}
		deduction := t.tracked[inheritedPri][untracked]
		if deduction.index > uptoIndex {
			break
		}
		f(deduction.index, deduction.originalPri, deduction.tokens)
		untracked += 1
	}

	t.tracked[inheritedPri] = t.tracked[inheritedPri][untracked:]
}

func (t *Tracker) UntrackGE(
	index uint64,
	f func(index uint64, inheritedPri raftpb.Priority, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens),
) {
	for i := range t.tracked {
		j := len(t.tracked[i]) - 1
		for j >= 0 {
			tr := t.tracked[i][j]
			if tr.index >= index {
				f(tr.index, tr.inheritedPri, tr.originalPri, tr.tokens)
				j--
			} else {
				break
			}
		}
		t.tracked[i] = t.tracked[i][:j+1]
	}
}

// UntrackAll iterates through all tracked token deductions, invoking the
// provided callback each deduction and untracking.
func (t *Tracker) UntrackAll(
	f func(index uint64, inheritedPri raftpb.Priority, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens),
) {
	for _, deductions := range t.tracked {
		for _, deduction := range deductions {
			f(deduction.index, deduction.inheritedPri, deduction.originalPri, deduction.tokens)
		}
	}
	t.tracked = [raftpb.NumPriorities][]tracked{}
}
