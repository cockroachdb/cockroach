// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry (identified by raft index position),
// with a given RaftPriority.
type Tracker struct {
	tracked [kvflowcontrolpb.NumRaftPriorities][]tracked

	stream kvflowcontrol.Stream // used for logging only
}

func (dt *Tracker) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "Tracked(%v)=[", dt.stream)
	i := 0
	for pri, tracked := range dt.tracked {
		if len(tracked) == 0 {
			continue
		}
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%v=%v", kvflowcontrolpb.RaftPriority(pri), tracked)
		i++
	}
	buf.WriteString("]")
	return buf.String()
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log position (typically where the proposed command is expected to end
// up).
type tracked struct {
	tokens                    kvflowcontrol.Tokens
	originalPri, inheritedPri kvflowcontrolpb.RaftPriority
	index                     uint64
}

func (t tracked) String() string {
	return fmt.Sprintf("{tokens=%s ogPri=%s inPri=%s index=%d}",
		t.tokens, t.originalPri, t.inheritedPri, t.index)
}

// Init constructs a new Tracker with the given lower bound raft log position
// (below which we're not allowed to deduct tokens).
func (dt *Tracker) Init(stream kvflowcontrol.Stream) {
	*dt = Tracker{
		tracked: [int(kvflowcontrolpb.NumRaftPriorities)][]tracked{},
		stream:  stream,
	}
}

// Track token deductions of the given priority with the given raft log index.
// originalPri is used to return eval tokens, while inheritedPri is used to
// return send tokens.
func (dt *Tracker) Track(
	ctx context.Context,
	index uint64,
	inheritedPri kvflowcontrolpb.RaftPriority,
	originalPri kvflowcontrolpb.RaftPriority,
	tokens kvflowcontrol.Tokens,
) bool {
	if len(dt.tracked[inheritedPri]) >= 1 {
		last := dt.tracked[inheritedPri][len(dt.tracked[inheritedPri])-1]
		if last.index >= index {
			log.Fatalf(ctx, "expected in order tracked log indexes (%d < %d)",
				last.index, index)
			return false
		}
	}

	dt.tracked[inheritedPri] = append(dt.tracked[inheritedPri], tracked{
		tokens:       tokens,
		originalPri:  originalPri,
		inheritedPri: inheritedPri,
		index:        index,
	})

	if log.V(1) {
		log.Infof(ctx, "tracking %s flow control tokens for pri=%s stream=%s index=%d",
			tokens, inheritedPri, dt.stream, index)
	}
	return true
}

// Untrack all token deductions of the given priority that have indexes less
// than or equal to the one provided.
func (dt *Tracker) Untrack(
	inheritedPri kvflowcontrolpb.RaftPriority,
	uptoIndex uint64,
	f func(index uint64, originalPri kvflowcontrolpb.RaftPriority, tokens kvflowcontrol.Tokens),
) {
	log.VInfof(context.TODO(), 1, "untracking uptoIndex=%d for inheritedPri=%v dt=%v", uptoIndex, inheritedPri, dt)
	if dt == nil {
		return
	}

	var untracked int
	for {
		if untracked == len(dt.tracked[inheritedPri]) {
			break
		}
		deduction := dt.tracked[inheritedPri][untracked]
		if deduction.index > uptoIndex {
			break
		}
		if log.V(1) {
			log.Infof(context.TODO(), "untracking %s flow control tokens for pri=%s stream=%s index=%d",
				deduction.tokens, deduction.originalPri, dt.stream, deduction.index)
		}
		f(deduction.index, deduction.originalPri, deduction.tokens)
		untracked += 1
	}

	dt.tracked[inheritedPri] = dt.tracked[inheritedPri][untracked:]
}

func (dt *Tracker) UntrackGE(
	index uint64,
	f func(index uint64, inheritedPri kvflowcontrolpb.RaftPriority, originalPri kvflowcontrolpb.RaftPriority, tokens kvflowcontrol.Tokens),
) {
	log.VInfof(context.TODO(), 1, "untracking >=%d dt=%v", index, dt)
	for i := range dt.tracked {
		j := len(dt.tracked[i]) - 1
		for j >= 0 {
			tr := dt.tracked[i][j]
			if tr.index >= index {
				f(tr.index, tr.inheritedPri, tr.originalPri, tr.tokens)
				j--
			} else {
				break
			}
		}
		dt.tracked[i] = dt.tracked[i][:j+1]
	}
}

// UntrackAll iterates through all tracked token deductions, invoking the
// provided callback each deduction and untracking.
func (dt *Tracker) UntrackAll(
	f func(index uint64, inheritedPri kvflowcontrolpb.RaftPriority, originalPri kvflowcontrolpb.RaftPriority, tokens kvflowcontrol.Tokens),
) {
	log.VInfof(context.TODO(), 1, "untracking all dt=%v", dt)
	for _, deductions := range dt.tracked {
		for _, deduction := range deductions {
			f(deduction.index, deduction.inheritedPri, deduction.originalPri, deduction.tokens)
		}
	}
	dt.tracked = [kvflowcontrolpb.NumRaftPriorities][]tracked{}
}
