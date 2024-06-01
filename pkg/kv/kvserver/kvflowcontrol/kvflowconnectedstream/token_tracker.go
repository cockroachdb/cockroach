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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO: track only NumRaftPriorities.
// TODO: remove the Term from the tracking.
// TODO: remove sendTokenWC.

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry (identified by raft log position), with
// a given admissionpb.WorkPriority.
type Tracker struct {
	// TODO(irfansharif,aaditya): Everytime we track something, we incur a map
	// assignment (shows up in CPU profiles). We could introduce a struct that
	// internally embeds this list of tracked deductions, and append there
	// instead. Do this as part of #104154.
	//
	// TODO: Priorities here are the original priorities. So the key will always be
	// based on the original priority. Just the AC StoreQueue will admit based on overridden
	// priority.
	trackedM map[admissionpb.WorkPriority][]tracked

	stream kvflowcontrol.Stream // used for logging only
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log position (typically where the proposed command is expected to end
// up).
type tracked struct {
	tokens      kvflowcontrol.Tokens
	sendTokenWC admissionpb.WorkClass
	// TODO: why do we care about the term?
	position kvflowcontrolpb.RaftLogPosition
}

// New constructs a new Tracker with the given lower bound raft log position
// (below which we're not allowed to deduct tokens).
func (dt *Tracker) Init(stream kvflowcontrol.Stream) {
	*dt = Tracker{
		trackedM: make(map[admissionpb.WorkPriority][]tracked),
		stream:   stream,
	}
}

// Track token deductions of the given priority with the given raft log
// position.
//
// sendTokenWC is not necessarily derived from pri. It is the wc from which
// send-tokens were deducted. Eval tokens are always deducted from the
// WorkClass derived from pri.
func (dt *Tracker) Track(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	sendTokenWC admissionpb.WorkClass,
	tokens kvflowcontrol.Tokens,
	pos kvflowcontrolpb.RaftLogPosition,
) bool {
	if len(dt.trackedM[pri]) >= 1 {
		last := dt.trackedM[pri][len(dt.trackedM[pri])-1]
		if !last.position.Less(pos) {
			log.Fatalf(ctx, "expected in order tracked log positions (%s < %s)",
				last.position, pos)
			return false
		}
	}
	// TODO(irfansharif,aaditya): The tracked instances here make up about ~0.4%
	// of allocations under kv0/enc=false/nodes=3/cpu=9. Maybe clean it up as
	// part of #104154, by using a sync.Pool perhaps.
	dt.trackedM[pri] = append(dt.trackedM[pri], tracked{
		tokens:      tokens,
		sendTokenWC: sendTokenWC,
		position:    pos,
	})
	if log.V(1) {
		log.Infof(ctx, "tracking %s flow control tokens for pri=%s stream=%s pos=%s",
			tokens, pri, dt.stream, pos)
	}
	return true
}

// Untrack all token deductions of the given priority that have log positions
// less than or equal to the one provided.
func (dt *Tracker) Untrack(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	upto kvflowcontrolpb.RaftLogPosition,
	f func(index uint64, sendTokenWC admissionpb.WorkClass, tokens kvflowcontrol.Tokens),
) {
	if dt == nil {
		return
	}
	if _, ok := dt.trackedM[pri]; !ok {
		return
	}

	var untracked int
	for {
		if untracked == len(dt.trackedM[pri]) {
			break
		}
		deduction := dt.trackedM[pri][untracked]
		if !deduction.position.LessEq(upto) {
			break
		}
		f(deduction.position.Index, deduction.sendTokenWC, deduction.tokens)
		untracked += 1
	}

	dt.trackedM[pri] = dt.trackedM[pri][untracked:]
	if len(dt.trackedM[pri]) == 0 {
		delete(dt.trackedM, pri)
	}
}

// UntrackAll iterates through all tracked token deductions, invoking the
// provided callback with the sum of all tokens at a per-priority level, and
// untracking.
func (dt *Tracker) UntrackAll(
	_ context.Context,
	f func(
		pri admissionpb.WorkPriority, sendTokenWC admissionpb.WorkClass, tokens kvflowcontrol.Tokens),
) {
	for pri, deductions := range dt.trackedM {
		var tokens [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		for _, deduction := range deductions {
			tokens[deduction.sendTokenWC] += deduction.tokens
		}
		f(pri, admissionpb.RegularWorkClass, tokens[admissionpb.RegularWorkClass])
		f(pri, admissionpb.ElasticWorkClass, tokens[admissionpb.ElasticWorkClass])
	}
}
