// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowtokentracker

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Tracker tracks flow token deductions. Tokens are deducted for proposed
// commands (identified by raft log position), with a given
// admissionpb.WorkPriority, for replication along an individual
// kvflowcontrol.Stream.
type Tracker struct {
	trackedM map[admissionpb.WorkPriority][]tracked
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log position (typically where the proposed command is expected to end
// up).
type tracked struct {
	tokens   kvflowcontrol.Tokens
	position kvflowcontrolpb.RaftLogPosition
}

// New constructs a new Tracker.
func New() *Tracker {
	return &Tracker{
		trackedM: make(map[admissionpb.WorkPriority][]tracked),
	}
}

// Track token deductions of the given priority with the given raft log
// position.
func (dt *Tracker) Track(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	pos kvflowcontrolpb.RaftLogPosition,
) {
	if len(dt.trackedM[pri]) >= 1 {
		last := dt.trackedM[pri][len(dt.trackedM[pri])-1]
		if !last.position.Less(pos) {
			log.Fatalf(ctx, "expected in order tracked log positions (%s < %s)",
				last.position, pos)
		}
	}
	dt.trackedM[pri] = append(dt.trackedM[pri], tracked{
		tokens:   tokens,
		position: pos,
	})
}

// Untrack all token deductions of the given priority that have log positions
// less than or equal to the one provided. The callback is invoked for every
// such token<->position pair.
func (dt *Tracker) Untrack(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	f func(kvflowcontrol.Tokens, kvflowcontrolpb.RaftLogPosition),
) {
	if dt == nil {
		return
	}
	if _, ok := dt.trackedM[pri]; !ok {
		return
	}

	var i int
	for {
		if i == len(dt.trackedM[pri]) {
			break
		}

		deduction := dt.trackedM[pri][i]
		if !deduction.position.LessEq(pos) {
			break
		}

		f(deduction.tokens, deduction.position)
		i += 1 // processed
	}

	before := len(dt.trackedM[pri])
	dt.trackedM[pri] = dt.trackedM[pri][i:]
	if log.ExpensiveLogEnabled(ctx, 1) {
		remaining := ""
		if len(dt.trackedM[pri]) > 0 {
			remaining = fmt.Sprintf(" (%s, ...)", dt.trackedM[pri][0].tokens)
		}
		log.VInfof(ctx, 1, "released flow control tokens for %d/%d pri=%s deductionsPerStream, until %s; %d deductionsPerStream(s) remain%s",
			i, before, pri, pos, len(dt.trackedM[pri]), remaining)
	}
	if len(dt.trackedM[pri]) == 0 {
		delete(dt.trackedM, pri)
	}
}

// Iter iterates through all tracked token deductions, invoking the provided
// callback with the specific priority and token<->position pair.
func (dt *Tracker) Iter(
	_ context.Context,
	f func(admissionpb.WorkPriority, kvflowcontrol.Tokens, kvflowcontrolpb.RaftLogPosition) bool,
) {
	for pri, deductions := range dt.trackedM {
		for _, deduction := range deductions {
			if !f(pri, deduction.tokens, deduction.position) {
				return
			}
		}
	}
}

// TestingPrintIter iterates through all tracked tokens and returns a printable
// string, for use in tests.
func (dt *Tracker) TestingPrintIter() string {
	type tracked struct {
		tokens          kvflowcontrol.Tokens
		raftLogPosition kvflowcontrolpb.RaftLogPosition
	}
	const numPriorities = int(admissionpb.HighPri) - int(admissionpb.LowPri)
	deductions := [numPriorities][]tracked{}
	dt.Iter(context.Background(),
		func(pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens, pos kvflowcontrolpb.RaftLogPosition) bool {
			i := int(pri) - int(admissionpb.LowPri)
			deductions[i] = append(deductions[i], tracked{
				tokens:          tokens,
				raftLogPosition: pos,
			})
			return true
		},
	)
	var buf strings.Builder
	for i, ds := range deductions {
		pri := i + int(admissionpb.LowPri)
		if len(ds) == 0 {
			continue
		}
		buf.WriteString(fmt.Sprintf("pri=%s\n", admissionpb.WorkPriority(pri)))
		for _, deduction := range ds {
			buf.WriteString(fmt.Sprintf("  tokens=%s %s\n",
				testingPrintTrimmedTokens(deduction.tokens), deduction.raftLogPosition))
		}
	}
	return buf.String()
}

func testingPrintTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.TrimPrefix(strings.ReplaceAll(t.String(), " ", ""), "+")
}
