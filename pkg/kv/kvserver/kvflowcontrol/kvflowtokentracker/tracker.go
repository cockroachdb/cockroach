// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowtokentracker

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Tracker tracks flow token deductions. Tokens are deducted for proposed
// commands (identified by raft log position), with a given
// admissionpb.WorkPriority, for replication along an individual
// kvflowcontrol.Stream.
type Tracker struct {
	// TODO(irfansharif,aaditya): Everytime we track something, we incur a map
	// assignment (shows up in CPU profiles). We could introduce a struct that
	// internally embeds this list of tracked deductions, and append there
	// instead. Do this as part of #104154.
	trackedM map[admissionpb.WorkPriority][]tracked

	// lowerBound tracks on a per-stream basis the log position below which
	// we ignore token deductions.
	lowerBound kvflowcontrolpb.RaftLogPosition

	stream kvflowcontrol.Stream // used for logging only

	knobs *kvflowcontrol.TestingKnobs
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log position (typically where the proposed command is expected to end
// up).
type tracked struct {
	tokens   kvflowcontrol.Tokens
	position kvflowcontrolpb.RaftLogPosition
}

// New constructs a new Tracker with the given lower bound raft log position
// (below which we're not allowed to deduct tokens).
func New(
	lb kvflowcontrolpb.RaftLogPosition,
	stream kvflowcontrol.Stream,
	knobs *kvflowcontrol.TestingKnobs,
) *Tracker {
	if knobs == nil {
		knobs = &kvflowcontrol.TestingKnobs{}
	}
	return &Tracker{
		trackedM:   make(map[admissionpb.WorkPriority][]tracked),
		lowerBound: lb,
		knobs:      knobs,
		stream:     stream,
	}
}

// Track token deductions of the given priority with the given raft log
// position.
func (dt *Tracker) Track(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	pos kvflowcontrolpb.RaftLogPosition,
) bool {
	if !(dt.lowerBound.Less(pos)) {
		// We're trying to track a token deduction at a position less than the
		// stream's lower-bound. Shout loudly but ultimately no-op. This
		// regression indicates buggy usage since:
		// - token deductions are done so with monotonically increasing log
		//   positions (see Handle.DeductTokensFor);
		//   - the monotonically increasing log positions for token deductions
		//     also extends to positions at which streams are connected,
		//     which typically happen when (a) members are added to the raft
		//     group, (b) previously crashed follower nodes restart, (c)
		//     replicas are unpaused, or (d) caught back up via snapshots (see
		//     Handle.ConnectStream).
		// - token returns upto some log position don't precede deductions at
		//   lower log positions (see Handle.ReturnTokensUpto);
		logFn := log.Errorf
		if buildutil.CrdbTestBuild {
			logFn = log.Fatalf
		}
		logFn(ctx, "observed raft log position less than per-stream lower bound (%s <= %s)",
			pos, dt.lowerBound)
		return false
	}
	dt.lowerBound = pos

	if len(dt.trackedM[pri]) >= 1 {
		last := dt.trackedM[pri][len(dt.trackedM[pri])-1]
		if !last.position.Less(pos) {
			logFn := log.Errorf
			if buildutil.CrdbTestBuild {
				logFn = log.Fatalf
			}
			logFn(ctx, "expected in order tracked log positions (%s < %s)",
				last.position, pos)
			return false
		}
	}

	// TODO(irfansharif,aaditya): The tracked instances here make up about ~0.4%
	// of allocations under kv0/enc=false/nodes=3/cpu=9. Maybe clean it up as
	// part of #104154, by using a sync.Pool perhaps.
	dt.trackedM[pri] = append(dt.trackedM[pri], tracked{
		tokens:   tokens,
		position: pos,
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
	ctx context.Context, pri admissionpb.WorkPriority, upto kvflowcontrolpb.RaftLogPosition,
) kvflowcontrol.Tokens {
	if dt == nil {
		return 0
	}
	if _, ok := dt.trackedM[pri]; !ok {
		return 0
	}

	var untracked int
	var tokens kvflowcontrol.Tokens
	for {
		if untracked == len(dt.trackedM[pri]) {
			break
		}

		deduction := dt.trackedM[pri][untracked]
		if !deduction.position.LessEq(upto) {
			break
		}

		if fn := dt.knobs.V1.UntrackTokensInterceptor; fn != nil {
			fn(deduction.tokens, deduction.position)
		}

		untracked += 1
		tokens += deduction.tokens
	}

	trackedBefore := len(dt.trackedM[pri])
	dt.trackedM[pri] = dt.trackedM[pri][untracked:]
	if log.V(1) {
		remaining := ""
		if len(dt.trackedM[pri]) > 0 {
			remaining = fmt.Sprintf(" (%s, ...)", dt.trackedM[pri][0].tokens)
		}
		log.Infof(ctx, "released %s flow control tokens for %d out of %d tracked deductions for pri=%s stream=%s, up to %s; %d tracked deduction(s) remain%s",
			tokens, untracked, trackedBefore, pri, dt.stream, upto, len(dt.trackedM[pri]), remaining)
	}
	if len(dt.trackedM[pri]) == 0 {
		delete(dt.trackedM, pri)
	}

	if dt.lowerBound.Less(upto) {
		dt.lowerBound = upto
	}
	return tokens
}

// Iter iterates through all tracked token deductions, invoking the provided
// callback with the sum of all tokens at a per-priority level.
func (dt *Tracker) Iter(_ context.Context, f func(admissionpb.WorkPriority, kvflowcontrol.Tokens)) {
	for pri, deductions := range dt.trackedM {
		var tokens kvflowcontrol.Tokens
		for _, deduction := range deductions {
			tokens += deduction.tokens
		}
		f(pri, tokens)
	}
}

// LowerBound returns the log position below which we ignore token deductions.
func (dt *Tracker) LowerBound() kvflowcontrolpb.RaftLogPosition {
	return dt.lowerBound
}

// Inspect returns a snapshot of all tracked token deductions. It's used to
// power /inspectz-style debugging pages.
func (dt *Tracker) Inspect(ctx context.Context) []kvflowinspectpb.TrackedDeduction {
	var deductions []kvflowinspectpb.TrackedDeduction
	dt.TestingIter(func(pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens, pos kvflowcontrolpb.RaftLogPosition) bool {
		deductions = append(deductions, kvflowinspectpb.TrackedDeduction{
			Priority:        int32(pri),
			Tokens:          int64(tokens),
			RaftLogPosition: pos,
		})
		return true
	})
	slices.SortFunc(deductions, func(a, b kvflowinspectpb.TrackedDeduction) int { // for determinism
		return cmp.Or(
			cmp.Compare(a.Priority, b.Priority),
			cmp.Compare(a.RaftLogPosition.Term, b.RaftLogPosition.Term),
			cmp.Compare(a.RaftLogPosition.Index, b.RaftLogPosition.Index),
			cmp.Compare(a.Tokens, b.Tokens),
		)
	})
	return deductions
}

// TestingIter is a testing-only re-implementation of Iter. It iterates through
// all tracked token deductions, invoking the provided callback with tracked
// pri<->token<->position triples.
func (dt *Tracker) TestingIter(
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
	dt.TestingIter(
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
