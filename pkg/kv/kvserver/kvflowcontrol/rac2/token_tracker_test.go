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
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

func formatTrackerState(t *Tracker) string {
	var result strings.Builder
	for pri, tracked := range t.tracked {
		if len(tracked) > 0 {
			result.WriteString(fmt.Sprintf("%v:\n", raftpb.Priority(pri)))
			for _, tr := range tracked {
				result.WriteString(fmt.Sprintf("  term=%d index=%-2d tokens=%-3d\n",
					tr.term, tr.index, tr.tokens))
			}
		}
	}
	return result.String()
}

func formatUntracked(untracked [raftpb.NumPriorities]kvflowcontrol.Tokens) string {
	var buf strings.Builder
	for pri, tokens := range untracked {
		if tokens > 0 {
			buf.WriteString(fmt.Sprintf("returned: tokens=%-4d pri=%v\n", tokens, raftpb.Priority(pri)))
		}
	}
	return buf.String()
}

func TestTokenTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tracker := &Tracker{}
	tracker.Init(kvflowcontrol.Stream{})

	// Used to marshal the output of the Inspect() method into a human-readable
	// formatted JSON string. See case "inspect" below.
	marshaller := jsonpb.Marshaler{
		Indent:       "  ",
		EmitDefaults: true,
		OrigName:     true,
	}
	datadriven.RunTest(t, "testdata/token_tracker", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "track":
			var buf strings.Builder
			for _, line := range strings.Split(d.Input, "\n") {
				line = strings.TrimSpace(line)
				parts := strings.Split(line, " ")
				require.Len(t, parts, 4)

				parts[0] = strings.TrimSpace(parts[0])
				require.True(t, strings.HasPrefix(parts[0], "term="))
				parts[0] = strings.TrimPrefix(parts[0], "term=")
				term, err := strconv.ParseUint(parts[0], 10, 64)
				require.NoError(t, err)

				parts[1] = strings.TrimSpace(parts[1])
				require.True(t, strings.HasPrefix(parts[1], "index="))
				parts[1] = strings.TrimPrefix(parts[1], "index=")
				index, err := strconv.ParseUint(parts[1], 10, 64)
				require.NoError(t, err)

				parts[2] = strings.TrimSpace(parts[2])
				require.True(t, strings.HasPrefix(parts[2], "tokens="))
				parts[2] = strings.TrimPrefix(parts[2], "tokens=")
				tokens, err := strconv.ParseInt(parts[2], 10, 64)
				require.NoError(t, err)

				parts[3] = strings.TrimSpace(parts[3])
				require.True(t, strings.HasPrefix(parts[3], "pri="))
				parts[3] = strings.TrimPrefix(parts[3], "pri=")
				pri := AdmissionToRaftPriority(parsePriority(t, parts[3]))

				tracker.Track(ctx, term, index, pri, kvflowcontrol.Tokens(tokens))
				buf.WriteString(fmt.Sprintf("tracked: term=%d index=%-2d tokens=%-3d pri=%v\n",
					term, index, tokens, pri))
			}
			return buf.String()

		case "untrack":
			var term uint64
			d.ScanArgs(t, "term", &term)
			var admitted [raftpb.NumPriorities]uint64
			for _, line := range strings.Split(d.Input, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				parts := strings.Split(line, "=")
				require.Len(t, parts, 2)
				priStr := strings.TrimSpace(parts[0])
				indexStr := strings.TrimSpace(parts[1])
				pri := AdmissionToRaftPriority(parsePriority(t, priStr))
				index, err := strconv.ParseUint(indexStr, 10, 64)
				require.NoError(t, err)
				admitted[pri] = index
			}
			return formatUntracked(tracker.Untrack(term, admitted))

		case "untrack_ge":
			var index uint64
			d.ScanArgs(t, "index", &index)
			return formatUntracked(tracker.UntrackGE(index))

		case "untrack_all":
			return formatUntracked(tracker.UntrackAll())

		case "state":
			return formatTrackerState(tracker)

		case "inspect":
			var buf strings.Builder
			for _, deduction := range tracker.Inspect() {
				marshaled, err := marshaller.MarshalToString(&deduction)
				require.NoError(t, err)
				fmt.Fprintf(&buf, "%s\n", marshaled)
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
