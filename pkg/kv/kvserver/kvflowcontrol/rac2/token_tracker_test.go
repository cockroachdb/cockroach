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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestTokenTracker is a comprehensive datadriven test for the Tracker struct.
// It covers various scenarios including initialization, tracking, untracking,
// and edge cases to ensure the correct functionality of the Tracker.
func TestTokenTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mockStream := kvflowcontrol.Stream{}
	tracker := &Tracker{}
	tracker.Init(mockStream)

	datadriven.RunTest(t, "testdata/token_tracker", func(t *testing.T, d *datadriven.TestData) string {
		ctx := context.Background()

		switch d.Cmd {
		case "init":
			// Test the initialization of a new Tracker.
			// This ensures that a new Tracker starts with an empty state.
			tracker = &Tracker{}
			tracker.Init(mockStream)
			return formatState(tracker)

		case "track":
			// Test the tracking functionality of the Tracker.
			// This handles multiple track operations, allowing us to test various
			// scenarios such as tracking entries with different priorities,
			// out-of-order indices, and potential edge cases.
			var result strings.Builder
			result.WriteString("actions:\n")

			for _, line := range strings.Split(d.Input, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				var index uint64
				var inheritedPri, originalPri raftpb.Priority
				var tokens kvflowcontrol.Tokens
				_, err := fmt.Sscanf(line, "index=%d inherited_pri=%d original_pri=%d tokens=%d",
					&index, &inheritedPri, &originalPri, &tokens)
				require.NoError(t, err)

				tracker.Track(ctx, index, inheritedPri, originalPri, tokens)
				result.WriteString(fmt.Sprintf("  tracked: index=%d, og_pri=%d, in_pri=%d, tokens=%d\n",
					index, originalPri, inheritedPri, tokens))
			}

			result.WriteString("after:\n")
			result.WriteString(formatState(tracker))
			return result.String()

		case "untrack":
			// Test the untracking functionality of the Tracker.
			// This verifies that the Tracker correctly untracks entries
			// up to a given index for a specific priority.
			var inheritedPri raftpb.Priority
			var uptoIndex uint64
			_, err := fmt.Sscanf(d.Input, "%d %d", &inheritedPri, &uptoIndex)
			require.NoError(t, err)

			var result strings.Builder
			result.WriteString("before:\n")
			result.WriteString(formatState(tracker))
			result.WriteString("actions:\n")

			tracker.Untrack(inheritedPri, uptoIndex, func(index uint64, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens) {
				result.WriteString(fmt.Sprintf("  untracked: index=%d, og_pri=%d, tokens=%d\n", index, originalPri, tokens))
			})

			result.WriteString("after:\n")
			result.WriteString(formatState(tracker))
			return result.String()

		case "untrack_ge":
			// Test the untracking of entries greater than or equal to a given index.
			// This ensures that the Tracker correctly handles untracking across all priorities.
			var index uint64
			_, err := fmt.Sscanf(d.Input, "%d", &index)
			require.NoError(t, err)

			var result strings.Builder
			result.WriteString("before:\n")
			result.WriteString(formatState(tracker))
			result.WriteString("actions:\n")

			tracker.UntrackGE(index, func(index uint64, inheritedPri, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens) {
				result.WriteString(fmt.Sprintf("  untracked ge: index=%d, og_pri=%d, in_pri=%d, tokens=%d\n",
					index, originalPri, inheritedPri, tokens))
			})

			result.WriteString("after:\n")
			result.WriteString(formatState(tracker))
			return result.String()

		case "untrack_all":
			// Test the untracking of all entries across all priorities.
			// This verifies that the Tracker can correctly clear its entire state.
			var result strings.Builder
			result.WriteString("before:\n")
			result.WriteString(formatState(tracker))
			result.WriteString("untracked: \n")

			tracker.UntrackAll(func(index uint64, inheritedPri, originalPri raftpb.Priority, tokens kvflowcontrol.Tokens) {
				result.WriteString(fmt.Sprintf("  (index=%d, og_pri=%d, in_pri=%d, tokens=%d)\n",
					index, originalPri, inheritedPri, tokens))
			})

			result.WriteString("after:\n")
			result.WriteString(formatState(tracker))
			return result.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// formatState converts the Tracker's state into a string representation.
// This helper function is used to display the Tracker's state before and after operations.
func formatState(t *Tracker) string {
	var result strings.Builder
	for pri, tracked := range t.tracked {
		if len(tracked) > 0 {
			result.WriteString(fmt.Sprintf("  %d:", pri))
			for _, tr := range tracked {
				result.WriteString(fmt.Sprintf("  (tokens=%d og_pri=%d in_pri=%d index=%d)\n",
					tr.tokens, tr.originalPri, tr.inheritedPri, tr.index))
			}
		}
	}
	return result.String()
}
