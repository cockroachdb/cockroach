// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowtokentracker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		var tracker *Tracker
		knobs := &kvflowcontrol.TestingKnobs{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				tracker = New(kvflowcontrolpb.RaftLogPosition{Term: 1, Index: 0}, kvflowcontrol.Stream{}, knobs)
				return ""

			case "track":
				require.NotNilf(t, tracker, "uninitialized tracker (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 3, "expected form 'pri=<string> tokens=<size> log-position=<int>/<int>'")

					var (
						tokens      kvflowcontrol.Tokens
						pri         admissionpb.WorkPriority
						logPosition kvflowcontrolpb.RaftLogPosition
					)

					for i := range parts {
						parts[i] = strings.TrimSpace(parts[i])
						inner := strings.Split(parts[i], "=")
						require.Len(t, inner, 2)
						arg := strings.TrimSpace(inner[1])

						switch {
						case strings.HasPrefix(parts[i], "pri="):
							var found bool
							pri, found = admissionpb.TestingReverseWorkPriorityDict[arg]
							require.True(t, found)

						case strings.HasPrefix(parts[i], "tokens="):
							// Parse tokens=<bytes>.
							bytes, err := humanize.ParseBytes(arg)
							require.NoError(t, err)
							tokens = kvflowcontrol.Tokens(int64(bytes))

						case strings.HasPrefix(parts[i], "log-position="):
							// Parse log-position=<int>/<int>.
							logPosition = parseLogPosition(t, arg)

						default:
							t.Fatalf("unrecognized prefix: %s", parts[i])
						}
					}
					require.True(t, tracker.Track(ctx, pri, tokens, logPosition))
				}
				return ""

			case "iter":
				require.NotNilf(t, tracker, "uninitialized tracker (did you use 'init'?)")
				return tracker.TestingPrintIter()

			case "inspect":
				var buf strings.Builder
				for _, tracked := range tracker.Inspect(ctx) {
					buf.WriteString(fmt.Sprintf("pri=%s tokens=%s %s\n",
						admissionpb.WorkPriority(tracked.Priority),
						testingPrintTrimmedTokens(kvflowcontrol.Tokens(tracked.Tokens)),
						tracked.RaftLogPosition,
					))
				}
				return buf.String()

			case "untrack":
				require.NotNilf(t, tracker, "uninitialized tracker (did you use 'init'?)")
				var priStr, logPositionStr string
				d.ScanArgs(t, "pri", &priStr)
				d.ScanArgs(t, "up-to-log-position", &logPositionStr)
				pri, found := admissionpb.TestingReverseWorkPriorityDict[priStr]
				require.True(t, found)
				logPosition := parseLogPosition(t, logPositionStr)

				count := 0
				var buf strings.Builder
				buf.WriteString(fmt.Sprintf("pri=%s\n", pri))
				knobs.V1.UntrackTokensInterceptor = func(tokens kvflowcontrol.Tokens, position kvflowcontrolpb.RaftLogPosition) {
					count += 1
					buf.WriteString(fmt.Sprintf("  tokens=%s %s\n",
						testingPrintTrimmedTokens(tokens), position))
				}
				tokens := tracker.Untrack(ctx, pri, logPosition)
				if count == 0 {
					return ""
				}
				buf.WriteString(fmt.Sprintf("total=%s\n",
					testingPrintTrimmedTokens(tokens)))
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func parseLogPosition(t *testing.T, input string) kvflowcontrolpb.RaftLogPosition {
	inner := strings.Split(input, "/")
	require.Len(t, inner, 2)
	term, err := strconv.Atoi(inner[0])
	require.NoError(t, err)
	index, err := strconv.Atoi(inner[1])
	require.NoError(t, err)
	return kvflowcontrolpb.RaftLogPosition{
		Term:  uint64(term),
		Index: uint64(index),
	}
}
