// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl_test

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	flagIgnoreWaitUntilMatch = flag.Bool(
		"ignore-wait-until-match",
		false,
		`ignore wait-until-match. This is desired in cases `+
			`of -rewrite, where we do not want to retry wait-until-match.`,
	)
)

func TestBoundedStalenessEnterpriseLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	queries := []string{
		`SELECT with_max_staleness('10s')`,
		`SELECT with_min_timestamp(statement_timestamp())`,
	}

	defer utilccl.TestingDisableEnterprise()()
	t.Run("disabled", func(t *testing.T) {
		for _, q := range queries {
			t.Run(q, func(t *testing.T) {
				_, err := tc.Conns[0].QueryContext(ctx, q)
				require.Error(t, err)
				require.Contains(t, err.Error(), "use of bounded staleness requires an enterprise license")
			})
		}
	})

	t.Run("enabled", func(t *testing.T) {
		defer utilccl.TestingEnableEnterprise()()
		for _, q := range queries {
			t.Run(q, func(t *testing.T) {
				r, err := tc.Conns[0].QueryContext(ctx, q)
				require.NoError(t, err)
				require.NoError(t, r.Close())
			})
		}
	})
}

// boundedStalenessDataDrivenEvent is an event during a bounded staleness datadriven test.
type boundedStalenessDataDrivenEvent interface {
	EventOutput() string
}

// boundedStalenessTraceEvent is a recorded trace of interest during a bounded staleness
// datadriven test.
type boundedStalenessTraceEvent struct {
	operation             string
	nodeIdx               int
	localRead             bool
	remoteLeaseholderRead bool
	followerRead          bool
}

func (ev *boundedStalenessTraceEvent) EventOutput() string {
	state := "??? undefined state ???"
	switch {
	case ev.localRead && ev.remoteLeaseholderRead && !ev.followerRead:
		state = "local read then remote leaseholder read"
	case ev.localRead && !ev.remoteLeaseholderRead && ev.followerRead:
		state = "local follower read"
	case ev.localRead && !ev.remoteLeaseholderRead && !ev.followerRead:
		state = "local read"
	}
	return fmt.Sprintf(
		"%s trace on node_idx %d: %s",
		ev.operation,
		ev.nodeIdx,
		state,
	)
}

// boundedStalenessEvents tracks bounded staleness datadriven related events.
type boundedStalenessEvents struct {
	stmt   string
	events []boundedStalenessDataDrivenEvent
}

func (bse *boundedStalenessEvents) reset() {
	bse.stmt = ""
	bse.events = nil
}

func (bse *boundedStalenessEvents) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("events (%d found):\n", len(bse.events)))
	for idx, event := range bse.events {
		sb.WriteString(
			fmt.Sprintf(
				" * event %d: %s\n",
				idx+1,
				event.EventOutput(),
			),
		)
	}
	return sb.String()
}

func (bse *boundedStalenessEvents) onStmtTrace(nodeIdx int, rec tracing.Recording, stmt string) {
	if bse.stmt != "" && bse.stmt == stmt {
		spans := make(map[uint64]tracingpb.RecordedSpan)
		for _, sp := range rec {
			spans[sp.SpanID] = sp
			if sp.Operation == "dist sender send" && spans[sp.ParentSpanID].Operation == "colbatchscan" {
				bse.events = append(bse.events, &boundedStalenessTraceEvent{
					operation:    spans[sp.ParentSpanID].Operation,
					nodeIdx:      nodeIdx,
					localRead:    tracing.LogsContainMsg(sp, kvbase.RoutingRequestLocallyMsg),
					followerRead: kv.OnlyFollowerReads(rec),
					remoteLeaseholderRead: tracing.LogsContainMsg(sp, "[NotLeaseHolderError] lease held by different store;") &&
						tracing.LogsContainMsg(sp, "trying next peer"),
				})
			}
		}
	}
}

func TestBoundedStalenessDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "1μs staleness reads may actually succeed due to the slow environment")
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	const numNodes = 3
	var bse boundedStalenessEvents
	for i := 0; i < numNodes; i++ {
		i := i
		clusterArgs.ServerArgsPerNode[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: func(trace tracing.Recording, stmt string) {
						bse.onStmtTrace(i, trace, stmt)
					},
				},
			},
		}
	}
	// Replace errors which are not deterministically output.
	// In this case, it is the HLC timestamp of
	// MinTimestampBoundUnsatisfiableErrors.
	errorRegexp := regexp.MustCompile(
		`((minimum timestamp bound|local resolved timestamp) of) [\d.,]*`,
	)
	replaceOutput := func(s string) string {
		return errorRegexp.ReplaceAllString(s, "$1 XXX")
	}

	datadriven.Walk(t, "testdata/boundedstaleness", func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var showEvents bool
			var waitUntilFollowerReads bool
			var waitUntilMatch bool
			defer func() {
				bse.reset()
			}()
			dbConn := tc.ServerConn(0)
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "wait-until-follower-read":
					bse.stmt = d.Input
					waitUntilFollowerReads = true
				case "wait-until-match":
					// We support both wait-until-match and wait-until-follower-read,
					// So during `-rewrite -ignore-wait-until-match` we rewrite with
					// the desired output after waiting for the follower read.
					if !*flagIgnoreWaitUntilMatch {
						waitUntilMatch = true
					}
				case "idx":
					serverNum, err := strconv.ParseInt(arg.Vals[0], 10, 64)
					require.NoError(t, err)
					dbConn = tc.ServerConn(int(serverNum))
				default:
					t.Fatalf("unknown arg: %s", arg.Key)
				}
			}

			executeCmd := func() (retStr string) {
				defer func() {
					retStr = replaceOutput(retStr)
				}()
				switch d.Cmd {
				case "exec":
					_, err := dbConn.Exec(d.Input)
					if err != nil {
						return err.Error()
					}
					return ""
				case "query":
					// Always show events.
					bse.stmt = d.Input
					showEvents = true
					rows, err := dbConn.Query(d.Input)
					if err != nil {
						return err.Error()
					}
					ret, err := sqlutils.RowsToDataDrivenOutput(rows)
					require.NoError(t, err)
					return ret
				}
				t.Fatalf("unknown command: %s", d.Cmd)
				return "unexpected"
			}
			var ret string
			testutils.SucceedsSoon(t, func() error {
				ret = executeCmd()
				// Append events to the output if desired.
				if showEvents {
					if !strings.HasSuffix(ret, "\n") {
						ret += "\n"
					}
					ret += bse.String()
				}
				// If we're waiting until follower reads, retry until we are always
				// reading follower reads.
				if waitUntilFollowerReads {
					allFollowerReads := true
					for _, ev := range bse.events {
						switch ev := ev.(type) {
						case *boundedStalenessTraceEvent:
							allFollowerReads = allFollowerReads && ev.followerRead
						}
					}
					if !allFollowerReads {
						bse.reset()
						bse.stmt = d.Input
						return errors.AssertionFailedf("not follower reads found:\b%s", bse.String())
					}
				}
				if waitUntilMatch {
					if d.Expected != ret {
						bse.reset()
						bse.stmt = d.Input
						return errors.AssertionFailedf("not yet a match, output:\n%s\n", ret)
					}
				}
				return nil
			})
			return ret
		})
	})
}
