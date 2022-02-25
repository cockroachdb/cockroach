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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	testCases := []struct {
		query string
		args  []interface{}
	}{
		{
			query: `SELECT with_max_staleness('10s')`,
		},
		{
			query: `SELECT with_min_timestamp(statement_timestamp())`,
		},
		{
			query: `SELECT $1::TEXT FROM generate_series(1,1) AS OF SYSTEM TIME with_max_staleness('1s')`,
			args:  []interface{}{"cat"},
		},
		{
			query: `SELECT $1::TEXT FROM generate_series(1,1) AS OF SYSTEM TIME with_min_timestamp(statement_timestamp())`,
			args:  []interface{}{"cat"},
		},
	}

	defer utilccl.TestingDisableEnterprise()()
	t.Run("disabled", func(t *testing.T) {
		for _, testCase := range testCases {
			t.Run(testCase.query, func(t *testing.T) {
				_, err := tc.Conns[0].QueryContext(ctx, testCase.query, testCase.args...)
				require.Error(t, err)
				require.Contains(t, err.Error(), "use of bounded staleness requires an enterprise license")
			})
		}
	})

	t.Run("enabled", func(t *testing.T) {
		defer utilccl.TestingEnableEnterprise()()
		for _, testCase := range testCases {
			t.Run(testCase.query, func(t *testing.T) {
				r, err := tc.Conns[0].QueryContext(ctx, testCase.query, testCase.args...)
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

// boundedStalenessRetryEvent is a retry that has occurred from within the
// transaction as a result of a nearest_only bounded staleness restart.
type boundedStalenessRetryEvent struct {
	nodeIdx int
	*roachpb.MinTimestampBoundUnsatisfiableError
	asOf tree.AsOfSystemTime
}

func (ev *boundedStalenessRetryEvent) EventOutput() string {
	return fmt.Sprintf("transaction retry on node_idx: %d", ev.nodeIdx)
}

// boundedStalenessEvents tracks bounded staleness datadriven related events.
type boundedStalenessEvents struct {
	// A mutex is needed as the event handlers (onStmtTrace) can race.
	mu struct {
		syncutil.Mutex
		stmt   string
		events []boundedStalenessDataDrivenEvent
	}
}

func (bse *boundedStalenessEvents) reset() {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	bse.mu.stmt = ""
	bse.mu.events = nil
}

func (bse *boundedStalenessEvents) clearEvents() {
	bse.mu.Lock()
	defer bse.mu.Unlock()
	bse.mu.events = nil
}

func (bse *boundedStalenessEvents) setStmt(s string) {
	bse.mu.Lock()
	defer bse.mu.Unlock()
	bse.mu.stmt = s
}

func (bse *boundedStalenessEvents) String() string {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("events (%d found):\n", len(bse.mu.events)))
	for idx, event := range bse.mu.events {
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

func (bse *boundedStalenessEvents) validate(t *testing.T) {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	var lastTxnRetry *boundedStalenessRetryEvent
	for _, ev := range bse.mu.events {
		switch ev := ev.(type) {
		case *boundedStalenessRetryEvent:
			if lastTxnRetry != nil {
				require.True(
					t,
					ev.MinTimestampBound.Less(lastTxnRetry.MinTimestampBound),
					ev.MinTimestampBound,
					lastTxnRetry.MinTimestampBound,
				)
				require.Equal(t, ev.asOf.Timestamp, lastTxnRetry.asOf.Timestamp)
			}
			require.True(t, ev.asOf.BoundedStaleness)
			lastTxnRetry = ev
		}
	}
}

func (bse *boundedStalenessEvents) onTxnRetry(
	nodeIdx int, autoRetryReason error, evalCtx *tree.EvalContext,
) {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	if bse.mu.stmt == "" {
		return
	}
	var minTSErr *roachpb.MinTimestampBoundUnsatisfiableError
	if autoRetryReason != nil && errors.As(autoRetryReason, &minTSErr) {
		ev := &boundedStalenessRetryEvent{
			nodeIdx:                             nodeIdx,
			MinTimestampBoundUnsatisfiableError: minTSErr,
			asOf:                                *evalCtx.AsOfSystemTime,
		}
		bse.mu.events = append(bse.mu.events, ev)
	}
}

func (bse *boundedStalenessEvents) onStmtTrace(nodeIdx int, rec tracing.Recording, stmt string) {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	if bse.mu.stmt != "" && bse.mu.stmt == stmt {
		spans := make(map[tracingpb.SpanID]tracingpb.RecordedSpan)
		for _, sp := range rec {
			spans[sp.SpanID] = sp
			if sp.Operation == "dist sender send" && spans[sp.ParentSpanID].Operation == "colbatchscan" {
				bse.mu.events = append(bse.mu.events, &boundedStalenessTraceEvent{
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
					OnTxnRetry: func(err error, evalCtx *tree.EvalContext) {
						bse.onTxnRetry(i, err, evalCtx)
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

	datadriven.Walk(t, testutils.TestDataPath(t, "boundedstaleness"), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		savedTraceStmt := ""
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			// Early exit non-query execution related commands.
			switch d.Cmd {
			// override-matching-stmt-for-tracing forces the next trace of events to
			// only  look for the given query instead of using d.Input. This is useful
			//for traces of prepared statements.
			case "override-matching-stmt-for-tracing":
				savedTraceStmt = d.Input
				return ""
			case "reset-matching-stmt-for-tracing":
				savedTraceStmt = ""
				return ""
			}

			var showEvents bool
			var waitUntilFollowerReads bool
			var waitUntilMatch bool
			defer func() {
				bse.reset()
			}()
			dbConn := tc.ServerConn(0)
			traceStmt := savedTraceStmt
			if traceStmt == "" {
				traceStmt = d.Input
			}
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "wait-until-follower-read":
					bse.setStmt(traceStmt)
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
					bse.setStmt(traceStmt)
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
				// reading follower reads from.
				if waitUntilFollowerReads {
					followerRead := false
					func() {
						bse.mu.Lock()
						defer bse.mu.Unlock()
						for _, ev := range bse.mu.events {
							switch ev := ev.(type) {
							case *boundedStalenessTraceEvent:
								followerRead = ev.followerRead
							}
						}
					}()
					if !followerRead {
						bse.clearEvents()
						return errors.AssertionFailedf("not follower reads found:\n%s", bse.String())
					}
				}
				if waitUntilMatch {
					if d.Expected != ret {
						bse.clearEvents()
						return errors.AssertionFailedf("not yet a match, output:\n%s\n", ret)
					}
				}
				return nil
			})
			bse.validate(t)
			return ret
		})
	})
}
