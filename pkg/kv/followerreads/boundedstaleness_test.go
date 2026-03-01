// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package followerreads_test

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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

// fullTraceDebug is a flag that controls whether full traces are printed in the
// case of some errors.
const fullTraceDebug = false

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
	*kvpb.MinTimestampBoundUnsatisfiableError
	asOf eval.AsOfSystemTime
}

func (ev *boundedStalenessRetryEvent) EventOutput() string {
	return fmt.Sprintf("transaction retry on node_idx: %d", ev.nodeIdx)
}

// boundedStalenessEvents tracks bounded staleness datadriven related events.
type boundedStalenessEvents struct {
	// A mutex is needed as the event handlers (onStmtTrace) can race.
	mu struct {
		syncutil.Mutex
		stmt string
		// Only populated if fullTraceDebug constant is true.
		traceForDebugging string
		events            []boundedStalenessDataDrivenEvent
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

func (bse *boundedStalenessEvents) fullTrace() string {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	return bse.mu.traceForDebugging
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
					"expected MinTimestampBound=%s to be less than previous retry's MinTimestampBound=%s",
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
	nodeIdx int, autoRetryReason error, evalCtx *eval.Context,
) {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	if bse.mu.stmt == "" {
		return
	}
	var minTSErr *kvpb.MinTimestampBoundUnsatisfiableError
	if autoRetryReason != nil && errors.As(autoRetryReason, &minTSErr) {
		ev := &boundedStalenessRetryEvent{
			nodeIdx:                             nodeIdx,
			MinTimestampBoundUnsatisfiableError: minTSErr,
			asOf:                                *evalCtx.AsOfSystemTime,
		}
		bse.mu.events = append(bse.mu.events, ev)
	}
}

func (bse *boundedStalenessEvents) onStmtTrace(nodeIdx int, rec tracingpb.Recording, stmt string) {
	bse.mu.Lock()
	defer bse.mu.Unlock()

	if bse.mu.stmt != "" && bse.mu.stmt == stmt {
		if fullTraceDebug {
			bse.mu.traceForDebugging = rec.String()
		}

		spans := make(map[tracingpb.SpanID]tracingpb.RecordedSpan)
		for _, sp := range rec {
			spans[sp.SpanID] = sp
			notLeaseHolderError := tracing.LogsContainMsg(sp, "[NotLeaseHolderError] lease held by different store;")
			notLeaseHolderError = notLeaseHolderError || tracing.LogsContainMsg(sp, "[NotLeaseHolderError] leader lease is not held locally, cannot determine validity;")

			if sp.Operation == "dist sender send" && spans[sp.ParentSpanID].Operation == "colbatchscan" {
				bse.mu.events = append(bse.mu.events, &boundedStalenessTraceEvent{
					operation:             spans[sp.ParentSpanID].Operation,
					nodeIdx:               nodeIdx,
					localRead:             tracing.LogsContainMsg(sp, kvbase.RoutingRequestLocallyMsg),
					followerRead:          kvtestutils.OnlyFollowerReads(rec),
					remoteLeaseholderRead: notLeaseHolderError && tracing.LogsContainMsg(sp, "trying next peer"),
				})
			}
		}
	}
}

func TestBoundedStalenessDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "1μs staleness reads may actually succeed due to the slow environment")

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
					WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
						bse.onStmtTrace(i, trace, stmt)
					},
					OnTxnRetry: func(err error, evalCtx *eval.Context) {
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

	datadriven.Walk(t, datapathutils.TestDataPath(t, "boundedstaleness"), func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		if tc.DefaultTenantDeploymentMode().IsExternal() {
			// This test wants to make assertions about local requests (requests to a
			// local replica) which is a concept that only applies to single-tenant
			// and shared-process deployment modes. Skip for external-process
			// multi-tenancy.
			skip.IgnoreLint(t, "test doesn't apply to external process multi-tenancy")
		}

		require.NoError(t, tc.WaitForFullReplication())
		tc.ToggleLeaseQueues(false)
		tc.ToggleSplitQueues(false)
		tc.ToggleReplicateQueues(false)

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

			var showEvents *bool
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
				case "ignore-events":
					f := false
					showEvents = &f
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
				case "exec-system-tenant":
					_, err := tc.SystemLayer(0).SQLConn(t).Exec(d.Input)
					if err != nil {
						return err.Error()
					}
					return ""
				case "query":
					// Default to showing events
					if showEvents == nil {
						t := true
						showEvents = &t
					}
					bse.setStmt(traceStmt)
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
				if showEvents != nil && *showEvents {
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
						var trace string
						if fullTraceDebug {
							trace = fmt.Sprintf("\nfull_trace:\n%s", bse.fullTrace())
						}
						bse.clearEvents()
						return errors.AssertionFailedf("not follower reads found:\n%s%s", bse.String(), trace)
					}
				}
				if waitUntilMatch {
					if d.Expected != ret {
						var trace string
						if fullTraceDebug {
							trace = fmt.Sprintf("\nfull_trace:\n%s", bse.fullTrace())
						}
						bse.clearEvents()
						return errors.AssertionFailedf("not yet a match, output:\n%s%s", ret, trace)
					}
				}
				return nil
			})
			bse.validate(t)
			return ret
		})
	})
}
