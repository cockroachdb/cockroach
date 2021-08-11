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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	return fmt.Sprintf(
		"%s: {node_idx:%d,local_read:%t,remote_leaseholder_read:%t,follower_read:%t}",
		ev.operation,
		ev.nodeIdx,
		ev.localRead,
		ev.remoteLeaseholderRead,
		ev.followerRead,
	)
}

// boundedStalenessEvents tracks bounded staleness datadriven related events.
type boundedStalenessEvents struct {
	stmt   string
	events []boundedStalenessDataDrivenEvent
}

func (ti *boundedStalenessEvents) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("events (%d found):\n", len(ti.events)))
	for idx, event := range ti.events {
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

func (ti *boundedStalenessEvents) onStmtTrace(
	t *testing.T, nodeIdx int, rec tracing.Recording, stmt string,
) {
	if ti.stmt == stmt {
		t.Logf("trace of stmt %s:\n%v\n", stmt, rec)

		spans := make(map[uint64]tracingpb.RecordedSpan)
		for _, sp := range rec {
			spans[sp.SpanID] = sp
			if sp.Operation == "dist sender send" && spans[sp.ParentSpanID].Operation == "colbatchscan" {
				ti.events = append(ti.events, &boundedStalenessTraceEvent{
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

	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}
	const numNodes = 3
	var onStmtTraceFunc func(*testing.T, int, tracing.Recording, string)
	for i := 0; i < numNodes; i++ {
		i := i
		clusterArgs.ServerArgsPerNode[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: func(trace tracing.Recording, stmt string) {
						if onStmtTraceFunc != nil {
							onStmtTraceFunc(t, i, trace, stmt)
						}
					},
				},
			},
		}
	}

	datadriven.Walk(t, "testdata/boundedstaleness", func(t *testing.T, path string) {
		tc := testcluster.StartTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var ti *boundedStalenessEvents
			var showEvents bool
			var waitUntilFollowerReads bool
			defer func() {
				onStmtTraceFunc = nil
			}()
			initEvents := func() {
				if ti == nil {
					ti = &boundedStalenessEvents{
						stmt: d.Input,
					}
					onStmtTraceFunc = ti.onStmtTrace
				}
			}
			dbConn := tc.ServerConn(0)
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "show-events":
					initEvents()
					showEvents = true
				case "wait-until-follower-read":
					initEvents()
					waitUntilFollowerReads = true
				case "idx":
					serverNum, err := strconv.ParseInt(arg.Vals[0], 10, 64)
					require.NoError(t, err)
					dbConn = tc.ServerConn(int(serverNum))
				default:
					t.Fatalf("unknown arg: %s", arg.Key)
				}
			}

			executeCmd := func() string {
				switch d.Cmd {
				case "exec":
					_, err := dbConn.Exec(d.Input)
					if err != nil {
						return err.Error()
					}
					return ""
				case "query":
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
				// If we're waiting until follower reads, retry.
				if waitUntilFollowerReads {
					allFollowerReads := true
					for _, ev := range ti.events {
						switch ev := ev.(type) {
						case *boundedStalenessTraceEvent:
							allFollowerReads = allFollowerReads && ev.followerRead
						}
					}
					if allFollowerReads {
						return nil
					}
					ti = nil
					initEvents()
					return errors.AssertionFailedf("not follower reads found:\b%s", ti.String())
				}
				return nil
			})
			// Append events to the output if desired.
			if showEvents {
				if !strings.HasSuffix(ret, "\n") {
					ret += "\n"
				}
				ret += ti.String()
			}
			return ret
		})
	})
}
