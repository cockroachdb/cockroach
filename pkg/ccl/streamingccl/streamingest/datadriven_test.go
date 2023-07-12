// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a datadriven test to test e2e cluster replication . The
// test files are in testdata/. The following syntax is provided:
//
// - create-replication-clusters : creates a source and destination cluster with
// a "source" tenant in the source cluster.
//
// - start-replication-stream : start a replication stream from the "source"
// tenant to a "destination" tenant on the destination cluster.
//
// - start-replicated-tenant : creates a SQL runner for the "destination"
// tenant. This operation will fail the test if it is run prior to the
// replication stream activating the tenant.
//
// - wait-until-replicated-time ts=<ts>
// Wait until the replication job has reached the specified timestamp.
//
// - cutover ts=<ts>
// Cutover the running replication stream as of the specified timestamp. This
// operation will wait until both the producer and the replication job have
// succeeded before returning.
//
// - compare-replication-results: runs the specified SQL query on both the
// "source" and "destination" tenants and asserts that the results are equal
//
// - compare-tenant-fingerprints from=<start-time> to=<end-time> [with_revisions,table_fingerprints]
// Runs `crdb_internal.fingerprint` on both the "source" and "destination"
// tenants with the provided options and asserts that the generated fingerprints
// are equal.
//
//   - the table_fingerprints option conducts another round of fingerprinting over each table in the
//     clusters. (Primarily used to test fingerprint helper functions).
//
// - sleep ms=TIME
// Sleep for TIME milliseconds.
//
// - let [args]
// Assigns the returned value of the SQL query to the provided args as
// variables.
//
// - exec-sql as=<source-system | source-tenant | destination-system | destination-tenant>
// Executes the specified SQL query as the specified tenant.
//
// - query-sql as=<source-system | source-tenant | destination-system | destination-tenant>
// Executes the specified SQL query as the specified tenant, and prints the
// results.
//
//   - job as=<source-system | destination-system > args
//
// Takes some action on the replication job. Some arguments:
//
//   - wait-for-state=<succeeded|paused|failed|reverting|cancelled>: wait for
//     the job referenced by the tag to reach the specified state.
//
//   - pause: pauses the job.
//
//   - resume: resumes the job.
//
// - skip issue-num=N
// Skips the test.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		ds := newDatadrivenTestState()
		defer ds.cleanup(t)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			for v := range ds.vars {
				d.Input = strings.ReplaceAll(d.Input, v, ds.vars[v])
			}

			switch d.Cmd {
			case "skip":
				var issue int
				d.ScanArgs(t, "issue-num", &issue)
				skip.WithIssue(t, issue)
				return ""

			case "create-replication-clusters":
				args := replicationtestutils.DefaultTenantStreamingClustersArgs
				var cleanup func()
				ds.replicationClusters, cleanup = replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
				ds.cleanupFns = append(ds.cleanupFns, func() error {
					cleanup()
					return nil
				})

			case "start-replication-stream":
				ds.producerJobID, ds.replicationJobID = ds.replicationClusters.StartStreamReplication(ctx)

			case "wait-until-replicated-time":
				var replicatedTimeTarget string
				d.ScanArgs(t, "ts", &replicatedTimeTarget)
				varValue, ok := ds.vars[replicatedTimeTarget]
				if ok {
					replicatedTimeTarget = varValue
				}
				ds.replicationClusters.WaitUntilReplicatedTime(stringToHLC(t, replicatedTimeTarget),
					jobspb.JobID(ds.replicationJobID))
			case "start-replicated-tenant":
				cleanupTenant := ds.replicationClusters.StartDestTenant(ctx)
				ds.cleanupFns = append(ds.cleanupFns, cleanupTenant)
			case "let":
				if len(d.CmdArgs) == 0 {
					t.Fatalf("Must specify at least one variable name.")
				}
				var as string
				d.ScanArgs(t, "as", &as)
				output := ds.queryAs(t, as, d.Input)
				output = strings.TrimSpace(output)
				values := strings.Split(output, "\n")
				if len(values) != len(d.CmdArgs)-1 {
					t.Fatalf("expecting %d vars, found %d", len(d.CmdArgs)-1, len(values))
				}
				var placeholders []string
				for _, k := range d.CmdArgs {
					key := k.Key
					if !strings.HasPrefix(key, "$") {
						continue
					}
					placeholders = append(placeholders, key)
				}
				for i := range values {
					ds.vars[placeholders[i]] = values[i]
				}

				return ""

			case "cutover":
				var cutoverTime string
				d.ScanArgs(t, "ts", &cutoverTime)
				varValue, ok := ds.vars[cutoverTime]
				if ok {
					cutoverTime = varValue
				}
				var async bool
				if d.HasArg("async") {
					async = true
				}
				timestamp, _, err := tree.ParseDTimestamp(nil, cutoverTime, time.Microsecond)
				require.NoError(t, err)
				ds.replicationClusters.Cutover(ds.producerJobID, ds.replicationJobID, timestamp.Time, async)
				return ""

			case "exec-sql":
				var as string
				d.ScanArgs(t, "as", &as)
				ds.execAs(t, as, d.Input)
				return ""

			case "query-sql":
				var as string
				d.ScanArgs(t, "as", &as)

				return ds.queryAs(t, as, d.Input)

			case "compare-replication-results":
				ds.replicationClusters.CompareResult(d.Input)

			case "compare-tenant-fingerprints":
				var to string
				d.ScanArgs(t, "to", &to)
				varValue, ok := ds.vars[to]
				if ok {
					to = varValue
				}
				var from string
				d.ScanArgs(t, "from", &from)
				varValue, ok = ds.vars[from]
				if ok {
					from = varValue
				}
				allRevisions := d.HasArg("with_revisions")
				fingerprintQuery := `SELECT * FROM crdb_internal.fingerprint(crdb_internal.tenant_span('%s'), '%s'::TIMESTAMPTZ, %t) AS OF SYSTEM TIME '%s'`
				var fingerprintSrcTenant int64
				ds.replicationClusters.SrcSysSQL.QueryRow(t, fmt.Sprintf(fingerprintQuery,
					ds.replicationClusters.Args.SrcTenantName, from, allRevisions, to)).Scan(&fingerprintSrcTenant)
				require.NotZero(t, fingerprintSrcTenant)
				var fingerprintDestTenant int64
				ds.replicationClusters.DestSysSQL.QueryRow(t, fmt.Sprintf(fingerprintQuery,
					ds.replicationClusters.Args.DestTenantName, from, allRevisions, to)).Scan(&fingerprintDestTenant)
				require.NotZero(t, fingerprintDestTenant)
				if fingerprintSrcTenant != fingerprintDestTenant {
					require.NoError(t, replicationutils.InvestigateFingerprints(ctx,
						ds.replicationClusters.SrcTenantConn,
						ds.replicationClusters.DestTenantConn, stringToHLC(t, from), stringToHLC(t, to)))
					t.Fatalf("tenant level fingerpint mismatch, but table level fingerprints match")
				}
				require.Equal(t, fingerprintSrcTenant, fingerprintDestTenant)

				if d.HasArg("table_fingerprints") {
					require.NoError(t, replicationutils.InvestigateFingerprints(ctx,
						ds.replicationClusters.SrcTenantConn,
						ds.replicationClusters.DestTenantConn, stringToHLC(t, from), stringToHLC(t, to)))
				}

			case "sleep":
				var msStr string
				if d.HasArg("ms") {
					d.ScanArgs(t, "ms", &msStr)
				} else {
					t.Fatalf("must specify sleep time in ms")
				}
				ms, err := strconv.ParseInt(msStr, 10, 64)
				if err != nil {
					t.Fatalf("invalid sleep time: %v", err)
				}
				time.Sleep(time.Duration(ms) * time.Millisecond)
				return ""

			case "job":
				var (
					as     string
					jobID  int
					runner *sqlutils.SQLRunner
				)
				d.ScanArgs(t, "as", &as)
				if as == "source-system" {
					jobID = ds.producerJobID
					runner = ds.replicationClusters.SrcSysSQL
				} else if as == "destination-system" {
					jobID = ds.replicationJobID
					runner = ds.replicationClusters.DestSysSQL
				} else {
					t.Fatalf("job cmd only works on consumer and producer jobs run on system tenant")
				}
				if d.HasArg("pause") {
					ds.execAs(t, as, fmt.Sprintf(`PAUSE JOB %d`, jobID))
				} else if d.HasArg("resume") {
					ds.execAs(t, as, fmt.Sprintf(`RESUME JOB %d`, jobID))
				} else if d.HasArg("wait-for-state") {
					var state string
					d.ScanArgs(t, "wait-for-state", &state)
					jobPBID := jobspb.JobID(jobID)
					switch state {
					case "succeeded":
						jobutils.WaitForJobToSucceed(t, runner, jobPBID)
					case "cancelled":
						jobutils.WaitForJobToCancel(t, runner, jobPBID)
					case "paused":
						jobutils.WaitForJobToPause(t, runner, jobPBID)
					case "failed":
						jobutils.WaitForJobToFail(t, runner, jobPBID)
					case "reverting":
						jobutils.WaitForJobReverting(t, runner, jobPBID)
					default:
						t.Fatalf("unknown state %s", state)
					}
				}
				return ""

			default:
				t.Fatalf("unsupported instruction: %s", d.Cmd)
			}
			return ""
		})
	})
}

func stringToHLC(t *testing.T, timestamp string) hlc.Timestamp {
	parsedTimestamp, _, err := tree.ParseDTimestamp(nil, timestamp, time.Microsecond)
	require.NoError(t, err)
	return hlc.Timestamp{WallTime: parsedTimestamp.UnixNano()}
}

type datadrivenTestState struct {
	producerJobID, replicationJobID int
	replicationClusters             *replicationtestutils.TenantStreamingClusters
	cleanupFns                      []func() error
	vars                            map[string]string
}

func (d *datadrivenTestState) cleanup(t *testing.T) {
	// To mimic the calling pattern of deferred functions in a single goroutine,
	// call cleanup functions in the opposite order they were appended.
	for i := len(d.cleanupFns) - 1; i >= 0; i-- {
		require.NoError(t, d.cleanupFns[i]())
	}
}

func (d *datadrivenTestState) queryAs(t *testing.T, as, query string) string {
	var rows *gosql.Rows
	switch as {
	case "source-system":
		rows = d.replicationClusters.SrcSysSQL.Query(t, query)
	case "source-tenant":
		rows = d.replicationClusters.SrcTenantSQL.Query(t, query)
	case "destination-system":
		rows = d.replicationClusters.DestSysSQL.Query(t, query)
	case "destination-tenant":
		rows = d.replicationClusters.DestTenantSQL.Query(t, query)
	default:
		t.Fatalf("unsupported value to run SQL query as: %s", as)
	}

	output, err := sqlutils.RowsToDataDrivenOutput(rows)
	require.NoError(t, err)
	return output
}

func (d *datadrivenTestState) execAs(t *testing.T, as, query string) {
	switch as {
	case "source-system":
		d.replicationClusters.SrcSysSQL.Exec(t, query)
	case "source-tenant":
		d.replicationClusters.SrcTenantSQL.Exec(t, query)
	case "destination-system":
		d.replicationClusters.DestSysSQL.Exec(t, query)
	case "destination-tenant":
		d.replicationClusters.DestTenantSQL.Exec(t, query)
	default:
		t.Fatalf("unsupported value to run SQL query as: %s", as)
	}
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		cleanupFns: make([]func() error, 0),
		vars:       make(map[string]string),
	}
}
