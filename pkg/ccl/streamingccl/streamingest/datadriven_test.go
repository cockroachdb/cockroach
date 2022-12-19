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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
// - wait-until-high-watermark ts=<ts>
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
// - compare-tenant-fingerprints from=<start-time> to=<end-time> [with_revisions]
// Runs `crdb_internal.fingerprint` on both the "source" and "destination"
// tenants with the provided options and asserts that the generated fingerprints
// are equal.
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

			case "wait-until-high-watermark":
				var highWaterMark string
				d.ScanArgs(t, "ts", &highWaterMark)
				varValue, ok := ds.vars[highWaterMark]
				if ok {
					highWaterMark = varValue
				}
				timestamp, _, err := tree.ParseDTimestamp(nil, highWaterMark, time.Microsecond)
				require.NoError(t, err)
				hw := hlc.Timestamp{WallTime: timestamp.UnixNano()}
				ds.replicationClusters.WaitUntilHighWatermark(hw, jobspb.JobID(ds.replicationJobID))

			case "start-replicated-tenant":
				cleanupTenant := ds.replicationClusters.CreateDestTenantSQL(ctx)
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
				timestamp, _, err := tree.ParseDTimestamp(nil, cutoverTime, time.Microsecond)
				require.NoError(t, err)
				ds.replicationClusters.Cutover(ds.producerJobID, ds.replicationJobID, timestamp.Time)

			case "exec-sql":
				var as string
				d.ScanArgs(t, "as", &as)
				switch as {
				case "source-system":
					ds.replicationClusters.SrcSysSQL.Exec(t, d.Input)
				case "source-tenant":
					ds.replicationClusters.SrcTenantSQL.Exec(t, d.Input)
				case "destination-system":
					ds.replicationClusters.DestSysSQL.Exec(t, d.Input)
				case "destination-tenant":
					ds.replicationClusters.DestTenantSQL.Exec(t, d.Input)
				default:
					t.Fatalf("unsupported value to run SQL query as: %s", as)
				}

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
				require.Equal(t, fingerprintSrcTenant, fingerprintDestTenant)

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

			default:
				t.Fatalf("unsupported instruction: %s", d.Cmd)
			}
			return ""
		})
	})
}

type datadrivenTestState struct {
	producerJobID, replicationJobID int
	replicationClusters             *replicationtestutils.TenantStreamingClusters
	cleanupFns                      []func() error
	vars                            map[string]string
}

func (d *datadrivenTestState) cleanup(t *testing.T) {
	for _, cleanup := range d.cleanupFns {
		require.NoError(t, cleanup())
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

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		cleanupFns: make([]func() error, 0),
		vars:       make(map[string]string),
	}
}
