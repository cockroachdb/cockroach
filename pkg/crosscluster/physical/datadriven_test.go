// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
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
		// Skip the test if it is a .txt file. This is to allow us to have non-test
		// testdata in the same directory as the test files.
		if strings.HasSuffix(path, ".txt") {
			return
		}
		ds := newDatadrivenTestState()
		defer ds.cleanup(t)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			for v := range ds.vars {
				d.Input = strings.ReplaceAll(d.Input, v, ds.vars[v])
			}

			// A few built-in replacements since we
			// already have these IDs in many cases, we
			// don't need to force the caller to get them
			// again.
			d.Input = strings.ReplaceAll(d.Input, "$_producerJobID", fmt.Sprintf("%d", ds.producerJobID))
			d.Input = strings.ReplaceAll(d.Input, "$_ingestionJobID", fmt.Sprintf("%d", ds.ingestionJobID))
			d.Expected = strings.ReplaceAll(d.Expected, "$_producerJobID", fmt.Sprintf("%d", ds.producerJobID))
			d.Expected = strings.ReplaceAll(d.Expected, "$_ingestionJobID", fmt.Sprintf("%d", ds.ingestionJobID))

			switch d.Cmd {
			case "skip":
				var issue int
				d.ScanArgs(t, "issue-num", &issue)
				skip.WithIssue(t, issue)
				return ""

			case "create-replication-clusters":
				args := replicationtestutils.DefaultTenantStreamingClustersArgs
				args.NoMetamorphicExternalConnection = d.HasArg("no-external-conn")
				tempDir, dirCleanup := testutils.TempDir(t)
				args.ExternalIODir = tempDir
				var cleanup func()
				ds.replicationClusters, cleanup = replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
				ds.cleanupFns = append(ds.cleanupFns, func() error {
					cleanup()
					dirCleanup()
					return nil
				})

			case "start-replication-stream":
				ds.producerJobID, ds.ingestionJobID = ds.replicationClusters.StartStreamReplication(ctx)

			case "wait-until-replicated-time":
				var replicatedTimeTarget string
				d.ScanArgs(t, "ts", &replicatedTimeTarget)
				varValue, ok := ds.vars[replicatedTimeTarget]
				if ok {
					replicatedTimeTarget = varValue
				}
				ds.replicationClusters.WaitUntilReplicatedTime(stringToHLC(t, replicatedTimeTarget),
					jobspb.JobID(ds.ingestionJobID))
			case "start-replicated-tenant":
				testingKnobs := replicationtestutils.DefaultAppTenantTestingKnobs()
				cleanupTenant := ds.replicationClusters.StartDestTenant(ctx, &testingKnobs, 0)
				ds.cleanupFns = append(ds.cleanupFns, func() error { cleanupTenant(); return nil })
			case "let":
				if len(d.CmdArgs) == 0 {
					t.Fatalf("Must specify at least one variable name.")
				}
				var as string
				d.ScanArgs(t, "as", &as)
				output, err := ds.queryAs(ctx, t, as, d.Input)
				require.NoError(t, err)
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
				ds.replicationClusters.Cutover(ctx, ds.producerJobID, ds.ingestionJobID, timestamp.Time, async)
				return ""

			case "exec-sql":
				var as string
				d.ScanArgs(t, "as", &as)
				ds.execAs(t, as, d.Input)
				return ""

			case "query-sql":
				var as string
				var regexError string
				d.MaybeScanArgs(t, "regex-error", &regexError)
				d.ScanArgs(t, "as", &as)
				if d.HasArg("retry") {
					ds.queryAsWithRetry(ctx, t, as, d.Input, d.Expected, regexError)
				}
				output, err := ds.queryAs(ctx, t, as, d.Input)
				if regexError != "" {
					require.NoError(t, handleRegex(t, err, regexError))
					return ""
				}
				return output

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
				fingerprintQuery := `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT '%s'] AS OF SYSTEM TIME '%s'`
				if allRevisions {
					fingerprintQuery = `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT '%s' WITH START TIMESTAMP = '%s'] AS OF SYSTEM TIME '%s'`
				}
				var fingerprintSrcTenant int64
				ds.replicationClusters.SrcSysSQL.QueryRow(t, fmt.Sprintf(fingerprintQuery,
					ds.replicationClusters.Args.SrcTenantName, from, to)).Scan(&fingerprintSrcTenant)
				require.NotZero(t, fingerprintSrcTenant)
				var fingerprintDestTenant int64
				ds.replicationClusters.DestSysSQL.QueryRow(t, fmt.Sprintf(fingerprintQuery,
					ds.replicationClusters.Args.DestTenantName, from, to)).Scan(&fingerprintDestTenant)
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
					jobID = ds.ingestionJobID
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
					case "running":
						jobutils.WaitForJobToRun(t, runner, jobPBID)
					default:
						t.Fatalf("unknown state %s", state)
					}
				}
				return ""
			case "list-ttls":
				var (
					as string
				)
				d.ScanArgs(t, "as", &as)
				var codec keys.SQLCodec
				switch {
				case strings.HasPrefix(as, "source"):
					codec = keys.MakeSQLCodec(ds.replicationClusters.Args.SrcTenantID)
				case strings.HasPrefix(as, "destination"):
					codec = keys.MakeSQLCodec(ds.replicationClusters.Args.DestTenantID)
				default:
					t.Fatalf("%s does not begin with 'source' or 'destination'", as)
				}

				getTableID := func(arg string) uint32 {
					var tableID string
					d.ScanArgs(t, arg, &tableID)
					varValue, ok := ds.vars[tableID]
					if ok {
						tableID = varValue
					}
					parsedID, err := strconv.Atoi(tableID)
					if err != nil {
						t.Fatalf("could not convert table ID %s", tableID)
					}
					return uint32(parsedID)
				}

				startKey := codec.TablePrefix(getTableID("min_table_id"))
				endKey := codec.TablePrefix(getTableID("max_table_id"))

				listQuery := fmt.Sprintf(
					`SELECT crdb_internal.pb_to_json('cockroach.roachpb.SpanConfig', config)->'gcPolicy'->'ttlSeconds'
FROM system.span_configurations
WHERE start_key >= '\x%x' AND start_key <= '\x%x'
ORDER BY start_key;`, startKey, endKey)
				return ds.queryAsWithRetry(ctx, t, as, listQuery, d.Expected, "")

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
	producerJobID       int
	ingestionJobID      int
	replicationClusters *replicationtestutils.TenantStreamingClusters
	cleanupFns          []func() error
	vars                map[string]string
}

func (d *datadrivenTestState) cleanup(t *testing.T) {
	// To mimic the calling pattern of deferred functions in a single goroutine,
	// call cleanup functions in the opposite order they were appended.
	for i := len(d.cleanupFns) - 1; i >= 0; i-- {
		require.NoError(t, d.cleanupFns[i]())
	}
}

func (d *datadrivenTestState) queryAs(
	ctx context.Context, t *testing.T, as, query string,
) (string, error) {
	var rows *gosql.Rows
	var err error
	switch as {
	case "source-system":
		rows, err = d.replicationClusters.SrcSysSQL.DB.QueryContext(ctx, query)
	case "source-tenant":
		rows, err = d.replicationClusters.SrcTenantSQL.DB.QueryContext(ctx, query)
	case "destination-system":
		rows, err = d.replicationClusters.DestSysSQL.DB.QueryContext(ctx, query)
	case "destination-tenant":
		rows, err = d.replicationClusters.DestTenantSQL.DB.QueryContext(ctx, query)
	default:
		t.Fatalf("unsupported value to run SQL query as: %s", as)
	}

	if err != nil {
		return "", err
	}
	output, err := sqlutils.RowsToDataDrivenOutput(rows)
	require.NoError(t, err)
	return output, nil
}

func (d *datadrivenTestState) queryAsWithRetry(
	ctx context.Context, t *testing.T, as, query, expected string, regexError string,
) string {
	var output string
	var err error
	testutils.SucceedsSoon(t, func() error {
		output, err = d.queryAs(ctx, t, as, query)
		if regexError != "" {
			output = ""
			return handleRegex(t, err, regexError)
		}
		if output != expected {
			return errors.Newf("latest output: %s\n expected: %s", output, expected)
		}
		return nil
	})
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

func handleRegex(t *testing.T, err error, regexError string) error {
	if err == nil {
		return errors.Newf("expected non nil error to match %s", regexError)
	}
	matched, matchStringErr := regexp.MatchString(regexError, err.Error())
	require.NoError(t, matchStringErr)
	if matched {
		return nil
	} else {
		return errors.Wrapf(err, "error does not match regex error %s", regexError)
	}
}

func newDatadrivenTestState() datadrivenTestState {
	return datadrivenTestState{
		cleanupFns: make([]func() error, 0),
		vars:       make(map[string]string),
	}
}
