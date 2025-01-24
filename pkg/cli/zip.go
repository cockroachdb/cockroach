// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	tracezipper "github.com/cockroachdb/cockroach/pkg/util/tracing/zipper"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/marusama/semaphore"
	"github.com/spf13/cobra"
)

// zipRequest abstracts a possible server API call to one of the API
// endpoints.
type zipRequest struct {
	fn       func(ctx context.Context) (interface{}, error)
	pathName string
}

type debugZipContext struct {
	z              *zipper
	clusterPrinter *zipReporter
	timeout        time.Duration
	admin          serverpb.AdminClient
	status         serverpb.StatusClient
	prefix         string

	firstNodeSQLConn clisqlclient.Conn

	sem semaphore.Semaphore
}

var filterFlags = map[string]struct{}{
	"cert-principal-map":                {},
	"certs-dir":                         {},
	"cluster-name":                      {},
	"disable-cluster-name-verification": {},
	"format":                            {},
	"host":                              {},
	"url":                               {},
	"enterprise-require-fips-ready":     {},
	"log":                               {},
	"log-config-file":                   {},
	"log-config-vars":                   {},
	"log-dir":                           {},
	"logtostderr":                       {},
	"vmodule":                           {},
}

func (zc *debugZipContext) runZipFn(
	ctx context.Context, s *zipReporter, fn func(ctx context.Context) error,
) error {
	return zc.runZipFnWithTimeout(ctx, s, zc.timeout, fn)
}

func (zc *debugZipContext) runZipFnWithTimeout(
	ctx context.Context, s *zipReporter, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	err := timeutil.RunWithTimeout(ctx, s.prefix, timeout, fn)
	s.progress("received response")
	return err
}

// runZipRequest runs a zipRequest and stores its JSON result or error
// message in the output zip.
func (zc *debugZipContext) runZipRequest(ctx context.Context, zr *zipReporter, r zipRequest) error {
	s := zr.start(redact.Sprintf("requesting data for %s", r.pathName))
	var data interface{}
	err := zc.runZipFn(ctx, s, func(ctx context.Context) error {
		thisData, err := r.fn(ctx)
		data = thisData
		return err
	})
	return zc.z.createJSONOrError(s, r.pathName+".json", data, err)
}

// forAllNodes runs fn on every node, possibly concurrently.
func (zc *debugZipContext) forAllNodes(
	ctx context.Context,
	nodesList *serverpb.NodesListResponse,
	redactedNodesList *serverpb.NodesListResponse,
	fn func(ctx context.Context, nodeDetails serverpb.NodeDetails, nodeStatus *statuspb.NodeStatus, redactedNodeDetails serverpb.NodeDetails) error,
) error {
	if nodesList == nil {
		// Nothing to do, return
		return errors.AssertionFailedf("nodes list is empty")
	}
	if zipCtx.concurrency == 1 {
		// Sequential case. Simplify.
		for _, nodeDetails := range nodesList.Nodes {
			var nodeStatus *statuspb.NodeStatus
			if err := fn(ctx, nodeDetails, nodeStatus, zc.getRedactedNodeDetails(redactedNodesList, nodeDetails.NodeID)); err != nil {
				return err
			}
		}
		return nil
	}

	// Multiple nodes concurrently.

	// nodeErrs collects the individual error objects.
	nodeErrs := make(chan error, len(nodesList.Nodes))
	// The wait group to wait for all concurrent collectors.
	var wg sync.WaitGroup
	for _, nodeDetails := range nodesList.Nodes {
		wg.Add(1)
		var nodeStatus *statuspb.NodeStatus
		go func(nodeDetails serverpb.NodeDetails, nodeStatus *statuspb.NodeStatus) {
			defer wg.Done()
			if err := zc.sem.Acquire(ctx, 1); err != nil {
				nodeErrs <- err
				return
			}
			defer zc.sem.Release(1)

			nodeErrs <- fn(ctx, nodeDetails, nodeStatus, zc.getRedactedNodeDetails(redactedNodesList, nodeDetails.NodeID))
		}(nodeDetails, nodeStatus)
	}
	wg.Wait()

	// The final error.
	var err error
	for range nodesList.Nodes {
		err = errors.CombineErrors(err, <-nodeErrs)
	}
	return err
}

// getRedactedNodeDetails finds out matching redacted node details using node Id.
// When we have a redacted nodelist response and unredacted nodelist response,
// there is no guarantee that the objects in the list are going to be in the same
// order by node Id. Hence, we are explicitly extracting the required object by node
// id.
func (zc *debugZipContext) getRedactedNodeDetails(
	redactedNodesList *serverpb.NodesListResponse, nodeId int32,
) serverpb.NodeDetails {
	if redactedNodesList == nil {
		return serverpb.NodeDetails{}
	}

	for i := range redactedNodesList.Nodes {
		if redactedNodesList.Nodes[i].NodeID == nodeId {
			return redactedNodesList.Nodes[i]
		}
	}

	return serverpb.NodeDetails{}
}

type nodeLivenesses = map[roachpb.NodeID]livenesspb.NodeLivenessStatus

func runDebugZip(cmd *cobra.Command, args []string) (retErr error) {
	if err := zipCtx.files.validate(); err != nil {
		return err
	}

	timeout := 60 * time.Second
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zr := zipCtx.newZipReporter("cluster")
	// Interpret the deprecated `--redact-logs` the same as the new `--redact` flag.
	// We later print a deprecation warning at the end of the zip operation for visibility.
	if zipCtx.redactLogs {
		zipCtx.redact = true
	}

	var tenants []*serverpb.Tenant
	if err := func() error {
		s := zr.start("discovering virtual clusters")
		conn, finish, err := getClientGRPCConn(ctx, serverCfg)
		if err != nil {
			return s.fail(err)
		}
		defer finish()

		var resp *serverpb.ListTenantsResponse
		if err := timeutil.RunWithTimeout(context.Background(), "list virtual clusters", timeout, func(ctx context.Context) error {
			resp, err = serverpb.NewAdminClient(conn).ListTenants(ctx, &serverpb.ListTenantsRequest{})
			return err
		}); err != nil {
			// For pre-v23.1 clusters, this endpoint in not implemented, proceed with
			// only querying the system tenant.
			resp, sErr := serverpb.NewStatusClient(conn).Details(ctx, &serverpb.DetailsRequest{NodeId: "local"})
			if sErr != nil {
				return s.fail(errors.CombineErrors(err, sErr))
			}
			tenants = append(tenants, &serverpb.Tenant{
				TenantId:   &roachpb.SystemTenantID,
				TenantName: catconstants.SystemTenantName,
				SqlAddr:    resp.SQLAddress.String(),
				RpcAddr:    serverCfg.Addr,
			})
		} else {
			tenants = resp.Tenants
		}
		s.done()

		return nil
	}(); err != nil {
		return err
	}

	dirName := args[0]
	s := zr.start(redact.Sprintf("creating output file %s", dirName))
	out, err := os.Create(dirName)
	if err != nil {
		return s.fail(err)
	}

	z := newZipper(out)
	defer func() {
		cErr := z.close()
		retErr = errors.CombineErrors(retErr, cErr)
	}()
	s.done()

	for _, tenant := range tenants {
		if err := func() error {
			cfg := serverCfg
			cfg.AdvertiseAddr = tenant.RpcAddr
			sqlAddr := tenant.SqlAddr

			s := zr.start(redact.Sprintf("establishing RPC connection to %s", cfg.AdvertiseAddr))
			conn, finish, err := getClientGRPCConn(ctx, cfg)
			if err != nil {
				return s.fail(err)
			}
			defer finish()

			status := serverpb.NewStatusClient(conn)
			admin := serverpb.NewAdminClient(conn)
			s.done()

			if sqlAddr == "" {
				// No SQL address: either a pre-19.2 node, or same address for both
				// SQL and RPC.
				sqlAddr = tenant.RpcAddr
			}
			s = zr.start(redact.Sprintf("using SQL address: %s", sqlAddr))

			cliCtx.clientOpts.ServerHost, cliCtx.clientOpts.ServerPort, err = net.SplitHostPort(sqlAddr)
			if err != nil {
				return s.fail(err)
			}

			// We're going to use the SQL code, but in non-interactive mode.
			// Override whatever terminal-driven defaults there may be out there.
			cliCtx.IsInteractive = false
			sqlExecCtx.TerminalOutput = false
			sqlExecCtx.ShowTimes = false

			if !cmd.Flags().Changed(cliflags.TableDisplayFormat.Name) {
				// Use a streaming format to avoid accumulating all rows in RAM.
				sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTSV
			}

			zr.sqlOutputFilenameExtension = computeSQLOutputFilenameExtension(sqlExecCtx.TableDisplayFormat)

			sqlConn, err := makeTenantSQLClient(ctx, "cockroach zip", useSystemDb, tenant.TenantName)
			// The zip output is sent directly into a text file, so the results should
			// be scanned into strings.
			_ = sqlConn.SetAlwaysInferResultTypes(false)
			if err != nil {
				_ = s.fail(errors.Wrap(err, "unable to open a SQL session. Debug information will be incomplete"))
			} else {
				// Note: we're not printing "connection established" because the driver we're using
				// does late binding.
				defer func() { retErr = errors.CombineErrors(retErr, sqlConn.Close()) }()
				s.progress("using SQL connection URL: %s", sqlConn.GetURL())
				s.done()
			}

			// Only add tenant prefix for non system tenants.
			var prefix string
			if tenant.TenantId.ToUint64() != roachpb.SystemTenantID.ToUint64() {
				prefix = fmt.Sprintf("/cluster/%s", tenant.TenantName)
			}

			zc := debugZipContext{
				clusterPrinter:   zr,
				z:                z,
				timeout:          timeout,
				admin:            admin,
				status:           status,
				firstNodeSQLConn: sqlConn,
				sem:              semaphore.New(zipCtx.concurrency),
				prefix:           debugBase + prefix,
			}

			// Fetch the cluster-wide details.
			// For a SQL only server, the nodeList will be a list of SQL nodes
			// and livenessByNodeID is null. For a KV server, the nodeList will
			// be a list of KV nodes along with the corresponding node liveness data.
			nodesList, redactedNodesList, livenessByNodeID, err := zc.collectClusterData(ctx)
			if err != nil {
				return err
			}
			// Collect the CPU profiles, before the other per-node requests
			// below possibly influences the nodes and thus CPU profiles.
			if err := zc.collectCPUProfiles(ctx, nodesList, livenessByNodeID); err != nil {
				return err
			}

			// Collect the per-node data.
			if err := zc.forAllNodes(ctx, nodesList, redactedNodesList, func(ctx context.Context, nodeDetails serverpb.NodeDetails, nodesStatus *statuspb.NodeStatus, redactedNodeDetails serverpb.NodeDetails) error {
				return zc.collectPerNodeData(ctx, nodeDetails, nodesStatus, livenessByNodeID, redactedNodeDetails)
			}); err != nil {
				return err
			}

			// Add a little helper script to draw attention to the existence of tags in
			// the profiles.
			{
				s = zc.clusterPrinter.start("pprof summary script")
				if err := z.createRaw(s, zc.prefix+"/pprof-summary.sh", []byte(`#!/bin/sh
find . -name cpu.pprof -print0 | xargs -0 go tool pprof -tags
`)); err != nil {
					return err
				}
			}

			// A script to summarize the hottest ranges for a storage server's range reports.
			if zipCtx.includeRangeInfo {
				s = zc.clusterPrinter.start("hot range summary script")
				if err := z.createRaw(s, zc.prefix+"/hot-ranges.sh", []byte(`#!/bin/sh
for stat in "queries" "writes" "reads" "write_bytes" "read_bytes" "cpu_time"; do
	echo "$stat"
	find . -path './nodes/*/ranges/*.json' -print0 | xargs -0 grep "$stat"_per_second | sort -rhk3 | head -n 10
done
`)); err != nil {
					return err
				}
			}

			// A script to summarize the hottest ranges for a tenant's range report.
			if zipCtx.includeRangeInfo {
				s = zc.clusterPrinter.start("tenant hot range summary script")
				if err := z.createRaw(s, zc.prefix+"/hot-ranges-tenant.sh", []byte(`#!/bin/sh
for stat in "queries" "writes" "reads" "write_bytes" "read_bytes" "cpu_time"; do
    echo "$stat"_per_second
    find . -path './tenant_ranges/*/*.json' -print0 | xargs -0 grep "$stat"_per_second | sort -rhk3 | head -n 10
done
`)); err != nil {
					return err
				}
			}

			s = zr.start("capture debug zip flags")
			flags := getCLIClusterFlags(true, cmd, func(flag string) bool {
				_, filter := filterFlags[flag]
				return filter
			})

			if err := z.createRaw(s, zc.prefix+"/debug_zip_command_flags.txt", []byte(flags)); err != nil {
				return err
			}

			return nil
		}(); err != nil {
			return err
		}
	}

	if !zipCtx.includeRunningJobTraces {
		zr.info("NOTE: Omitted traces of running jobs from this debug zip bundle." +
			" Use the --" + cliflags.ZipIncludeRunningJobTraces.Name + " flag to enable the fetching of this" +
			" data.")
	}

	if !zipCtx.includeStacks {
		zr.info("NOTE: Omitted node-level goroutine stack dumps from this debug zip bundle." +
			" Use the --" + cliflags.ZipIncludeGoroutineStacks.Name + " flag to enable the fetching of this" +
			" data.")
	}

	// TODO(obs-infra): remove deprecation warning once process completed in v23.2.
	if zipCtx.redactLogs {
		zr.info("WARNING: The --" + cliflags.ZipRedactLogs.Name +
			" flag has been deprecated in favor of the --" + cliflags.ZipRedact.Name + " flag. " +
			"The flag has been interpreted as --" + cliflags.ZipRedact.Name + " instead.")
	}

	return nil
}

type jobTrace struct {
	jobID   jobspb.JobID
	traceID tracingpb.TraceID
}

// dumpTraceableJobTraces collects the traces for some "traceable" jobs that are
// in a running state. The job types in this list are the ones that have
// explicitly implemented the TraceableJob interface.
func (zc *debugZipContext) dumpTraceableJobTraces(ctx context.Context) error {
	rows, err := zc.firstNodeSQLConn.Query(ctx,
		`WITH
latestprogress AS (
  SELECT job_id, value
  FROM system.job_info AS progress
  WHERE info_key = 'legacy_progress'
  ORDER BY written desc
),
jobpage AS (
  SELECT id
  FROM system.jobs@jobs_status_created_idx
  WHERE (job_type IN ($1, $2, $3, $4)) AND (status IN ($5, $6))
  ORDER BY id
)
SELECT distinct (id), latestprogress.value AS progress
FROM jobpage AS j
INNER JOIN latestprogress ON j.id = latestprogress.job_id;`,
		jobspb.TypeBackup.String(),
		jobspb.TypeRestore.String(),
		jobspb.TypeImport.String(),
		jobspb.TypeReplicationStreamIngestion.String(),
		"running",
		"reverting",
	)
	if err != nil {
		return err
	}
	vals := make([]driver.Value, 2)
	jobTraces := make([]jobTrace, 0)
	for err = rows.Next(vals); err == nil; err = rows.Next(vals) {
		jobID, ok := vals[0].(int64)
		if !ok {
			return errors.New("failed to parse jobID")
		}
		progressBytes, ok := vals[1].([]byte)
		if !ok {
			return errors.New("failed to parse progress bytes")
		}
		progress := &jobspb.Progress{}
		if err := protoutil.Unmarshal(progressBytes, progress); err != nil {
			return err
		}
		jobTraces = append(jobTraces, jobTrace{jobID: jobspb.JobID(jobID), traceID: progress.TraceID})
	}
	if err != io.EOF {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	func() {
		// Debug zip collection sets this to false since results from the query are
		// all dumped into txt files. In our case we parse the results of the query
		// with their respective types and pre-process the information before
		// dumping into a zip file.
		reset := zc.firstNodeSQLConn.SetAlwaysInferResultTypes(true)
		defer reset()
		for _, jobTrace := range jobTraces {
			inflightTraceZipper := tracezipper.MakeSQLConnInflightTraceZipper(zc.firstNodeSQLConn.GetDriverConn())
			jobZip, err := inflightTraceZipper.Zip(ctx, int64(jobTrace.traceID))
			if err != nil {
				log.Warningf(ctx, "failed to collect inflight trace zip for job %d: %v", jobTrace.jobID, err)
				continue
			}

			ts := timeutil.Now().Format(`20060102150405`)
			name := fmt.Sprintf("%s/jobs/%d/%s/trace.zip", zc.prefix, jobTrace.jobID, ts)
			s := zc.clusterPrinter.start(redact.Sprintf("requesting traces for job %d", jobTrace.jobID))
			if err := zc.z.createRaw(s, name, jobZip); err != nil {
				log.Warningf(ctx, "failed to write inflight trace zip for job %d to file %s: %v",
					jobTrace.jobID, name, err)
				continue
			}
		}
	}()

	return nil
}

// dumpTableDataForZip runs the specified SQL query and stores the
// result. Errors encountered while running the SQL query are stored
// in an error file in the zip file, and dumpTableDataForZip() returns
// nil in that case.
//
// An error is returned by this function if it is unable to write to
// the output file or some other unrecoverable error is encountered.
func (zc *debugZipContext) dumpTableDataForZip(
	zr *zipReporter, conn clisqlclient.Conn, base, table string, tableQuery TableQuery,
) error {
	ctx := context.Background()
	fileName := sanitizeFilename(table)
	baseName := path.Join(base, fileName)
	fileNameWithExtension := fileName + "." + zc.clusterPrinter.sqlOutputFilenameExtension
	if !zipCtx.files.shouldIncludeFile(fileNameWithExtension) {
		zr.info("skipping table data for %s due to file filters", table)
		return nil
	}
	s := zr.start(redact.Sprintf("retrieving SQL data for %s", table))
	const maxRetries = 5
	suffix := ""

	query := tableQuery.query
	fallback := tableQuery.fallback != ""

	for numRetries := 1; numRetries <= maxRetries; numRetries++ {
		name := baseName + suffix + "." + zc.clusterPrinter.sqlOutputFilenameExtension
		s.progress("writing output: %s", name)
		sqlErr := func() (err error) {
			zc.z.Lock()
			defer zc.z.Unlock()

			// Use a time travel query intentionally to avoid contention.
			if !buildutil.CrdbTestBuild {
				if err := conn.Exec(ctx, "BEGIN AS OF SYSTEM TIME '-0.1s';"); err != nil {
					return err
				}
				defer func() {
					rollbackErr := conn.Exec(ctx, "ROLLBACK;")
					if rollbackErr != nil {
						err = errors.WithSecondaryError(err, errors.Wrapf(rollbackErr, "failed rolling back"))
					}
				}()
			}

			// TODO(knz): This can use context cancellation now that query
			// cancellation is supported in v22.1 and later.
			// SET must be run separately from the query so that the command tag output
			// doesn't get added to the debug file.
			err = conn.Exec(ctx, fmt.Sprintf(`SET statement_timeout = '%s'`, zc.timeout))
			if err != nil {
				return err
			}

			w, err := zc.z.createLocked(name, time.Time{})
			if err != nil {
				return err
			}
			// Pump the SQL rows directly into the zip writer, to avoid
			// in-RAM buffering.
			return sqlExecCtx.RunQueryAndFormatResults(ctx, conn, w, io.Discard, stderr, clisqlclient.MakeQuery(query))
		}()
		if sqlErr != nil {
			if cErr := zc.z.createError(s, name, errors.CombineErrors(sqlErr, errors.Newf("query: %s", query))); cErr != nil {
				return cErr
			}
			var pgErr = (*pgconn.PgError)(nil)
			if !errors.As(sqlErr, &pgErr) {
				// Not a SQL error. Nothing to retry.
				break
			}
			if pgcode.MakeCode(pgErr.Code) != pgcode.SerializationFailure {
				// A non-retry error. If we have a fallback, try with that.
				if fallback {
					fallback = false

					query = tableQuery.fallback
					numRetries = 1 // Reset counter since this is a different query.
					baseName = baseName + ".fallback"
					s = zr.start(redact.Sprintf("retrieving SQL data for %s (fallback)", table))

					continue
				}
				// A non-retry error, no fallback. We've printed the error, and
				// there's nothing to retry. Stop here.
				break
			}
			// We've encountered a retry error. Add a suffix then loop.
			suffix = fmt.Sprintf(".%d", numRetries)
			s = zr.start(redact.Sprintf("retrying %s", table))
			continue
		}
		s.done()
		break
	}
	return nil
}

func sanitizeFilename(f string) string {
	return strings.TrimPrefix(f, `"".`)
}

func computeSQLOutputFilenameExtension(tfmt clisqlexec.TableDisplayFormat) string {
	switch tfmt {
	case clisqlexec.TableDisplayTSV:
		// Backward-compatibility with previous versions.
		return "txt"
	default:
		return tfmt.String()
	}
}
