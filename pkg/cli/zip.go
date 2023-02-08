// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/profiler"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
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

	firstNodeSQLConn clisqlclient.Conn

	sem semaphore.Semaphore
}

func (zc *debugZipContext) runZipFn(
	ctx context.Context, s *zipReporter, fn func(ctx context.Context) error,
) error {
	return zc.runZipFnWithTimeout(ctx, s, zc.timeout, fn)
}

func (zc *debugZipContext) runZipFnWithTimeout(
	ctx context.Context, s *zipReporter, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	err := contextutil.RunWithTimeout(ctx, s.prefix, timeout, fn)
	s.progress("received response")
	return err
}

// runZipRequest runs a zipRequest and stores its JSON result or error
// message in the output zip.
func (zc *debugZipContext) runZipRequest(ctx context.Context, zr *zipReporter, r zipRequest) error {
	s := zr.start("requesting data for %s", r.pathName)
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
	ni nodesInfo,
	fn func(ctx context.Context, nodeDetails serverpb.NodeDetails, nodeStatus *statuspb.NodeStatus) error,
) error {
	if ni.nodesListResponse == nil {
		// Nothing to do, return
		return errors.AssertionFailedf("nodes list is empty")
	}
	if ni.nodesStatusResponse != nil && len(ni.nodesStatusResponse.Nodes) != len(ni.nodesListResponse.Nodes) {
		return errors.AssertionFailedf("mismatching node status response and node list")
	}
	if zipCtx.concurrency == 1 {
		// Sequential case. Simplify.
		for index, nodeDetails := range ni.nodesListResponse.Nodes {
			var nodeStatus *statuspb.NodeStatus
			// nodeStatusResponse is expected to be nil for SQL only servers.
			if ni.nodesStatusResponse != nil {
				nodeStatus = &ni.nodesStatusResponse.Nodes[index]
			}
			if err := fn(ctx, nodeDetails, nodeStatus); err != nil {
				return err
			}
		}
		return nil
	}

	// Multiple nodes concurrently.

	// nodeErrs collects the individual error objects.
	nodeErrs := make(chan error, len(ni.nodesListResponse.Nodes))
	// The wait group to wait for all concurrent collectors.
	var wg sync.WaitGroup
	for index, nodeDetails := range ni.nodesListResponse.Nodes {
		wg.Add(1)
		var nodeStatus *statuspb.NodeStatus
		if ni.nodesStatusResponse != nil {
			nodeStatus = &ni.nodesStatusResponse.Nodes[index]
		}
		go func(nodeDetails serverpb.NodeDetails, nodeStatus *statuspb.NodeStatus) {
			defer wg.Done()
			if err := zc.sem.Acquire(ctx, 1); err != nil {
				nodeErrs <- err
				return
			}
			defer zc.sem.Release(1)

			nodeErrs <- fn(ctx, nodeDetails, nodeStatus)
		}(nodeDetails, nodeStatus)
	}
	wg.Wait()

	// The final error.
	var err error
	for range ni.nodesListResponse.Nodes {
		err = errors.CombineErrors(err, <-nodeErrs)
	}
	return err
}

type nodeLivenesses = map[roachpb.NodeID]livenesspb.NodeLivenessStatus

func runDebugZip(_ *cobra.Command, args []string) (retErr error) {
	if err := zipCtx.files.validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zr := zipCtx.newZipReporter("cluster")
	// Interpret the deprecated `--redact-logs` the same as the new `--redact` flag.
	// We later print a deprecation warning at the end of the zip operation for visibility.
	if zipCtx.redactLogs {
		zipCtx.redact = true
	}

	s := zr.start("establishing RPC connection to %s", serverCfg.AdvertiseAddr)
	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return s.fail(err)
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)
	admin := serverpb.NewAdminClient(conn)
	s.done()

	s = zr.start("retrieving the node status to get the SQL address")
	firstNodeDetails, err := status.Details(ctx, &serverpb.DetailsRequest{NodeId: "local"})
	if err != nil {
		return s.fail(err)
	}
	s.done()

	sqlAddr := firstNodeDetails.SQLAddress
	if sqlAddr.IsEmpty() {
		// No SQL address: either a pre-19.2 node, or same address for both
		// SQL and RPC.
		sqlAddr = firstNodeDetails.Address
	}
	s = zr.start("using SQL address: %s", sqlAddr.AddressField)

	cliCtx.clientOpts.ServerHost, cliCtx.clientOpts.ServerPort, err = net.SplitHostPort(sqlAddr.AddressField)
	if err != nil {
		return s.fail(err)
	}

	// We're going to use the SQL code, but in non-interactive mode.
	// Override whatever terminal-driven defaults there may be out there.
	cliCtx.IsInteractive = false
	sqlExecCtx.TerminalOutput = false
	sqlExecCtx.ShowTimes = false
	// Use a streaming format to avoid accumulating all rows in RAM.
	sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTSV

	sqlConn, err := makeSQLClient("cockroach zip", useSystemDb)
	// The zip output is sent directly into a text file, so the results should
	// be scanned into strings.
	sqlConn.SetAlwaysInferResultTypes(false)
	if err != nil {
		_ = s.fail(errors.Wrap(err, "unable to open a SQL session. Debug information will be incomplete"))
	} else {
		// Note: we're not printing "connection established" because the driver we're using
		// does late binding.
		defer func() { retErr = errors.CombineErrors(retErr, sqlConn.Close()) }()
		s.progress("using SQL connection URL: %s", sqlConn.GetURL())
		s.done()
	}

	name := args[0]
	s = zr.start("creating output file %s", name)
	out, err := os.Create(name)
	if err != nil {
		return s.fail(err)
	}

	z := newZipper(out)
	defer func() {
		cErr := z.close()
		retErr = errors.CombineErrors(retErr, cErr)
	}()
	s.done()

	timeout := 10 * time.Second
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}

	zc := debugZipContext{
		clusterPrinter:   zr,
		z:                z,
		timeout:          timeout,
		admin:            admin,
		status:           status,
		firstNodeSQLConn: sqlConn,
		sem:              semaphore.New(zipCtx.concurrency),
	}

	// Fetch the cluster-wide details.
	// For a SQL only server, the nodeList will be a list of SQL nodes
	// and livenessByNodeID is null. For a KV server, the nodeList will
	// be a list of KV nodes along with the corresponding node liveness data.
	ni, livenessByNodeID, err := zc.collectClusterData(ctx, firstNodeDetails)
	if err != nil {
		return err
	}
	// Collect the CPU profiles, before the other per-node requests
	// below possibly influences the nodes and thus CPU profiles.
	if err := zc.collectCPUProfiles(ctx, ni, livenessByNodeID); err != nil {
		return err
	}

	// Collect the per-node data.
	if err := zc.forAllNodes(ctx, ni, func(ctx context.Context, nodeDetails serverpb.NodeDetails, nodesStatus *statuspb.NodeStatus) error {
		return zc.collectPerNodeData(ctx, nodeDetails, nodesStatus, livenessByNodeID)
	}); err != nil {
		return err
	}

	// Add a little helper script to draw attention to the existence of tags in
	// the profiles.
	{
		s := zc.clusterPrinter.start("pprof summary script")
		if err := z.createRaw(s, debugBase+"/pprof-summary.sh", []byte(`#!/bin/sh
find . -name cpu.pprof -print0 | xargs -0 go tool pprof -tags
`)); err != nil {
			return err
		}
	}

	// A script to summarize the hottest ranges for a storage server's range reports.
	{
		s := zc.clusterPrinter.start("hot range summary script")
		if err := z.createRaw(s, debugBase+"/hot-ranges.sh", []byte(`#!/bin/sh
for stat in "queries" "writes" "reads" "write_bytes" "read_bytes" "cpu_time"; do
	echo "$stat"
	find . -path './nodes/*/ranges/*.json' -print0 | xargs -0 grep "$stat"_per_second | sort -rhk3 | head -n 10
done
`)); err != nil {
			return err
		}
	}

	// A script to summarize the hottest ranges for a tenant's range report.
	{
		s := zc.clusterPrinter.start("tenant hot range summary script")
		if err := z.createRaw(s, debugBase+"/hot-ranges-tenant.sh", []byte(`#!/bin/sh
for stat in "queries" "writes" "reads" "write_bytes" "read_bytes" "cpu_time"; do
    echo "$stat"_per_second
    find . -path './tenant_ranges/*/*.json' -print0 | xargs -0 grep "$stat"_per_second | sort -rhk3 | head -n 10
done
`)); err != nil {
			return err
		}
	}

	// TODO(obs-infra): remove deprecation warning once process completed in v23.2.
	if zipCtx.redactLogs {
		zr.info("WARNING: The --" + cliflags.ZipRedactLogs.Name +
			" flag has been deprecated in favor of the --" + cliflags.ZipRedact.Name + " flag. " +
			"The flag has been interpreted as --" + cliflags.ZipRedact.Name + " instead.")
	}

	return nil
}

// maybeAddProfileSuffix adds a file extension if this was not done
// already on the server. This is necessary as pre-20.2 servers did
// not use any extension for memory profiles.
//
// TODO(knz): Remove this in v21.1.
func maybeAddProfileSuffix(name string) string {
	switch {
	case strings.HasPrefix(name, profiler.HeapFileNamePrefix+".") && !strings.HasSuffix(name, profiler.HeapFileNameSuffix):
		name += profiler.HeapFileNameSuffix
	case strings.HasPrefix(name, profiler.StatsFileNamePrefix+".") && !strings.HasSuffix(name, profiler.StatsFileNameSuffix):
		name += profiler.StatsFileNameSuffix
	case strings.HasPrefix(name, profiler.JemallocFileNamePrefix+".") && !strings.HasSuffix(name, profiler.JemallocFileNameSuffix):
		name += profiler.JemallocFileNameSuffix
	}
	return name
}

// dumpTableDataForZip runs the specified SQL query and stores the
// result. Errors encountered while running the SQL query are stored
// in an error file in the zip file, and dumpTableDataForZip() returns
// nil in that case.
//
// An error is returned by this function if it is unable to write to
// the output file or some other unrecoverable error is encountered.
func (zc *debugZipContext) dumpTableDataForZip(
	zr *zipReporter, conn clisqlclient.Conn, base, table, query string,
) error {
	ctx := context.Background()
	baseName := base + "/" + sanitizeFilename(table)

	s := zr.start("retrieving SQL data for %s", table)
	const maxRetries = 5
	suffix := ""
	for numRetries := 1; numRetries <= maxRetries; numRetries++ {
		name := baseName + suffix + ".txt"
		s.progress("writing output: %s", name)
		sqlErr := func() error {
			zc.z.Lock()
			defer zc.z.Unlock()

			// TODO(knz): This can use context cancellation now that query
			// cancellation is supported in v22.1 and later.
			// SET must be run separately from the query so that the command tag output
			// doesn't get added to the debug file.
			err := conn.Exec(ctx, fmt.Sprintf(`SET statement_timeout = '%s'`, zc.timeout))
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
				// A non-retry error. We've printed the error, and
				// there's nothing to retry. Stop here.
				break
			}
			// We've encountered a retry error. Add a suffix then loop.
			suffix = fmt.Sprintf(".%d", numRetries)
			s = zr.start("retrying %s", table)
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
