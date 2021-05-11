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
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/heapprofiler"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/marusama/semaphore"
	"github.com/spf13/cobra"
)

// zipRequest abstracts a possible server API call to one of the API
// endpoints.
type zipRequest struct {
	fn       func(ctx context.Context) (interface{}, error)
	pathName string
}

// Override for the default SELECT * FROM table when dumping one of the tables
// in `debugZipTablesPerNode` or `debugZipTablesPerCluster`
var customQuery = map[string]string{
	"crdb_internal.node_inflight_trace_spans": "WITH spans AS (" +
		"SELECT * FROM crdb_internal.node_inflight_trace_spans " +
		"WHERE duration > INTERVAL '10' ORDER BY trace_id ASC, duration DESC" +
		") SELECT * FROM spans, LATERAL crdb_internal.payloads_for_span(span_id)",
	"system.jobs":       "SELECT *, to_hex(payload) AS hex_payload, to_hex(progress) AS hex_progress FROM system.jobs",
	"system.descriptor": "SELECT *, to_hex(descriptor) AS hex_descriptor FROM system.descriptor",
}

type debugZipContext struct {
	z              *zipper
	clusterPrinter *zipReporter
	timeout        time.Duration
	admin          serverpb.AdminClient
	status         serverpb.StatusClient

	firstNodeSQLConn *sqlConn

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
	nodeList []statuspb.NodeStatus,
	fn func(ctx context.Context, node statuspb.NodeStatus) error,
) error {
	if zipCtx.concurrency == 1 {
		// Sequential case. Simplify.
		for _, node := range nodeList {
			if err := fn(ctx, node); err != nil {
				return err
			}
		}
		return nil
	}

	// Multiple nodes concurrently.

	// nodeErrs collects the individual error objects.
	nodeErrs := make(chan error, len(nodeList))
	// The wait group to wait for all concurrent collectors.
	var wg sync.WaitGroup
	for _, node := range nodeList {
		wg.Add(1)
		go func(node statuspb.NodeStatus) {
			defer wg.Done()
			if err := zc.sem.Acquire(ctx, 1); err != nil {
				nodeErrs <- err
				return
			}
			defer zc.sem.Release(1)

			nodeErrs <- fn(ctx, node)
		}(node)
	}
	wg.Wait()

	// The final error.
	var err error
	for range nodeList {
		err = errors.CombineErrors(err, <-nodeErrs)
	}
	return err
}

type nodeLivenesses = map[roachpb.NodeID]livenesspb.NodeLivenessStatus

func runDebugZip(cmd *cobra.Command, args []string) (retErr error) {
	if err := zipCtx.files.validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zr := zipCtx.newZipReporter("cluster")

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

	cliCtx.clientConnHost, cliCtx.clientConnPort, err = net.SplitHostPort(sqlAddr.AddressField)
	if err != nil {
		return s.fail(err)
	}

	// We're going to use the SQL code, but in non-interactive mode.
	// Override whatever terminal-driven defaults there may be out there.
	cliCtx.isInteractive = false
	cliCtx.terminalOutput = false
	sqlCtx.showTimes = false
	// Use a streaming format to avoid accumulating all rows in RAM.
	cliCtx.tableDisplayFormat = tableDisplayTSV

	sqlConn, err := makeSQLClient("cockroach zip", useSystemDb)
	if err != nil {
		_ = s.fail(errors.Wrap(err, "unable to open a SQL session. Debug information will be incomplete"))
	} else {
		// Note: we're not printing "connection established" because the driver we're using
		// does late binding.
		defer sqlConn.Close()
		s.progress("using SQL connection URL: %s", sqlConn.url)
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
	nodeList, livenessByNodeID, err := zc.collectClusterData(ctx, firstNodeDetails)
	if err != nil {
		return err
	}

	// Collect the CPU profiles, before the other per-node requests
	// below possibly influences the nodes and thus CPU profiles.
	if err := zc.collectCPUProfiles(ctx, nodeList, livenessByNodeID); err != nil {
		return err
	}

	// Collect the per-node data.
	if err := zc.forAllNodes(ctx, nodeList, func(ctx context.Context, node statuspb.NodeStatus) error {
		return zc.collectPerNodeData(ctx, node, livenessByNodeID)
	}); err != nil {
		return err
	}

	// Collect the SQL schema.
	if err := zc.collectSchemaData(ctx); err != nil {
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

	// A script to summarize the hottest ranges.
	{
		s := zc.clusterPrinter.start("hot range summary script")
		if err := z.createRaw(s, debugBase+"/hot-ranges.sh", []byte(`#!/bin/sh
find . -path './nodes/*/ranges/*.json' -print0 | xargs -0 grep per_second | sort -rhk3 | head -n 20
`)); err != nil {
			return err
		}
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
	case strings.HasPrefix(name, heapprofiler.HeapFileNamePrefix+".") && !strings.HasSuffix(name, heapprofiler.HeapFileNameSuffix):
		name += heapprofiler.HeapFileNameSuffix
	case strings.HasPrefix(name, heapprofiler.StatsFileNamePrefix+".") && !strings.HasSuffix(name, heapprofiler.StatsFileNameSuffix):
		name += heapprofiler.StatsFileNameSuffix
	case strings.HasPrefix(name, heapprofiler.JemallocFileNamePrefix+".") && !strings.HasSuffix(name, heapprofiler.JemallocFileNameSuffix):
		name += heapprofiler.JemallocFileNameSuffix
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
	zr *zipReporter, conn *sqlConn, base, table, query string,
) error {
	fullQuery := fmt.Sprintf(`SET statement_timeout = '%s'; %s`, zc.timeout, query)
	baseName := base + "/" + table

	s := zr.start("retrieving SQL data for %s", table)
	const maxRetries = 5
	suffix := ""
	for numRetries := 1; numRetries <= maxRetries; numRetries++ {
		name := baseName + suffix + ".txt"
		s.progress("writing output: %s", name)
		sqlErr := func() error {
			zc.z.Lock()
			defer zc.z.Unlock()

			w, err := zc.z.createLocked(name, time.Time{})
			if err != nil {
				return err
			}
			// Pump the SQL rows directly into the zip writer, to avoid
			// in-RAM buffering.
			return runQueryAndFormatResults(conn, w, makeQuery(fullQuery))
		}()
		if sqlErr != nil {
			if cErr := zc.z.createError(s, name, sqlErr); cErr != nil {
				return cErr
			}
			var pqErr *pq.Error
			if !errors.As(sqlErr, &pqErr) {
				// Not a SQL error. Nothing to retry.
				break
			}
			if pgcode.MakeCode(string(pqErr.Code)) != pgcode.SerializationFailure {
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
