// Copyright 2021 The Cockroach Authors.
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
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugJobTraceFromClusterCmd = &cobra.Command{
	Use:   "job-trace <job_id> <file_path> --url=<cluster connection string>",
	Short: "get the trace payloads for the executing job",
	Args:  cobra.MinimumNArgs(2),
	RunE:  MaybeDecorateGRPCError(runDebugJobTrace),
}

func runDebugJobTrace(_ *cobra.Command, args []string) error {
	jobID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}

	sqlConn, err := makeSQLClient("cockroach debug job-trace", useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer sqlConn.Close()

	return writeTracePayloadJSON(sqlConn, jobID, args[1])
}

func getJobTraceID(sqlConn *sqlConn, jobID int64) (int64, error) {
	var traceID int64
	rows, err := sqlConn.Query(`SELECT trace_id FROM crdb_internal.jobs WHERE job_id=$1`, []driver.Value{jobID})
	if err != nil {
		return traceID, err
	}
	vals := make([]driver.Value, 1)
	for {
		var err error
		if err = rows.Next(vals); err == io.EOF {
			break
		}
		if err != nil {
			return traceID, err
		}
	}
	if err := rows.Close(); err != nil {
		return traceID, err
	}
	if vals[0] == nil {
		return traceID, errors.Newf("no job entry found for %d", jobID)
	}
	var ok bool
	traceID, ok = vals[0].(int64)
	if !ok {
		return traceID, errors.New("failed to parse traceID")
	}
	return traceID, nil
}

func writeTracePayloadJSON(sqlConn *sqlConn, jobID int64, traceFilePath string) error {
	maybePrint := func(stmt string) string {
		if debugCtx.verbose {
			fmt.Println("querying " + stmt)
		}
		return stmt
	}

	// Check if a timeout has been set for this command.
	if cliCtx.cmdTimeout != 0 {
		stmt := fmt.Sprintf(`SET statement_timeout = '%s'`, cliCtx.cmdTimeout)
		if err := sqlConn.Exec(maybePrint(stmt), nil); err != nil {
			return err
		}
	}

	traceID, err := getJobTraceID(sqlConn, jobID)
	if err != nil {
		return err
	}

	var inflightSpanQuery = `
WITH spans AS(
SELECT span_id, goroutine_id, operation, start_time, duration
FROM crdb_internal.node_inflight_trace_spans 
WHERE trace_id=$1
) SELECT *
FROM spans, LATERAL crdb_internal.payloads_for_span(spans.span_id)`

	var f *os.File
	if f, err = os.Create(traceFilePath); err != nil {
		return err
	}
	defer f.Close()

	return runQueryAndFormatResults(sqlConn, f, makeQuery(inflightSpanQuery, traceID))
}
