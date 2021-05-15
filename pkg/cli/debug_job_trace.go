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
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const jobTraceDir = "job-trace"

var debugJobTraceFromClusterCmd = &cobra.Command{
	Use:   "job-trace <job_id> <file_name> --url=<cluster connection string>",
	Short: "get the trace payloads for the executing job",
	Args:  cobra.MinimumNArgs(2),
	RunE: MaybeDecorateGRPCError(
		func(command *cobra.Command, args []string) error {
			sqlConn, err := makeSQLClient("cockroach doctor", useSystemDb)
			if err != nil {
				return errors.Wrap(err, "could not establish connection to cluster")
			}
			defer sqlConn.Close()

			if err = os.MkdirAll(jobTraceDir, 0755); err != nil {
				return err
			}
			traceJSONFile := filepath.Join(jobTraceDir, args[1])
			return writeTracePayloadJSON(sqlConn, args[0], traceJSONFile)
		}),
}

func writeTracePayloadJSON(sqlConn *sqlConn, jobIDStr string, traceFile string) error {
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

	jobID, err := strconv.Atoi(jobIDStr)
	if err != nil {
		return err
	}

	var jobQuery = fmt.Sprintf(`SELECT trace_id FROM crdb_internal.jobs WHERE job_id='%d'`, jobID)
	rows, err := sqlConn.Query(jobQuery, nil)
	if err != nil {
		return err
	}
	vals := make([]driver.Value, 1)
	for {
		var err error
		if err = rows.Next(vals); err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if vals[0] == nil {
		return errors.Newf("no job entry found for %d", jobID)
	}
	traceID, ok := vals[0].(int64)
	if !ok {
		return errors.New("failed to parse traceID")
	}

	var query = fmt.Sprintf(`WITH spans AS(
									SELECT span_id, goroutine_id, operation, start_time, duration
  	 							FROM crdb_internal.node_inflight_trace_spans
 		 							WHERE trace_id = %d
									) SELECT * 
										FROM spans, LATERAL crdb_internal.payloads_for_span(spans.span_id)`, traceID)

	cols := []string{"span_id", "goroutine_id", "operation", "start_time", "duration", "payload"}

	var resRows [][]string
	spanRows, err := sqlConn.Query(query, nil)
	if err != nil {
		return errors.Wrapf(err, "query '%s'", query)
	}

	populateRow := func(vals []driver.Value) ([]string, error) {
		row := make([]string, 7)
		if spanID, ok := vals[0].(int64); ok {
			row[0] = strconv.Itoa(int(spanID))
		} else {
			return nil, errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
		}
		if gID, ok := vals[1].(int64); ok {
			row[1] = strconv.Itoa(int(gID))
		} else {
			return nil, errors.Errorf("unexpected value: %T of %v", vals[1], vals[1])
		}
		if opName, ok := vals[2].(string); ok {
			row[2] = opName
		} else {
			return nil, errors.Errorf("unexpected value: %T of %v", vals[2], vals[2])
		}
		if startTime, ok := vals[3].(time.Time); ok {
			row[3] = startTime.String()
		} else {
			return nil, errors.Errorf("unexpected startTime value: %T of %v", vals[3], vals[3])
		}
		if duration, ok := vals[4].([]byte); ok {
			durationDatum, err := tree.ParseDInterval(string(duration))
			if err != nil {
				return nil, err
			}
			row[4] = durationDatum.String()
		} else {
			return nil, errors.Errorf("unexpected duration value: %T of %v", vals[4], vals[4])
		}
		if payloadType, ok := vals[5].(string); ok {
			row[5] = payloadType
		} else {
			return nil, errors.Errorf("unexpected payloadType value: %T of %v", vals[5], vals[5])
		}
		if payload, ok := vals[6].([]byte); ok {
			jsonDatum, err := tree.ParseDJSON(string(payload))
			if err != nil {
				return nil, err
			}
			row[6] = jsonDatum.String()
		} else {
			return nil, errors.Errorf("unexpected payload value: %T of %v", vals[6], vals[6])
		}
		return row, nil
	}

	payloadVals := make([]driver.Value, 7)
	for {
		var err error
		if err = spanRows.Next(payloadVals); err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		r, err := populateRow(payloadVals)
		if err != nil {
			return err
		}
		resRows = append(resRows, r)
	}

	f, err := os.OpenFile(traceFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return PrintQueryOutput(f, cols, NewRowSliceIter(resRows, "ll"))
}
