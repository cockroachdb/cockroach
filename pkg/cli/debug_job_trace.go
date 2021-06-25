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
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugJobTraceFromClusterCmd = &cobra.Command{
	Use:   "job-trace <job_id> --url=<cluster connection string>",
	Short: "get the trace payloads for the executing job",
	Args:  cobra.MinimumNArgs(1),
	RunE:  MaybeDecorateGRPCError(runDebugJobTrace),
}

const jobTraceZipSuffix = "job-trace.zip"

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

	return constructJobTraceZipBundle(context.Background(), sqlConn, jobID)
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

type inflightTraceRow struct {
	nodeID     int64
	rootOpName string
	traceStr   string
	jaegerJSON string
}

// stitchJaegerJSON adds the trace spans from jaegerJSON into the
// nodeTraceCollection object, and returns a new cumulative
// tracing.TraceCollection object.
func stitchJaegerJSON(
	nodeTraceCollection *tracing.TraceCollection, jaegerJSON string,
) (*tracing.TraceCollection, error) {
	var cumulativeTraceCollection *tracing.TraceCollection

	// Unmarshal the jaegerJSON string to a TraceCollection.
	var curTraceCollection tracing.TraceCollection
	if err := json.Unmarshal([]byte(jaegerJSON), &curTraceCollection); err != nil {
		return cumulativeTraceCollection, err
	}

	// Sanity check that the TraceCollection has a single trace entry.
	if len(curTraceCollection.Data) != 1 {
		return cumulativeTraceCollection, errors.AssertionFailedf("expected a single trace but found %d",
			len(curTraceCollection.Data))
	}

	// Check if this is the first entry to be stitched.
	if nodeTraceCollection == nil {
		cumulativeTraceCollection = &curTraceCollection
		return cumulativeTraceCollection, nil
	}
	cumulativeTraceCollection = nodeTraceCollection

	// Sanity check that the TraceID of the new and cumulative TraceCollections is
	// the same.
	if cumulativeTraceCollection.Data[0].TraceID != curTraceCollection.Data[0].TraceID {
		return cumulativeTraceCollection, errors.AssertionFailedf(
			"expected traceID of nodeTrace: %s and curTrace: %s to be equal",
			cumulativeTraceCollection.Data[0].TraceID, curTraceCollection.Data[0].TraceID)
	}

	// Add spans from the curTraceCollection to the nodeTraceCollection.
	cumulativeTraceCollection.Data[0].Spans = append(cumulativeTraceCollection.Data[0].Spans,
		curTraceCollection.Data[0].Spans...)

	return cumulativeTraceCollection, nil
}

func populateInflightTraceRow(vals []driver.Value) (inflightTraceRow, error) {
	var row inflightTraceRow
	if len(vals) != 4 {
		return row, errors.AssertionFailedf("expected vals to have 4 values but found %d", len(vals))
	}

	if id, ok := vals[0].(int64); ok {
		row.nodeID = id
	} else {
		return row, errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
	}

	if rootOpName, ok := vals[1].(string); ok {
		row.rootOpName = rootOpName
	} else {
		return row, errors.Errorf("unexpected value: %T of %v", vals[1], vals[1])
	}

	if traceStr, ok := vals[2].(string); ok {
		row.traceStr = traceStr
	} else {
		return row, errors.Errorf("unexpected value: %T of %v", vals[2], vals[2])
	}

	if jaegerJSON, ok := vals[3].(string); ok {
		row.jaegerJSON = jaegerJSON
	} else {
		return row, errors.Errorf("unexpected value: %T of %v", vals[3], vals[3])
	}
	return row, nil
}

func constructJobTraceZipBundle(ctx context.Context, sqlConn *sqlConn, jobID int64) error {
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

	// Initialize a zipper that will contain all the trace files.
	z := &sql.MemZipper{}
	z.Init()

	constructFilename := func(nodeID int64, suffix string) string {
		return fmt.Sprintf("node%d-%s", nodeID, suffix)
	}

	var inflightTracesQuery = `
SELECT node_id, root_op_name, trace_str, jaeger_json FROM crdb_internal.cluster_inflight_traces WHERE trace_id=$1 ORDER BY node_id
`
	rows, err := sqlConn.Query(inflightTracesQuery, []driver.Value{traceID})
	if err != nil {
		return err
	}
	vals := make([]driver.Value, 4)
	var traceStrBuf bytes.Buffer
	var nodeTraceCollection *tracing.TraceCollection
	flushAndReset := func(ctx context.Context, nodeID int64) {
		z.AddFile(constructFilename(nodeID, "trace.txt"), traceStrBuf.String())

		// Marshal the jaeger TraceCollection before writing it to a file.
		if nodeTraceCollection != nil {
			json, err := json.MarshalIndent(*nodeTraceCollection, "" /* prefix */, "\t" /* indent */)
			if err != nil {
				log.Infof(ctx, "error while marshaling jaeger json %v", err)
				return
			}
			z.AddFile(constructFilename(nodeID, "jaeger.json"), string(json))
		}
		traceStrBuf.Reset()
		nodeTraceCollection = nil
	}

	var prevNodeID int64
	isFirstRow := true
	for {
		var err error
		if err = rows.Next(vals); err == io.EOF {
			flushAndReset(ctx, prevNodeID)
			break
		}
		if err != nil {
			return err
		}

		row, err := populateInflightTraceRow(vals)
		if err != nil {
			return err
		}

		if isFirstRow {
			prevNodeID = row.nodeID
			isFirstRow = false
		}

		// If the nodeID is the same as that seen in the previous row, then continue
		// to buffer in the same file.
		if row.nodeID != prevNodeID {
			// If the nodeID is different from that seen in the previous row, create
			// new files in the zip bundle to hold information for this node.
			flushAndReset(ctx, prevNodeID)
		}

		// If we are reading another row (tracing.Recording) from the same node as
		// prevNodeID then we want to stitch the JaegerJSON into the existing
		// JaegerJSON object for this node. This allows us to output a per node
		// Jaeger file that can easily be imported into JaegerUI.
		//
		// It is safe to do this since the tracing.Recording returned as rows are
		// sorted by the StartTime of the root span, and so appending to the
		// existing JaegerJSON will maintain the chronological order of the traces.
		//
		// In practice, it is more useful to view all the Jaeger tracing.Recordings
		// on a node for a given TraceID in a single view, rather than having to
		// generate different Jaeger files for each tracing.Recording, and going
		// through the hassle of importing each one and toggling through the tabs.
		if nodeTraceCollection, err = stitchJaegerJSON(nodeTraceCollection, row.jaegerJSON); err != nil {
			return err
		}

		_, err = traceStrBuf.WriteString(fmt.Sprintf("\n\n-- Root Operation: %s --\n\n", row.rootOpName))
		if err != nil {
			return err
		}
		_, err = traceStrBuf.WriteString(row.traceStr)
		if err != nil {
			return err
		}

		prevNodeID = row.nodeID
	}

	buf, err := z.Finalize()
	if err != nil {
		return err
	}

	var f *os.File
	filename := fmt.Sprintf("%d-%s", jobID, jobTraceZipSuffix)
	if f, err = os.Create(filename); err != nil {
		return err
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}
