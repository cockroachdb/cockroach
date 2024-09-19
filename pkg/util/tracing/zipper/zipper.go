// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package zipper

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

var inflightTracesQuery = `
SELECT node_id, root_op_name, trace_str, jaeger_json FROM crdb_internal.cluster_inflight_traces WHERE trace_id=%d ORDER BY node_id
`

type inflightTraceRow struct {
	nodeID     int64
	rootOpName string
	traceStr   string
	jaegerJSON string
}

// InflightTraceZipper provides a method to generate a trace zip containing
// per-node traces for all inflight trace spans of a particular traceID.
type InflightTraceZipper interface {
	getNodeTraceCollection() *tracingpb.TraceCollection
	getTraceStrBuffer() *bytes.Buffer
	getZipper() *memzipper.Zipper
	reset()

	Zip(context.Context, int64) ([]byte, error)
}

// InternalInflightTraceZipper is the InflightTraceZipper which uses an internal
// SQL connection to collect cluster wide traces.
type InternalInflightTraceZipper struct {
	traceStrBuf         *bytes.Buffer
	nodeTraceCollection *tracingpb.TraceCollection
	ie                  isql.Executor
	z                   *memzipper.Zipper
}

func (i *InternalInflightTraceZipper) getNodeTraceCollection() *tracingpb.TraceCollection {
	return i.nodeTraceCollection
}

func (i *InternalInflightTraceZipper) getTraceStrBuffer() *bytes.Buffer {
	return i.traceStrBuf
}

func (i *InternalInflightTraceZipper) getZipper() *memzipper.Zipper {
	return i.z
}

// Zip implements the InflightTraceZipper interface.
//
// Zip uses crdb_internal.cluster_inflight_traces to collect inflight trace
// spans for traceID, from all nodes in the cluster. It converts the recordings
// into text, and jaegerJSON formats before creating a zip with per-node trace
// files.
func (i *InternalInflightTraceZipper) Zip(
	ctx context.Context, traceID int64,
) (zipBytes []byte, retErr error) {
	it, err := i.ie.QueryIterator(ctx, "internal-zipper", nil, fmt.Sprintf(inflightTracesQuery, traceID))
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()
	var prevNodeID int64
	isFirstRow := true
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		traceRow, err := i.populateInflightTraceRow(row)
		if err != nil {
			return nil, err
		}

		if isFirstRow {
			prevNodeID = traceRow.nodeID
			isFirstRow = false
		}

		// If the nodeID is the same as that seen in the previous row, then continue
		// to buffer in the same file.
		if traceRow.nodeID != prevNodeID {
			// If the nodeID is different from that seen in the previous row, create
			// new files in the zip bundle to hold information for this node.
			flushAndReset(ctx, prevNodeID, i)
		}

		// If we are reading another row (tracingpb.Recording) from the same node as
		// prevNodeID then we want to stitch the JaegerJSON into the existing
		// JaegerJSON object for this node. This allows us to output a per node
		// Jaeger file that can easily be imported into JaegerUI.
		//
		// It is safe to do this since the tracingpb.Recording returned as rows are
		// sorted by the StartTime of the root span, and so appending to the
		// existing JaegerJSON will maintain the chronological order of the traces.
		//
		// In practice, it is more useful to view all the Jaeger tracing.Recordings
		// on a node for a given TraceID in a single view, rather than having to
		// generate different Jaeger files for each tracingpb.Recording, and going
		// through the hassle of importing each one and toggling through the tabs.
		if i.nodeTraceCollection, err = stitchJaegerJSON(i.nodeTraceCollection, traceRow.jaegerJSON); err != nil {
			return nil, err
		}

		_, err = i.traceStrBuf.WriteString(fmt.Sprintf("\n\n-- Root Operation: %s --\n\n", traceRow.rootOpName))
		if err != nil {
			return nil, err
		}
		_, err = i.traceStrBuf.WriteString(traceRow.traceStr)
		if err != nil {
			return nil, err
		}

		prevNodeID = traceRow.nodeID
	}
	if err != nil {
		return nil, err
	}
	flushAndReset(ctx, prevNodeID, i)
	buf, err := i.z.Finalize()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (i *InternalInflightTraceZipper) reset() {
	i.nodeTraceCollection = nil
	i.traceStrBuf.Reset()
}

func (i *InternalInflightTraceZipper) populateInflightTraceRow(
	row tree.Datums,
) (inflightTraceRow, error) {
	var traceRow inflightTraceRow
	if len(row) != 4 {
		return traceRow, errors.AssertionFailedf("expected vals to have 4 values but found %d", len(row))
	}

	if id, ok := row[0].(*tree.DInt); ok {
		traceRow.nodeID = int64(*id)
	} else {
		return traceRow, errors.Errorf("unexpected value: %T of %v", row[0], row[0])
	}

	if rootOpName, ok := row[1].(*tree.DString); ok {
		traceRow.rootOpName = string(*rootOpName)
	} else {
		return traceRow, errors.Errorf("unexpected value: %T of %v", row[1], row[1])
	}

	if traceStr, ok := row[2].(*tree.DString); ok {
		traceRow.traceStr = string(*traceStr)
	} else {
		return traceRow, errors.Errorf("unexpected value: %T of %v", row[2], row[2])
	}

	if jaegerJSON, ok := row[3].(*tree.DString); ok {
		traceRow.jaegerJSON = string(*jaegerJSON)
	} else {
		return traceRow, errors.Errorf("unexpected value: %T of %v", row[3], row[3])
	}
	return traceRow, nil
}

// MakeInternalExecutorInflightTraceZipper returns an instance of
// InternalInflightTraceZipper.
func MakeInternalExecutorInflightTraceZipper(ie isql.Executor) *InternalInflightTraceZipper {
	t := &InternalInflightTraceZipper{
		traceStrBuf:         &bytes.Buffer{},
		nodeTraceCollection: nil,
		ie:                  ie,
	}
	t.z = &memzipper.Zipper{}
	t.z.Init()
	return t
}

var _ InflightTraceZipper = &InternalInflightTraceZipper{}

type queryI interface {
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
}

// SQLConnInflightTraceZipper is the InflightTraceZipper which uses a network
// backed SQL connection to collect cluster wide traces.
type SQLConnInflightTraceZipper struct {
	traceStrBuf         *bytes.Buffer
	nodeTraceCollection *tracingpb.TraceCollection
	z                   *memzipper.Zipper
	sqlConn             queryI
}

func (s *SQLConnInflightTraceZipper) getNodeTraceCollection() *tracingpb.TraceCollection {
	return s.nodeTraceCollection
}

func (s *SQLConnInflightTraceZipper) getTraceStrBuffer() *bytes.Buffer {
	return s.traceStrBuf
}

func (s *SQLConnInflightTraceZipper) getZipper() *memzipper.Zipper {
	return s.z
}

func (s *SQLConnInflightTraceZipper) reset() {
	s.nodeTraceCollection = nil
	s.traceStrBuf.Reset()
}

// Zip implements the InflightTraceZipper interface.
//
// Zip uses crdb_internal.cluster_inflight_traces to collect inflight trace
// spans for traceID from all nodes in the cluster. It converts the recordings
// into text, and jaegerJSON formats before creating a zip with per-node trace
// files.
func (s *SQLConnInflightTraceZipper) Zip(ctx context.Context, traceID int64) ([]byte, error) {
	rows, err := s.sqlConn.Query(ctx, fmt.Sprintf(inflightTracesQuery, traceID))
	if err != nil {
		return nil, err
	}
	vals := make([]driver.Value, 4)
	var prevNodeID int64
	isFirstRow := true
	for {
		var err error
		if err = rows.Next(vals); err == io.EOF {
			flushAndReset(ctx, prevNodeID, s)
			break
		}
		if err != nil {
			return nil, err
		}

		row, err := s.populateInflightTraceRow(vals)
		if err != nil {
			return nil, err
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
			flushAndReset(ctx, prevNodeID, s)
		}

		// If we are reading another row (tracingpb.Recording) from the same node as
		// prevNodeID then we want to stitch the JaegerJSON into the existing
		// JaegerJSON object for this node. This allows us to output a per node
		// Jaeger file that can easily be imported into JaegerUI.
		//
		// It is safe to do this since the tracingpb.Recording returned as rows are
		// sorted by the StartTime of the root span, and so appending to the
		// existing JaegerJSON will maintain the chronological order of the traces.
		//
		// In practice, it is more useful to view all the Jaeger tracing.Recordings
		// on a node for a given TraceID in a single view, rather than having to
		// generate different Jaeger files for each tracingpb.Recording, and going
		// through the hassle of importing each one and toggling through the tabs.
		if s.nodeTraceCollection, err = stitchJaegerJSON(s.nodeTraceCollection, row.jaegerJSON); err != nil {
			return nil, err
		}

		_, err = s.traceStrBuf.WriteString(fmt.Sprintf("\n\n-- Root Operation: %s --\n\n", row.rootOpName))
		if err != nil {
			return nil, err
		}
		_, err = s.traceStrBuf.WriteString(row.traceStr)
		if err != nil {
			return nil, err
		}

		prevNodeID = row.nodeID
	}

	buf, err := s.z.Finalize()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *SQLConnInflightTraceZipper) populateInflightTraceRow(
	vals []driver.Value,
) (inflightTraceRow, error) {
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

// MakeSQLConnInflightTraceZipper returns an instance of
// SQLConnInflightTraceZipper.
func MakeSQLConnInflightTraceZipper(sqlConn queryI) *SQLConnInflightTraceZipper {
	t := &SQLConnInflightTraceZipper{
		traceStrBuf:         &bytes.Buffer{},
		nodeTraceCollection: nil,
		sqlConn:             sqlConn,
	}
	t.z = &memzipper.Zipper{}
	t.z.Init()
	return t
}

var _ InflightTraceZipper = &SQLConnInflightTraceZipper{}

func constructFilename(nodeID int64, suffix string) string {
	return fmt.Sprintf("node%d-%s", nodeID, suffix)
}

func flushAndReset(ctx context.Context, nodeID int64, t InflightTraceZipper) {
	z := t.getZipper()
	traceStrBuf := t.getTraceStrBuffer()
	nodeTraceCollection := t.getNodeTraceCollection()
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
	t.reset()
}

// stitchJaegerJSON adds the trace spans from jaegerJSON into the
// nodeTraceCollection object, and returns a new cumulative
// tracing.TraceCollection object.
func stitchJaegerJSON(
	nodeTraceCollection *tracingpb.TraceCollection, jaegerJSON string,
) (*tracingpb.TraceCollection, error) {
	var cumulativeTraceCollection *tracingpb.TraceCollection

	// Unmarshal the jaegerJSON string to a TraceCollection.
	var curTraceCollection tracingpb.TraceCollection
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
