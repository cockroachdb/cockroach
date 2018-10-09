// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []sqlbase.ColumnType
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(
	types []sqlbase.ColumnType, rows sqlbase.EncDatumRows,
) *RepeatableRowSource {
	if types == nil {
		panic("types required")
	}
	return &RepeatableRowSource{rows: rows, types: types}
}

// OutputTypes is part of the RowSource interface.
func (r *RepeatableRowSource) OutputTypes() []sqlbase.ColumnType {
	return r.types
}

// Start is part of the RowSource interface.
func (r *RepeatableRowSource) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end.
	if r.nextRowIdx >= len(r.rows) {
		return nil, nil
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, nil
}

// Reset resets the RepeatableRowSource such that a subsequent call to Next()
// returns the first row.
func (r *RepeatableRowSource) Reset() {
	r.nextRowIdx = 0
}

// ConsumerDone is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerClosed() {}

// RowDisposer is a RowReceiver that discards any rows Push()ed.
type RowDisposer struct{}

var _ RowReceiver = &RowDisposer{}

// Push is part of the RowReceiver interface.
func (r *RowDisposer) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
	return NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *RowDisposer) ProducerDone() {}

// NextNoMeta is a version of Next which fails the test if
// it encounters any metadata.
func (rb *RowBuffer) NextNoMeta(tb testing.TB) sqlbase.EncDatumRow {
	row, meta := rb.Next()
	if meta != nil {
		tb.Fatalf("unexpected metadata: %v", meta)
	}
	return row
}

// GetRowsNoMeta returns the rows in the buffer; it fails the test if it
// encounters any metadata.
func (rb *RowBuffer) GetRowsNoMeta(t *testing.T) sqlbase.EncDatumRows {
	var res sqlbase.EncDatumRows
	for {
		row := rb.NextNoMeta(t)
		if row == nil {
			break
		}
		res = append(res, row)
	}
	return res
}

var (
	intType      = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	boolType     = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}
	decType      = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}
	strType      = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}
	oneIntCol    = []sqlbase.ColumnType{intType}
	twoIntCols   = []sqlbase.ColumnType{intType, intType}
	threeIntCols = []sqlbase.ColumnType{intType, intType, intType}
)

func makeIntCols(numCols int) []sqlbase.ColumnType {
	ret := make([]sqlbase.ColumnType, numCols)
	for i := 0; i < numCols; i++ {
		ret[i] = intType
	}
	return ret
}

func intEncDatum(i int) sqlbase.EncDatum {
	return sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
}

func strEncDatum(s string) sqlbase.EncDatum {
	return sqlbase.EncDatum{Datum: tree.NewDString(s)}
}

func nullEncDatum() sqlbase.EncDatum {
	return sqlbase.EncDatum{Datum: tree.DNull}
}

// genEncDatumRowsInt converts rows of ints to rows of EncDatum DInts.
// If an int is negative, the corresponding value is NULL.
func genEncDatumRowsInt(inputRows [][]int) sqlbase.EncDatumRows {
	rows := make(sqlbase.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			if x < 0 {
				rows[i] = append(rows[i], nullEncDatum())
			} else {
				rows[i] = append(rows[i], intEncDatum(x))
			}
		}
	}
	return rows
}

// startMockDistSQLServer starts a MockDistSQLServer and returns the address on
// which it's listening.
func startMockDistSQLServer(stopper *stop.Stopper) (*MockDistSQLServer, net.Addr, error) {
	rpcContext := newInsecureRPCContext(stopper)
	server := rpc.NewServer(rpcContext)
	mock := newMockDistSQLServer()
	RegisterDistSQLServer(server, mock)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
	if err != nil {
		return nil, nil, err
	}
	return mock, ln.Addr(), nil
}

func newInsecureRPCContext(stopper *stop.Stopper) *rpc.Context {
	return rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
}

// MockDistSQLServer implements the DistSQLServer (gRPC) interface and allows
// clients to control the inbound streams.
type MockDistSQLServer struct {
	inboundStreams   chan InboundStreamNotification
	runSyncFlowCalls chan RunSyncFlowCall
}

// InboundStreamNotification is the MockDistSQLServer's way to tell its clients
// that a new gRPC call has arrived and thus a stream has arrived. The rpc
// handler is blocked until donec is signaled.
type InboundStreamNotification struct {
	stream DistSQL_FlowStreamServer
	donec  chan<- error
}

type RunSyncFlowCall struct {
	stream DistSQL_RunSyncFlowServer
	donec  chan<- error
}

// MockDistSQLServer implements the DistSQLServer interface.
var _ DistSQLServer = &MockDistSQLServer{}

func newMockDistSQLServer() *MockDistSQLServer {
	return &MockDistSQLServer{
		inboundStreams:   make(chan InboundStreamNotification),
		runSyncFlowCalls: make(chan RunSyncFlowCall),
	}
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) RunSyncFlow(stream DistSQL_RunSyncFlowServer) error {
	donec := make(chan error)
	ds.runSyncFlowCalls <- RunSyncFlowCall{stream: stream, donec: donec}
	return <-donec
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) SetupFlow(
	_ context.Context, req *SetupFlowRequest,
) (*SimpleResponse, error) {
	return nil, nil
}

// FlowStream is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) FlowStream(stream DistSQL_FlowStreamServer) error {
	donec := make(chan error)
	ds.inboundStreams <- InboundStreamNotification{stream: stream, donec: donec}
	return <-donec
}

// createDummyStream creates the server and client side of a FlowStream stream.
// This can be use by tests to pretend that then have received a FlowStream RPC.
// The stream can be used to send messages (ConsumerSignal's) on it (within a
// gRPC window limit since nobody's reading from the stream), for example
// Handshake messages.
//
// We do this by creating a mock server, dialing into it and capturing the
// server stream. The server-side RPC call will be blocked until the caller
// calls the returned cleanup function.
func createDummyStream() (
	serverStream DistSQL_FlowStreamServer,
	clientStream DistSQL_FlowStreamClient,
	cleanup func(),
	err error,
) {
	stopper := stop.NewStopper()
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		return nil, nil, nil, err
	}
	rpcCtx := newInsecureRPCContext(stopper)
	conn, err := rpcCtx.GRPCDial(addr.String()).Connect(context.Background())
	if err != nil {
		return nil, nil, nil, err
	}
	client := NewDistSQLClient(conn)
	clientStream, err = client.FlowStream(context.TODO())
	if err != nil {
		return nil, nil, nil, err
	}
	streamNotification := <-mockServer.inboundStreams
	serverStream = streamNotification.stream
	cleanup = func() {
		close(streamNotification.donec)
		stopper.Stop(context.TODO())
	}
	return serverStream, clientStream, cleanup, nil
}

// makeIntRows constructs a numRows x numCols table where rows[i][j] = i + j.
func makeIntRows(numRows, numCols int) sqlbase.EncDatumRows {
	rows := make(sqlbase.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(sqlbase.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = intEncDatum(i + j)
		}
	}
	return rows
}

// makeRandIntRows constructs a numRows x numCols table where the values are random.
func makeRandIntRows(rng *rand.Rand, numRows int, numCols int) sqlbase.EncDatumRows {
	rows := make(sqlbase.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(sqlbase.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = intEncDatum(rng.Int())
		}
	}
	return rows
}

// makeRepeatedIntRows constructs a numRows x numCols table where blocks of n
// consecutive rows have the same value.
func makeRepeatedIntRows(n int, numRows int, numCols int) sqlbase.EncDatumRows {
	rows := make(sqlbase.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(sqlbase.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = intEncDatum(i/n + j)
		}
	}
	return rows
}

// runProcessorTest instantiates a processor with the provided spec, runs it
// with the given inputs, and asserts that the outputted rows are as expected.
func runProcessorTest(
	t *testing.T,
	core ProcessorCoreUnion,
	post PostProcessSpec,
	inputTypes []sqlbase.ColumnType,
	inputRows sqlbase.EncDatumRows,
	outputTypes []sqlbase.ColumnType,
	expected sqlbase.EncDatumRows,
	txn *client.Txn,
) {
	in := NewRowBuffer(inputTypes, inputRows, RowBufferArgs{})
	out := &RowBuffer{}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
		txn:      txn,
	}

	p, err := newProcessor(
		context.Background(), &flowCtx, 0 /* processorID */, &core, &post,
		[]RowSource{in}, []RowReceiver{out}, []LocalProcessor{})
	if err != nil {
		t.Fatal(err)
	}

	switch pt := p.(type) {
	case *joinReader:
		// Reduce batch size to exercise batching logic.
		pt.batchSize = 2
	case *indexJoiner:
		// Reduce batch size to exercise batching logic.
		pt.batchSize = 2
	}

	p.Run(context.Background(), nil /* wg */)
	if !out.ProducerClosed {
		t.Fatalf("output RowReceiver not closed")
	}
	var res sqlbase.EncDatumRows
	for {
		row := out.NextNoMeta(t).Copy()
		if row == nil {
			break
		}
		res = append(res, row)
	}

	if result := res.String(outputTypes); result != expected.String(outputTypes) {
		t.Errorf(
			"invalid results: %s, expected %s'", result, expected.String(outputTypes))
	}
}

type rowsAccessor interface {
	getRows() *diskBackedRowContainer
}

func (s *sorterBase) getRows() *diskBackedRowContainer {
	return s.rows.(*diskBackedRowContainer)
}
