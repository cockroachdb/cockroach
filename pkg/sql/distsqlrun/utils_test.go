// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

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
	serverStream execinfrapb.DistSQL_FlowStreamServer,
	clientStream execinfrapb.DistSQL_FlowStreamClient,
	cleanup func(),
	err error,
) {
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, staticNodeID)
	if err != nil {
		return nil, nil, nil, err
	}

	rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	conn, err := rpcContext.GRPCDialNode(addr.String(), staticNodeID,
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		return nil, nil, nil, err
	}
	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err = client.FlowStream(context.TODO())
	if err != nil {
		return nil, nil, nil, err
	}
	streamNotification := <-mockServer.InboundStreams
	serverStream = streamNotification.Stream
	cleanup = func() {
		close(streamNotification.Donec)
		stopper.Stop(context.TODO())
	}
	return serverStream, clientStream, cleanup, nil
}

// runProcessorTest instantiates a processor with the provided spec, runs it
// with the given inputs, and asserts that the outputted rows are as expected.
func runProcessorTest(
	t *testing.T,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	inputTypes []types.T,
	inputRows sqlbase.EncDatumRows,
	outputTypes []types.T,
	expected sqlbase.EncDatumRows,
	txn *client.Txn,
) {
	in := newRowBuffer(inputTypes, inputRows, rowBufferArgs{})
	out := &RowBuffer{}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := distsql.FlowCtx{
		Cfg:     &distsql.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Txn:     txn,
	}

	p, err := newProcessor(
		context.Background(), &flowCtx, 0 /* processorID */, &core, &post,
		[]distsql.RowSource{in}, []distsql.RowReceiver{out}, []distsql.LocalProcessor{})
	if err != nil {
		t.Fatal(err)
	}

	switch pt := p.(type) {
	case *distsql.JoinReader:
		// Reduce batch size to exercise batching logic.
		pt.SetBatchSize(2 /* batchSize */)
	case *distsql.IndexJoiner:
		//	Reduce batch size to exercise batching logic.
		pt.SetBatchSize(2 /* batchSize */)
	}

	p.Run(context.Background())
	if !out.ProducerClosed() {
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
	getRows() *rowcontainer.DiskBackedRowContainer
}

func (s *sorterBase) getRows() *rowcontainer.DiskBackedRowContainer {
	return s.rows.(*rowcontainer.DiskBackedRowContainer)
}

// rowDisposer is a distsql.RowReceiver that discards any rows Push()ed.
type rowDisposer struct{}

var _ distsql.RowReceiver = &rowDisposer{}

// Push is part of the distsql.RowReceiver interface.
func (r *rowDisposer) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) distsql.ConsumerStatus {
	return distsql.NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *rowDisposer) ProducerDone() {}

// Types is part of the RowReceiver interface.
func (r *rowDisposer) Types() []types.T {
	return nil
}
