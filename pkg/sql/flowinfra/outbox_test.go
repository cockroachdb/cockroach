// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// staticAddressResolver maps StaticNodeID to the given address.
func staticAddressResolver(addr net.Addr) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		if nodeID == execinfra.StaticNodeID {
			return addr, nil
		}
		return nil, errors.Errorf("node %d not found", nodeID)
	}
}

func TestOutbox(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a mock server that the outbox will connect and push rows to.
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	if err != nil {
		t.Fatal(err)
	}
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	clientRPC := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
		Cfg: &execinfra.ServerConfig{
			Settings:   st,
			Stopper:    stopper,
			NodeDialer: nodedialer.New(clientRPC, staticAddressResolver(addr)),
		},
		NodeID: base.TestingIDContainer,
	}
	streamID := execinfrapb.StreamID(42)
	outbox := flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)
	outbox.Init(types.OneIntCol)
	var outboxWG sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.Start(ctx, &outboxWG, cancel)

	// Start a producer. It will send one row 0, then send rows -1 until a drain
	// request is observed, then send row 2 and some metadata.
	producerC := make(chan error)
	go func() {
		producerC <- func() error {
			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(0))),
			}
			if consumerStatus := outbox.Push(row, nil /* meta */); consumerStatus != execinfra.NeedMoreRows {
				return errors.Errorf("expected status: %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}

			// Send rows until the drain request is observed.
			for {
				row = rowenc.EncDatumRow{
					rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(-1))),
				}
				consumerStatus := outbox.Push(row, nil /* meta */)
				if consumerStatus == execinfra.DrainRequested {
					break
				}
				if consumerStatus == execinfra.ConsumerClosed {
					return errors.Errorf("consumer closed prematurely")
				}
			}

			// Now send another row that the outbox will discard.
			row = rowenc.EncDatumRow{rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(2)))}
			if consumerStatus := outbox.Push(row, nil /* meta */); consumerStatus != execinfra.DrainRequested {
				return errors.Errorf("expected status: %d, got: %d", execinfra.NeedMoreRows, consumerStatus)
			}

			// Send some metadata.
			outbox.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.Errorf("meta 0")})
			outbox.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: errors.Errorf("meta 1")})
			// Send the termination signal.
			outbox.ProducerDone()

			return nil
		}()
	}()

	// Wait for the outbox to connect the stream.
	streamNotification := <-mockServer.InboundStreams
	serverStream := streamNotification.Stream

	// Consume everything that the outbox sends on the stream.
	var decoder flowinfra.StreamDecoder
	var rows rowenc.EncDatumRows
	var metas []execinfrapb.ProducerMetadata
	drainSignalSent := false
	for {
		msg, err := serverStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(context.Background(), msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
		// Eliminate the "-1" rows, that were sent before the producer found out
		// about the draining.
		last := -1
		for i := 0; i < len(rows); i++ {
			if rows[i].String(types.OneIntCol) != "[-1]" {
				last = i
				continue
			}
			for j := i; j < len(rows); j++ {
				if rows[j].String(types.OneIntCol) == "[-1]" {
					continue
				}
				rows[i] = rows[j]
				i = j
				last = j
				break
			}
		}
		rows = rows[0 : last+1]

		// After we receive one row, we're going to ask the producer to drain.
		if !drainSignalSent && len(rows) > 0 {
			sig := execinfrapb.ConsumerSignal{DrainRequest: &execinfrapb.DrainRequest{}}
			if err := serverStream.Send(&sig); err != nil {
				t.Fatal(err)
			}
			drainSignalSent = true
		}
	}
	if err := <-producerC; err != nil {
		t.Fatalf("%+v", err)
	}

	if len(metas) != 2 {
		t.Fatalf("expected 2 metadata records, got: %d", len(metas))
	}
	for i, m := range metas {
		expectedStr := fmt.Sprintf("meta %d", i)
		if !testutils.IsError(m.Err, expectedStr) {
			t.Fatalf("expected: %q, got: %q", expectedStr, m.Err.Error())
		}
	}
	str := rows.String(types.OneIntCol)
	expected := "[[0]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// The outbox should shut down since the producer closed.
	outboxWG.Wait()
	// Signal the server to shut down the stream.
	streamNotification.Donec <- nil
}

// Test that an outbox connects its stream as soon as possible (i.e. before
// receiving any rows). This is important, since there's a timeout on waiting on
// the server-side for the streams to be connected.
func TestOutboxInitializesStreamBeforeReceivingAnyRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	if err != nil {
		t.Fatal(err)
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	clientRPC := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
		Cfg: &execinfra.ServerConfig{
			Settings:   st,
			Stopper:    stopper,
			NodeDialer: nodedialer.New(clientRPC, staticAddressResolver(addr)),
		},
		NodeID: base.TestingIDContainer,
	}
	streamID := execinfrapb.StreamID(42)
	outbox := flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)

	var outboxWG sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outbox.Init(types.OneIntCol)
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.Start(ctx, &outboxWG, cancel)

	streamNotification := <-mockServer.InboundStreams
	serverStream := streamNotification.Stream
	producerMsg, err := serverStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if producerMsg.Header == nil {
		t.Fatal("missing header")
	}
	if producerMsg.Header.FlowID != flowCtx.ID || producerMsg.Header.StreamID != streamID {
		t.Fatalf("wrong header: %v", producerMsg)
	}

	// Signal the server to shut down the stream. This should also prompt the
	// outbox (the client) to terminate its loop.
	streamNotification.Donec <- nil
	outboxWG.Wait()
}

// Test that the outbox responds to the consumer shutting down in an unexpected
// way by closing.
func TestOutboxClosesWhenConsumerCloses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		// Indicates whether the consumer (i.e. the server) returns an error
		// from running the flow. This error will be translated into a grpc
		// error received by the client (i.e. the outbox) in its stream.Recv())
		// call. Otherwise, the client doesn't return an error (and the outbox
		// should receive io.EOF).
		serverReturnsError bool
	}{
		{serverReturnsError: false},
		{serverReturnsError: true},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
			if err != nil {
				t.Fatal(err)
			}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())

			clientRPC := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
				Cfg: &execinfra.ServerConfig{
					Settings:   st,
					Stopper:    stopper,
					NodeDialer: nodedialer.New(clientRPC, staticAddressResolver(addr)),
				},
				NodeID: base.TestingIDContainer,
			}
			streamID := execinfrapb.StreamID(42)
			var outbox *flowinfra.Outbox
			var wg sync.WaitGroup
			var expectedErr error
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			outbox = flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)
			outbox.Init(types.OneIntCol)
			outbox.Start(ctx, &wg, cancel)

			// Wait for the outbox to connect the stream.
			streamNotification := <-mockServer.InboundStreams
			// Wait for the consumer to receive the header message that the
			// outbox sends on start. If we don't wait, the consumer returning
			// from the FlowStream() RPC races with the outbox sending the
			// header msg and the send might get an io.EOF error.
			if _, err := streamNotification.Stream.Recv(); err != nil {
				t.Errorf("expected err: %q, got %v", expectedErr, err)
			}

			// Have the server return from the FlowStream call. This should prompt the
			// outbox to finish.
			if tc.serverReturnsError {
				expectedErr = errors.Errorf("FlowStream server error")
			} else {
				expectedErr = nil
			}
			streamNotification.Donec <- expectedErr

			wg.Wait()
			if expectedErr == nil {
				if outbox.Err() != nil {
					t.Fatalf("unexpected outbox.err: %s", outbox.Err())
				}
			} else {
				// We use error string comparison because we actually expect a grpc
				// error wrapping the expected error.
				if !testutils.IsError(outbox.Err(), expectedErr.Error()) {
					t.Fatalf("expected err: %q, got %v", expectedErr, outbox.Err())
				}
			}
		})
	}
}

// Test Outbox cancels flow context when FlowStream returns a non-nil error.
func TestOutboxCancelsFlowOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	if err != nil {
		t.Fatal(err)
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	clientRPC := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
		Cfg: &execinfra.ServerConfig{
			Settings:   st,
			Stopper:    stopper,
			NodeDialer: nodedialer.New(clientRPC, staticAddressResolver(addr)),
		},
		NodeID: base.TestingIDContainer,
	}
	streamID := execinfrapb.StreamID(42)
	var outbox *flowinfra.Outbox
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We could test this on ctx.cancel(), but this mock
	// cancellation method is simpler.
	ctxCanceled := false
	mockCancel := func() {
		ctxCanceled = true
	}

	outbox = flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)
	outbox.Init(types.OneIntCol)
	outbox.Start(ctx, &wg, mockCancel)

	// Wait for the outbox to connect the stream.
	streamNotification := <-mockServer.InboundStreams
	if _, err := streamNotification.Stream.Recv(); err != nil {
		t.Fatal(err)
	}

	streamNotification.Donec <- cancelchecker.QueryCanceledError

	wg.Wait()
	if !ctxCanceled {
		t.Fatal("flow ctx was not canceled")
	}
}

// Test that the outbox unblocks its producers if it fails to connect during
// startup.
func TestOutboxUnblocksProducers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
		Cfg: &execinfra.ServerConfig{
			Settings: st,
			Stopper:  stopper,
			// a nil nodeDialer will always fail to connect.
			NodeDialer: nil,
		},
		NodeID: base.TestingIDContainer,
	}
	streamID := execinfrapb.StreamID(42)
	var outbox *flowinfra.Outbox
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	outbox = flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)
	outbox.Init(types.OneIntCol)

	// Fill up the outbox.
	for i := 0; i < flowinfra.OutboxBufRows; i++ {
		outbox.Push(nil, &execinfrapb.ProducerMetadata{})
	}

	var blockedPusherWg sync.WaitGroup
	blockedPusherWg.Add(1)
	go func() {
		// Push to the outbox one last time, which will block since the channel
		// is full.
		outbox.Push(nil, &execinfrapb.ProducerMetadata{})
		// We should become unblocked once outbox.Start fails.
		blockedPusherWg.Done()
	}()

	// This outbox will fail to connect, because it has a nil nodeDialer.
	outbox.Start(ctx, &wg, cancel)

	wg.Wait()
	// Also, make sure that pushing to the outbox after its failed shows that
	// it's been correctly ConsumerClosed.
	status := outbox.Push(nil, &execinfrapb.ProducerMetadata{})
	if status != execinfra.ConsumerClosed {
		t.Fatalf("expected status=ConsumerClosed, got %s", status)
	}

	blockedPusherWg.Wait()
}

func BenchmarkOutbox(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Create a mock server that the outbox will connect and push rows to.
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	if err != nil {
		b.Fatal(err)
	}
	st := cluster.MakeTestingClusterSettings()
	for _, numCols := range []int{1, 2, 4, 8} {
		row := rowenc.EncDatumRow{}
		for i := 0; i < numCols; i++ {
			row = append(row, rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(2))))
		}
		b.Run(fmt.Sprintf("numCols=%d", numCols), func(b *testing.B) {
			streamID := execinfrapb.StreamID(42)
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())

			clientRPC := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				ID:      execinfrapb.FlowID{UUID: uuid.MakeV4()},
				Cfg: &execinfra.ServerConfig{
					Settings:   st,
					Stopper:    stopper,
					NodeDialer: nodedialer.New(clientRPC, staticAddressResolver(addr)),
				},
				NodeID: base.TestingIDContainer,
			}
			outbox := flowinfra.NewOutbox(&flowCtx, execinfra.StaticNodeID, streamID, nil /* numOutboxes */, false /* isGatewayNode */)
			outbox.Init(types.MakeIntCols(numCols))
			var outboxWG sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Start the outbox. This should cause the stream to connect, even though
			// we're not sending any rows.
			outbox.Start(ctx, &outboxWG, cancel)

			// Wait for the outbox to connect the stream.
			streamNotification := <-mockServer.InboundStreams
			serverStream := streamNotification.Stream
			go func() {
				for {
					_, err := serverStream.Recv()
					if err != nil {
						break
					}
				}
			}()

			b.SetBytes(int64(numCols * 8))
			for i := 0; i < b.N; i++ {
				if err := outbox.AddRow(ctx, row, nil); err != nil {
					b.Fatal(err)
				}
			}
			outbox.ProducerDone()
			outboxWG.Wait()
			streamNotification.Donec <- nil
		})
	}
}
