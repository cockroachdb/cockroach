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
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestOutbox(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a mock server that the outbox will connect and push rows to.
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		stopper:  stopper,
		EvalCtx:  evalCtx,
		rpcCtx:   newInsecureRPCContext(stopper),
	}
	flowID := FlowID{uuid.MakeV4()}
	streamID := StreamID(42)
	outbox := newOutbox(&flowCtx, addr.String(), flowID, streamID)
	outbox.init(oneIntCol)
	var outboxWG sync.WaitGroup
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.start(ctx, &outboxWG, cancel)

	// Start a producer. It will send one row 0, then send rows -1 until a drain
	// request is observed, then send row 2 and some metadata.
	producerC := make(chan error)
	go func() {
		producerC <- func() error {
			row := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(intType, tree.NewDInt(tree.DInt(0))),
			}
			if consumerStatus := outbox.Push(row, nil /* meta */); consumerStatus != NeedMoreRows {
				return errors.Errorf("expected status: %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Send rows until the drain request is observed.
			for {
				row = sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(intType, tree.NewDInt(tree.DInt(-1))),
				}
				consumerStatus := outbox.Push(row, nil /* meta */)
				if consumerStatus == DrainRequested {
					break
				}
				if consumerStatus == ConsumerClosed {
					return errors.Errorf("consumer closed prematurely")
				}
			}

			// Now send another row that the outbox will discard.
			row = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(intType, tree.NewDInt(tree.DInt(2)))}
			if consumerStatus := outbox.Push(row, nil /* meta */); consumerStatus != DrainRequested {
				return errors.Errorf("expected status: %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Send some metadata.
			outbox.Push(nil /* row */, &ProducerMetadata{Err: errors.Errorf("meta 0")})
			outbox.Push(nil /* row */, &ProducerMetadata{Err: errors.Errorf("meta 1")})
			// Send the termination signal.
			outbox.ProducerDone()

			return nil
		}()
	}()

	// Wait for the outbox to connect the stream.
	streamNotification := <-mockServer.inboundStreams
	serverStream := streamNotification.stream

	// Consume everything that the outbox sends on the stream.
	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []ProducerMetadata
	drainSignalSent := false
	for {
		msg, err := serverStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
		// Eliminate the "-1" rows, that were sent before the producer found out
		// about the draining.
		last := -1
		for i := 0; i < len(rows); i++ {
			if rows[i].String(oneIntCol) != "[-1]" {
				last = i
				continue
			}
			for j := i; j < len(rows); j++ {
				if rows[j].String(oneIntCol) == "[-1]" {
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
			sig := ConsumerSignal{DrainRequest: &DrainRequest{}}
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
		if m.Err.Error() != expectedStr {
			t.Fatalf("expected: %q, got: %q", expectedStr, m.Err.Error())
		}
	}
	str := rows.String(oneIntCol)
	expected := "[[0]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// The outbox should shut down since the producer closed.
	outboxWG.Wait()
	// Signal the server to shut down the stream.
	streamNotification.donec <- nil
}

// Test that an outbox connects its stream as soon as possible (i.e. before
// receiving any rows). This is important, since there's a timeout on waiting on
// the server-side for the streams to be connected.
func TestOutboxInitializesStreamBeforeRecevingAnyRows(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		stopper:  stopper,
		EvalCtx:  evalCtx,
		rpcCtx:   newInsecureRPCContext(stopper),
	}
	flowID := FlowID{uuid.MakeV4()}
	streamID := StreamID(42)
	outbox := newOutbox(&flowCtx, addr.String(), flowID, streamID)

	var outboxWG sync.WaitGroup
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	outbox.init(oneIntCol)
	// Start the outbox. This should cause the stream to connect, even though
	// we're not sending any rows.
	outbox.start(ctx, &outboxWG, cancel)

	streamNotification := <-mockServer.inboundStreams
	serverStream := streamNotification.stream
	producerMsg, err := serverStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if producerMsg.Header == nil {
		t.Fatal("missing header")
	}
	if producerMsg.Header.FlowID != flowID || producerMsg.Header.StreamID != streamID {
		t.Fatalf("wrong header: %v", producerMsg)
	}

	// Signal the server to shut down the stream. This should also prompt the
	// outbox (the client) to terminate its loop.
	streamNotification.donec <- nil
	outboxWG.Wait()
}

// Test that the outbox responds to the consumer shutting down in an unexpected
// way by closing.
func TestOutboxClosesWhenConsumerCloses(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		// When set, the outbox will establish the stream with a FlowRpc call. When
		// not set, the consumer will establish the stream with RunSyncFlow.
		outboxIsClient bool
		// Only takes effect with outboxIsClient is set. When set, the consumer
		// (i.e. the server) returns an error from RunSyncFlow. This error will be
		// translated into a grpc error received by the client (i.e. the outbox) in
		// its stream.Recv()) call. Otherwise, the client doesn't return an error
		// (and the outbox should receive io.EOF).
		serverReturnsError bool
	}{
		{outboxIsClient: true, serverReturnsError: false},
		{outboxIsClient: true, serverReturnsError: true},
		{outboxIsClient: false},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			mockServer, addr, err := startMockDistSQLServer(stopper)
			if err != nil {
				t.Fatal(err)
			}

			evalCtx := tree.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Settings: cluster.MakeTestingClusterSettings(),
				stopper:  stopper,
				EvalCtx:  evalCtx,
				rpcCtx:   newInsecureRPCContext(stopper),
			}
			flowID := FlowID{uuid.MakeV4()}
			streamID := StreamID(42)
			var outbox *outbox
			var wg sync.WaitGroup
			var expectedErr error
			consumerReceivedMsg := make(chan struct{})
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			if tc.outboxIsClient {
				outbox = newOutbox(&flowCtx, addr.String(), flowID, streamID)
				outbox.init(oneIntCol)
				outbox.start(ctx, &wg, cancel)

				// Wait for the outbox to connect the stream.
				streamNotification := <-mockServer.inboundStreams
				// Wait for the consumer to receive the header message that the outbox
				// sends on start. If we don't wait, the consumer returning from the
				// FlowStream() RPC races with the outbox sending the header msg and the
				// send might get an io.EOF error.
				if _, err := streamNotification.stream.Recv(); err != nil {
					t.Errorf("expected err: %q, got %v", expectedErr, err)
				}

				// Have the server return from the FlowStream call. This should prompt the
				// outbox to finish.
				if tc.serverReturnsError {
					expectedErr = errors.Errorf("FlowStream server error")
				} else {
					expectedErr = nil
				}
				streamNotification.donec <- expectedErr
			} else {
				// We're going to perform a RunSyncFlow call and then have the client
				// cancel the call's context.
				conn, err := flowCtx.rpcCtx.GRPCDial(addr.String()).Connect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				client := NewDistSQLClient(conn)
				var outStream DistSQL_RunSyncFlowClient
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				expectedErr = errors.Errorf("context canceled")
				go func() {
					outStream, err = client.RunSyncFlow(ctx)
					if err != nil {
						t.Error(err)
					}
					// Check that Recv() receives an error once the context is canceled.
					// Perhaps this is not terribly important to test; one can argue that
					// the client should either not be Recv()ing after it canceled the
					// ctx or that it otherwise should otherwise be aware of the
					// cancellation when processing the results, but I've put it here
					// because bidi streams are confusing and this provides some
					// information.
					for {
						_, err := outStream.Recv()
						if err == nil {
							consumerReceivedMsg <- struct{}{}
							continue
						}
						if !testutils.IsError(err, expectedErr.Error()) {
							t.Errorf("expected err: %q, got %v", expectedErr, err)
						}
						break
					}
				}()
				// Wait for the consumer to connect.
				call := <-mockServer.runSyncFlowCalls
				outbox = newOutboxSyncFlowStream(call.stream)
				outbox.setFlowCtx(&FlowCtx{Settings: cluster.MakeTestingClusterSettings(), stopper: stopper})
				outbox.init(oneIntCol)
				// In a RunSyncFlow call, the outbox runs under the call's context.
				outbox.start(call.stream.Context(), &wg, cancel)
				// Wait for the consumer to receive the header message that the outbox
				// sends on start. If we don't wait, the context cancellation races with
				// the outbox sending the header msg; if the cancellation makes it to
				// the outbox right as the outbox is trying to send the header, the
				// outbox might finish with a "the stream has been done" error instead
				// of "context canceled".
				<-consumerReceivedMsg
				// cancel the RPC's context. This is how a streaming RPC client can inform
				// the server that it's done. We expect the outbox to finish.
				cancel()
				defer func() {
					// Allow the RunSyncFlow RPC to finish.
					call.donec <- nil
				}()
			}

			wg.Wait()
			if expectedErr == nil {
				if outbox.err != nil {
					t.Fatalf("unexpected outbox.err: %s", outbox.err)
				}
			} else {
				// We use error string comparison because we actually expect a grpc
				// error wrapping the expected error.
				if !testutils.IsError(outbox.err, expectedErr.Error()) {
					t.Fatalf("expected err: %q, got %v", expectedErr, outbox.err)
				}
			}
		})
	}
}

// Test Outbox cancels flow context when FlowStream returns a non-nil error.
func TestOutboxCancelsFlowOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		stopper:  stopper,
		EvalCtx:  evalCtx,
		rpcCtx:   newInsecureRPCContext(stopper),
	}
	flowID := FlowID{uuid.MakeV4()}
	streamID := StreamID(42)
	var outbox *outbox
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// We could test this on ctx.cancel(), but this mock
	// cancellation method is simpler.
	ctxCanceled := false
	mockCancel := func() {
		ctxCanceled = true
	}

	outbox = newOutbox(&flowCtx, addr.String(), flowID, streamID)
	outbox.init(oneIntCol)
	outbox.start(ctx, &wg, mockCancel)

	// Wait for the outbox to connect the stream.
	streamNotification := <-mockServer.inboundStreams
	if _, err := streamNotification.stream.Recv(); err != nil {
		t.Fatal(err)
	}

	streamNotification.donec <- sqlbase.NewQueryCanceledError()

	wg.Wait()
	if !ctxCanceled {
		t.Fatal("flow ctx was not canceled")
	}
}
