// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colrpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockFlowStreamClient struct {
	pmChan chan *execinfrapb.ProducerMessage
	csChan chan *execinfrapb.ConsumerSignal
}

var _ flowStreamClient = mockFlowStreamClient{}

func (c mockFlowStreamClient) Send(m *execinfrapb.ProducerMessage) error {
	c.pmChan <- m
	return nil
}

func (c mockFlowStreamClient) Recv() (*execinfrapb.ConsumerSignal, error) {
	s := <-c.csChan
	if s == nil {
		return nil, io.EOF
	}
	return s, nil
}

func (c mockFlowStreamClient) CloseSend() error {
	close(c.pmChan)
	return nil
}

type mockFlowStreamServer struct {
	pmChan chan *execinfrapb.ProducerMessage
	csChan chan *execinfrapb.ConsumerSignal
}

func (s mockFlowStreamServer) Send(cs *execinfrapb.ConsumerSignal) error {
	s.csChan <- cs
	return nil
}

func (s mockFlowStreamServer) Recv() (*execinfrapb.ProducerMessage, error) {
	pm := <-s.pmChan
	if pm == nil {
		return nil, io.EOF
	}
	return pm, nil
}

var _ flowStreamServer = mockFlowStreamServer{}

// mockFlowStreamRPCLayer mocks out a bidirectional FlowStream RPC. The client
// and server simply send messages over channels and return io.EOF when these
// channels are closed. This RPC layer does not aim to implement more than that.
// Use MockDistSQLServer for more involved RPC behavior testing.
type mockFlowStreamRPCLayer struct {
	client mockFlowStreamClient
	server mockFlowStreamServer
}

func makeMockFlowStreamRPCLayer() mockFlowStreamRPCLayer {
	// Buffer channels to simulate non-blocking sends.
	pmChan := make(chan *execinfrapb.ProducerMessage, 16)
	csChan := make(chan *execinfrapb.ConsumerSignal, 16)
	return mockFlowStreamRPCLayer{
		client: mockFlowStreamClient{pmChan: pmChan, csChan: csChan},
		server: mockFlowStreamServer{pmChan: pmChan, csChan: csChan},
	}
}

// handleStream spawns a goroutine to call Inbox.RunWithStream with the
// provided stream and returns any error on the returned channel. handleStream
// will call doneFn if non-nil once the handler returns.
func handleStream(
	ctx context.Context, inbox *Inbox, stream flowStreamServer, doneFn func(),
) chan error {
	handleStreamErrCh := make(chan error, 1)
	go func() {
		handleStreamErrCh <- inbox.RunWithStream(ctx, stream)
		if doneFn != nil {
			doneFn()
		}
	}()
	return handleStreamErrCh
}

func TestOutboxInbox(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up the RPC layer.
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	require.NoError(t, err)

	// Generate a random cancellation scenario.
	rng, _ := randutil.NewPseudoRand()
	type cancellationType int
	const (
		// In this scenario, no cancellation happens and all the data is pushed from
		// the Outbox to the Inbox.
		noCancel cancellationType = iota
		// streamCtxCancel models a scenario in which the Outbox host cancels the
		// flow.
		streamCtxCancel
		// readerCtxCancel models a scenario in which the Inbox host cancels the
		// flow.
		readerCtxCancel
		// transportBreaks models a scenario in which the transport breaks.
		transportBreaks
	)
	var (
		cancellationScenario     cancellationType
		cancellationScenarioName string
	)
	switch randVal := rng.Float64(); {
	case randVal <= 0.25:
		cancellationScenario = noCancel
		cancellationScenarioName = "noCancel"
	case randVal <= 0.50:
		cancellationScenario = streamCtxCancel
		cancellationScenarioName = "streamCtxCancel"
	case randVal <= 0.75:
		cancellationScenario = readerCtxCancel
		cancellationScenarioName = "readerCtxCancel"
	case randVal <= 1:
		cancellationScenario = transportBreaks
		cancellationScenarioName = "transportBreaks"
	}

	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	if cancellationScenario != transportBreaks {
		defer func() {
			require.NoError(t, conn.Close())
		}()
	}

	streamCtx, streamCancelFn := context.WithCancel(ctx)
	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err := client.FlowStream(streamCtx)
	require.NoError(t, err)

	serverStreamNotification := <-mockServer.InboundStreams
	serverStream := serverStreamNotification.Stream

	// Do the actual testing.
	t.Run(fmt.Sprintf("cancellationScenario=%s", cancellationScenarioName), func(t *testing.T) {
		var (
			typs        = []*types.T{types.Int}
			inputBuffer = colexecbase.NewBatchBuffer()
			// Generate some random behavior before passing the random number
			// generator to be used in the Outbox goroutine (to avoid races). These
			// sleep variables enable a sleep for up to half a millisecond with a .25
			// probability before cancellation.
			sleepBeforeCancellation = rng.Float64() <= 0.25
			sleepTime               = time.Microsecond * time.Duration(rng.Intn(500))
		)

		// Test random selection as the Outbox should be deselecting before sending
		// over data. Nulls and types are not worth testing as those are tested in
		// colserde.
		args := coldatatestutils.RandomDataOpArgs{
			DeterministicTyps: typs,
			NumBatches:        64,
			Selection:         true,
			BatchAccumulator:  inputBuffer.Add,
		}

		if cancellationScenario != noCancel {
			// Crank up the number of batches so cancellation always happens in the
			// middle of execution (or before).
			args.NumBatches = math.MaxInt64
			// Disable accumulation to avoid memory blowups.
			args.BatchAccumulator = nil
		}
		inputMemAcc := testMemMonitor.MakeBoundAccount()
		defer inputMemAcc.Close(ctx)
		input := coldatatestutils.NewRandomDataOp(
			colmem.NewAllocator(ctx, &inputMemAcc, coldata.StandardColumnFactory), rng, args,
		)

		outboxMemAcc := testMemMonitor.MakeBoundAccount()
		defer outboxMemAcc.Close(ctx)
		outbox, err :=
			NewOutbox(colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory), input, typs, nil /* metadataSource */, nil /* toClose */)
		require.NoError(t, err)

		inboxMemAcc := testMemMonitor.MakeBoundAccount()
		defer inboxMemAcc.Close(ctx)
		inbox, err := NewInbox(
			colmem.NewAllocator(ctx, &inboxMemAcc, coldata.StandardColumnFactory), typs, execinfrapb.StreamID(0),
		)
		require.NoError(t, err)

		streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

		var (
			canceled uint32
			wg       sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			outbox.runWithStream(streamCtx, clientStream, func() { atomic.StoreUint32(&canceled, 1) })
			wg.Done()
		}()

		readerCtx, readerCancelFn := context.WithCancel(ctx)
		wg.Add(1)
		go func() {
			if sleepBeforeCancellation {
				time.Sleep(sleepTime)
			}
			switch cancellationScenario {
			case noCancel:
			case streamCtxCancel:
				streamCancelFn()
			case readerCtxCancel:
				readerCancelFn()
			case transportBreaks:
				_ = conn.Close()
			}
			wg.Done()
		}()

		// Use a deselector op to verify that the Outbox gets rid of the selection
		// vector.
		deselectorMemAcc := testMemMonitor.MakeBoundAccount()
		defer deselectorMemAcc.Close(ctx)
		inputBatches := colexec.NewDeselectorOp(
			colmem.NewAllocator(ctx, &deselectorMemAcc, coldata.StandardColumnFactory), inputBuffer, typs,
		)
		inputBatches.Init()
		outputBatches := colexecbase.NewBatchBuffer()
		var readerErr error
		for {
			var outputBatch coldata.Batch
			if err := colexecerror.CatchVectorizedRuntimeError(func() {
				outputBatch = inbox.Next(readerCtx)
			}); err != nil {
				readerErr = err
				break
			}
			if cancellationScenario == noCancel {
				// Accumulate batches to check for correctness.
				// Copy batch since it's not safe to reuse after calling Next.
				if outputBatch == coldata.ZeroBatch {
					outputBatches.Add(coldata.ZeroBatch, typs)
				} else {
					batchCopy := testAllocator.NewMemBatchWithSize(typs, outputBatch.Length())
					testAllocator.PerformOperation(batchCopy.ColVecs(), func() {
						for i := range typs {
							batchCopy.ColVec(i).Append(
								coldata.SliceArgs{
									Src:       outputBatch.ColVec(i),
									SrcEndIdx: outputBatch.Length(),
								},
							)
						}
					})
					batchCopy.SetLength(outputBatch.Length())
					outputBatches.Add(batchCopy, typs)
				}
			}
			if outputBatch.Length() == 0 {
				break
			}
		}

		// Wait for the Outbox to return, and any cancellation scenario to take
		// place.
		wg.Wait()
		// Make sure the Inbox stream handler returned.
		streamHandlerErr := <-streamHandlerErrCh

		// Verify expected state.
		switch cancellationScenario {
		case noCancel:
			// Verify that the Outbox terminated gracefully (did not cancel its flow).
			require.True(t, atomic.LoadUint32(&canceled) == 0)
			// And the Inbox did as well.
			require.NoError(t, streamHandlerErr)
			require.NoError(t, readerErr)

			// If no cancellation happened, the output can be fully verified against
			// the input.
			for batchNum := 0; ; batchNum++ {
				outputBatch := outputBatches.Next(ctx)
				inputBatch := inputBatches.Next(ctx)
				require.Equal(t, outputBatch.Length(), inputBatch.Length())
				if outputBatch.Length() == 0 {
					break
				}
				for i := range typs {
					require.Equal(
						t,
						inputBatch.ColVec(i).Window(0, inputBatch.Length()),
						outputBatch.ColVec(i).Window(0, outputBatch.Length()),
						"batchNum: %d", batchNum,
					)
				}
			}
		case streamCtxCancel:
			// If the stream context gets canceled, GRPC should take care of closing
			// and cleaning up the stream. The Inbox stream handler should have
			// received the context cancellation and returned.
			require.Regexp(t, "context canceled", streamHandlerErr)
			// The Inbox propagates this cancellation on its host.
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			// Recving on a canceled stream produces a context canceled error, but
			// Sending produces an EOF, that should have triggered a flow context
			// cancellation (which is redundant) in the Outbox.
			require.True(t, atomic.LoadUint32(&canceled) == 1)
		case readerCtxCancel:
			// If the reader context gets canceled, the Inbox should have returned
			// from the stream handler.
			require.Regexp(t, "context canceled", streamHandlerErr)
			// The Inbox should propagate this error upwards.
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			// The cancellation should have been communicated to the Outbox, resulting
			// in a Send EOF and a flow cancellation on the Outbox's host.
			require.True(t, atomic.LoadUint32(&canceled) == 1)
		case transportBreaks:
			// If the transport breaks, the scenario is very similar to
			// streamCtxCancel. GRPC will cancel the stream handler's context.
			require.True(t, testutils.IsError(streamHandlerErr, "context canceled"), streamHandlerErr)
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			require.True(t, atomic.LoadUint32(&canceled) == 1)
		}
	})
}

func TestOutboxInboxMetadataPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, execinfra.StaticNodeID,
	)
	require.NoError(t, err)

	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	rng, _ := randutil.NewPseudoRand()
	// numNextsBeforeDrain is used in ExplicitDrainRequest. This number is
	// generated now to avoid racing on rng accesses between this main goroutine
	// and the Outbox generating random batches.
	numNextsBeforeDrain := rng.Intn(10)

	testCases := []struct {
		name       string
		numBatches int
		// test is the body of the test to be run. Metadata should be returned to
		// be verified.
		test func(context.Context, *Inbox) []execinfrapb.ProducerMetadata
	}{
		{
			// ExplicitDrainRequest verifies that an Outbox responds to an explicit drain
			// request even if it is not finished processing data.
			name: "ExplicitDrainRequest",
			// Set a high number of batches to ensure that the Outbox is very far
			// from being finished when it receives a DrainRequest.
			numBatches: math.MaxInt64,
			test: func(ctx context.Context, inbox *Inbox) []execinfrapb.ProducerMetadata {
				// Simulate the inbox flow calling Next an arbitrary amount of times
				// (including none).
				for i := 0; i < numNextsBeforeDrain; i++ {
					inbox.Next(ctx)
				}
				return inbox.DrainMeta(ctx)
			},
		},
		{
			// AfterSuccessfulCompletion is the usual way DrainMeta is called: after
			// Next has returned a zero batch.
			name:       "AfterSuccessfulCompletion",
			numBatches: 4,
			test: func(ctx context.Context, inbox *Inbox) []execinfrapb.ProducerMetadata {
				for {
					b := inbox.Next(ctx)
					if b.Length() == 0 {
						break
					}
				}
				return inbox.DrainMeta(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := execinfrapb.NewDistSQLClient(conn)
			clientStream, err := client.FlowStream(ctx)
			require.NoError(t, err)

			var (
				serverStreamNotification = <-mockServer.InboundStreams
				serverStream             = serverStreamNotification.Stream
				typs                     = []*types.T{types.Int}
				input                    = coldatatestutils.NewRandomDataOp(
					testAllocator,
					rng,
					coldatatestutils.RandomDataOpArgs{
						DeterministicTyps: typs,
						NumBatches:        tc.numBatches,
						Selection:         true,
					},
				)
			)

			const expectedMeta = "someError"

			outboxMemAcc := testMemMonitor.MakeBoundAccount()
			defer outboxMemAcc.Close(ctx)
			outbox, err := NewOutbox(colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory), input, typs, []execinfrapb.MetadataSource{
				execinfrapb.CallbackMetadataSource{
					DrainMetaCb: func(context.Context) []execinfrapb.ProducerMetadata {
						return []execinfrapb.ProducerMetadata{{Err: errors.New(expectedMeta)}}
					},
				},
			}, nil /* toClose */)
			require.NoError(t, err)

			inboxMemAcc := testMemMonitor.MakeBoundAccount()
			defer inboxMemAcc.Close(ctx)
			inbox, err := NewInbox(
				colmem.NewAllocator(ctx, &inboxMemAcc, coldata.StandardColumnFactory),
				typs, execinfrapb.StreamID(0),
			)
			require.NoError(t, err)

			var (
				canceled uint32
				wg       sync.WaitGroup
			)
			wg.Add(1)
			go func() {
				outbox.runWithStream(ctx, clientStream, func() { atomic.StoreUint32(&canceled, 1) })
				wg.Done()
			}()

			streamHanderErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

			meta := tc.test(ctx, inbox)

			wg.Wait()
			require.NoError(t, <-streamHanderErrCh)
			// Require that the outbox did not cancel the flow, this is a graceful
			// drain.
			require.True(t, atomic.LoadUint32(&canceled) == 0)

			// Verify that we received the expected metadata.
			require.True(t, len(meta) == 1)
			require.True(t, testutils.IsError(meta[0].Err, expectedMeta), meta[0].Err)
		})
	}
}

func BenchmarkOutboxInbox(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, execinfra.StaticNodeID,
	)
	require.NoError(b, err)

	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(b, err)
	defer func() { require.NoError(b, conn.Close()) }()

	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err := client.FlowStream(ctx)
	require.NoError(b, err)

	serverStreamNotification := <-mockServer.InboundStreams
	serverStream := serverStreamNotification.Stream

	typs := []*types.T{types.Int}

	batch := testAllocator.NewMemBatch(typs)
	batch.SetLength(coldata.BatchSize())

	input := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)

	outboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer outboxMemAcc.Close(ctx)
	outbox, err :=
		NewOutbox(colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory), input, typs, nil /* metadataSources */, nil /* toClose */)
	require.NoError(b, err)

	inboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer inboxMemAcc.Close(ctx)
	inbox, err := NewInbox(
		colmem.NewAllocator(ctx, &inboxMemAcc, coldata.StandardColumnFactory), typs, execinfrapb.StreamID(0),
	)
	require.NoError(b, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, clientStream, nil /* cancelFn */)
		wg.Done()
	}()

	streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

	b.SetBytes(8 * int64(coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inbox.Next(ctx)
	}
	b.StopTimer()

	// This is a way of telling the Outbox we're satisfied with the data received.
	meta := inbox.DrainMeta(ctx)
	require.True(b, len(meta) == 0)

	require.NoError(b, <-streamHandlerErrCh)
	wg.Wait()
}

func TestOutboxStreamIDPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	outboxStreamID := execinfrapb.StreamID(1234)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, execinfra.StaticNodeID,
	)
	require.NoError(t, err)
	dialer := &execinfrapb.MockDialer{Addr: addr}
	defer dialer.Close()

	typs := []*types.T{types.Int}

	var inTags *logtags.Buffer

	nextDone := make(chan struct{})
	input := &colexecbase.CallbackOperator{NextCb: func(ctx context.Context) coldata.Batch {
		b := testAllocator.NewMemBatchWithSize(typs, 0)
		b.SetLength(0)
		inTags = logtags.FromContext(ctx)
		nextDone <- struct{}{}
		return b
	}}

	outboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer outboxMemAcc.Close(ctx)
	outbox, err :=
		NewOutbox(colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory), input, typs, nil /* metadataSources */, nil /* toClose */)
	require.NoError(t, err)

	outboxDone := make(chan struct{})
	go func() {
		outbox.Run(
			ctx,
			dialer,
			roachpb.NodeID(0),
			execinfrapb.FlowID{UUID: uuid.MakeV4()},
			outboxStreamID,
			nil,
		)
		outboxDone <- struct{}{}
	}()

	<-nextDone
	serverStreamNotification := <-mockServer.InboundStreams
	close(serverStreamNotification.Donec)
	<-outboxDone

	// Assert that the ctx passed to Next() has no caller tags (e.g. streamID).
	require.Equal(t, (*logtags.Buffer)(nil), inTags)
}

func TestInboxCtxStreamIDTagging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	streamID := execinfrapb.StreamID(1234)
	ctx := context.Background()
	inboxInternalCtx := context.Background()
	taggedCtx := logtags.AddTag(context.Background(), "streamID", streamID)

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testCases := []struct {
		name string
		// test is the body of the test to be run.
		test func(context.Context, *Inbox)
	}{
		{
			// CtxTaggedInNext verifies that Next adds StreamID to the Context in maybeInit.
			name: "CtxTaggedInNext",
			test: func(ctx context.Context, inbox *Inbox) {
				inbox.Next(ctx)
			},
		},
		{
			// CtxTaggedInDrainMeta verifies that DrainMeta adds StreamID to the Context in maybeInit.
			name: "CtxTaggedInDrainMeta",
			test: func(ctx context.Context, inbox *Inbox) {
				inbox.DrainMeta(ctx)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			rpcLayer := makeMockFlowStreamRPCLayer()

			typs := []*types.T{types.Int}

			inbox, err := NewInbox(testAllocator, typs, streamID)
			require.NoError(t, err)

			ctxExtract := make(chan struct{})
			inbox.ctxInterceptorFn = func(ctx context.Context) {
				inboxInternalCtx = ctx
				ctxExtract <- struct{}{}
			}

			streamHandlerErr := handleStream(ctx, inbox, callbackFlowStreamServer{
				server: rpcLayer.server,
				recvCb: nil,
			}, nil)

			inboxTested := make(chan struct{})
			go func() {
				tc.test(ctx, inbox)
				inboxTested <- struct{}{}
			}()

			<-ctxExtract
			require.NoError(t, rpcLayer.client.CloseSend())
			require.NoError(t, <-streamHandlerErr)
			<-inboxTested

			// Assert that ctx passed to Next and DrainMeta was not modified.
			require.Equal(t, (*logtags.Buffer)(nil), logtags.FromContext(ctx))
			// Assert that inboxInternalCtx has streamID tag, after init is called.
			require.Equal(t, logtags.FromContext(taggedCtx), logtags.FromContext(inboxInternalCtx))

		})
	}
}
