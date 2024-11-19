// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	ctx := context.Background()
	// Set up the RPC layer.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	_, mockServer, addr, err := flowinfra.StartMockDistSQLServer(ctx, clock, stopper, execinfra.StaticSQLInstanceID)
	require.NoError(t, err)

	// Generate a random cancellation scenario.
	rng, _ := randutil.NewTestRand()
	type cancellationType int
	const (
		// In this scenario, no cancellation happens and all the data is pushed
		// from the Outbox to the Inbox.
		noCancel cancellationType = iota
		// streamCtxCancel models a scenario in which the Outbox host cancels
		// the flow.
		streamCtxCancel
		// flowCtxCancel models a scenario in which the flow context of the
		// Inbox host is canceled which is an ungraceful shutdown.
		flowCtxCancel
		// readerCtxCancel models a scenario in which the consumer of the Inbox
		// cancels the context while the host's flow context is not canceled.
		// This is considered a graceful termination.
		readerCtxCancel
		// transportBreaks models a scenario in which the transport breaks.
		transportBreaks
	)
	var (
		cancellationScenario     cancellationType
		cancellationScenarioName string
	)
	switch randVal := rng.Float64(); {
	case randVal <= 0.2:
		cancellationScenario = noCancel
		cancellationScenarioName = "noCancel"
	case randVal <= 0.4:
		cancellationScenario = streamCtxCancel
		cancellationScenarioName = "streamCtxCancel"
	case randVal <= 0.6:
		cancellationScenario = flowCtxCancel
		cancellationScenarioName = "flowCtxCancel"
	case randVal <= 0.8:
		cancellationScenario = readerCtxCancel
		cancellationScenarioName = "readerCtxCancel"
	default:
		cancellationScenario = transportBreaks
		cancellationScenarioName = "transportBreaks"
	}

	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	if cancellationScenario != transportBreaks {
		defer func() {
			err := conn.Close() // nolint:grpcconnclose
			require.NoError(t, err)
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
			inputBuffer = colexecop.NewBatchBuffer()
			// Generate some random behavior before passing the random number
			// generator to be used in the Outbox goroutine (to avoid races).
			// These sleep variables enable a sleep for up to five milliseconds
			// with a .5 probability before cancellation.
			sleepBeforeCancellation = rng.Float64() <= 0.5
			sleepTime               = time.Microsecond * time.Duration(rng.Intn(5000))
			// stopwatch is used to measure how long it takes for the outbox to
			// exit once the transport broke.
			stopwatch                    = timeutil.NewStopWatch()
			transportBreaksProducerSleep = 4 * time.Second
		)

		// Test random selection as the Outbox should be deselecting before
		// sending over data. Nulls and types are not worth testing as those are
		// tested in colserde.
		args := coldatatestutils.RandomDataOpArgs{
			DeterministicTyps: typs,
			NumBatches:        64,
			Selection:         true,
			BatchAccumulator: func(_ context.Context, b coldata.Batch, typs []*types.T) {
				inputBuffer.Add(b, typs)
			},
		}

		if cancellationScenario != noCancel {
			// Crank up the number of batches so cancellation always happens in
			// the middle of execution (or before).
			args.NumBatches = math.MaxInt64
			if cancellationScenario == transportBreaks {
				// Insert an artificial sleep in order to simulate that the
				// input to the outbox takes a while to produce each batch.
				args.BatchAccumulator = func(ctx context.Context, b coldata.Batch, typs []*types.T) {
					select {
					case <-ctx.Done():
					case <-time.After(transportBreaksProducerSleep):
					}
				}
			} else {
				// Disable accumulation to avoid memory blowups.
				args.BatchAccumulator = nil
			}
		}

		var (
			flowCtxCanceled uint32
			// Because the outboxCtx must be a child of the flow context, we
			// assume that if flowCtxCanceled is non-zero, then
			// outboxCtxCanceled is too and don't check that explicitly.
			outboxCtxCanceled uint32
			wg                sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			flowCtx, flowCtxCancelFn := context.WithCancel(ctx)
			flowCtxCancel := func() {
				atomic.StoreUint32(&flowCtxCanceled, 1)
				flowCtxCancelFn()
			}
			outboxCtx, outboxCtxCancelFn := context.WithCancel(flowCtx)
			outboxCtxCancel := func() {
				atomic.StoreUint32(&outboxCtxCanceled, 1)
				outboxCtxCancelFn()
			}

			inputMemAcc := testMemMonitor.MakeBoundAccount()
			defer inputMemAcc.Close(outboxCtx)
			input, _ := coldatatestutils.NewRandomDataOp(
				colmem.NewAllocator(outboxCtx, &inputMemAcc, coldata.StandardColumnFactory), rng, args,
			)
			outboxMemAcc := testMemMonitor.MakeBoundAccount()
			defer outboxMemAcc.Close(outboxCtx)
			outboxConverterMemAcc := testMemMonitor.MakeBoundAccount()
			defer outboxConverterMemAcc.Close(ctx)
			outbox, err := NewOutbox(
				&execinfra.FlowCtx{Gateway: false},
				0, /* processorID */
				colmem.NewAllocator(outboxCtx, &outboxMemAcc, coldata.StandardColumnFactory),
				&outboxConverterMemAcc, colexecargs.OpWithMetaInfo{Root: input}, typs, nil, /* getStats */
			)
			require.NoError(t, err)

			// There is a bit of trickery going on here with the context
			// management caused by the fact that we're using an internal
			// runWithStream method rather than exported Run method. The goal is
			// to create a context of the node on which the outbox runs and keep
			// it different from the streamCtx. This matters in
			// 'transportBreaks' scenario.
			outbox.runnerCtx = outboxCtx
			outbox.runWithStream(streamCtx, clientStream, flowCtxCancel, outboxCtxCancel)
			wg.Done()
		}()

		inboxFlowCtx, inboxFlowCtxCancelFn := context.WithCancel(ctx)
		readerCtx, readerCancelFn := context.WithCancel(inboxFlowCtx)
		wg.Add(1)
		go func() {
			if sleepBeforeCancellation {
				time.Sleep(sleepTime)
			}
			switch cancellationScenario {
			case noCancel:
			case streamCtxCancel:
				streamCancelFn()
			case flowCtxCancel:
				inboxFlowCtxCancelFn()
			case readerCtxCancel:
				readerCancelFn()
			case transportBreaks:
				err := conn.Close() // nolint:grpcconnclose
				require.NoError(t, err)
				stopwatch.Start()
			}
			wg.Done()
		}()

		inboxMemAcc := testMemMonitor.MakeBoundAccount()
		defer inboxMemAcc.Close(readerCtx)
		inbox, err := NewInboxWithFlowCtxDone(
			colmem.NewAllocator(readerCtx, &inboxMemAcc, coldata.StandardColumnFactory),
			typs, execinfrapb.StreamID(0), inboxFlowCtx.Done(),
		)
		require.NoError(t, err)

		streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

		// Use a deselector op to verify that the Outbox gets rid of the selection
		// vector.
		deselectorMemAcc := testMemMonitor.MakeBoundAccount()
		defer deselectorMemAcc.Close(readerCtx)
		inputBatches := colexecutils.NewDeselectorOp(
			colmem.NewAllocator(readerCtx, &deselectorMemAcc, coldata.StandardColumnFactory), inputBuffer, typs,
		)
		inputBatches.Init(readerCtx)
		outputBatches := colexecop.NewBatchBuffer()
		var readerErr error
		for {
			var outputBatch coldata.Batch
			if err := colexecerror.CatchVectorizedRuntimeError(func() {
				// Note that it is ok that we call Init on every iteration - it
				// is a noop every time except for the first one.
				inbox.Init(readerCtx)
				outputBatch = inbox.Next()
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
					batchCopy := testAllocator.NewMemBatchWithFixedCapacity(typs, outputBatch.Length())
					testAllocator.PerformOperation(batchCopy.ColVecs(), func() {
						for i := range typs {
							batchCopy.ColVec(i).Copy(
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
			// Verify that the Outbox terminated gracefully (did not cancel the
			// flow context).
			require.True(t, atomic.LoadUint32(&flowCtxCanceled) == 0)
			require.True(t, atomic.LoadUint32(&outboxCtxCanceled) == 1)
			// And the Inbox did as well.
			require.NoError(t, streamHandlerErr)
			require.NoError(t, readerErr)

			// If no cancellation happened, the output can be fully verified
			// against the input.
			for batchNum := 0; ; batchNum++ {
				outputBatch := outputBatches.Next()
				inputBatch := inputBatches.Next()
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
			// If the stream context gets canceled, gRPC should take care of
			// closing and cleaning up the stream. The Inbox stream handler
			// should have received the context cancellation and returned.
			require.Regexp(t, "context canceled", streamHandlerErr)
			// The Inbox propagates this cancellation on its host.
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			// Recving on a canceled stream produces a context canceled error
			// which prompts the watchdog goroutine of the outbox to cancel the
			// flow.
			require.True(t, atomic.LoadUint32(&flowCtxCanceled) == 1)
		case flowCtxCancel:
			// If the flow context of the Inbox host gets canceled, it is
			// treated as an ungraceful termination of the stream, so we expect
			// an error from the stream handler.
			require.True(t, errors.Is(streamHandlerErr, cancelchecker.QueryCanceledError))
			// The Inbox propagates this cancellation on its host. Depending on
			// when the cancellation is noticed by the reader, a different error
			// is used, so we allow for both of them.
			//
			// QueryCanceledError is used when the flow ctx cancellation is
			// observed before the stream arrived whereas wrapped
			// context.Canceled error is used when the inbox handler goroutine
			// notices the cancellation first and ungracefully shuts down the
			// stream.
			ok := errors.Is(readerErr, cancelchecker.QueryCanceledError) ||
				testutils.IsError(readerErr, "context canceled")
			require.True(t, ok, readerErr)

			// In the production setup, the watchdog goroutine of the outbox
			// would receive non-io.EOF error indicating an ungraceful
			// completion of the FlowStream RPC call which would prompt the
			// outbox to cancel the whole flow.
			//
			// However, because we're using a mock server, the propagation
			// doesn't take place, so the flow context on the outbox side should
			// not be canceled.
			require.True(t, atomic.LoadUint32(&flowCtxCanceled) == 0)
			require.True(t, atomic.LoadUint32(&outboxCtxCanceled) == 1)
		case readerCtxCancel:
			// If the reader context gets canceled while the inbox's host flow
			// context doesn't, it is treated as a graceful termination of the
			// stream, so we expect no error from the stream handler.
			require.Nil(t, streamHandlerErr)
			// The Inbox should still propagate this error upwards.
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			// The cancellation should have been communicated to the Outbox,
			// resulting in the watchdog goroutine canceling the outbox context
			// (but not the flow).
			require.True(t, atomic.LoadUint32(&flowCtxCanceled) == 0)
			require.True(t, atomic.LoadUint32(&outboxCtxCanceled) == 1)
		case transportBreaks:
			// If the transport breaks, the scenario is very similar to
			// streamCtxCancel. gRPC will cancel the stream handler's context.
			stopwatch.Stop()
			// We expect that the outbox exits much sooner than it receives the
			// next batch from its input in this scenario.
			require.Less(t, int64(stopwatch.Elapsed()), int64(transportBreaksProducerSleep/2), "Outbox took too long to exit on transport breakage")
			require.True(t, testutils.IsError(streamHandlerErr, "context canceled"), streamHandlerErr)
			require.True(t, testutils.IsError(readerErr, "context canceled"), readerErr)

			require.True(t, atomic.LoadUint32(&flowCtxCanceled) == 1)
		}
	})
}

// TestInboxHostCtxCancellation verifies that the inbox-outbox pair is properly
// shutdown if the inbox host's flow context is canceled and Inbox.Init is never
// called.
func TestInboxHostCtxCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	// Set up the RPC layer.
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)
	_, mockServer, addr, err := flowinfra.StartMockDistSQLServer(ctx, clock, stopper, execinfra.StaticSQLInstanceID)
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() {
		err := conn.Close() // nolint:grpcconnclose
		require.NoError(t, err)
	}()

	// Simulate the "remote" node with a separate context.
	outboxHostCtx, outboxHostCtxCancel := context.WithCancel(ctx)
	// Derive a separate context for the outbox itself (this is what is done in
	// Outbox.Run).
	outboxCtx, outboxCtxCancel := context.WithCancel(outboxHostCtx)

	// Initiate the FlowStream RPC from the outbox.
	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err := client.FlowStream(outboxCtx)
	require.NoError(t, err)

	// Create and run the outbox.
	//
	// The input to the outbox doesn't matter, so we just create an arbitrary
	// operator that returns a single row with no columns.
	typs := []*types.T{}
	outboxInput := colexecutils.NewFixedNumTuplesNoInputOp(testAllocator, 1 /* numTuples */, nil /* opToInitialize */)
	outboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer outboxMemAcc.Close(outboxHostCtx)
	outbox, err := NewOutbox(
		&execinfra.FlowCtx{Gateway: false},
		0, /* processorID */
		colmem.NewAllocator(outboxHostCtx, &outboxMemAcc, coldata.StandardColumnFactory),
		testMemAcc, colexecargs.OpWithMetaInfo{Root: outboxInput}, typs, nil, /* getStats */
	)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(outboxCtx, clientStream, outboxHostCtxCancel, outboxCtxCancel)
		wg.Done()
	}()

	// Create the inbox on the "local" node (simulated by a separate context).
	inboxHostCtx, inboxHostCtxCancel := context.WithCancel(ctx)
	inboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer inboxMemAcc.Close(inboxHostCtx)
	inbox, err := NewInboxWithFlowCtxDone(
		colmem.NewAllocator(inboxHostCtx, &inboxMemAcc, coldata.StandardColumnFactory),
		typs, execinfrapb.StreamID(0), inboxHostCtx.Done(),
	)
	require.NoError(t, err)

	// Spawn up the stream handler (a separate goroutine) for the server side
	// of the FlowStream RPC.
	serverStreamNotification := <-mockServer.InboundStreams
	serverStream := serverStreamNotification.Stream
	streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

	// Here is the meat of the test - the inbox is never initialized, and,
	// instead, the inbox host's flow context is canceled after some delay.
	var sleepBeforeCancellation = rng.Float64() <= 0.25
	var sleepTime = time.Microsecond * time.Duration(rng.Intn(500))
	wg.Add(1)
	go func() {
		if sleepBeforeCancellation {
			time.Sleep(sleepTime)
		}
		inboxHostCtxCancel()
		wg.Done()
	}()

	// Wait for the Outbox to return.
	wg.Wait()
	// Make sure the Inbox stream handler returned.
	streamHandlerErr := <-streamHandlerErrCh
	require.Equal(t, cancelchecker.QueryCanceledError, streamHandlerErr)
}

func TestOutboxInboxMetadataPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := flowinfra.StartMockDistSQLServer(ctx,
		hlc.NewClockForTesting(nil), stopper, execinfra.StaticSQLInstanceID,
	)
	require.NoError(t, err)

	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer func() {
		err := conn.Close() // nolint:grpcconnclose
		require.NoError(t, err)
	}()

	rng, _ := randutil.NewTestRand()
	// numNextsBeforeDrain is used in ExplicitDrainRequest. This number is
	// generated now to avoid racing on rng accesses between this main goroutine
	// and the Outbox generating random batches.
	numNextsBeforeDrain := rng.Intn(10)

	expectedError := errors.New("someError")

	testCases := []struct {
		name       string
		numBatches int
		// overrideExpectedMetadata, if set, will override the expected metadata
		// the test harness uses.
		overrideExpectedMetadata []execinfrapb.ProducerMetadata
		// verifyExpectedMetadata, if set, will override the equality check the
		// metadata test harness uses.
		verifyExpectedMetadata func([]execinfrapb.ProducerMetadata) bool
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
					inbox.Next()
				}
				return inbox.DrainMeta()
			},
		},
		{
			// AfterSuccessfulCompletion is the usual way DrainMeta is called: after
			// Next has returned a zero batch.
			name:       "AfterSuccessfulCompletion",
			numBatches: 4,
			test: func(ctx context.Context, inbox *Inbox) []execinfrapb.ProducerMetadata {
				for {
					b := inbox.Next()
					if b.Length() == 0 {
						break
					}
				}
				return inbox.DrainMeta()
			},
		},
		{
			// ErrorPropagationDuringExecution is a scenario in which the outbox
			// returns an error after the last batch.
			name:                     "ErrorPropagationDuringExecution",
			numBatches:               4,
			overrideExpectedMetadata: []execinfrapb.ProducerMetadata{{Err: expectedError}},
			verifyExpectedMetadata: func(meta []execinfrapb.ProducerMetadata) bool {
				return len(meta) == 1 && errors.Is(meta[0].Err, expectedError)
			},
			test: func(ctx context.Context, inbox *Inbox) []execinfrapb.ProducerMetadata {
				defer func() {
					// Make sure that the error is not propagated for the second
					// time.
					//
					// We still need to drain to simulate what happens in
					// production - there, the consumer of the inbox would
					// transition into draining upon receiving the error.
					require.True(t, len(inbox.DrainMeta()) == 0)
				}()
				for {
					var b coldata.Batch
					if err := colexecerror.CatchVectorizedRuntimeError(func() {
						b = inbox.Next()
					}); err != nil {
						return []execinfrapb.ProducerMetadata{{Err: err}}
					}
					if b.Length() == 0 {
						return nil
					}
				}
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
				input, _                 = coldatatestutils.NewRandomDataOp(
					testAllocator,
					rng,
					coldatatestutils.RandomDataOpArgs{
						DeterministicTyps: typs,
						NumBatches:        tc.numBatches,
						Selection:         true,
					},
				)
			)

			outboxMemAcc := testMemMonitor.MakeBoundAccount()
			defer outboxMemAcc.Close(ctx)
			expectedMetadata := []execinfrapb.ProducerMetadata{{RowNum: &execinfrapb.RemoteProducerMetadata_RowNum{LastMsg: true}}}
			if tc.overrideExpectedMetadata != nil {
				expectedMetadata = tc.overrideExpectedMetadata
			}
			outbox, err := NewOutbox(
				&execinfra.FlowCtx{Gateway: false},
				0, /* processorID */
				colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory),
				testMemAcc,
				colexecargs.OpWithMetaInfo{
					Root: input,
					MetadataSources: []colexecop.MetadataSource{
						colexectestutils.CallbackMetadataSource{
							DrainMetaCb: func() []execinfrapb.ProducerMetadata {
								return expectedMetadata
							},
						},
					},
				},
				typs,
				nil, /* getStats */
			)
			require.NoError(t, err)

			inboxMemAcc := testMemMonitor.MakeBoundAccount()
			defer inboxMemAcc.Close(ctx)
			inbox, err := NewInbox(colmem.NewAllocator(ctx, &inboxMemAcc, coldata.StandardColumnFactory), typs, execinfrapb.StreamID(0))
			require.NoError(t, err)

			var (
				flowCanceled, outboxCanceled uint32
				wg                           sync.WaitGroup
			)
			wg.Add(1)
			go func() {
				outbox.runWithStream(
					ctx,
					clientStream,
					func() { atomic.StoreUint32(&flowCanceled, 1) },
					func() { atomic.StoreUint32(&outboxCanceled, 1) },
				)
				wg.Done()
			}()

			streamHanderErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

			inbox.Init(ctx)
			meta := tc.test(ctx, inbox)

			wg.Wait()
			require.NoError(t, <-streamHanderErrCh)
			// Require that the outbox did not cancel the flow and did cancel
			// the outbox since this is a graceful drain.
			require.True(t, atomic.LoadUint32(&flowCanceled) == 0)
			require.True(t, atomic.LoadUint32(&outboxCanceled) == 1)

			// Verify that we received the expected metadata.
			if tc.verifyExpectedMetadata != nil {
				require.True(t, tc.verifyExpectedMetadata(meta), "unexpected meta: %v", meta)
			} else {
				require.True(t, len(meta) == len(expectedMetadata))
				require.Equal(t, expectedMetadata, meta, "unexpected meta: %v", meta)
			}
		})
	}
}

func BenchmarkOutboxInbox(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	_, mockServer, addr, err := flowinfra.StartMockDistSQLServer(ctx,
		hlc.NewClockForTesting(nil), stopper, execinfra.StaticSQLInstanceID,
	)
	require.NoError(b, err)

	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(b, err)
	defer func() {
		err := conn.Close() // nolint:grpcconnclose
		require.NoError(b, err)
	}()

	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err := client.FlowStream(ctx)
	require.NoError(b, err)

	serverStreamNotification := <-mockServer.InboundStreams
	serverStream := serverStreamNotification.Stream

	typs := []*types.T{types.Int}

	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	batch.SetLength(coldata.BatchSize())

	input := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)

	outboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer outboxMemAcc.Close(ctx)
	outbox, err := NewOutbox(
		&execinfra.FlowCtx{Gateway: false},
		0, /* processorID */
		colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory),
		testMemAcc, colexecargs.OpWithMetaInfo{Root: input}, typs, nil, /* getStats */
	)
	require.NoError(b, err)

	inboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer inboxMemAcc.Close(ctx)
	inbox, err := NewInbox(colmem.NewAllocator(ctx, &inboxMemAcc, coldata.StandardColumnFactory), typs, execinfrapb.StreamID(0))
	require.NoError(b, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, clientStream, nil /* flowCtxCancel */, nil /* outboxCtxCancel */)
		wg.Done()
	}()

	streamHandlerErrCh := handleStream(serverStream.Context(), inbox, serverStream, func() { close(serverStreamNotification.Donec) })

	inbox.Init(ctx)
	b.SetBytes(8 * int64(coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inbox.Next()
	}
	b.StopTimer()

	// This is a way of telling the Outbox we're satisfied with the data received.
	meta := inbox.DrainMeta()
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

	_, mockServer, addr, err := flowinfra.StartMockDistSQLServer(ctx,
		hlc.NewClockForTesting(nil), stopper, execinfra.StaticSQLInstanceID,
	)
	require.NoError(t, err)
	dialer := &flowinfra.MockDialer{Addr: addr}
	defer dialer.Close()

	typs := []*types.T{types.Int}

	var inTags *logtags.Buffer

	nextDone := make(chan struct{})
	input := &colexecop.CallbackOperator{NextCb: func() coldata.Batch {
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, 0)
		inTags = logtags.FromContext(ctx)
		nextDone <- struct{}{}
		return b
	}}

	outboxMemAcc := testMemMonitor.MakeBoundAccount()
	defer outboxMemAcc.Close(ctx)
	outbox, err := NewOutbox(
		&execinfra.FlowCtx{Gateway: false},
		0, /* processorID */
		colmem.NewAllocator(ctx, &outboxMemAcc, coldata.StandardColumnFactory),
		testMemAcc, colexecargs.OpWithMetaInfo{Root: input}, typs, nil, /* getStats */
	)
	require.NoError(t, err)

	outboxDone := make(chan struct{})
	go func() {
		outbox.Run(
			ctx,
			dialer,
			base.SQLInstanceID(0),
			outboxStreamID,
			nil, /* flowCtxCancel */
			0,   /* connectionTimeout */
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
		test func(*Inbox)
	}{
		{
			// CtxTaggedInNext verifies that Next adds StreamID to the Context in maybeInit.
			name: "CtxTaggedInNext",
			test: func(inbox *Inbox) {
				inbox.Next()
			},
		},
		{
			// CtxTaggedInDrainMeta verifies that DrainMeta adds StreamID to the Context in maybeInit.
			name: "CtxTaggedInDrainMeta",
			test: func(inbox *Inbox) {
				inbox.DrainMeta()
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
			go func(tester func(*Inbox)) {
				inbox.Init(ctx)
				tester(inbox)
				inboxTested <- struct{}{}
			}(tc.test)

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
