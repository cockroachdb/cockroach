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
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type callbackFlowStreamServer struct {
	server flowStreamServer
	sendCb func()
	recvCb func()
}

func (s callbackFlowStreamServer) Send(cs *execinfrapb.ConsumerSignal) error {
	if s.sendCb != nil {
		s.sendCb()
	}
	return s.server.Send(cs)
}

func (s callbackFlowStreamServer) Recv() (*execinfrapb.ProducerMessage, error) {
	if s.recvCb != nil {
		s.recvCb()
	}
	return s.server.Recv()
}

var _ flowStreamServer = callbackFlowStreamServer{}

func TestInboxCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	typs := []*types.T{types.Int}
	t.Run("ReaderWaitingForStreamHandler", func(t *testing.T) {
		inbox, err := NewInbox(testAllocator, typs, execinfrapb.StreamID(0))
		require.NoError(t, err)
		ctx, cancelFn := context.WithCancel(context.Background())
		// Cancel the context.
		cancelFn()
		// Next should not block if the context is canceled.
		err = colexecerror.CatchVectorizedRuntimeError(func() { inbox.Next(ctx) })
		require.True(t, testutils.IsError(err, "context canceled"), err)
		// Now, the remote stream arrives.
		err = inbox.RunWithStream(context.Background(), mockFlowStreamServer{})
		require.True(t, testutils.IsError(err, "while waiting for stream"), err)
	})

	t.Run("DuringRecv", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		inbox, err := NewInbox(testAllocator, typs, execinfrapb.StreamID(0))
		require.NoError(t, err)
		ctx, cancelFn := context.WithCancel(context.Background())

		// Setup reader and stream.
		go func() {
			inbox.Next(ctx)
		}()
		recvCalled := make(chan struct{})
		streamHandlerErrCh := handleStream(context.Background(), inbox, callbackFlowStreamServer{
			server: rpcLayer.server,
			recvCb: func() {
				recvCalled <- struct{}{}
			},
		}, func() { close(rpcLayer.server.csChan) })

		// Now wait for the Inbox to call Recv on the stream.
		<-recvCalled

		// Cancel the context.
		cancelFn()
		err = <-streamHandlerErrCh
		require.True(t, testutils.IsError(err, "readerCtx in Inbox stream handler"), err)

		// The mock RPC layer does not unblock the Recv for us on the server side,
		// so manually send an io.EOF to the reader goroutine.
		close(rpcLayer.server.pmChan)
	})

	t.Run("StreamHandlerWaitingForReader", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		inbox, err := NewInbox(testAllocator, typs, execinfrapb.StreamID(0))
		require.NoError(t, err)

		ctx, cancelFn := context.WithCancel(context.Background())

		cancelFn()
		// A stream arrives but there is no reader.
		err = <-handleStream(ctx, inbox, rpcLayer.server, func() { close(rpcLayer.client.csChan) })
		require.True(t, testutils.IsError(err, "while waiting for reader"), err)
	})
}

// TestInboxNextPanicDoesntLeakGoroutines verifies that goroutines that are
// spawned as part of an Inbox's normal operation are cleaned up even on a
// panic.
func TestInboxNextPanicDoesntLeakGoroutines(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inbox, err := NewInbox(testAllocator, []*types.T{types.Int}, execinfrapb.StreamID(0))
	require.NoError(t, err)

	rpcLayer := makeMockFlowStreamRPCLayer()
	streamHandlerErrCh := handleStream(context.Background(), inbox, rpcLayer.server, func() { close(rpcLayer.client.csChan) })

	m := &execinfrapb.ProducerMessage{}
	m.Data.RawBytes = []byte("garbage")

	go func() {
		_ = rpcLayer.client.Send(m)
	}()

	// inbox.Next should panic given that the deserializer will encounter garbage
	// data.
	require.Panics(t, func() { inbox.Next(context.Background()) })

	// We require no error from the stream handler as nothing was canceled. The
	// panic is bubbled up through the Next chain on the Inbox's host.
	require.NoError(t, <-streamHandlerErrCh)
}

func TestInboxTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	inbox, err := NewInbox(testAllocator, []*types.T{types.Int}, execinfrapb.StreamID(0))
	require.NoError(t, err)

	var (
		readerErrCh = make(chan error)
		rpcLayer    = makeMockFlowStreamRPCLayer()
	)
	go func() {
		readerErrCh <- colexecerror.CatchVectorizedRuntimeError(func() { inbox.Next(ctx) })
	}()

	// Timeout the inbox.
	const timeoutErr = "timeout error"
	inbox.Timeout(errors.New(timeoutErr))

	// Ensure that the reader gets the error.
	readerErr := <-readerErrCh
	require.True(t, testutils.IsError(readerErr, timeoutErr), readerErr)

	// And now the stream arrives.
	streamHandlerErrCh := handleStream(ctx, inbox, rpcLayer.server, nil /* doneFn */)
	streamErr := <-streamHandlerErrCh
	require.True(t, testutils.IsError(streamErr, "stream arrived too late"), streamErr)
}

// TestInboxShutdown is a random test that spawns a goroutine for handling a
// FlowStream RPC (setting up an inbound stream, or RunWithStream), a goroutine
// to read from an Inbox (Next goroutine), and a goroutine to drain the Inbox
// (DrainMeta goroutine). These goroutines race against each other and the
// desired state is that everything is cleaned up at the end. Examples of
// scenarios that are tested by this test include but are not limited to:
//  - DrainMeta called before Next and before a stream arrives.
//  - DrainMeta called concurrently with Next with an active stream.
//  - A forceful cancellation of Next but no call to DrainMeta.
func TestInboxShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		rng, _ = randutil.NewPseudoRand()
		// infiniteBatches will influence whether or not we're likely to test a
		// graceful shutdown (since other shutdown mechanisms might happen before
		// we reach the end of the data stream). If infiniteBatches is true,
		// shutdown scenarios in the middle of data processing are always tested. If
		// false, they sometimes will be.
		infiniteBatches    = rng.Float64() < 0.5
		drainMetaSleep     = time.Millisecond * time.Duration(rng.Intn(10))
		nextSleep          = time.Millisecond * time.Duration(rng.Intn(10))
		runWithStreamSleep = time.Millisecond * time.Duration(rng.Intn(10))
		typs               = []*types.T{types.Int}
		batch              = coldatatestutils.RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), 0 /* length */, rng.Float64())
	)

	for _, runDrainMetaGoroutine := range []bool{false, true} {
		for _, runNextGoroutine := range []bool{false, true} {
			for _, runRunWithStreamGoroutine := range []bool{false, true} {
				if runDrainMetaGoroutine == false && runNextGoroutine == false && runRunWithStreamGoroutine == true {
					// This is sort of like a remote node connecting to the inbox, but the
					// inbox will never be spawned. This is dealt with by another part of
					// the code (the flow registry times out inbound RPCs if a consumer is
					// not scheduled in time), so this case is skipped.
					continue
				}
				rpcLayer := makeMockFlowStreamRPCLayer()

				t.Run(fmt.Sprintf(
					"drain=%t/next=%t/stream=%t/inf=%t",
					runDrainMetaGoroutine, runNextGoroutine, runRunWithStreamGoroutine, infiniteBatches,
				), func(t *testing.T) {
					inboxCtx, inboxCancel := context.WithCancel(context.Background())
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(inboxCtx)
					inbox, err := NewInbox(
						colmem.NewAllocator(inboxCtx, &inboxMemAccount, coldata.StandardColumnFactory),
						typs, execinfrapb.StreamID(0),
					)
					require.NoError(t, err)
					c, err := colserde.NewArrowBatchConverter(typs)
					require.NoError(t, err)
					r, err := colserde.NewRecordBatchSerializer(typs)
					require.NoError(t, err)

					goroutines := []struct {
						name           string
						asyncOperation func() chan error
					}{
						{
							name: "RunWithStream",
							asyncOperation: func() chan error {
								errCh := make(chan error)
								go func() {
									var wg sync.WaitGroup
									defer close(errCh)
									if runWithStreamSleep != 0 {
										time.Sleep(runWithStreamSleep)
									}
									if !runRunWithStreamGoroutine {
										// The inbox needs to be timed out. This is called by the inbound
										// stream code during normal operation. This timeout simulates a
										// stream not arriving in time.
										inbox.Timeout(errors.New("artificial timeout"))
										return
									}
									quitSending := make(chan struct{})
									wg.Add(1)
									go func() {
										defer wg.Done()
										arrowData, err := c.BatchToArrow(batch)
										if err != nil {
											errCh <- err
											return
										}
										var buffer bytes.Buffer
										_, _, err = r.Serialize(&buffer, arrowData)
										if err != nil {
											errCh <- err
											return
										}
										var draining uint32
										if runDrainMetaGoroutine {
											// Listen for the drain signal.
											wg.Add(1)
											go func() {
												defer wg.Done()
												for {
													cs, err := rpcLayer.client.Recv()
													if cs != nil && cs.DrainRequest != nil {
														atomic.StoreUint32(&draining, 1)
														return
													}
													// TODO(asubiotto): Generate some metadata and test
													//  that it is received.
													if err != nil {
														if err == io.EOF {
															return
														}
														errCh <- err
													}
												}
											}()
										}
										msg := &execinfrapb.ProducerMessage{Data: execinfrapb.ProducerData{RawBytes: buffer.Bytes()}}
										batchesToSend := rng.Intn(65536)
										for i := 0; infiniteBatches || i < batchesToSend; i++ {
											if atomic.LoadUint32(&draining) == 1 {
												break
											}
											quitLoop := false
											select {
											case rpcLayer.client.pmChan <- msg:
											case <-quitSending:
												quitLoop = true
											}
											if quitLoop {
												break
											}
										}
										if err := rpcLayer.client.CloseSend(); err != nil {
											errCh <- err
										}
									}()
									// Use context.Background() because it's separate from the
									// inbox context.
									handleErr := <-handleStream(context.Background(), inbox, rpcLayer.server, func() { close(rpcLayer.server.csChan) })
									close(quitSending)
									wg.Wait()
									errCh <- handleErr
								}()
								return errCh
							},
						},
						{
							name: "Next",
							asyncOperation: func() chan error {
								errCh := make(chan error)
								go func() {
									defer close(errCh)
									if !runNextGoroutine {
										return
									}
									if nextSleep != 0 {
										time.Sleep(nextSleep)
									}
									var (
										done bool
										err  error
									)
									for !done && err == nil {
										err = colexecerror.CatchVectorizedRuntimeError(func() { b := inbox.Next(inboxCtx); done = b.Length() == 0 })
									}
									errCh <- err
								}()
								return errCh
							},
						},
						{
							name: "DrainMeta",
							asyncOperation: func() chan error {
								errCh := make(chan error)
								go func() {
									defer func() {
										inboxCancel()
										close(errCh)
									}()
									// Sleep before checking for whether to run a drain meta
									// goroutine or not, because we want to insert a potential delay
									// before canceling the inbox context in any case.
									if drainMetaSleep != 0 {
										time.Sleep(drainMetaSleep)
									}
									if !runDrainMetaGoroutine {
										return
									}
									_ = inbox.DrainMeta(inboxCtx)
								}()
								return errCh
							},
						},
					}

					// goroutineIndices will be shuffled around to randomly change the order in
					// which the goroutines are spawned.
					goroutineIndices := make([]int, len(goroutines))
					for i := range goroutineIndices {
						goroutineIndices[i] = i
					}
					rng.Shuffle(len(goroutineIndices), func(i, j int) { goroutineIndices[i], goroutineIndices[j] = goroutineIndices[j], goroutineIndices[i] })
					errChans := make([]chan error, 0, len(goroutines))
					for _, i := range goroutineIndices {
						errChans = append(errChans, goroutines[i].asyncOperation())
					}

					for i, errCh := range errChans {
						for err := <-errCh; err != nil; err = <-errCh {
							if !testutils.IsError(err, "context canceled|artificial timeout") {
								// Error to keep on draining errors but mark this test as failed.
								t.Errorf("unexpected error %v from %s goroutine", err, goroutines[goroutineIndices[i]].name)
							}
						}
					}
				})
			}
		}
	}
}
