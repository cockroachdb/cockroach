// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type shutdownScenario struct {
	string
}

var (
	consumerDone      = shutdownScenario{"ConsumerDone"}
	consumerClosed    = shutdownScenario{"ConsumerClosed"}
	shutdownScenarios = []shutdownScenario{consumerDone, consumerClosed}
)

type callbackCloser struct {
	closeCb func() error
}

func (c callbackCloser) IdempotentClose(_ context.Context) error {
	return c.closeCb()
}

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely:
// - on a remote node, it creates an exec.HashRouter with 3 outputs (with a
// corresponding to each Outbox) as well as 3 standalone Outboxes;
// - on a local node, it creates 6 exec.Inboxes that feed into an unordered
// synchronizer which then outputs all the data into a materializer.
// The resulting scheme looks as follows:
//
//            Remote Node             |                  Local Node
//                                    |
//             -> output -> Outbox -> | -> Inbox -> |
//            |                       |
// Hash Router -> output -> Outbox -> | -> Inbox -> |
//            |                       |
//             -> output -> Outbox -> | -> Inbox -> |
//                                    |              -> Synchronizer -> materializer
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//                                    |
//                          Outbox -> | -> Inbox -> |
//
// Also, with 50% probability, another remote node with the chain of an Outbox
// and Inbox is placed between the synchronizer and materializer. The resulting
// scheme then looks as follows:
//
//            Remote Node             |            Another Remote Node             |         Local Node
//                                    |                                            |
//             -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
// Hash Router -> output -> Outbox -> | -> Inbox ->                                |
//            |                       |             |                              |
//             -> output -> Outbox -> | -> Inbox ->                                |
//                                    |             | -> Synchronizer -> Outbox -> | -> Inbox -> materializer
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//                                    |             |                              |
//                          Outbox -> | -> Inbox ->                                |
//
// Remote nodes are simulated by having separate contexts and separate outbox
// registries.
//
// Additionally, all Outboxes have a single metadata source. In ConsumerDone
// shutdown scenario, we check that the metadata has been successfully
// propagated from all of the metadata sources.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	_, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(
		hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, execinfra.StaticNodeID,
	)
	require.NoError(t, err)
	dialer := &execinfrapb.MockDialer{Addr: addr}
	defer dialer.Close()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	for run := 0; run < 10; run++ {
		for _, shutdownOperation := range shutdownScenarios {
			t.Run(fmt.Sprintf("shutdownScenario=%s", shutdownOperation.string), func(t *testing.T) {
				ctxLocal := context.Background()
				ctxRemote, cancelRemote := context.WithCancel(context.Background())
				// Linter says there is a possibility of "context leak" because
				// cancelRemote variable may not be used, so we defer the call to it.
				// This does not change anything about the test since we're blocking on
				// the wait group and we will call cancelRemote() below, so this defer
				// is actually a noop.
				defer cancelRemote()
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctxLocal)
				flowCtx := &execinfra.FlowCtx{
					EvalCtx: &evalCtx,
					Cfg:     &execinfra.ServerConfig{Settings: st},
				}
				rng, _ := randutil.NewPseudoRand()
				var (
					err             error
					wg              sync.WaitGroup
					typs            = []*types.T{types.Int}
					hashRouterInput = coldatatestutils.NewRandomDataOp(
						testAllocator,
						rng,
						coldatatestutils.RandomDataOpArgs{
							DeterministicTyps: typs,
							// Set a high number of batches to ensure that the HashRouter is
							// very far from being finished when the flow is shut down.
							NumBatches: math.MaxInt64,
							Selection:  true,
						},
					)
					numHashRouterOutputs = 3
					numInboxes           = numHashRouterOutputs + 3
					inboxes              = make([]*colrpc.Inbox, 0, numInboxes+1)
					handleStreamErrCh    = make([]chan error, numInboxes+1)
					synchronizerInputs   = make([]colexec.SynchronizerInput, 0, numInboxes)
					streamID             = 0
					addAnotherRemote     = rng.Float64() < 0.5
				)

				// Create an allocator for each output.
				allocators := make([]*colmem.Allocator, numHashRouterOutputs)
				diskAccounts := make([]*mon.BoundAccount, numHashRouterOutputs)
				for i := range allocators {
					acc := testMemMonitor.MakeBoundAccount()
					defer acc.Close(ctxRemote)
					allocators[i] = colmem.NewAllocator(ctxRemote, &acc, testColumnFactory)
					diskAcc := testDiskMonitor.MakeBoundAccount()
					diskAccounts[i] = &diskAcc
					defer diskAcc.Close(ctxRemote)
				}
				createMetadataSourceForID := func(id int) execinfrapb.MetadataSource {
					return execinfrapb.CallbackMetadataSource{
						DrainMetaCb: func(ctx context.Context) []execinfrapb.ProducerMetadata {
							return []execinfrapb.ProducerMetadata{{Err: errors.Errorf("%d", id)}}
						},
					}
				}
				// The first numHashRouterOutputs streamIDs are allocated to the
				// outboxes that drain these outputs. The outboxes will drain the router
				// outputs which should in turn drain the HashRouter that will return
				// this metadata.
				toDrain := make([]execinfrapb.MetadataSource, numHashRouterOutputs)
				for i := range toDrain {
					toDrain[i] = createMetadataSourceForID(i)
				}
				hashRouter, hashRouterOutputs := colexec.NewHashRouter(
					allocators,
					hashRouterInput,
					typs,
					[]uint32{0},
					64<<20,
					queueCfg,
					&colexecbase.TestingSemaphore{},
					diskAccounts,
					toDrain,
					nil, /* toClose */
				)
				for i := 0; i < numInboxes; i++ {
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxLocal)
					inbox, err := colrpc.NewInbox(
						colmem.NewAllocator(ctxLocal, &inboxMemAccount, testColumnFactory), typs, execinfrapb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					synchronizerInputs = append(
						synchronizerInputs,
						colexec.SynchronizerInput{
							Op:              colexecbase.Operator(inbox),
							MetadataSources: []execinfrapb.MetadataSource{inbox},
						},
					)
				}
				synchronizer := colexec.NewParallelUnorderedSynchronizer(synchronizerInputs, &wg)
				materializerMetadataSource := execinfrapb.MetadataSource(synchronizer)
				flowID := execinfrapb.FlowID{UUID: uuid.MakeV4()}

				// idToClosed keeps track of whether Close was called for a given id.
				idToClosed := struct {
					syncutil.Mutex
					mapping map[int]bool
				}{}
				idToClosed.mapping = make(map[int]bool)
				runOutboxInbox := func(
					ctx context.Context,
					cancelFn context.CancelFunc,
					outboxMemAcc *mon.BoundAccount,
					outboxInput colexecbase.Operator,
					inbox *colrpc.Inbox,
					id int,
					outboxMetadataSources []execinfrapb.MetadataSource,
				) {
					idToClosed.Lock()
					idToClosed.mapping[id] = false
					idToClosed.Unlock()
					outbox, err := colrpc.NewOutbox(
						colmem.NewAllocator(ctx, outboxMemAcc, testColumnFactory), outboxInput, typs, outboxMetadataSources,
						[]colexec.IdempotentCloser{callbackCloser{closeCb: func() error {
							idToClosed.Lock()
							idToClosed.mapping[id] = true
							idToClosed.Unlock()
							return nil
						}}},
					)

					require.NoError(t, err)
					wg.Add(1)
					go func(id int) {
						outbox.Run(ctx, dialer, execinfra.StaticNodeID, flowID, execinfrapb.StreamID(id), cancelFn)
						wg.Done()
					}(id)

					require.NoError(t, err)
					serverStreamNotification := <-mockServer.InboundStreams
					serverStream := serverStreamNotification.Stream
					handleStreamErrCh[id] = make(chan error, 1)
					doneFn := func() { close(serverStreamNotification.Donec) }
					wg.Add(1)
					go func(id int, stream execinfrapb.DistSQL_FlowStreamServer, doneFn func()) {
						handleStreamErrCh[id] <- inbox.RunWithStream(stream.Context(), stream)
						doneFn()
						wg.Done()
					}(id, serverStream, doneFn)
				}

				wg.Add(1)
				go func() {
					hashRouter.Run(ctxRemote)
					wg.Done()
				}()
				for i := 0; i < numInboxes; i++ {
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxRemote)
					if i < numHashRouterOutputs {
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, hashRouterOutputs[i], inboxes[i], streamID, []execinfrapb.MetadataSource{hashRouterOutputs[i]})
					} else {
						sourceMemAccount := testMemMonitor.MakeBoundAccount()
						defer sourceMemAccount.Close(ctxRemote)
						remoteAllocator := colmem.NewAllocator(ctxRemote, &sourceMemAccount, testColumnFactory)
						batch := remoteAllocator.NewMemBatch(typs)
						batch.SetLength(coldata.BatchSize())
						runOutboxInbox(ctxRemote, cancelRemote, &outboxMemAccount, colexecbase.NewRepeatableBatchSource(remoteAllocator, batch, typs), inboxes[i], streamID, []execinfrapb.MetadataSource{createMetadataSourceForID(streamID)})
					}
					streamID++
				}

				var materializerInput colexecbase.Operator
				ctxAnotherRemote, cancelAnotherRemote := context.WithCancel(context.Background())
				if addAnotherRemote {
					// Add another "remote" node to the flow.
					inboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer inboxMemAccount.Close(ctxAnotherRemote)
					inbox, err := colrpc.NewInbox(
						colmem.NewAllocator(ctxAnotherRemote, &inboxMemAccount, testColumnFactory),
						typs, execinfrapb.StreamID(streamID),
					)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					outboxMemAccount := testMemMonitor.MakeBoundAccount()
					defer outboxMemAccount.Close(ctxAnotherRemote)
					runOutboxInbox(ctxAnotherRemote, cancelAnotherRemote, &outboxMemAccount, synchronizer, inbox, streamID, []execinfrapb.MetadataSource{materializerMetadataSource, createMetadataSourceForID(streamID)})
					streamID++
					// There is now only a single Inbox on the "local" node which is the
					// only metadata source.
					materializerMetadataSource = inbox
					materializerInput = inbox
				} else {
					materializerInput = synchronizer
				}

				ctxLocal, cancelLocal := context.WithCancel(ctxLocal)
				materializerCalledClose := false
				materializer, err := colexec.NewMaterializer(
					flowCtx,
					1, /* processorID */
					materializerInput,
					typs,
					nil, /* output */
					[]execinfrapb.MetadataSource{materializerMetadataSource},
					[]colexec.IdempotentCloser{callbackCloser{closeCb: func() error {
						materializerCalledClose = true
						return nil
					}}}, /* toClose */
					nil, /* outputStatsToTrace */
					func() context.CancelFunc { return cancelLocal },
				)
				require.NoError(t, err)
				materializer.Start(ctxLocal)

				for i := 0; i < 10; i++ {
					row, meta := materializer.Next()
					require.NotNil(t, row)
					require.Nil(t, meta)
				}
				switch shutdownOperation {
				case consumerDone:
					materializer.ConsumerDone()
					receivedMetaFromID := make([]bool, streamID)
					metaCount := 0
					for {
						row, meta := materializer.Next()
						require.Nil(t, row)
						if meta == nil {
							break
						}
						metaCount++
						require.NotNil(t, meta.Err)
						id, err := strconv.Atoi(meta.Err.Error())
						require.NoError(t, err)
						require.False(t, receivedMetaFromID[id])
						receivedMetaFromID[id] = true
					}
					require.Equal(t, streamID, metaCount, fmt.Sprintf("received metadata from Outbox %+v", receivedMetaFromID))
				case consumerClosed:
					materializer.ConsumerClosed()
				}

				// When Outboxes are setup through vectorizedFlowCreator, the latter
				// keeps track of how many outboxes are on the node. When the last one
				// exits (and if there is no materializer on that node),
				// vectorizedFlowCreator will cancel the flow context of the node. To
				// simulate this, we manually cancel contexts of both remote nodes.
				cancelRemote()
				cancelAnotherRemote()

				for i := range inboxes {
					err = <-handleStreamErrCh[i]
					// We either should get no error or a context cancellation error.
					if err != nil {
						require.True(t, testutils.IsError(err, "context canceled"), err)
					}
				}
				wg.Wait()
				// Ensure all the outboxes called Close.
				for id, closed := range idToClosed.mapping {
					require.True(t, closed, "outbox with ID %d did not call Close on closers", id)
				}
				require.True(t, materializerCalledClose)
			})
		}
	}
}
