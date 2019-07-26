// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockDialer struct {
	addr  net.Addr
	conns []*grpc.ClientConn
}

func (d *mockDialer) Dial(context.Context, roachpb.NodeID) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(d.addr.String(), grpc.WithInsecure())
	d.conns = append(d.conns, conn)
	return conn, err
}

// close must be called after the test.
func (d *mockDialer) close() {
	for _, conn := range d.conns {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}
}

type shutdownScenario struct {
	string
}

var (
	consumerDone   = shutdownScenario{"ConsumerDone"}
	consumerClosed = shutdownScenario{"ConsumerClosed"}
	// TODO(yuzefovich): use both scenarios.
	shutdownScenarios = []shutdownScenario{ /*consumerDone, */ consumerClosed}
)

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely, on a remote node, it creates an exec.HashRouter with 3
// outputs (with a corresponding to each Outbox) as well as 3 simple Outboxes;
// on a local node, it creates 6 exec.Inboxes that feed into an unordered
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
//            Remote Node             |            Another Remote Node            |         Local Node
//                                    |                                           |
//             -> output -> Outbox -> | -> Inbox -> |                             |
//            |                       |                                           |
// Hash Router -> output -> Outbox -> | -> Inbox -> |                             |
//            |                       |                                           |
//             -> output -> Outbox -> | -> Inbox -> |                             |
//                                    |              -> Synchronizer -> Outbox -> | -> Inbox -> materializer
//                          Outbox -> | -> Inbox -> |                             |
//                                    |                                           |
//                          Outbox -> | -> Inbox -> |                             |
//                                    |                                           |
//                          Outbox -> | -> Inbox -> |                             |
//
// Remote nodes are simulates by having separate contexts.
//
// Additionally, all Outboxes have a single metadata source. In ConsumerDone
// shutdown scenario, we check that the metadata has been successfully
// propagated from all of the metadata sources.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for run := 0; run < 10; run++ {
		for _, shutdownOperation := range shutdownScenarios {
			t.Run(fmt.Sprintf("shutdownScenario=%s", shutdownOperation.string), func(t *testing.T) {
				ctxLocal := context.Background()
				stopper := stop.NewStopper()
				defer stopper.Stop(ctxLocal)
				st := cluster.MakeTestingClusterSettings()
				evalCtx := tree.MakeTestingEvalContext(st)
				defer evalCtx.Stop(ctxLocal)
				flowCtx := &FlowCtx{
					EvalCtx:  &evalCtx,
					Settings: st,
				}

				rng, _ := randutil.NewPseudoRand()
				var (
					err             error
					wg              sync.WaitGroup
					typs            = []types.T{types.Int64}
					semtyps         = []semtypes.T{*semtypes.Int}
					hashRouterInput = exec.NewRandomDataOp(
						rng,
						exec.RandomDataOpArgs{
							DeterministicTyps: typs,
							// Set a high number of batches to ensure that the HashRouter is
							// very far from being finished when the flow is shut down.
							NumBatches: math.MaxInt64,
							Selection:  true,
						},
					)
					// TODO(yuzefovich): use 3 of each.
					numHashRouterOutputs = 1
					numInboxes           = numHashRouterOutputs + 1
					inboxes              = make([]*colrpc.Inbox, 0, numInboxes+1)
					handleStreamErrCh    = make([]chan error, numInboxes+1)
					synchronizerInputs   = make([]exec.Operator, 0, numInboxes)
					metadataSources      = make([]distsqlpb.MetadataSource, 0, numInboxes+1)
					streamID             = 0
				)

				addInbox := func() {
					inbox, err := colrpc.NewInbox(typs)
					require.NoError(t, err)
					inboxes = append(inboxes, inbox)
					metadataSources = append(metadataSources, inbox)
				}

				hashRouter, hashRouterOutputs := exec.NewHashRouter(hashRouterInput, typs, []int{0}, numHashRouterOutputs)
				for i := 0; i < numInboxes; i++ {
					addInbox()
					synchronizerInputs = append(synchronizerInputs, exec.Operator(inboxes[i]))
				}
				synchronizer := exec.NewUnorderedSynchronizer(synchronizerInputs, typs, &wg)
				_, mockServer, addr, err := distsqlpb.StartMockDistSQLServer(
					hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, staticNodeID,
				)
				require.NoError(t, err)
				dialer := &mockDialer{addr: addr}
				defer dialer.close()
				flowID := distsqlpb.FlowID{UUID: uuid.MakeV4()}

				runOutboxInbox := func(ctx context.Context, cancelFn context.CancelFunc, outboxInput exec.Operator, inbox *colrpc.Inbox, id int) {
					outbox, err := colrpc.NewOutbox(
						outboxInput,
						typs,
						[]distsqlpb.MetadataSource{
							distsqlpb.CallbackMetadataSource{
								DrainMetaCb: func(context.Context) []distsqlpb.ProducerMetadata {
									return []distsqlpb.ProducerMetadata{{Err: errors.Errorf("%d", id)}}
								},
							},
						})
					require.NoError(t, err)
					wg.Add(1)
					go func(id int) {
						log.VEventf(ctx, 2, "Outbox %d is running", id)
						outbox.Run(ctx, dialer, staticNodeID, flowID, distsqlpb.StreamID(id), cancelFn)
						wg.Done()
						log.VEventf(ctx, 2, "Outbox %d is done", id)
					}(id)

					require.NoError(t, err)
					serverStreamNotification := <-mockServer.InboundStreams
					serverStream := serverStreamNotification.Stream
					handleStreamErrCh[id] = make(chan error, 1)
					doneFn := func() { close(serverStreamNotification.Donec) }
					wg.Add(1)
					go func(id int, stream distsqlpb.DistSQL_FlowStreamServer, doneFn func()) {
						log.VEventf(ctx, 2, "Inbox %d is running", id)
						handleStreamErrCh[id] <- inbox.RunWithStream(stream.Context(), stream)
						doneFn()
						wg.Done()
						log.VEventf(ctx, 2, "Inbox %d is done", id)
					}(id, serverStream, doneFn)
				}

				var materializerInput exec.Operator
				anotherRemoteUsed := false
				if rng.Float64() < 0.5 {
					addInbox()
					materializerInput = inboxes[len(inboxes)-1]
					anotherRemoteUsed = true
				} else {
					materializerInput = synchronizer
				}

				ctxLocal, cancelLocal := context.WithCancel(ctxLocal)
				materializer, err := newMaterializer(
					flowCtx,
					1, /* processorID */
					materializerInput,
					semtyps,
					[]int{0},
					&distsqlpb.PostProcessSpec{},
					nil, /* output */
					metadataSources,
					nil, /* outputStatsToTrace */
					cancelLocal,
				)
				require.NoError(t, err)

				ctxRemote, cancelRemote := context.WithCancel(context.Background())
				for i := 0; i < numInboxes; i++ {
					if i < numHashRouterOutputs {
						runOutboxInbox(ctxRemote, cancelRemote, hashRouterOutputs[i], inboxes[i], streamID)
					} else {
						batch := coldata.NewMemBatch(typs)
						batch.SetLength(coldata.BatchSize)
						runOutboxInbox(ctxRemote, cancelRemote, exec.NewRepeatableBatchSource(batch), inboxes[i], streamID)
					}
					streamID++
				}
				if anotherRemoteUsed {
					ctxAnotherRemote, cancelAnotherRemote := context.WithCancel(context.Background())
					runOutboxInbox(ctxAnotherRemote, cancelAnotherRemote, synchronizer, inboxes[numInboxes], streamID)
				}
				wg.Add(1)
				go func() {
					log.VEventf(ctxRemote, 2, "HashRouter is running")
					hashRouter.Run(ctxRemote)
					wg.Done()
					log.VEventf(ctxRemote, 2, "HashRouter is done")
				}()

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
					require.Equal(t, streamID, metaCount)
				case consumerClosed:
					materializer.ConsumerClosed()
				}

				for i := range inboxes {
					err = <-handleStreamErrCh[i]
					// We either should get no error or a context cancellation error.
					if err != nil {
						require.True(t, testutils.IsError(err, "context canceled"), err)
					}
				}
				wg.Wait()
			})
		}
	}
}
