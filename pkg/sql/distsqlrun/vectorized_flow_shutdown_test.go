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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockDialer struct {
	addr net.Addr
}

func (d *mockDialer) Dial(context.Context, roachpb.NodeID) (*grpc.ClientConn, error) {
	return grpc.Dial(d.addr.String(), grpc.WithInsecure())
}

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely, it creates an exec.HashRouter with 3 outputs as
// well as 3 exec.Inboxes (with the corresponding exec.Outboxes) and connects
// all of these 6 inputs to exec.UnorderedSynchronizer. This synchronizer is
// the input to the materializer. The test then proceeds onto reading some
// batches from the materializer and then closes it which should trigger
// closure of everything else.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type shutdownScenario struct {
		string
	}
	var (
		consumerDone   = shutdownScenario{"ConsumerDone"}
		consumerClosed = shutdownScenario{"ConsumerClosed"}
		drainMeta      = shutdownScenario{"DrainMeta"}
	)

	for _, shutdownOperation := range []shutdownScenario{
		consumerDone,
		consumerClosed,
		drainMeta,
	} {
		t.Run(fmt.Sprintf("shutdownScenario=%s", shutdownOperation.string), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(ctx)
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
						// Set a high number of batches to ensure that the HashRouter is very
						// far from being finished when the flow is shut down.
						NumBatches: math.MaxInt64,
						Selection:  true,
					},
				)
				numHashRouterOutputs = 3
				numInboxes           = 3
				handleStreamErrCh    = make([]chan error, numInboxes)
			)

			hashRouter, hashRouterOutputs := exec.NewHashRouter(hashRouterInput, typs, []int{0}, numHashRouterOutputs)
			inboxes := make([]*colrpc.Inbox, numInboxes)
			synchronizerInputs := hashRouterOutputs
			for i := range inboxes {
				inboxes[i], err = colrpc.NewInbox(typs)
				require.NoError(t, err)
				synchronizerInputs = append(synchronizerInputs, exec.Operator(inboxes[i]))
			}
			synchronizer := exec.NewUnorderedSynchronizer(synchronizerInputs, typs, &sync.WaitGroup{})
			metadataSources := make([]distsqlpb.MetadataSource, 0, 4)
			metadataSources = append(metadataSources, hashRouter)
			for _, inbox := range inboxes {
				metadataSources = append(metadataSources, inbox)
			}
			materializer, err := newMaterializer(
				flowCtx,
				1, /* processorID */
				synchronizer,
				semtyps,
				[]int{0},
				&distsqlpb.PostProcessSpec{},
				nil, /* output */
				metadataSources,
				nil, /* outputStatsToTrace */
			)
			require.NoError(t, err)

			_, mockServer, addr, err := distsqlpb.StartMockDistSQLServer(
				hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, staticNodeID,
			)
			require.NoError(t, err)
			d := &mockDialer{addr: addr}
			flowID := distsqlpb.FlowID{UUID: uuid.MakeV4()}

			for i := range inboxes {
				batch := coldata.NewMemBatch(typs)
				outbox, err := colrpc.NewOutbox(exec.NewRepeatableBatchSource(batch), typs, nil /* metadataSources */)
				require.NoError(t, err)
				wg.Add(1)
				go func(id int) {
					outbox.Run(ctx, d, staticNodeID, flowID, distsqlpb.StreamID(id), func() {}, true /* shouldCloseConn */)
					wg.Done()
				}(i)

				serverStreamNotification := <-mockServer.InboundStreams
				serverStream := serverStreamNotification.Stream
				handleStreamErrCh[i] = make(chan error, 1)
				doneFn := func(donec chan<- error) func() {
					return func() { close(donec) }
				}(serverStreamNotification.Donec)
				wg.Add(1)
				go func(id int, stream distsqlpb.DistSQL_FlowStreamServer, doneFn func()) {
					handleStreamErrCh[id] <- inboxes[id].RunWithStream(stream.Context(), stream)
					if doneFn != nil {
						doneFn()
					}
					wg.Done()
				}(i, serverStream, doneFn)
			}

			ctx = materializer.Start(ctx)
			wg.Add(1)
			go func() {
				hashRouter.Run(ctx)
				wg.Done()
			}()

			for i := 0; i < 10; i++ {
				_, meta := materializer.Next()
				require.Nil(t, meta)
			}
			switch shutdownOperation {
			case consumerDone:
				materializer.ConsumerDone()
			case consumerClosed:
				materializer.ConsumerClosed()
			case drainMeta:
				materializer.trailingMetaCallback(ctx)
			}

			for i := range inboxes {
				<-handleStreamErrCh[i]
				// TODO(yuzefovich): do we want to check for an error here? I expected
				// that it would either be nil or context canceled, but TeamCity ran
				// into "rpc error: code = Internal desc = grpc: failed to unmarshal
				// the received message proto: illegal wireType 7".
			}
			wg.Wait()
		})
	}
}
