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
	"strings"
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

type shutdownScenario struct {
	string
}

var (
	consumerDone      = shutdownScenario{"ConsumerDone"}
	consumerClosed    = shutdownScenario{"ConsumerClosed"}
	shutdownScenarios = []shutdownScenario{consumerDone, consumerClosed}
)

// TestVectorizedFlowShutdown tests that closing the materializer correctly
// closes all the infrastructure corresponding to the flow ending in that
// materializer. Namely, it creates an exec.HashRouter with 3 outputs as
// well as 3 exec.Inboxes (with the corresponding exec.Outboxes) and connects
// all of these 6 inputs to exec.UnorderedSynchronizer. This synchronizer is
// the input to the materializer. The test then proceeds onto reading some
// batches from the materializer and then closes it which should trigger
// closure of everything else.
// Additionally, we wrap three hash router outputs with dummy metadata sources
// and add a dummy metadata source to each of the Outboxes. In ConsumerDone
// shutdown scenario, we check that the metadata has been successfully
// propagated from all of the metadata sources.
func TestVectorizedFlowShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, shutdownOperation := range shutdownScenarios {
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
				synchronizerInputs   = make([]exec.Operator, 0, numHashRouterOutputs+numInboxes)
				metadataSources      = make([]distsqlpb.MetadataSource, 0, numHashRouterOutputs+numInboxes)
				metadataSourceID     = 0
			)

			hashRouter, hashRouterOutputs := exec.NewHashRouter(hashRouterInput, typs, []int{0}, numHashRouterOutputs)
			inboxes := make([]*colrpc.Inbox, numInboxes)
			for _, o := range hashRouterOutputs {
				ms := exec.NewVectorizedDummyMetadataSource(o, metadataSourceID)
				metadataSourceID++
				synchronizerInputs = append(synchronizerInputs, ms)
				metadataSources = append(metadataSources, ms.(*exec.VectorizedDummyMetadataSource))
			}
			for i := range inboxes {
				inboxes[i], err = colrpc.NewInbox(typs)
				require.NoError(t, err)
				synchronizerInputs = append(synchronizerInputs, exec.Operator(inboxes[i]))
				metadataSources = append(metadataSources, inboxes[i])
			}
			_, mockServer, addr, err := distsqlpb.StartMockDistSQLServer(
				hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, staticNodeID,
			)
			require.NoError(t, err)
			dialer := &mockDialer{addr: addr}
			flowID := distsqlpb.FlowID{UUID: uuid.MakeV4()}

			for i := range inboxes {
				batch := coldata.NewMemBatch(typs)
				batch.SetLength(coldata.BatchSize)
				ms := exec.NewVectorizedDummyMetadataSource(exec.NewRepeatableBatchSource(batch), metadataSourceID)
				metadataSourceID++
				outbox, err := colrpc.NewOutbox(exec.NewRepeatableBatchSource(batch), typs, []distsqlpb.MetadataSource{ms.(*exec.VectorizedDummyMetadataSource)})
				require.NoError(t, err)
				wg.Add(1)
				go func(id int) {
					outbox.Run(ctx, dialer, staticNodeID, flowID, distsqlpb.StreamID(id), func() {}, true /* shouldCloseConn */)
					wg.Done()
				}(i)

				serverStreamNotification := <-mockServer.InboundStreams
				serverStream := serverStreamNotification.Stream
				handleStreamErrCh[i] = make(chan error, 1)
				doneFn := func() { close(serverStreamNotification.Donec) }
				wg.Add(1)
				go func(id int, stream distsqlpb.DistSQL_FlowStreamServer, doneFn func()) {
					handleStreamErrCh[id] <- inboxes[id].RunWithStream(stream.Context(), stream)
					doneFn()
					wg.Done()
				}(i, serverStream, doneFn)
			}
			synchronizer := exec.NewUnorderedSynchronizer(synchronizerInputs, typs, &wg)
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

			ctx = materializer.Start(ctx)
			wg.Add(1)
			go func() {
				hashRouter.Run(ctx)
				wg.Done()
			}()

			for i := 0; i < 10; i++ {
				row, meta := materializer.Next()
				require.NotNil(t, row)
				require.Nil(t, meta)
			}
			switch shutdownOperation {
			case consumerDone:
				materializer.ConsumerDone()
				receivedMetaFromID := make([]bool, metadataSourceID)
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
				require.Equal(t, metadataSourceID, metaCount)
			case consumerClosed:
				materializer.ConsumerClosed()
			}

			for i := range inboxes {
				err = <-handleStreamErrCh[i]
				// We either should get no error or a context cancelation error.
				if err != nil {
					require.True(t, strings.Contains(err.Error(), "context canceled"))
				}
			}
			wg.Wait()
		})
	}
}

// TestVectorizedOutboxInboxShutdown sets up a chain as follows:
// exec.Outbox -> exec.Inbox -> materializer, and verifies that when
// materializer is closed, everything is properly shutdown. Additionally, in
// ConsumerDone shutdown scenario, it checks that the metadata from Outbox is
// correctly propagated to materializer.
func TestVectorizedOutboxInboxShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, shutdownOperation := range shutdownScenarios {
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

			var (
				err               error
				inbox             *colrpc.Inbox
				outbox            *colrpc.Outbox
				wg                sync.WaitGroup
				typs              = []types.T{types.Int64}
				semtyps           = []semtypes.T{*semtypes.Int}
				handleStreamErrCh = make(chan error)
				metadataSources   = make([]distsqlpb.MetadataSource, 0, 1)
				id                = 1
			)

			inbox, err = colrpc.NewInbox(typs)
			require.NoError(t, err)
			metadataSources = append(metadataSources, inbox)
			_, mockServer, addr, err := distsqlpb.StartMockDistSQLServer(
				hlc.NewClock(hlc.UnixNano, time.Nanosecond), stopper, staticNodeID,
			)
			require.NoError(t, err)
			dialer := &mockDialer{addr: addr}
			flowID := distsqlpb.FlowID{UUID: uuid.MakeV4()}

			batch := coldata.NewMemBatch(typs)
			batch.SetLength(coldata.BatchSize)
			ms := exec.NewVectorizedDummyMetadataSource(exec.NewRepeatableBatchSource(batch), id)
			outbox, err = colrpc.NewOutbox(exec.NewRepeatableBatchSource(batch), typs, []distsqlpb.MetadataSource{ms.(*exec.VectorizedDummyMetadataSource)})
			require.NoError(t, err)
			wg.Add(1)
			go func() {
				outbox.Run(ctx, dialer, staticNodeID, flowID, distsqlpb.StreamID(id), func() {}, true /* shouldCloseConn */)
				wg.Done()
			}()

			serverStreamNotification := <-mockServer.InboundStreams
			serverStream := serverStreamNotification.Stream
			doneFn := func() { close(serverStreamNotification.Donec) }
			wg.Add(1)
			go func(stream distsqlpb.DistSQL_FlowStreamServer, doneFn func()) {
				handleStreamErrCh <- inbox.RunWithStream(stream.Context(), stream)
				doneFn()
				wg.Done()
			}(serverStream, doneFn)

			materializer, err := newMaterializer(
				flowCtx,
				1, /* processorID */
				inbox,
				semtyps,
				[]int{0},
				&distsqlpb.PostProcessSpec{},
				nil, /* output */
				metadataSources,
				nil, /* outputStatsToTrace */
			)
			require.NoError(t, err)

			materializer.Start(ctx)

			for i := 0; i < 10; i++ {
				_, meta := materializer.Next()
				require.Nil(t, meta)
			}
			switch shutdownOperation {
			case consumerDone:
				materializer.ConsumerDone()
				row, meta := materializer.Next()
				require.Nil(t, row)
				require.NotNil(t, meta)
				require.NotNil(t, meta.Err)
				srcID, err := strconv.Atoi(meta.Err.Error())
				require.NoError(t, err)
				require.Equal(t, id, srcID)
				row, meta = materializer.Next()
				require.Nil(t, row)
				require.Nil(t, meta)
			case consumerClosed:
				materializer.ConsumerClosed()
			}

			err = <-handleStreamErrCh
			// We either should get no error or a context cancelation error.
			if err != nil {
				require.True(t, strings.Contains(err.Error(), "context canceled"))
			}
			wg.Wait()
		})
	}
}
