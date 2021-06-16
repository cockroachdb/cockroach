// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colmeta_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colmeta"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamingMetadataHandler verifies that colmeta.StreamingMetadataHandler
// works correctly when used to propagate coldata.Batches from the input (the
// propagation of rowenc.EncDatumRows or the remote producer messages uses the
// same underlying internal methods, so there isn't much benefit in checking
// those here too).
//
// It works by spinning up an input reading goroutine (that is pushing some
// random batches into the handler) as well as some small number of streaming
// metadata producing goroutines. Random sleeps are injected throughout to
// simulate "hard" work within those goroutines to produce their data.
func TestStreamingMetadataHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()
	testMemMonitor := execinfra.NewTestMemMonitor(context.Background(), st)
	defer testMemMonitor.Stop(context.Background())
	testMemAcc := testMemMonitor.MakeBoundAccount()
	defer testMemAcc.Close(context.Background())
	testAllocator := colmem.NewAllocator(context.Background(), &testMemAcc, coldata.StandardColumnFactory)

	var handler colmeta.StreamingMetadataHandler
	var wg sync.WaitGroup
	typs := []*types.T{types.Int}

	// If gracefulShutdown is true, then the input is fully exhausted;
	// otherwise, the flow context is canceled prematurely.
	for _, gracefulShutdown := range []bool{true, false} {
		t.Run(fmt.Sprintf("gracefulShutdown=%t", gracefulShutdown), func(t *testing.T) {
			flowCtx, flowCtxCancel := context.WithCancel(context.Background())
			// Make the linter happy.
			defer flowCtxCancel()

			handler.Init()

			// Spin up the input reading goroutine.
			wg.Add(1)
			var sentBatches []coldata.Batch
			go func(producer colmeta.DataProducer) {
				defer wg.Done()
				defer log.Info(flowCtx, "data producer exited")
				defer producer.ProducerDone()

				<-producer.WaitForConsumer()
				log.Info(flowCtx, "data producer performed a handshake with the consumer")
				rng, _ := randutil.NewPseudoRand()
				numBatches := rng.Intn(16) + 1
				if !gracefulShutdown {
					// In case of an ungraceful shutdown scenario, use very
					// large number of batches so that the context cancellation
					// occurs before the input runs out of the batches to
					// return.
					numBatches = math.MaxInt64
				}
				input := coldatatestutils.NewRandomDataOp(
					testAllocator,
					rng,
					coldatatestutils.RandomDataOpArgs{
						DeterministicTyps: typs,
						NumBatches:        numBatches,
						BatchAccumulator: func(_ context.Context, b coldata.Batch, typs []*types.T) {
							// Inject random short sleep.
							time.Sleep(time.Duration(rng.Intn(10) * int(time.Millisecond)))
							sentBatches = append(sentBatches, coldatatestutils.CopyBatch(b, typs, coldata.StandardColumnFactory))
						},
					},
				)
				for {
					b := input.Next()
					if err := producer.SendBatch(flowCtx, b); err != nil {
						log.Info(flowCtx, "data producer noticed the context cancellation")
						return
					}
					if b.Length() == 0 {
						log.Info(flowCtx, "data producer sent a zero batch")
						return
					}
				}
			}(&handler)

			// Spin up some number of streaming metadata producing goroutines.
			var sentStreamingMeta []*execinfrapb.ProducerMetadata
			var sentStreamingMetaMu syncutil.Mutex
			globalRng, _ := randutil.NewPseudoRand()
			numStreamingGoroutines := globalRng.Intn(5) + 1
			for i := 0; i < numStreamingGoroutines; i++ {
				wg.Add(1)
				go func(streamingMeta colmeta.StreamingMetadataProducer, id int) {
					defer wg.Done()
					defer log.Infof(flowCtx, "streaming meta producer %d exited", id)
					log.Infof(flowCtx, "streaming meta producer %d started", id)
					rng, _ := randutil.NewPseudoRand()
					count := 0
					for {
						time.Sleep(time.Duration(rng.Intn(30) * int(time.Millisecond)))
						meta := execinfrapb.GetProducerMeta()
						meta.Err = errors.Newf("streaming meta %d from goroutine %d", count, id)
						if err := streamingMeta.SendStreamingMeta(flowCtx, meta); err != nil {
							log.Infof(flowCtx, "streaming meta producer %d failed to send streaming meta, exiting", id)
							return
						}
						sentStreamingMetaMu.Lock()
						sentStreamingMeta = append(sentStreamingMeta, meta)
						sentStreamingMetaMu.Unlock()
						count++
						// This goroutine will exit early with 10% probability.
						if rng.Float64() < 0.1 {
							log.Infof(flowCtx, "streaming meta producer %d finished early", id)
							return
						}
					}
				}(&handler, i)
			}

			if !gracefulShutdown {
				// In case of an ungraceful shutdown, cancel the flow context
				// after some noticeable amount of time.
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng, _ := randutil.NewPseudoRand()
					time.Sleep(time.Duration((250 + rng.Intn(750)) * int(time.Millisecond)))
					log.Info(flowCtx, "the flow context is canceled")
					flowCtxCancel()
					// The context cancellation error is not explicitly sent,
					// but the handler will propagate it to the consumer, so we
					// "fake" that propagation here.
					meta := execinfrapb.GetProducerMeta()
					meta.Err = flowCtx.Err()
					sentStreamingMetaMu.Lock()
					sentStreamingMeta = append(sentStreamingMeta, meta)
					sentStreamingMetaMu.Unlock()
				}()
			}

			consumer := colmeta.DataConsumer(&handler)
			consumer.ConsumerArrived()
			var receivedBatches []coldata.Batch
			var receivedMeta []*execinfrapb.ProducerMetadata
			for {
				batch, meta := consumer.NextBatchAndMeta()
				if batch == nil && meta == nil {
					break
				}
				if batch != nil {
					receivedBatches = append(receivedBatches, coldatatestutils.CopyBatch(batch, typs, coldata.StandardColumnFactory))
				} else {
					receivedMeta = append(receivedMeta, meta)
				}
			}
			consumer.ConsumerClosed()

			// Wait for all goroutines to exit and run the verification.
			wg.Wait()

			// In case of an ungraceful shutdown, there is some non-determinism
			// whether the very last batch was properly sent or not (it depends
			// on whether the context cancellation is observed when sending on
			// the nextCh or when blocking on producerBlock channel). If it
			// turns to be the latter case, we remove the last sent batch.
			if !gracefulShutdown && len(sentBatches) == len(receivedBatches)+1 {
				sentBatches = sentBatches[:len(sentBatches)-1]
			}

			// Check that all batches are received and in the correct order.
			require.Equal(t, len(sentBatches), len(receivedBatches))
			for i := range sentBatches {
				coldata.AssertEquivalentBatches(t, sentBatches[i], receivedBatches[i])
			}

			// Check that all of the streaming metadata is received, but in an
			// arbitrary order.
			require.Equal(t, len(sentStreamingMeta), len(receivedMeta))
			sort.Slice(sentStreamingMeta, func(i, j int) bool {
				return strings.Compare(sentStreamingMeta[i].Err.Error(), sentStreamingMeta[j].Err.Error()) < 0
			})
			sort.Slice(receivedMeta, func(i, j int) bool {
				return strings.Compare(receivedMeta[i].Err.Error(), receivedMeta[j].Err.Error()) < 0
			})
			require.Equal(t, sentStreamingMeta, receivedMeta)
		})
	}
}
