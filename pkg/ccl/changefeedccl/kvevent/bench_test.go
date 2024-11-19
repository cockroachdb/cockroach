// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkMemBuffer(b *testing.B) {
	log.Infof(context.Background(), "b.N=%d", b.N)

	eventPool := func() []kvevent.Event {
		const valSize = 16 << 10
		rng, _ := randutil.NewTestRand()
		p := make([]kvevent.Event, 32<<10)
		for i := range p {
			if rng.Int31()%20 == 0 {
				p[i] = kvevent.MakeResolvedEvent(generateRangeFeedCheckpoint(rng), jobspb.ResolvedSpan_NONE)
			} else {
				p[i] = kvevent.MakeKVEvent(makeRangeFeedEvent(rng, valSize, valSize))
			}
		}
		return p
	}()

	ba, release := getBoundAccountWithBudget(1 << 30)
	defer release()

	b.ResetTimer()
	b.ReportAllocs()

	metrics := kvevent.MakeMetrics(time.Minute).AggregatorBufferMetricsWithCompat
	st := cluster.MakeTestingClusterSettings()

	buf := kvevent.NewMemBuffer(ba, &st.SV, &metrics)
	defer func() {
		require.NoError(b, buf.CloseWithReason(context.Background(), nil))
	}()

	addToBuff := func(ctx context.Context) error {
		rng, _ := randutil.NewTestRand()
		for {
			err := buf.Add(ctx, eventPool[rng.Intn(len(eventPool))])
			if err != nil {
				return err
			}
		}
	}

	consumedN := make(chan struct{})
	consumeBuf := func(ctx context.Context) error {
		const flushBytes = 32 << 20
		var alloc kvevent.Alloc
		defer alloc.Release(ctx)

		for consumed := 0; ; {
			e, err := buf.Get(ctx)
			if err != nil {
				return err
			}
			a := e.DetachAlloc()
			alloc.Merge(&a)

			if alloc.Bytes() > flushBytes || e.Type() == kvevent.TypeFlush || consumed+int(alloc.Events()) >= b.N {
				consumed += int(alloc.Events())
				alloc.Release(ctx)
			}

			if consumed >= b.N {
				close(consumedN)
				return nil
			}
		}
	}

	producerCtx, stopProducers := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)

	// During backfill, we start 3*number of nodes producers; Simulate ~10 node cluster.
	for i := 0; i < 32; i++ {
		wg.GoCtx(addToBuff)
	}
	// We only have 1 consumer.
	wg.GoCtx(consumeBuf)

	<-consumedN
	b.StopTimer() // Done with this benchmark after we consumed N events.

	stopProducers()
	_ = wg.Wait() // Ignore error -- this group returns context cancellation.
}

func generateRangeFeedCheckpoint(rng *rand.Rand) *kvpb.RangeFeedEvent {
	start := rng.Intn(2 << 20)
	end := start + rng.Intn(2<<20)
	startDatum := tree.NewDInt(tree.DInt(start))
	endDatum := tree.NewDInt(tree.DInt(end))
	const tableID = 42

	startKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		startDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	return &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span: roachpb.Span{
				Key:    startKey,
				EndKey: endKey,
			},
		},
	}
}
