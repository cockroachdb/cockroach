// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent_test

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func BenchmarkMemBuffer(b *testing.B) {
	log.Infof(context.Background(), "b.N=%d", b.N)
	rng, _ := randutil.NewTestRand()
	eventPool := make([]kvevent.Event, 64<<10)
	for i := range eventPool {
		if i%25 == 0 {
			eventPool[i] = kvevent.MakeResolvedEvent(generateSpan(b, rng), hlc.Timestamp{}, jobspb.ResolvedSpan_NONE)
		} else {
			eventPool[i] = kvevent.MakeKVEvent(makeKV(b, rng), makeKV(b, rng).Value, hlc.Timestamp{})
		}
	}

	ba, release := getBoundAccountWithBudget(1 << 30)
	defer release()

	b.ResetTimer()
	b.ReportAllocs()

	metrics := kvevent.MakeMetrics(time.Minute)
	st := cluster.MakeTestingClusterSettings()

	buf := kvevent.NewMemBuffer(ba, &st.SV, &metrics)
	defer func() {
		require.NoError(b, buf.CloseWithReason(context.Background(), nil))
	}()

	addToBuff := func(ctx context.Context) error {
		for {
			event := eventPool[rng.Intn(len(eventPool))]
			err := buf.Add(ctx, event)
			if err != nil {
				return err
			}
		}
	}

	consumedN := make(chan struct{})
	consumeBuf := func(ctx context.Context) error {
		// <-time.After(5 * time.Second)
		consumed := 0
		for {
			e, err := buf.Get(ctx)
			if err != nil {
				return err
			}
			a := e.DetachAlloc()
			a.Release(ctx)
			consumed++
			if consumed == b.N {
				close(consumedN)
				return nil
			}
		}
	}

	producerCtx, stopProducers := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)

	// During backfill, we start 3*number of nodes producers; Simulate that.
	for i := 0; i < 32; i++ {
		wg.GoCtx(addToBuff)
	}
	// We only have 1 consumer.
	wg.GoCtx(consumeBuf)

	<-consumedN
	b.StopTimer() // Done with this benchmark after we consumed N events.
	stopProducers()
	_ = wg.Wait() // Ignore error -- this group returns context cancellation.
	log.Infof(context.Background(), "in=%d out=%d", metrics.BufferEntriesIn.Count(), metrics.BufferEntriesOut.Count())
}

func generateSpan(b *testing.B, rng *rand.Rand) roachpb.Span {
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
		b.Fatal("could not generate key")
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		b.Fatal("could not generate key")
	}

	return roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}
}
