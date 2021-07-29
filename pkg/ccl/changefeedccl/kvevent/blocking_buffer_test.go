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
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func makeKV(t *testing.T, rnd *rand.Rand) roachpb.KeyValue {
	const tableID = 42

	key, err := rowenc.EncodeTableKey(
		keys.SystemSQLCodec.TablePrefix(tableID),
		randgen.RandDatumSimple(rnd, types.String),
		encoding.Ascending,
	)
	require.NoError(t, err)

	return roachpb.KeyValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  randutil.RandBytes(rnd, 256),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}
}

func getBoundAccountWithBudget(budget int64) (account mon.BoundAccount, cleanup func()) {
	mm := mon.NewMonitorWithLimit(
		"test-mm", mon.MemoryResource, budget,
		nil, nil,
		128 /* small allocation increment */, 100,
		cluster.MakeTestingClusterSettings())
	mm.Start(context.Background(), nil, mon.MakeStandaloneBudget(budget))
	return mm.MakeBoundAccount(), func() { mm.Stop(context.Background()) }
}

func TestBlockingBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metrics := kvevent.MakeMetrics(time.Minute)
	ba, release := getBoundAccountWithBudget(4096)
	defer release()

	buf := kvevent.NewMemBuffer(ba, &metrics)
	defer buf.Close(context.Background())

	producerCtx, stopProducers := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)
	defer wg.Wait() // Ignore error -- this group returns context cancellation.

	// Start 10 workers, each one adding events to the buffer.
	// Eventually, AddKV should block, waiting for resources.
	for i := 0; i < 10; i++ {
		wg.GoCtx(func(ctx context.Context) error {
			rnd, _ := randutil.NewTestPseudoRand()
			// Keep adding events until context cancelled.
			for {
				err := buf.AddKV(ctx, makeKV(t, rnd), roachpb.Value{}, hlc.Timestamp{})
				if err != nil {
					return nil
				}
			}
		})
	}

	// Keep consuming events until buffer enters pushback.
	for metrics.BufferPushbackNanos.Count() == 0 {
		_, err := buf.Get(context.Background())
		require.NoError(t, err)
	}
	stopProducers()
}
