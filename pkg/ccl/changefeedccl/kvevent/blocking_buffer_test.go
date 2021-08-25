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
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
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

	// Arrange for mem buffer to notify us when it waits for resources.
	waitCh := make(chan struct{}, 1)
	notifyWait := func(ctx context.Context, poolName string, r quotapool.Request) {
		select {
		case waitCh <- struct{}{}:
		default:
		}
	}
	st := cluster.MakeTestingClusterSettings()
	buf := kvevent.NewMemBuffer(ba, &st.SV, &metrics, quotapool.OnWaitStart(notifyWait))
	defer func() {
		require.NoError(t, buf.Close(context.Background()))
	}()

	producerCtx, stopProducers := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)
	defer func() {
		_ = wg.Wait() // Ignore error -- this group returns context cancellation.
	}()

	// Start adding KVs to the buffer until we block.
	wg.GoCtx(func(ctx context.Context) error {
		rnd, _ := randutil.NewTestPseudoRand()
		for {
			err := buf.Add(ctx, kvevent.MakeKVEvent(makeKV(t, rnd), roachpb.Value{}, hlc.Timestamp{}))
			if err != nil {
				return nil
			}
		}
	})

	<-waitCh

	// Keep consuming events until we get pushback metrics updated.
	for metrics.BufferPushbackNanos.Count() == 0 {
		e, err := buf.Get(context.Background())
		require.NoError(t, err)
		a := e.DetachAlloc()
		a.Release(context.Background())
	}
	stopProducers()
}
