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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkMemBuffer(b *testing.B) {
	rand, _ := randutil.NewTestRand()

	run := func() {
		ba, release := getBoundAccountWithBudget(4096)
		defer release()

		metrics := kvevent.MakeMetrics(time.Minute)

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
			require.NoError(b, buf.CloseWithReason(context.Background(), nil))
		}()

		producerCtx, stopProducers := context.WithCancel(context.Background())
		wg := ctxgroup.WithContext(producerCtx)
		defer func() {
			_ = wg.Wait() // Ignore error -- this group returns context cancellation.
		}()

		numRows := 0
		wg.GoCtx(func(ctx context.Context) error {
			for {
				err := buf.Add(ctx, kvevent.MakeResolvedEvent(generateSpan(b, rand), hlc.Timestamp{}, jobspb.ResolvedSpan_NONE))
				if err != nil {
					return err
				}
				numRows++
			}
		})

		<-waitCh
		writtenRows := numRows

		for i := 0; i < writtenRows; i++ {
			e, err := buf.Get(context.Background())
			if err != nil {
				b.Fatal("could not read from buffer")
			}
			a := e.DetachAlloc()
			a.Release(context.Background())
		}
		stopProducers()
	}

	for i := 0; i < b.N; i++ {
		run()
	}
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
