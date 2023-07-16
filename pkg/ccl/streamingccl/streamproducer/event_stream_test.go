// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//	https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
package streamproducer

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkEventStream(b *testing.B) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	const (
		keySize        = 20
		valueSize      = 100
		eventCount     = 10000
		eventsPerBatch = 10

		flushWait = time.Duration(0)
	)

	randKey := roachpb.Key(randutil.RandBytes(rng, keySize))
	randVal := roachpb.Value{RawBytes: randutil.RandBytes(rng, valueSize)}
	tenPrefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(10))
	tenantSpan := roachpb.Span{
		Key:    tenPrefix,
		EndKey: tenPrefix.PrefixEnd(),
	}
	st := cluster.MakeTestingClusterSettings()
	m := mon.NewMonitor("test-monitor", mon.MemoryResource, nil, nil, 0, 0, st)
	m.Start(ctx, nil, mon.NewStandaloneBudget(128<<20))

	var subscribedSpans roachpb.SpanGroup
	subscribedSpans.Add(tenantSpan)
	sps := streampb.StreamPartitionSpec{}
	sps.Config.BatchByteSize = (keySize + valueSize) * eventsPerBatch
	sps.Spans = append(sps.Spans, tenantSpan)
	sps.InitialScanTimestamp = hlc.Timestamp{WallTime: 1}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		doneChan := make(chan struct{})
		e := &eventStream{
			streamID:        0,
			spec:            sps,
			subscribedSpans: subscribedSpans,
			rangefeedFactory: &fakeRangefeedFactory{
				tb:          b,
				eventCount:  eventCount,
				onDone:      doneChan,
				randomKey:   randKey,
				randomValue: randVal,
			},
			mon: m,
		}
		require.NoError(b, e.Start(ctx, nil))
		vr := newValueReader(b, e, flushWait)
		grp := ctxgroup.WithContext(ctx)
		grp.GoCtx(vr.run)
		<-doneChan
		e.Close(ctx)
		require.NoError(b, grp.Wait())
		require.Equal(b, eventCount/eventsPerBatch, vr.count)
	}
}

type valueReader struct {
	t              testing.TB
	waitMax        time.Duration
	valueGenerator eval.ValueGenerator

	rng *rand.Rand

	count int
}

func newValueReader(t testing.TB, v eval.ValueGenerator, max time.Duration) *valueReader {
	rng, _ := randutil.NewTestRand()
	return &valueReader{
		t:              t,
		waitMax:        max,
		valueGenerator: v,
		rng:            rng,
	}
}

func (v *valueReader) run(ctx context.Context) error {
	for {
		more, err := v.valueGenerator.Next(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
		v.count += 1
		v.wait()
	}
}

func (v *valueReader) wait() {
	if v.waitMax > 0 {
		time.Sleep(time.Duration(v.rng.Int63n(int64(v.waitMax))))
	}
}

type fakeRangefeedFactory struct {
	eventCount int
	onDone     chan struct{}
	tb         testing.TB

	// TODO(ssd): more realistic keys and values
	randomKey   roachpb.Key
	randomValue roachpb.Value
}

func (frf *fakeRangefeedFactory) New(
	name string, ts hlc.Timestamp, onValue rangefeed.OnValue, opts ...rangefeed.Option,
) rf {
	return &fakeRangefeed{
		tb:            frf.tb,
		eventCount:    frf.eventCount,
		onValue:       onValue,
		parentFactory: frf,
	}
}

type fakeRangefeed struct {
	tb            testing.TB
	eventCount    int
	onValue       rangefeed.OnValue
	parentFactory *fakeRangefeedFactory
}

func (f *fakeRangefeed) Start(ctx context.Context, _ []roachpb.Span) error {
	go func() {
		for i := 0; i < f.eventCount; i++ {
			f.onValue(ctx, &kvpb.RangeFeedValue{
				Key:   f.parentFactory.randomKey,
				Value: f.parentFactory.randomValue,
			})
		}
		close(f.parentFactory.onDone)
	}()
	return nil
}

func (f *fakeRangefeed) Close() {
	<-f.parentFactory.onDone
}
