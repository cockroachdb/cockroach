// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"go.uber.org/atomic"
)

// TestParallelPreprocessorContextCancellation verifies that the parallel event
// consumer routines do not leak during context cancellation.
func TestParallelPreprocessorContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rand, seed := randutil.NewPseudoRand()
	log.Infof(context.Background(), "seed %d", seed)
	ctx, cancel := context.WithCancel(context.Background())
	testProducer := newTestEventProducer(rand, 0, 32, true)
	_, err := newParallelEventProcessor(ctx, newTestConsumer, testProducer.getEvent, 32, 16)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
}

func TestParallelPreprocessorOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rand, seed := randutil.NewPseudoRand()
	log.Infof(context.Background(), "seed %d", seed)

	numEventsToRead := 1000
	numProcessors := 8
	processorQueueSize := 16
	keyspaceSize := 32

	// Since the parallel processor can buffer up to 2*numProcessors*processorQueueSize events,
	// be ready to produce that many extra events.
	numProducerEvents := numEventsToRead + 2*numProcessors*processorQueueSize

	ctx := context.Background()
	testProducer := newTestEventProducer(rand, numProducerEvents, keyspaceSize, false)
	processor, err := newParallelEventProcessor(ctx, newTestConsumer, testProducer.getEvent, numProcessors, processorQueueSize)
	if err != nil {
		t.Fatal(err)
	}

	seenEvents := make(map[string]int)
	for i := 0; i < numEventsToRead; i++ {
		event, _, err := processor.GetNextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// Ignore resolved spans.
		if event.Type() == kvevent.TypeKV {
			key, val := parseKV(event.KV())
			if _, ok := seenEvents[key]; !ok {
				seenEvents[key] = val
			}
			if val < seenEvents[key] {
				t.Fatalf("Order Violation. Key: %s, Val: %d, Previous Val: %d", key, val, seenEvents[key])
			}
			seenEvents[key] = val
		}
	}

	if err := processor.Close(); err != nil {
		t.Fatal(err)
	}
}

type testConsumer struct {
}

func (*testConsumer) ConsumeEvent(ctx context.Context, ev kvevent.Event) error {
	return nil
}

func (*testConsumer) PreProcessEvent(ctx context.Context, ev kvevent.Event) (consumeFunc, error) {
	return noopConsumer, nil
}

func newTestConsumer() (eventConsumer, error) {
	return &testConsumer{}, nil
}

type testEventProducer struct {
	queue          []kvevent.Event
	nextEventCount *atomic.Int32
	queueSize      int
	allowOverflow  bool
}

func newTestEventProducer(
	rand *rand.Rand, numEventsToProduce int, keyspaceSize int, allowOverflow bool,
) *testEventProducer {
	kvHistory := make(map[int]int)

	producer := &testEventProducer{
		queue:          make([]kvevent.Event, numEventsToProduce),
		nextEventCount: atomic.NewInt32(-1),
		queueSize:      numEventsToProduce,
		allowOverflow:  allowOverflow,
	}

	for i := 0; i < numEventsToProduce; i++ {

		// 95% of the time, create a kv event. Otherwise, create a resolved event.
		if rand.Int31()%20 != 0 {
			key := rand.Intn(keyspaceSize)
			if _, ok := kvHistory[key]; ok {
				kvHistory[key]++
			} else {
				kvHistory[key] = 0
			}
			kv := makeKV(key, kvHistory[key])
			producer.queue[i] = kvevent.MakeKVEvent(kv, kv.Value, hlc.Timestamp{})
		} else {
			producer.queue[i] = kvevent.MakeResolvedEvent(roachpb.Span{}, hlc.Timestamp{}, jobspb.ResolvedSpan_NONE)
		}
	}
	return producer
}

func (p *testEventProducer) getEvent(ctx context.Context) (kvevent.Event, error) {
	idx := int(p.nextEventCount.Add(1))
	if idx < p.queueSize {
		return p.queue[idx], nil
	}

	if !p.allowOverflow {
		panic("testEventProducer was expected to produce more events than specified")
	}

	return kvevent.MakeResolvedEvent(roachpb.Span{}, hlc.Timestamp{}, jobspb.ResolvedSpan_NONE), nil
}

const testTableID = 42

func makeKV(k int, v int) roachpb.KeyValue {
	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(testTableID),
		tree.NewDInt(tree.DInt(k)),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	return roachpb.KeyValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  []byte(strconv.Itoa(v)),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}
}

func parseKV(kv roachpb.KeyValue) (k string, v int) {
	val, err := strconv.Atoi(string(kv.Value.RawBytes))
	if err != nil {
		panic(err)
	}

	return kv.Key.String(), val
}
