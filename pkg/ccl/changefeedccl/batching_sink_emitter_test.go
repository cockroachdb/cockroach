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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testingSinkClient struct {
	emittedBatches [][]messagePayload

	mu struct {
		syncutil.Mutex
		pendingEncodeErrors int
		pendingEmitErrors   int
	}
}

var _ SinkClient = (*testingSinkClient)(nil)

func (tc *testingSinkClient) Close() error {
	return nil
}

func (tc *testingSinkClient) EncodeBatch(topic string, msgs []messagePayload) (SinkPayload, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.pendingEncodeErrors > 0 {
		tc.mu.pendingEncodeErrors--
		return nil, errors.Errorf("injected encode error")
	}

	return msgs, nil
}

func (tc *testingSinkClient) EncodeResolvedMessage(p resolvedMessagePayload) (SinkPayload, error) {
	return p, nil
}

func (tc *testingSinkClient) EmitPayload(payload SinkPayload) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.pendingEmitErrors > 0 {
		tc.mu.pendingEmitErrors--
		return errors.Errorf("injected emit error")
	}

	msgs := payload.([]messagePayload)
	tc.emittedBatches = append(tc.emittedBatches, msgs)
	return nil
}

type testingBatchingEmitter struct {
	wrapped *batchingSinkEmitter
	t       *testing.T
	pool    testAllocPool
}

func (te *testingBatchingEmitter) EmitN(n int) {
	for i := 0; i < n; i++ {
		te.wrapped.Emit(makeSinkEvent(&te.pool))
	}
}

func (te *testingBatchingEmitter) Flush() {
	te.wrapped.Emit(newSinkFlushEvent())
}

func (te *testingBatchingEmitter) Next() []messagePayload {
	require.Nil(te.t, contextutil.RunWithTimeout(
		context.Background(),
		"testingBatchingEmitter.Next",
		timeout(),
		func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-te.wrapped.successCh:
				return nil
			}
		},
	))
	tc := te.wrapped.client.(*testingSinkClient)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	next := tc.emittedBatches[0]
	tc.emittedBatches = tc.emittedBatches[1:]
	return next
}

func (te *testingBatchingEmitter) ExpectErr() {
	require.Nil(te.t, contextutil.RunWithTimeout(
		context.Background(),
		"testingBatchingEmitter.ExpectErr",
		timeout(),
		func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-te.wrapped.errorCh:
				return nil
			}
		},
	))
}

func (te *testingBatchingEmitter) InjectEmitError(errorCount int) {
	tc := te.wrapped.client.(*testingSinkClient)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.pendingEmitErrors += errorCount
}

func (te *testingBatchingEmitter) InjectEncodeError(errorCount int) {
	tc := te.wrapped.client.(*testingSinkClient)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.pendingEncodeErrors += errorCount
}

func (te *testingBatchingEmitter) Empty() bool {
	tc := te.wrapped.client.(*testingSinkClient)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return len(tc.emittedBatches) == 0
}

func (te *testingBatchingEmitter) Close() {
	te.wrapped.Close()
	_ = te.wrapped.client.Close()
}

func makeTestingBatchEmitter(
	t *testing.T, config sinkBatchConfig, timeSource timeutil.TimeSource,
) testingBatchingEmitter {
	return testingBatchingEmitter{
		wrapped: makeBatchingSinkEmitter(
			context.Background(),
			&testingSinkClient{
				emittedBatches: make([][]messagePayload, 0),
			},
			config,
			retry.Options{},
			"test",
			make(chan int, 256),
			make(chan error, 1),
			timeSource,
			nilMetricsRecorderBuilder(false),
			&nilSinkPacer{},
		),
		t: t,
	}
}

func makeSinkEvent(pool *testAllocPool) *sinkEvent {
	return &sinkEvent{
		msg: messagePayload{
			key: []byte("[1001]"),
			val: []byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"key\":[1001],\"topic:\":\"foo\"}"),
		},
		alloc: pool.alloc(),
		mvcc:  zeroTS,
	}
}

func TestBatchingSinkEmitterMessageLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Default is flush every time
	emitter := makeTestingBatchEmitter(
		t,
		sinkBatchConfig{},
		timeutil.DefaultTimeSource{},
	)
	emitter.EmitN(1)
	require.Equal(t, 1, len(emitter.Next()))
	emitter.EmitN(2)
	require.Equal(t, 1, len(emitter.Next()))
	require.Equal(t, 1, len(emitter.Next()))
	emitter.Close()
	require.EqualValues(t, 0, emitter.pool.used())

	// Emit enough messages, flushes
	emitter = makeTestingBatchEmitter(
		t,
		sinkBatchConfig{
			Messages: 3,
		},
		timeutil.DefaultTimeSource{},
	)
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.EmitN(1)
	require.Equal(t, 3, len(emitter.Next()))

	emitter.Close()
	require.EqualValues(t, 0, emitter.pool.used())
}

func TestBatchingSinkEmitterSizeFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()

	row := makeSinkEvent(&testAllocPool{})
	rowSize := len(row.msg.key) + len(row.msg.val)

	emitter := makeTestingBatchEmitter(
		t,
		sinkBatchConfig{
			Bytes:    rowSize * 3,
			Messages: 5,
		},
		timeutil.DefaultTimeSource{},
	)
	defer emitter.Close()

	// Should flush once 3 messages are sent rather than 5 since the size limit
	// was reached
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.EmitN(1)
	require.Equal(t, 3, len(emitter.Next()))
}

func TestBatchingSinkEmitterTimedFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mt := timeutil.NewManualTime(timeutil.Now())
	frequency := 1 * time.Hour
	waitForTimerSet := func() {
		testutils.SucceedsSoon(t, func() error {
			// wait for the timer in batch worker to be set (1 hour from now, as specified by config) before advancing time.
			if len(mt.Timers()) == 1 && mt.Timers()[0] == mt.Now().Add(frequency) {
				return nil
			}
			return errors.New("Waiting for timer to be created by batch worker")
		})
	}
	emitter := makeTestingBatchEmitter(t, sinkBatchConfig{
		Frequency: jsonDuration(frequency),
	}, mt)
	defer emitter.Close()

	appendCount := 0
	emitter.wrapped.knobs.OnAppend = func(_ *sinkEvent) {
		appendCount += 1
	}

	emitter.EmitN(2)
	require.True(t, emitter.Empty())

	// Shouldn't emit if some but not enough time has passed
	waitForTimerSet()
	mt.Advance(30 * time.Minute)
	emitter.EmitN(1)
	require.True(t, emitter.Empty())

	testutils.SucceedsSoon(t, func() error {
		if appendCount == 3 {
			return nil
		}
		return errors.New("waiting for events to be batched")
	})

	// Should emit after the time has passed
	mt.Advance(30 * time.Minute)
	require.Equal(t, 3, len(emitter.Next()))

	// More time shouldn't cause anything to be emitted
	mt.Advance(120 * time.Minute)
	require.True(t, emitter.Empty())

	// Shouldn't emit successive messages until a new time interval has passed
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	waitForTimerSet()
	mt.Advance(30 * time.Minute)
	require.True(t, emitter.Empty())
	mt.Advance(30 * time.Minute)
	require.Equal(t, 1, len(emitter.Next()))
}

func TestBatchingSinkEmitterError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	emitter := makeTestingBatchEmitter(t, sinkBatchConfig{
		Messages: 2,
	}, timeutil.DefaultTimeSource{})

	emitter.EmitN(2)
	require.Equal(t, 2, len(emitter.Next()))

	// Ensure emit errors correctly cause failure
	emitter.EmitN(1)
	emitter.InjectEmitError(1)
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.ExpectErr()
	emitter.EmitN(2)
	require.True(t, emitter.Empty())
	emitter.Close()

	// Ensure encoding errors correctly cause failure
	emitter = makeTestingBatchEmitter(t, sinkBatchConfig{
		Messages: 2,
	}, timeutil.DefaultTimeSource{})
	emitter.EmitN(1)
	emitter.InjectEncodeError(1)
	emitter.EmitN(1)
	require.True(t, emitter.Empty())
	emitter.EmitN(2)
	require.True(t, emitter.Empty())
	emitter.ExpectErr()
	emitter.Close()
}

func TestBatchingSinkEmitterForceFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()

	emitter := makeTestingBatchEmitter(t, sinkBatchConfig{
		Messages: 10,
	}, timeutil.DefaultTimeSource{})
	defer emitter.Close()

	emitter.EmitN(5)
	emitter.Flush()
	testutils.SucceedsSoon(t, func() error {
		if !emitter.Empty() {
			return nil
		}
		return errors.New("Waiting for events to flush")
	})
	require.Equal(t, 5, len(emitter.Next()))
}

// TODO: Assert metrics are correct
