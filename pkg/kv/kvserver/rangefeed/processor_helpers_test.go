// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type testBufferedStream struct {
	Stream
}

var _ BufferedStream = (*testBufferedStream)(nil)

func (tb *testBufferedStream) SendBuffered(
	e *kvpb.RangeFeedEvent, _ *SharedBudgetAllocation,
) error {
	// In production code, buffered stream would be responsible for properly using
	// and releasing the alloc. We just ignore memory accounting here.
	return tb.SendUnbuffered(e)
}

func (tb *testBufferedStream) Disconnect(err *kvpb.Error) {
	tb.Stream.SendError(err)
}

func makeLogicalOp(val interface{}) enginepb.MVCCLogicalOp {
	var op enginepb.MVCCLogicalOp
	op.MustSetValue(val)
	return op
}

func writeValueOpWithKV(key roachpb.Key, ts hlc.Timestamp, val []byte) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteValueOp{
		Key:       key,
		Timestamp: ts,
		Value:     val,
	})
}

func writeValueOpWithPrevValue(
	key roachpb.Key, ts hlc.Timestamp, val, prevValue []byte,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteValueOp{
		Key:       key,
		Timestamp: ts,
		Value:     val,
		PrevValue: prevValue,
	})
}

func writeValueOp(ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeValueOpWithKV(roachpb.Key("a"), ts, []byte("val"))
}

func writeIntentOpWithDetails(
	txnID uuid.UUID, key []byte, iso isolation.Level, minTS, ts hlc.Timestamp,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteIntentOp{
		TxnID:           txnID,
		TxnKey:          key,
		TxnIsoLevel:     iso,
		TxnMinTimestamp: minTS,
		Timestamp:       ts,
	})
}

func writeIntentOpFromMeta(txn enginepb.TxnMeta) enginepb.MVCCLogicalOp {
	return writeIntentOpWithDetails(
		txn.ID, txn.Key, txn.IsoLevel, txn.MinTimestamp, txn.WriteTimestamp)
}

func writeIntentOpWithKey(
	txnID uuid.UUID, key []byte, iso isolation.Level, ts hlc.Timestamp,
) enginepb.MVCCLogicalOp {
	return writeIntentOpWithDetails(txnID, key, iso, ts /* minTS */, ts)
}

func writeIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeIntentOpWithKey(txnID, nil /* key */, 0, ts)
}

func updateIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCUpdateIntentOp{
		TxnID:     txnID,
		Timestamp: ts,
	})
}

func commitIntentOpWithKV(
	txnID uuid.UUID,
	key roachpb.Key,
	ts hlc.Timestamp,
	val []byte,
	omitInRangefeeds bool,
	originID uint32,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCCommitIntentOp{
		TxnID:            txnID,
		Key:              key,
		Timestamp:        ts,
		Value:            val,
		OmitInRangefeeds: omitInRangefeeds,
		OriginID:         originID,
	})
}

func commitIntentOpWithPrevValue(
	txnID uuid.UUID, key roachpb.Key, ts hlc.Timestamp, val, prevValue []byte, omitInRangefeeds bool,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCCommitIntentOp{
		TxnID:            txnID,
		Key:              key,
		Timestamp:        ts,
		Value:            val,
		PrevValue:        prevValue,
		OmitInRangefeeds: omitInRangefeeds,
	})
}

func commitIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return commitIntentOpWithKV(txnID, roachpb.Key("a"), ts, nil /* val */, false /* omitInRangefeeds */, 0 /* originID */)
}

func abortIntentOp(txnID uuid.UUID) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCAbortIntentOp{
		TxnID: txnID,
	})
}

func abortTxnOp(txnID uuid.UUID) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCAbortTxnOp{
		TxnID: txnID,
	})
}

func deleteRangeOp(startKey, endKey roachpb.Key, timestamp hlc.Timestamp) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCDeleteRangeOp{
		StartKey:  startKey,
		EndKey:    endKey,
		Timestamp: timestamp,
	})
}

func makeRangeFeedEvent(val interface{}) *kvpb.RangeFeedEvent {
	var event kvpb.RangeFeedEvent
	event.MustSetValue(val)
	return &event
}

func rangeFeedValueWithPrev(key roachpb.Key, val, prev roachpb.Value) *kvpb.RangeFeedEvent {
	return makeRangeFeedEvent(&kvpb.RangeFeedValue{
		Key:       key,
		Value:     val,
		PrevValue: prev,
	})
}

func rangeFeedValue(key roachpb.Key, val roachpb.Value) *kvpb.RangeFeedEvent {
	return rangeFeedValueWithPrev(key, val, roachpb.Value{})
}

func rangeFeedCheckpoint(span roachpb.Span, ts hlc.Timestamp) *kvpb.RangeFeedEvent {
	return makeRangeFeedEvent(&kvpb.RangeFeedCheckpoint{
		Span:       span,
		ResolvedTS: ts,
	})
}

type storeOp struct {
	kv  storage.MVCCKeyValue
	txn *roachpb.Transaction
}

func makeTestEngineWithData(ops []storeOp) (storage.Engine, error) {
	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	for _, op := range ops {
		kv := op.kv
		_, err := storage.MVCCPut(ctx, engine, kv.Key.Key, kv.Key.Timestamp, roachpb.Value{RawBytes: kv.Value}, storage.MVCCWriteOptions{Txn: op.txn})
		if err != nil {
			engine.Close()
			return nil, err
		}
	}
	return engine, nil
}

const testProcessorEventCCap = 16
const testProcessorEventCTimeout = 10 * time.Millisecond

type processorTestHelper struct {
	span                     roachpb.RSpan
	rts                      *resolvedTimestamp
	syncEventC               func()
	sendSpanSync             func(*roachpb.Span)
	scheduler                *ClientScheduler
	toBufferedStreamIfNeeded func(s Stream) Stream
}

// syncEventAndRegistrations waits for all previously sent events to be
// processed *and* for all registration output loops to fully process their own
// internal buffers.
func (h *processorTestHelper) syncEventAndRegistrations() {
	h.sendSpanSync(&all)
}

// syncEventAndRegistrations waits for all previously sent events to be
// processed *and* for matching registration output loops to fully process their
// own internal buffers.
func (h *processorTestHelper) syncEventAndRegistrationsSpan(span roachpb.Span) {
	h.sendSpanSync(&span)
}

// triggerTxnPushUntilPushed will schedule PushTxnQueued events until pushedC
// indicates that a transaction push attempt has started by posting an event.
// If a push does not happen in 10 seconds, the attempt fails.
func (h *processorTestHelper) triggerTxnPushUntilPushed(t *testing.T, pushedC <-chan struct{}) {
	timeoutC := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if h.scheduler != nil {
			h.scheduler.Enqueue(PushTxnQueued)
		}
		select {
		case <-pushedC:
			return
		case <-ticker.C:
			// We keep sending events to avoid the situation where event arrives
			// but flag indicating that push is still running is not reset.
		case <-timeoutC:
			t.Fatal("failed to get txn push notification")
		}
	}
}

type rangefeedTestType bool

var (
	scheduledProcessorWithUnbufferedSender rangefeedTestType
	scheduledProcessorWithBufferedSender   rangefeedTestType
)

var testTypes = []rangefeedTestType{
	scheduledProcessorWithUnbufferedSender,
	scheduledProcessorWithBufferedSender,
}

func (t rangefeedTestType) String() string {
	switch t {
	case scheduledProcessorWithUnbufferedSender:
		return "scheduled"
	case scheduledProcessorWithBufferedSender:
		return "scheduled/registration=buffered"
	default:
		panic("unknown rangefeed test type")
	}
}

type testConfig struct {
	Config
	isc      IntentScannerConstructor
	feedType rangefeedTestType
}

type option func(*testConfig)

func withPusher(txnPusher TxnPusher) option {
	return func(config *testConfig) {
		config.PushTxnsAge = 50 * time.Millisecond
		config.TxnPusher = txnPusher
	}
}

func withRangefeedTestType(t rangefeedTestType) option {
	return func(config *testConfig) {
		config.feedType = t
	}
}

func withBudget(b *FeedBudget) option {
	return func(config *testConfig) {
		config.MemBudget = b
	}
}

func withMetrics(m *Metrics) option {
	return func(config *testConfig) {
		config.Metrics = m
	}
}

func withRtsScanner(scanner IntentScanner) option {
	return func(config *testConfig) {
		if scanner != nil {
			config.isc = func() IntentScanner {
				return scanner
			}
		}
	}
}

func withChanTimeout(d time.Duration) option {
	return func(config *testConfig) {
		config.EventChanTimeout = d
	}
}

func withChanCap(cap int) option {
	return func(config *testConfig) {
		config.EventChanCap = cap
	}
}

func withEventTimeout(timeout time.Duration) option {
	return func(config *testConfig) {
		config.EventChanTimeout = timeout
	}
}

func withSpan(span roachpb.RSpan) option {
	return func(config *testConfig) {
		config.Span = span
	}
}

func withSettings(st *cluster.Settings) option {
	return func(config *testConfig) {
		config.Settings = st
	}
}

func withPushTxnsIntervalAge(interval, age time.Duration) option {
	return func(config *testConfig) {
		config.PushTxnsAge = age
	}
}

// blockingScanner is a test intent scanner that allows test to track lifecycle
// of tasks.
//  1. it will always block on startup and will wait for block to be closed to
//     proceed
//  2. when closed it will close done channel to signal completion
type blockingScanner struct {
	wrapped IntentScanner

	block chan interface{}
	done  chan interface{}
}

func (s *blockingScanner) ConsumeIntents(
	ctx context.Context, startKey roachpb.Key, endKey roachpb.Key, consumer eventConsumer,
) error {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.wrapped.ConsumeIntents(ctx, startKey, endKey, consumer)
}

func (s *blockingScanner) Close() {
	s.wrapped.Close()
	close(s.done)
}

func makeIntentScanner(data []storeOp, span roachpb.RSpan) (*blockingScanner, func(), error) {
	engine, err := makeTestEngineWithData(data)
	if err != nil {
		return nil, nil, err
	}
	scanner, err := NewSeparatedIntentScanner(context.Background(), engine, span)
	if err != nil {
		return nil, nil, err
	}
	return &blockingScanner{
			wrapped: scanner,
			block:   make(chan interface{}),
			done:    make(chan interface{}),
		}, func() {
			engine.Close()
		}, nil
}

func newTestProcessor(
	t testing.TB, opts ...option,
) (Processor, *processorTestHelper, *stop.Stopper) {
	t.Helper()
	stopper := stop.NewStopper()
	st := cluster.MakeTestingClusterSettings()

	cfg := testConfig{
		Config: Config{
			RangeID:          2,
			Stopper:          stopper,
			Settings:         st,
			AmbientContext:   log.MakeTestingAmbientCtxWithNewTracer(),
			Clock:            hlc.NewClockForTesting(nil),
			Span:             roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
			EventChanTimeout: testProcessorEventCTimeout,
			EventChanCap:     testProcessorEventCCap,
			Metrics:          NewMetrics(),
		},
	}
	for _, o := range opts {
		o(&cfg)
	}
	sch := NewScheduler(SchedulerConfig{
		Workers:         1,
		PriorityWorkers: 1,
		Metrics:         NewSchedulerMetrics(time.Second),
	})
	require.NoError(t, sch.Start(context.Background(), stopper))
	cfg.Scheduler = sch
	// Also create a dummy priority processor to populate priorityIDs for
	// BenchmarkRangefeed. It should never be called.
	noop := func(e processorEventType) processorEventType {
		if e != Stopped {
			t.Errorf("unexpected event %s for noop priority processor", e)
		}
		return 0
	}
	require.NoError(t, sch.register(9, noop, true /* priority */))
	s := NewProcessor(cfg.Config)
	h := processorTestHelper{}
	switch p := s.(type) {
	case *ScheduledProcessor:
		h.rts = &p.rts
		h.span = p.Span
		h.syncEventC = p.syncEventC
		h.sendSpanSync = func(span *roachpb.Span) {
			p.syncSendAndWait(&syncEvent{c: make(chan struct{}), testRegCatchupSpan: span})
		}
		h.scheduler = &p.scheduler
		switch cfg.feedType {
		case scheduledProcessorWithUnbufferedSender:
			h.toBufferedStreamIfNeeded = func(s Stream) Stream {
				return s
			}
		case scheduledProcessorWithBufferedSender:
			h.toBufferedStreamIfNeeded = func(s Stream) Stream {
				return &testBufferedStream{Stream: s}
			}
		default:
			panic("unknown rangefeed test type")
		}
	default:
		panic("unknown processor type")
	}
	require.NoError(t, s.Start(stopper, cfg.isc))
	return s, &h, stopper
}
