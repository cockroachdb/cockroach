// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

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

func writeValueOp(ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeValueOpWithKV(roachpb.Key("a"), ts, nil /* val */)
}

func writeIntentOpWithKey(txnID uuid.UUID, key []byte, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteIntentOp{
		TxnID:     txnID,
		TxnKey:    key,
		Timestamp: ts,
	})
}

func writeIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeIntentOpWithKey(txnID, nil /* key */, ts)
}

func updateIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCUpdateIntentOp{
		TxnID:     txnID,
		Timestamp: ts,
	})
}

func commitIntentOpWithKV(
	txnID uuid.UUID, key roachpb.Key, ts hlc.Timestamp, val []byte,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCCommitIntentOp{
		TxnID:     txnID,
		Key:       key,
		Timestamp: ts,
		Value:     val,
	})
}

func commitIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return commitIntentOpWithKV(txnID, roachpb.Key("a"), ts, nil /* val */)
}

func abortIntentOp(txnID uuid.UUID) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCAbortIntentOp{
		TxnID: txnID,
	})
}

func makeRangeFeedEvent(val interface{}) *roachpb.RangeFeedEvent {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(val)
	return &event
}

func rangeFeedValue(key roachpb.Key, val roachpb.Value) *roachpb.RangeFeedEvent {
	return makeRangeFeedEvent(&roachpb.RangeFeedValue{
		Key:   key,
		Value: val,
	})
}

func rangeFeedCheckpoint(span roachpb.Span, ts hlc.Timestamp) *roachpb.RangeFeedEvent {
	return makeRangeFeedEvent(&roachpb.RangeFeedCheckpoint{
		Span:       span,
		ResolvedTS: ts,
	})
}

func newTestProcessor(rtsIter engine.SimpleIterator) (*Processor, *stop.Stopper) {
	stopper := stop.NewStopper()
	p := NewProcessor(Config{
		AmbientContext:       log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:                hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		Span:                 roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		EventChanCap:         16,
		PushIntentsInterval:  0, // disable
		CheckStreamsInterval: 10 * time.Millisecond,
	})
	p.Start(stopper, rtsIter)
	return p, stopper
}

func TestProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	// Test processor without registrations.
	require.Equal(t, 0, p.Len())
	require.NotPanics(t, func() { p.ConsumeLogicalOps() })
	require.NotPanics(t, func() { p.ConsumeLogicalOps([]enginepb.MVCCLogicalOp{}...) })
	require.NotPanics(t, func() {
		txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
		p.ConsumeLogicalOps(
			writeValueOp(hlc.Timestamp{WallTime: 1}),
			writeIntentOp(txn1, hlc.Timestamp{WallTime: 2}),
			updateIntentOp(txn1, hlc.Timestamp{WallTime: 3}),
			commitIntentOp(txn1, hlc.Timestamp{WallTime: 4}),
			writeIntentOp(txn2, hlc.Timestamp{WallTime: 5}),
			abortIntentOp(txn2),
		)
		p.syncEventC()
		require.Equal(t, 0, p.rts.intentQ.Len())
	})
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{}) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{WallTime: 1}) })

	// Add a registration.
	r1Stream := newTestStream()
	r1ErrC := make(chan *roachpb.Error, 1)
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil, /* catchUpIter */
		r1Stream,
		r1ErrC,
	)
	require.Equal(t, 1, p.Len())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 1},
		)},
		r1Stream.Events(),
	)

	// Test checkpoint with one registration.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
	p.syncEventC()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 5},
		)},
		r1Stream.Events(),
	)

	// Test value with one registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("c"), hlc.Timestamp{WallTime: 6}, []byte("val")),
	)
	p.syncEventC()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedValue(
			roachpb.Key("c"),
			roachpb.Value{
				RawBytes:  []byte("val"),
				Timestamp: hlc.Timestamp{WallTime: 6},
			},
		)},
		r1Stream.Events(),
	)

	// Test value to non-overlapping key with one registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")),
	)
	p.syncEventC()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())

	// Test intent that is aborted with one registration.
	txn1 := uuid.MakeV4()
	// Write intent.
	p.ConsumeLogicalOps(writeIntentOp(txn1, hlc.Timestamp{WallTime: 6}))
	p.syncEventC()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	// Abort.
	p.ConsumeLogicalOps(abortIntentOp(txn1))
	p.syncEventC()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	require.Equal(t, 0, p.rts.intentQ.Len())

	// Test intent that is committed with one registration.
	txn2 := uuid.MakeV4()
	// Write intent.
	p.ConsumeLogicalOps(writeIntentOp(txn2, hlc.Timestamp{WallTime: 10}))
	p.syncEventC()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	// Forward closed timestamp. Should now be stuck on intent.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 15})
	p.syncEventC()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 9},
		)},
		r1Stream.Events(),
	)
	// Update the intent. Should forward resolved timestamp.
	p.ConsumeLogicalOps(updateIntentOp(txn2, hlc.Timestamp{WallTime: 12}))
	p.syncEventC()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 11},
		)},
		r1Stream.Events(),
	)
	// Commit intent. Should forward resolved timestamp to closed timestamp.
	p.ConsumeLogicalOps(
		commitIntentOpWithKV(txn2, roachpb.Key("e"), hlc.Timestamp{WallTime: 13}, []byte("ival")),
	)
	p.syncEventC()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{
			rangeFeedValue(
				roachpb.Key("e"),
				roachpb.Value{
					RawBytes:  []byte("ival"),
					Timestamp: hlc.Timestamp{WallTime: 13},
				},
			),
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
				hlc.Timestamp{WallTime: 15},
			),
		},
		r1Stream.Events(),
	)

	// Add another registration.
	r2Stream := newTestStream()
	r2ErrC := make(chan *roachpb.Error, 1)
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("c"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{WallTime: 1},
		nil, /* catchUpIter */
		r2Stream,
		r2ErrC,
	)
	require.Equal(t, 2, p.Len())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 15},
		)},
		r2Stream.Events(),
	)

	// Both registrations should see checkpoint.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 20})
	p.syncEventC()
	chEvent := []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
		hlc.Timestamp{WallTime: 20},
	)}
	require.Equal(t, chEvent, r1Stream.Events())
	require.Equal(t, chEvent, r2Stream.Events())

	// Test value with two registration that overlaps both.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("k"), hlc.Timestamp{WallTime: 22}, []byte("val2")),
	)
	p.syncEventC()
	valEvent := []*roachpb.RangeFeedEvent{rangeFeedValue(
		roachpb.Key("k"),
		roachpb.Value{
			RawBytes:  []byte("val2"),
			Timestamp: hlc.Timestamp{WallTime: 22},
		},
	)}
	require.Equal(t, valEvent, r1Stream.Events())
	require.Equal(t, valEvent, r2Stream.Events())

	// Test value that only overlaps the second registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("v"), hlc.Timestamp{WallTime: 23}, []byte("val3")),
	)
	p.syncEventC()
	valEvent2 := []*roachpb.RangeFeedEvent{rangeFeedValue(
		roachpb.Key("v"),
		roachpb.Value{
			RawBytes:  []byte("val3"),
			Timestamp: hlc.Timestamp{WallTime: 23},
		},
	)}
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	require.Equal(t, valEvent2, r2Stream.Events())

	// Cancel the first registration.
	r1Stream.Cancel()
	require.NotNil(t, <-r1ErrC)

	// Stop the processor with an error.
	pErr := roachpb.NewErrorf("stop err")
	p.StopWithErr(pErr)
	require.NotNil(t, <-r2ErrC)
}

func TestNilProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p *Processor

	// All of the following should be no-ops.
	require.Equal(t, 0, p.Len())
	require.NotPanics(t, func() { p.Stop() })
	require.NotPanics(t, func() { p.StopWithErr(nil) })
	require.NotPanics(t, func() { p.ConsumeLogicalOps() })
	require.NotPanics(t, func() { p.ConsumeLogicalOps(make([]enginepb.MVCCLogicalOp, 5)...) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{}) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{WallTime: 1}) })

	// The following should panic because they are not safe
	// to call on a nil Processor.
	require.Panics(t, func() { p.Start(stop.NewStopper(), nil) })
	require.Panics(t, func() { p.Register(roachpb.RSpan{}, hlc.Timestamp{}, nil, nil, nil) })
}

// TestProcessorInitializeResolvedTimestamp tests that when a Processor is given
// a resolved timestamp iterator, it doesn't initialize its resolved timestamp
// until it has consumed all intents in the iterator.
func TestProcessorInitializeResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	rtsIter := newTestIterator([]engine.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeKV("z", "val11", 4),
	})
	rtsIter.block = make(chan struct{})

	p, stopper := newTestProcessor(rtsIter)
	defer stopper.Stop(context.Background())

	// The resolved timestamp should not be initialized.
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Add a registration.
	r1Stream := newTestStream()
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil, /* catchUpIter */
		r1Stream,
		make(chan *roachpb.Error, 1),
	)
	require.Equal(t, 1, p.Len())

	// The resolved timestamp should still not be initialized.
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Forward the closed timestamp. The resolved timestamp should still
	// not be initialized.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 20})
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Let the scan proceed.
	close(rtsIter.block)
	<-rtsIter.done
	require.True(t, rtsIter.closed)

	// Synchronize the event channel then verify that the resolved timestamp is
	// initialized and that it's blocked on the oldest unresolved intent's txn
	// timestamp. Txn1 has intents at many times but the unresolvedIntentQueue
	// tracks its latest, which is 19, so the resolved timestamp is
	// 19.FloorPrev() = 18.
	p.syncEventC()
	require.True(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{WallTime: 18}, p.rts.Get())

	// The registration should have been informed of the new resolved timestamp.
	chEvent := []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
		hlc.Timestamp{WallTime: 18},
	)}
	require.Equal(t, chEvent, r1Stream.Events())
}

func TestProcessorCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	// The resolved timestamp should be initialized.
	p.syncEventC()
	require.True(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	catchUpIter := newTestIterator([]engine.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeKV("z", "val11", 4),
	})
	catchUpIter.block = make(chan struct{})

	// Add a registration with the catch-up iterator.
	r1Stream := newTestStream()
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("w")},
		hlc.Timestamp{WallTime: 2}, // too large to see key @ m
		catchUpIter,
		r1Stream,
		make(chan *roachpb.Error, 1),
	)
	require.Equal(t, 1, p.Len())

	// The registration should not have gotten an initial checkpoint.
	require.Nil(t, r1Stream.Events())

	// Forward the closed timestamp. The resolved timestamp should be
	// initialized and should move forward, but the registration should
	// still not get a checkpoint.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 20})
	p.syncEventC()
	require.True(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{WallTime: 20}, p.rts.Get())
	require.Nil(t, r1Stream.Events())

	// Let the scan proceed.
	close(catchUpIter.block)
	<-catchUpIter.done
	require.True(t, catchUpIter.closed)

	// Synchronize the event channel then verify that the registration's stream
	// was sent all values in its range and the resolved timestamp once the
	// catch-up scan was complete.
	p.syncEventC()
	expEvents := []*roachpb.RangeFeedEvent{
		rangeFeedValue(
			roachpb.Key("a"),
			roachpb.Value{RawBytes: []byte("val1"), Timestamp: hlc.Timestamp{WallTime: 10}},
		),
		rangeFeedValue(
			roachpb.Key("b"),
			roachpb.Value{RawBytes: []byte("val2"), Timestamp: hlc.Timestamp{WallTime: 0}},
		),
		rangeFeedValue(
			roachpb.Key("c"),
			roachpb.Value{RawBytes: []byte("val3"), Timestamp: hlc.Timestamp{WallTime: 11}},
		),
		rangeFeedValue(
			roachpb.Key("c"),
			roachpb.Value{RawBytes: []byte("val4"), Timestamp: hlc.Timestamp{WallTime: 9}},
		),
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val5"), Timestamp: hlc.Timestamp{WallTime: 20}},
		),
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val6"), Timestamp: hlc.Timestamp{WallTime: 19}},
		),
		rangeFeedValue(
			roachpb.Key("g"),
			roachpb.Value{RawBytes: []byte("val7"), Timestamp: hlc.Timestamp{WallTime: 0}},
		),
		rangeFeedValue(
			roachpb.Key("r"),
			roachpb.Value{RawBytes: []byte("val9"), Timestamp: hlc.Timestamp{WallTime: 4}},
		),
		rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 20},
		),
	}
	require.Equal(t, expEvents, r1Stream.Events())

	// Forward the closed timestamp. The registration should get a checkpoint
	// this time.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 25})
	p.syncEventC()
	require.True(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{WallTime: 25}, p.rts.Get())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 25},
		)},
		r1Stream.Events(),
	)

	// Add a second registration, this time with an iterator that will throw an
	// error.
	r2Stream := newTestStream()
	r2ErrC := make(chan *roachpb.Error, 1)
	errCatchUpIter := newErrorIterator(errors.New("iteration error"))
	errCatchUpIter.block = make(chan struct{})
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		errCatchUpIter,
		r2Stream,
		r2ErrC,
	)
	require.Equal(t, 2, p.Len())

	// Wait for the scan to hit the error and finish.
	close(errCatchUpIter.block)
	<-errCatchUpIter.done
	require.True(t, errCatchUpIter.closed)

	// The registration should throw an error and be unregistered.
	require.NotNil(t, <-r2ErrC)
	p.syncEventC()
	require.Equal(t, 1, p.Len())
}

// TestProcessorConcurrentStop tests that all methods in Processor's API
// correctly handle the processor concurrently shutting down. If they did
// not then it would be possible for them to deadlock.
func TestProcessorConcurrentStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const trials = 10
	for i := 0; i < trials; i++ {
		p, stopper := newTestProcessor(nil /* rtsIter */)

		var wg sync.WaitGroup
		wg.Add(6)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			s := newTestStream()
			errC := make(chan<- *roachpb.Error, 1)
			p.Register(p.Span, hlc.Timestamp{}, nil, s, errC)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.Len()
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.ConsumeLogicalOps(
				writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")),
			)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.ForwardClosedTS(hlc.Timestamp{WallTime: 2})
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.Stop()
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			stopper.Stop(context.Background())
		}()
		wg.Wait()
	}
}

// TestProcessorRegistrationObservesOnlyNewEvents tests that a registration
// observes only operations that are consumed after it has registered.
func TestProcessorRegistrationObservesOnlyNewEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	firstC := make(chan int64)
	regDone := make(chan struct{})
	regs := make(map[*testStream]int64)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := int64(1); i < 250; i++ {
			// Add a new registration every 10 ops.
			if i%10 == 0 {
				firstC <- i
				<-regDone
			}

			// Consume the logical op. Encode the index in the timestamp.
			p.ConsumeLogicalOps(writeValueOp(hlc.Timestamp{WallTime: i}))
		}
		p.syncEventC()
		close(firstC)
	}()
	go func() {
		defer wg.Done()
		for firstIdx := range firstC {
			// For each index, create a new registration. The first
			// operation is should see is firstIdx.
			s := newTestStream()
			regs[s] = firstIdx
			errC := make(chan *roachpb.Error, 1)
			p.Register(p.Span, hlc.Timestamp{}, nil, s, errC)
			regDone <- struct{}{}
		}
	}()
	wg.Wait()

	// Verify that no registrations were given operations
	// from before they registered.
	for s, expFirstIdx := range regs {
		events := s.Events()
		require.IsType(t, &roachpb.RangeFeedCheckpoint{}, events[0].GetValue())
		require.IsType(t, &roachpb.RangeFeedValue{}, events[1].GetValue())

		firstVal := events[1].GetValue().(*roachpb.RangeFeedValue)
		firstIdx := firstVal.Value.Timestamp.WallTime
		require.Equal(t, expFirstIdx, firstIdx)
	}
}
