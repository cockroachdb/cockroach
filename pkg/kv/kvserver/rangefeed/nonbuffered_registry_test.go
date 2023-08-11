// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func newNonBufferedTestRegistration(
	span roachpb.Span,
	ts hlc.Timestamp,
	catchup storage.SimpleMVCCIterator,
	catchupSize int,
	withDiff bool,
) (*nonBufferedRegistration, <-chan struct{}, *testStream,) {
	s := newTestStream()
	drainedC := make(chan struct{}, 1)
	reg := newNonBufferedRegistration(
		span,
		ts,
		makeCatchUpIterator(catchup, span, ts),
		catchupSize,
		withDiff,
		NewMetrics(),
		s.buffered(func() {
			close(drainedC)
		}),
	)
	return reg, drainedC, s
}

func TestNonBufferedRegistrationBasic(t *testing.T) {
	ctx := context.Background()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val})

	// Verify that data send to registration ends up in output stream.
	noCatchupReg, _, stream := newNonBufferedTestRegistration(spAB, hlc.Timestamp{}, nil, 100, false)
	noCatchupReg.runCatchupScan(context.Background(), 3)

	noCatchupReg.publish(ctx, ev1, nil /* alloc */)
	noCatchupReg.publish(ctx, ev2, nil /* alloc */)
	require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, stream.Events())
	noCatchupReg.disconnect(nil)
}

// Verify that catch up scan buffers data until it processes initial iter.
func TestNonBufferedRegistrationCatchUpScan(t *testing.T) {
	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
		makeIntent("c", txn1, "txnKeyC", 15),
		makeProvisionalKV("c", "txnKeyC", 15),
		makeKV("c", "valC2", 11),
		makeKV("c", "valC1", 9),
		makeIntent("d", txn2, "txnKeyD", 21),
		makeProvisionalKV("d", "txnKeyD", 21),
		makeKV("d", "valD5", 20),
		makeKV("d", "valD4", 19),
		makeKV("d", "valD3", 16),
		makeKV("d", "valD2", 3),
		makeKV("d", "valD1", 1),
		makeKV("e", "valE3", 6),
		makeKV("e", "valE2", 5),
		makeKV("e", "valE1", 4),
		makeKV("f", "valF3", 7),
		makeKV("f", "valF2", 6),
		makeKV("f", "valF1", 5),
		makeKV("h", "valH1", 15),
		makeKV("m", "valM1", 1),
		makeIntent("n", txn1, "txnKeyN", 12),
		makeProvisionalKV("n", "txnKeyN", 12),
		makeIntent("r", txn1, "txnKeyR", 19),
		makeProvisionalKV("r", "txnKeyR", 19),
		makeKV("r", "valR1", 4),
		makeIntent("w", txn1, "txnKeyW", 3),
		makeProvisionalKV("w", "txnKeyW", 3),
		makeIntent("z", txn2, "txnKeyZ", 21),
		makeProvisionalKV("z", "txnKeyZ", 21),
		makeKV("z", "valZ1", 4),
	}, roachpb.Key("w"))

	r, _, stream := newNonBufferedTestRegistration(roachpb.Span{
		Key:    roachpb.Key("d"),
		EndKey: roachpb.Key("w"),
	}, hlc.Timestamp{WallTime: 4}, iter, 100, true)
	r.runCatchupScan(context.Background(), 3)

	require.True(t, iter.closed)
	require.NotZero(t, r.metrics.RangeFeedCatchUpScanNanos.Count())

	// Compare the events sent on the registration's Stream to the expected events.
	expEvents := []*kvpb.RangeFeedEvent{
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD3", 16),
			makeVal("valD2"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD4", 19),
			makeVal("valD3"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD5", 20),
			makeVal("valD4"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			makeValWithTs("valE2", 5),
			makeVal("valE1"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			makeValWithTs("valE3", 6),
			makeVal("valE2"),
		),
		rangeFeedValue(
			roachpb.Key("f"),
			makeValWithTs("valF1", 5),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			makeValWithTs("valF2", 6),
			makeVal("valF1"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			makeValWithTs("valF3", 7),
			makeVal("valF2"),
		),
		rangeFeedValue(
			roachpb.Key("h"),
			makeValWithTs("valH1", 15),
		),
	}
	require.Equal(t, expEvents, stream.Events())
}

func TestNonBufferedRegistrationWaitsCatchUp(t *testing.T) {
	ctx := context.Background()
	// prep iterator
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
		makeKV("c", "valC2", 11),
		makeKV("c", "valC1", 9),
	}, roachpb.Key("z"))
	// registration with catch up
	r, _, stream := newNonBufferedTestRegistration(roachpb.Span{
		Key:    roachpb.Key("a"),
		EndKey: roachpb.Key("z"),
	}, hlc.Timestamp{WallTime: 4}, iter, 100, true)
	// block sink
	unblock := stream.BlockSend()
	// start catch up in background
	go r.runCatchupScan(context.Background(), 3)
	// publish events to registration
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF1", 15),
	), nil)
	// unblock sink
	unblock()
	// wait for registration to close
	<-r.catchUpDrained
	// verify events
	expEvents := []*kvpb.RangeFeedEvent{
		rangeFeedValue(
			roachpb.Key("a"),
			makeValWithTs("valA1", 10),
		),
		rangeFeedValue(
			roachpb.Key("c"),
			makeValWithTs("valC1", 9),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("c"),
			makeValWithTs("valC2", 11),
			makeVal("valC1"),
		),
		rangeFeedValue(
			roachpb.Key("f"),
			makeValWithTs("valF1", 15),
		),
	}
	require.Equal(t, expEvents, stream.Events())
}

func TestNonBufferedRegistrationCatchUpOverflow(t *testing.T) {
	ctx := context.Background()
	// prep iterator
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
	}, roachpb.Key("z"))
	// registration with catch up
	r, _, stream := newNonBufferedTestRegistration(roachpb.Span{
		Key:    roachpb.Key("a"),
		EndKey: roachpb.Key("z"),
	}, hlc.Timestamp{WallTime: 4}, iter, 2, false)
	// block sink
	unblock := stream.BlockSend()
	// start catch up in background
	go r.runCatchupScan(context.Background(), 3)
	// publish events to registration
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF1", 15),
	), nil)
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF2", 16),
	), nil)
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF3", 17),
	), nil)
	// unblock sink
	unblock()
	// wait for registration to close
	<-r.catchUpDrained
	// verify events
	expEvents := []*kvpb.RangeFeedEvent{
		rangeFeedValue(
			roachpb.Key("a"),
			makeValWithTs("valA1", 10),
		),
		rangeFeedValue(
			roachpb.Key("f"),
			makeValWithTs("valF1", 15),
		),
		rangeFeedValue(
			roachpb.Key("f"),
			makeValWithTs("valF2", 16),
		),
	}
	require.Equal(t, expEvents, stream.Events())
	require.Error(t, stream.Done(t, 30*time.Second), "failed to get overflow error from stream")
}

func TestNonBufferedRegistrationCatchUpScanCancel(t *testing.T) {
	ctx := context.Background()
	// prep iterator
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
		makeKV("c", "valC2", 11),
		makeKV("c", "valC1", 9),
	}, roachpb.Key("z"))
	// registration with catch up
	r, _, stream := newNonBufferedTestRegistration(roachpb.Span{
		Key:    roachpb.Key("a"),
		EndKey: roachpb.Key("z"),
	}, hlc.Timestamp{WallTime: 4}, iter, 100, true)
	// block sink
	unblock := stream.BlockSend()
	// start catch up in background
	go r.runCatchupScan(context.Background(), 3)
	// publish events to registration
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF1", 15),
	), nil)
	// cancel registration asynchronously since it needs to wait for catch up
	// to drain before reporting.
	err := kvpb.NewError(errors.New("stopping registration"))
	go r.disconnect(err)
	// unblock sink
	unblock()
	// wait for registration to close
	<-r.catchUpDrained
	// verify error received
	require.Error(t, stream.Done(t, 30*time.Second), "failed to get disconnect error from stream")
}

// Verify that catch up scan can be stopped early.
func TestNonBufferedRegistrationCatchUpScanCancelBeforeStart(t *testing.T) {
	ctx := context.Background()
	// prep iterator
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
		makeKV("c", "valC2", 11),
		makeKV("c", "valC1", 9),
	}, roachpb.Key("z"))
	// registration with catch up
	r, _, stream := newNonBufferedTestRegistration(roachpb.Span{
		Key:    roachpb.Key("a"),
		EndKey: roachpb.Key("z"),
	}, hlc.Timestamp{WallTime: 4}, iter, 100, true)
	// start catch up in background
	go r.runCatchupScan(context.Background(), 3)
	// cancel registration before it has a chance to start
	r.disconnect(kvpb.NewError(errors.New("stopping registration")))
	// publish events to registration
	r.publish(ctx, rangeFeedValue(
		roachpb.Key("f"),
		makeValWithTs("valF1", 15),
	), nil)
	// wait for registration to close
	<-r.catchUpDrained
	// verify error
	require.Error(t, stream.Done(t, 30*time.Second), "failed to get disconnect error from stream")
}
