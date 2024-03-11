// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type kvs = storageutils.KVs

var (
	pointKV = storageutils.PointKV
	rangeKV = storageutils.RangeKV
)
var (
	testKey            = roachpb.Key("/db1")
	testTxnID          = uuid.MakeV4()
	testIsolationLevel = isolation.Serializable
	testTxnTS          = hlc.Timestamp{Logical: 1}
	testNewClosedTs    = hlc.Timestamp{WallTime: 10, Logical: 2}
	testMinTs          = hlc.Timestamp{}
	testRSpan          = roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")}
	testSpan           = roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	testTs             = hlc.Timestamp{WallTime: 1}
	testStartKey       = roachpb.Key("a")
	testEndKey         = roachpb.Key("z")
	testValue          = []byte("1")
	sstKVs             = kvs{
		pointKV("a", 1, "1"),
		pointKV("b", 1, "2"),
		pointKV("c", 1, "3"),
		rangeKV("d", "e", 1, ""),
	}
)

func writeValueOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCWriteValueOp{
		Key:       testKey,
		Timestamp: testTxnTS,
	})
	var futureEvent kvpb.RangeFeedEvent
	futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		Key: testKey,
		Value: roachpb.Value{
			Timestamp: testTxnTS,
		},
	})
	expectedMemUsage += mvccLogicalOp + mvccWriteValueOp + int64(op.Size())
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(futureEvent.Size())
	return op, expectedMemUsage, expectedFutureMemUsage
}

func deleteRangeOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCDeleteRangeOp{
		StartKey:  testStartKey,
		EndKey:    testEndKey,
		Timestamp: testTs,
	})
	var futureEvent kvpb.RangeFeedEvent
	futureEvent.MustSetValue(&kvpb.RangeFeedDeleteRange{
		Span:      roachpb.Span{Key: testStartKey, EndKey: testEndKey},
		Timestamp: testTs,
	})
	expectedMemUsage += mvccLogicalOp + mvccDeleteRangeOp + int64(op.Size())
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedDeleteRangeOverhead + int64(futureEvent.Size())
	return op, expectedMemUsage, expectedFutureMemUsage
}

func writeIntentOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCWriteIntentOp{
		TxnID:           testTxnID,
		TxnKey:          testKey,
		TxnIsoLevel:     testIsolationLevel,
		TxnMinTimestamp: testMinTs,
		Timestamp:       testTxnTS,
	})
	expectedMemUsage += mvccLogicalOp + mvccWriteIntentOp + int64(op.Size())
	// no future event to publish
	return op, expectedMemUsage, 0
}

func updateIntentOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCUpdateIntentOp{
		TxnID:     testTxnID,
		Timestamp: hlc.Timestamp{Logical: 3},
	})
	expectedMemUsage += mvccLogicalOp + mvccUpdateIntentOp + int64(op.Size())
	// no future event to publish
	return op, expectedMemUsage, 0
}

func commitIntentOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCCommitIntentOp{
		Key:       testKey,
		Timestamp: hlc.Timestamp{Logical: 4},
		PrevValue: testValue,
	})

	var prevVal roachpb.Value
	if op.CommitIntent.PrevValue != nil {
		prevVal.RawBytes = op.CommitIntent.PrevValue
	}
	var futureEvent kvpb.RangeFeedEvent
	futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		Key: testKey,
		Value: roachpb.Value{
			Timestamp: op.CommitIntent.Timestamp,
		},
		PrevValue: prevVal,
	})

	expectedMemUsage += mvccLogicalOp + mvccCommitIntentOp + int64(op.Size())
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(futureEvent.Size())
	return op, expectedMemUsage, expectedFutureMemUsage
}

func abortIntentOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCAbortIntentOp{
		TxnID: testTxnID,
	})
	expectedMemUsage += mvccLogicalOp + mvccAbortIntentOp + int64(op.Size())
	// no future event to publish
	return op, expectedMemUsage, 0
}

func abortTxnOpEvent() (
	op enginepb.MVCCLogicalOp,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = makeLogicalOp(&enginepb.MVCCAbortTxnOp{
		TxnID: testTxnID,
	})
	expectedMemUsage += mvccLogicalOp + mvccAbortTxnOp + int64(op.Size())
	// no future event to publish
	return op, expectedMemUsage, 0
}

func generateLogicalOpEvent(
	typesOfOps string, span roachpb.RSpan, rts resolvedTimestamp,
) (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	var op enginepb.MVCCLogicalOp
	var mem, futureMem int64
	switch typesOfOps {
	case "write_value":
		op, mem, futureMem = writeValueOpEvent()
	case "delete_range":
		op, mem, futureMem = deleteRangeOpEvent()
	case "write_intent":
		op, mem, futureMem = writeIntentOpEvent()
	case "update_intent":
		op, mem, futureMem = updateIntentOpEvent()
	case "commit_intent":
		op, mem, futureMem = commitIntentOpEvent()
	case "abort_intent":
		op, mem, futureMem = abortIntentOpEvent()
	case "abort_txn":
		op, mem, futureMem = abortTxnOpEvent()
	}

	ev = event{
		ops: []enginepb.MVCCLogicalOp{op},
	}
	expectedMemUsage += eventOverhead + mem
	expectedFutureMemUsage += futureMem
	if rts.ConsumeLogicalOp(context.Background(), op) {
		ce := checkpointEvent(span, rts)
		expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead + int64(ce.Size())
	}
	return
}

func checkpointEvent(span roachpb.RSpan, rts resolvedTimestamp) kvpb.RangeFeedEvent {
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       span.AsRawSpanWithNoLocals(),
		ResolvedTS: rts.Get(),
	})
	return event
}

func generateCtEvent(
	span roachpb.RSpan, rts resolvedTimestamp,
) (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	ev = event{
		ct: ctEvent{
			Timestamp: testNewClosedTs,
		},
	}
	expectedMemUsage += eventOverhead
	if rts.ForwardClosedTS(context.Background(), testNewClosedTs) {
		ce := checkpointEvent(span, rts)
		expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead + int64(ce.Size())
	}
	return
}

func generateInitRTSEvent(
	span roachpb.RSpan, rts resolvedTimestamp,
) (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	ev = event{
		initRTS: true,
	}
	expectedMemUsage += eventOverhead
	if rts.Init(context.Background()) {
		ce := checkpointEvent(span, rts)
		expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead + int64(ce.Size())
	}
	return
}

func generateSSTEvent(
	data []byte,
) (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	ev = event{
		sst: &sstEvent{
			data: data,
			span: testSpan,
			ts:   testTs,
		},
	}

	var futureEvent kvpb.RangeFeedEvent
	futureEvent.MustSetValue(&kvpb.RangeFeedSSTable{
		Data:    data,
		Span:    testSpan,
		WriteTS: testTs,
	})
	expectedMemUsage += eventOverhead + sstEventOverhead + int64(cap(data)+cap(testSpan.Key)+cap(testSpan.EndKey))
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedSSTTableOverhead + int64(futureEvent.Size())
	return
}

func generateSyncEvent() (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	ev = event{
		sync: &syncEvent{c: make(chan struct{})},
	}
	expectedMemUsage += eventOverhead + syncEventOverhead
	return
}

func TestEventSizeCalculation(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	t.Run("empty_event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev := event{}
		require.Equal(t, eventOverhead, ev.MemUsage())
		require.Equal(t, int64(0), ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("write_value event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("write_value", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("delete_range event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("delete_range", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("write_intent event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("write_intent", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("update_intent event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("update_intent", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("commit_intent event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("commit_intent", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("abort_intent event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("abort_intent", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("abort_txn event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("abort_txn", testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("ct event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateCtEvent(testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("initRTS event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		generateLogicalOpEvent("write_intent", testRSpan, rts)
		ev, expectedMemUsage, expectedFutureMemUsage := generateInitRTSEvent(testRSpan, rts)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("sst event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		sst, _, _ := storageutils.MakeSST(t, st, sstKVs)
		ev, expectedMemUsage, expectedFutureMemUsage := generateSSTEvent(sst)
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})

	t.Run("sync event", func(t *testing.T) {
		rts := makeResolvedTimestamp(st)
		rts.Init(ctx)
		ev, expectedMemUsage, expectedFutureMemUsage := generateSyncEvent()
		require.Equal(t, expectedMemUsage, ev.MemUsage())
		require.Equal(t, expectedFutureMemUsage, ev.FutureMemUsage(ctx, testRSpan, rts))
	})
}
