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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	testSpan           = roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	testTs             = hlc.Timestamp{WallTime: 1}
	testStartKey       = roachpb.Key("a")
	testEndKey         = roachpb.Key("z")
	testValue          = []byte("1")
	testPrevValue      = []byte("1234")
	testSSTKVs         = kvs{
		pointKV("a", 1, "1"),
		pointKV("b", 1, "2"),
		pointKV("c", 1, "3"),
		rangeKV("d", "e", 1, ""),
	}
)

type testData struct {
	numOfLogicalOps  int
	kvs              []interface{}
	span             roachpb.Span
	key              roachpb.Key
	timestamp        hlc.Timestamp
	value            []byte
	prevValue        []byte
	startKey, endKey roachpb.Key
	txnID            uuid.UUID
	txnKey           []byte
	txnIsoLevel      isolation.Level
	txnMinTimestamp  hlc.Timestamp
	omitInRangefeeds bool
}

func generateStaticTestdata() testData {
	return testData{
		kvs:              testSSTKVs,
		span:             testSpan,
		key:              testKey,
		timestamp:        testTs,
		value:            testValue,
		prevValue:        testPrevValue,
		startKey:         testStartKey,
		endKey:           testEndKey,
		txnID:            testTxnID,
		txnKey:           testKey,
		txnIsoLevel:      testIsolationLevel,
		txnMinTimestamp:  testTs,
		omitInRangefeeds: false,
	}
}

// TestEventSizeCalculation tests the memory usage of events. This test serves
// as a reminder to update memory accounting MemUsage calculations when new
// fields are added to event or  RangeFeedEvent. If this test fails, you need to
// make sure MemUsage is updated accordingly to account the new field. If this
// new field is a slice, map, pointer, string, or a nested struct, you need to
// be careful and account the memory usage of the additional underlying data
// structure. Otherwise, you can simply update the expected values in this test.
func TestEventSizeCalculation(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	data := generateStaticTestdata()

	t.Run("write_value event", func(t *testing.T) {
		key := data.key
		timestamp := data.timestamp
		value := data.value
		prevValue := data.prevValue
		op := writeValueOpWithPrevValue(key, timestamp, value, prevValue)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(241), mem)
		//var prevVal roachpb.Value
		//if prevValue != nil {
		//	prevVal.RawBytes = prevValue
		//}
		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		//	Key: key,
		//	Value: roachpb.Value{
		//		RawBytes:  value,
		//		Timestamp: timestamp,
		//	},
		//	PrevValue: prevVal,
		//})
		futureMemUsage := futureEventBaseOverhead + rangefeedValueOverhead +
			int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue))
		require.Equal(t, int64(201), futureMemUsage)
		require.Greater(t, mem, futureMemUsage)
	})

	t.Run("delete_range event", func(t *testing.T) {
		startKey := data.startKey
		endKey := data.endKey
		timestamp := data.timestamp
		op := deleteRangeOp(startKey, endKey, timestamp)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(202), mem)
		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedDeleteRange{
		//	Span:      roachpb.Span{Key: startKey, EndKey: endKey},
		//	Timestamp: testTs,
		//})
		futureMemUsage := futureEventBaseOverhead + rangefeedValueOverhead +
			int64(cap(startKey)) + int64(cap(endKey))
		require.Equal(t, int64(194), futureMemUsage)
		require.Greater(t, mem, futureMemUsage)
	})

	t.Run("write_intent event", func(t *testing.T) {
		txnID := data.txnID
		txnKey := data.txnKey
		txnIsoLevel := data.txnIsoLevel
		txnMinTimestamp := data.txnMinTimestamp
		timestamp := data.timestamp
		op := writeIntentOpWithDetails(txnID, txnKey, txnIsoLevel, txnMinTimestamp, timestamp)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(236), mem)
		// No future events to publish.
	})

	t.Run("update_intent event", func(t *testing.T) {
		txnID := data.txnID
		timestamp := data.timestamp
		op := updateIntentOp(txnID, timestamp)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(184), mem)
		// No future event to publish.
	})

	t.Run("commit_intent event", func(t *testing.T) {
		txnID := data.txnID
		key := data.key
		timestamp := data.timestamp
		value := data.value
		prevValue := data.prevValue
		omitInRangefeeds := data.omitInRangefeeds

		op := commitIntentOpWithPrevValue(txnID, key, timestamp, value, prevValue, omitInRangefeeds)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(273), mem)

		//var prevVal roachpb.Value
		//if prevValue != nil {
		//	prevVal.RawBytes = prevValue
		//}
		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		//	Key: key,
		//	Value: roachpb.Value{
		//		RawBytes:  value,
		//		Timestamp: timestamp,
		//	},
		//	PrevValue: prevVal,
		//})

		futureMemUsage := futureEventBaseOverhead + rangefeedValueOverhead +
			int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue))
		require.Equal(t, int64(201), futureMemUsage)
		require.Greater(t, mem, futureMemUsage)
	})

	t.Run("abort_intent event", func(t *testing.T) {
		txnID := data.txnID
		op := abortIntentOp(txnID)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(168), mem)
		// No future event to publish.
	})

	t.Run("abort_txn event", func(t *testing.T) {
		txnID := data.txnID
		op := abortTxnOp(txnID)
		ev := event{ops: []enginepb.MVCCLogicalOp{op}}
		mem := ev.MemUsage()
		require.Equal(t, int64(168), mem)
		// No future event to publish.
	})

	t.Run("ct event", func(t *testing.T) {
		ev := event{ct: ctEvent{Timestamp: data.timestamp}}
		require.Equal(t, int64(80), eventOverhead)

		mem := ev.MemUsage()
		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedCheckpoint{
		//	Span:       p.Span.AsRawSpanWithNoLocals(),
		//	ResolvedTS: p.rts.Get(),
		//})
		futureMemUsage := futureEventBaseOverhead + rangefeedCheckpointOverhead
		require.Equal(t, int64(152), mem)
		require.Equal(t, int64(152), futureMemUsage)
		require.Greater(t, futureMemUsage, eventOverhead)
	})

	t.Run("initRTS event", func(t *testing.T) {
		ev := event{initRTS: true}
		require.Equal(t, int64(80), eventOverhead)

		mem := ev.MemUsage()
		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedCheckpoint{
		//	Span:       p.Span.AsRawSpanWithNoLocals(),
		//	ResolvedTS: p.rts.Get(),
		//})
		futureMemUsage := futureEventBaseOverhead + rangefeedCheckpointOverhead
		require.Equal(t, int64(152), mem)
		require.Equal(t, int64(152), futureMemUsage)
		require.Greater(t, futureMemUsage, eventOverhead)
	})

	t.Run("sst event", func(t *testing.T) {
		span := data.span
		timestamp := data.timestamp
		sst, _, _ := storageutils.MakeSST(t, st, data.kvs)
		ev := event{
			sst: &sstEvent{
				data: sst,
				span: span,
				ts:   timestamp,
			},
		}
		mem := ev.MemUsage()
		curr := eventOverhead + sstEventOverhead +
			int64(cap(sst)+cap(data.span.Key)+cap(data.span.EndKey))
		require.Equal(t, int64(1962), curr)

		//var futureEvent kvpb.RangeFeedEvent
		//futureEvent.MustSetValue(&kvpb.RangeFeedSSTable{
		//	Data:    sst,
		//	Span:    data.span,
		//	WriteTS: data.timestamp,
		//})
		futureMemUsage := futureEventBaseOverhead + rangefeedSSTTableOverhead +
			int64(cap(sst)+cap(data.span.Key)+cap(data.span.EndKey))
		require.Equal(t, int64(1970), futureMemUsage)
		require.Equal(t, int64(1970), mem)
		require.Greater(t, futureMemUsage, curr)
	})

	t.Run("sync event", func(t *testing.T) {
		ev := event{sync: &syncEvent{c: make(chan struct{})}}
		mem := ev.MemUsage()
		require.Equal(t, int64(96), mem)
		// No future event to publish.
	})
}

func generateRandomizedTs(rand *rand.Rand) hlc.Timestamp {
	// Avoid generating zero timestamp which will equal to an empty event.
	return hlc.Timestamp{WallTime: int64(rand.Intn(100)) + 1}
}

func generateRandomizedBytes(rand *rand.Rand) []byte {
	const tableID = 42
	dataTypes := []*types.T{types.String, types.Int, types.Decimal, types.Bytes, types.Bool, types.Date, types.Timestamp, types.Float}
	randType := dataTypes[rand.Intn(len(dataTypes))]

	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		randgen.RandDatumSimple(rand, randType),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return key
}

func generateStartAndEndKey(rand *rand.Rand) (roachpb.Key, roachpb.Key) {
	start := rand.Intn(2 << 20)
	end := start + rand.Intn(2<<20)
	startDatum := tree.NewDInt(tree.DInt(start))
	endDatum := tree.NewDInt(tree.DInt(end))
	const tableID = 42

	startKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		startDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return startKey, endKey
}

func generateRandomizedTxnId(rand *rand.Rand) uuid.UUID {
	var txnID uuid.UUID
	n := rand.Intn(100)
	if n == 0 {
		// rand.Intn(0) panics
		n = 1
	}
	i := rand.Intn(n)
	txnID.DeterministicV4(uint64(i), uint64(n))
	return txnID
}

func generateRandomizedSpan(rand *rand.Rand) roachpb.RSpan {
	startKey, endKey := generateStartAndEndKey(rand)
	return roachpb.RSpan{
		Key:    roachpb.RKey(startKey),
		EndKey: roachpb.RKey(endKey),
	}
}

func generateRandomTestData(rand *rand.Rand) testData {
	startKey, endkey := generateStartAndEndKey(rand)
	return testData{
		// avoid 0 empty event
		numOfLogicalOps:  rand.Intn(100) + 1,
		kvs:              testSSTKVs,
		span:             generateRandomizedSpan(rand).AsRawSpanWithNoLocals(),
		key:              generateRandomizedBytes(rand),
		timestamp:        generateRandomizedTs(rand),
		value:            generateRandomizedBytes(rand),
		startKey:         startKey,
		endKey:           endkey,
		txnID:            generateRandomizedTxnId(rand),
		txnKey:           generateRandomizedBytes(rand),
		txnIsoLevel:      isolation.Levels()[rand.Intn(len(isolation.Levels()))],
		txnMinTimestamp:  generateRandomizedTs(rand),
		omitInRangefeeds: rand.Intn(2) == 1,
	}
}

func generateLogicalOpEvents(data testData) (ev event, expectedMemUsage int64) {
	var ops []enginepb.MVCCLogicalOp
	var mem int64
	expectedMemUsage += eventOverhead
	for i := 0; i < data.numOfLogicalOps; i++ {
		var op enginepb.MVCCLogicalOp
		switch i % 7 {
		case 0:
			op = writeValueOpWithPrevValue(data.key, data.timestamp, data.value, data.prevValue)
			mem = mvccWriteValueOp + int64(cap(data.key)) + int64(cap(data.value)) + int64(cap(data.prevValue))
		case 1:
			op = deleteRangeOp(data.startKey, data.endKey, data.timestamp)
			mem = mvccDeleteRangeOp + int64(cap(data.startKey)) + int64(cap(data.endKey))
		case 2:
			op = writeIntentOpWithDetails(data.txnID, data.txnKey, data.txnIsoLevel, data.txnMinTimestamp, data.timestamp)
			mem = mvccWriteIntentOp + int64(cap(data.txnID)) + int64(cap(data.txnKey))
		case 3:
			op = updateIntentOp(data.txnID, data.timestamp)
			mem = mvccUpdateIntentOp + int64(cap(data.txnID))
		case 4:
			op = commitIntentOpWithPrevValue(data.txnID, data.key, data.timestamp, data.value, data.prevValue, data.omitInRangefeeds)
			mem = mvccCommitIntentOp + int64(cap(data.txnID)) + int64(cap(data.key)) + int64(cap(data.value)) + int64(cap(data.prevValue))
		case 5:
			op = abortIntentOp(data.txnID)
			mem = mvccAbortIntentOp + int64(cap(data.txnID))
		case 6:
			op = abortTxnOp(data.txnID)
			mem = mvccAbortTxnOp + int64(cap(data.txnID))
		}
		ops = append(ops, op)
		expectedMemUsage += mem
	}
	ev = event{ops: ops}
	expectedMemUsage += mvccLogicalOp * int64(cap(ev.ops))
	return ev, expectedMemUsage
}

func TestMultipleLogicalOpsEventsSizeCalculation(t *testing.T) {
	randRand, _ := randutil.NewTestRand()
	data := generateRandomTestData(randRand)
	ev, mem := generateLogicalOpEvents(data)
	t.Logf("chosen event: %v\n", &ev)
	require.Equal(t, ev.MemUsage(), mem)
}
