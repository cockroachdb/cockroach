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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
	startKey, endKey roachpb.Key
	txnID            uuid.UUID
	txnKey           []byte
	txnIsoLevel      isolation.Level
	txnMinTimestamp  hlc.Timestamp
	omitInRangefeeds bool
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
		numOfLogicalOps:  rand.Intn(100),
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

func writeValueOpEvent(
	key roachpb.Key, timestamp hlc.Timestamp, value []byte,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = writeValueOpWithKV(key, timestamp, value)
	futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
	})
	expectedMemUsage += mvccWriteValueOp + int64(cap(key)) + int64(cap(value))
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(cap(key)) + int64(cap(value))
	return op, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func deleteRangeOpEvent(
	startKey, endKey roachpb.Key, timestamp hlc.Timestamp,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = deleteRangeOp(startKey, endKey, timestamp)
	futureEvent.MustSetValue(&kvpb.RangeFeedDeleteRange{
		Span:      roachpb.Span{Key: startKey, EndKey: endKey},
		Timestamp: testTs,
	})
	expectedMemUsage += mvccDeleteRangeOp + int64(cap(startKey)) + int64(cap(endKey))
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedDeleteRangeOverhead + int64(cap(startKey)) + int64(cap(endKey))
	return op, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func writeIntentOpEvent(
	txnID uuid.UUID,
	txnKey []byte,
	txnIsoLevel isolation.Level,
	txnMinTimestamp hlc.Timestamp,
	timestamp hlc.Timestamp,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = writeIntentOpWithDetails(txnID, txnKey, txnIsoLevel, txnMinTimestamp, timestamp)
	expectedMemUsage += mvccWriteIntentOp + int64(cap(txnID)) + int64(cap(txnKey))
	// No future event to publish.
	return op, futureEvent, expectedMemUsage, 0
}

func updateIntentOpEvent(
	txnID uuid.UUID, timestamp hlc.Timestamp,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = updateIntentOp(txnID, timestamp)
	expectedMemUsage += mvccUpdateIntentOp + int64(cap(txnID))
	// No future event to publish.
	return op, futureEvent, expectedMemUsage, 0
}

func commitIntentOpEvent(
	txnID uuid.UUID,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value, prevValue []byte,
	omitInRangefeeds bool,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = commitIntentOpWithKV(txnID, key, timestamp, value, omitInRangefeeds)

	futureEvent.MustSetValue(&kvpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
	})

	expectedMemUsage += mvccCommitIntentOp + int64(cap(txnID)) + int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue))
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedValueOverhead + int64(cap(key)) + int64(cap(value))
	return op, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func abortIntentOpEvent(
	txnID uuid.UUID,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = abortIntentOp(txnID)
	expectedMemUsage += mvccAbortIntentOp + int64(cap(txnID))
	// No future event to publish.
	return op, futureEvent, expectedMemUsage, 0
}

func abortTxnOpEvent(
	txnID uuid.UUID,
) (
	op enginepb.MVCCLogicalOp,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	op = abortTxnOp(txnID)
	expectedMemUsage += mvccAbortTxnOp + int64(cap(txnID))
	// No future event to publish.
	return op, futureEvent, expectedMemUsage, 0
}

func generateLogicalOpEvents(
	data testData,
) (ev event, expectedMemUsage int64, expectedFutureMemUsage int64) {
	var ops []enginepb.MVCCLogicalOp
	var mem, futureMem int64
	expectedMemUsage += eventOverhead
	for i := 0; i < data.numOfLogicalOps; i++ {
		var op enginepb.MVCCLogicalOp
		switch i % 7 {
		case 0:
			op, _, mem, futureMem = writeValueOpEvent(data.key, data.timestamp, data.value)
		case 1:
			op, _, mem, futureMem = deleteRangeOpEvent(data.startKey, data.endKey, data.timestamp)
		case 2:
			op, _, mem, futureMem = writeIntentOpEvent(data.txnID, data.txnKey, data.txnIsoLevel, data.txnMinTimestamp, data.timestamp)
		case 3:
			op, _, mem, futureMem = updateIntentOpEvent(data.txnID, data.timestamp)
		case 4:
			op, _, mem, futureMem = commitIntentOpEvent(data.txnID, data.key, data.timestamp, data.value, nil, data.omitInRangefeeds)
		case 5:
			op, _, mem, futureMem = abortIntentOpEvent(data.txnID)
		case 6:
			op, _, mem, futureMem = abortTxnOpEvent(data.txnID)
		}
		ops = append(ops, op)
		expectedMemUsage += mem
		expectedFutureMemUsage += futureMem
	}
	ev = event{ops: ops}
	expectedMemUsage += mvccLogicalOp * int64(cap(ev.ops))
	return ev, expectedMemUsage, expectedFutureMemUsage
}

func generateOneLogicalOpEvent(
	typesOfOps string, data testData,
) (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedCurrMemUsage int64,
	expectedFutureMemUsage int64,
) {
	var op enginepb.MVCCLogicalOp
	var mem, futureMem int64
	switch typesOfOps {
	case "write_value":
		op, futureEvent, mem, futureMem = writeValueOpEvent(data.key, data.timestamp, data.value)
	case "delete_range":
		op, futureEvent, mem, futureMem = deleteRangeOpEvent(data.startKey, data.endKey, data.timestamp)
	case "write_intent":
		op, futureEvent, mem, futureMem = writeIntentOpEvent(data.txnID, data.txnKey, data.txnIsoLevel, data.txnMinTimestamp, data.timestamp)
	case "update_intent":
		op, futureEvent, mem, futureMem = updateIntentOpEvent(data.txnID, data.timestamp)
	case "commit_intent":
		op, futureEvent, mem, futureMem = commitIntentOpEvent(data.txnID, data.key, data.timestamp, data.value, nil, data.omitInRangefeeds)
	case "abort_intent":
		op, futureEvent, mem, futureMem = abortIntentOpEvent(data.txnID)
	case "abort_txn":
		op, futureEvent, mem, futureMem = abortTxnOpEvent(data.txnID)
	}

	ev = event{
		ops: []enginepb.MVCCLogicalOp{op},
	}
	expectedCurrMemUsage += eventOverhead + mem + mvccLogicalOp
	expectedFutureMemUsage += futureMem
	return ev, futureEvent, expectedCurrMemUsage, expectedFutureMemUsage
}

func generateCtEvent(
	data testData,
) (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	ev = event{
		ct: ctEvent{
			Timestamp: data.timestamp,
		},
	}
	expectedMemUsage += eventOverhead
	// Publish checkpoint event.
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead
	return ev, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func generateInitRTSEvent() (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	ev = event{
		initRTS: true,
	}
	expectedMemUsage += eventOverhead
	// Publish checkpoint event.
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead
	return ev, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func generateSSTEvent(
	t *testing.T, data testData, st *cluster.Settings,
) (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	sst, _, _ := storageutils.MakeSST(t, st, data.kvs)
	ev = event{
		sst: &sstEvent{
			data: sst,
			span: data.span,
			ts:   data.timestamp,
		},
	}

	futureEvent.MustSetValue(&kvpb.RangeFeedSSTable{
		Data:    sst,
		Span:    data.span,
		WriteTS: data.timestamp,
	})
	expectedMemUsage += eventOverhead + sstEventOverhead + int64(cap(sst)+cap(data.span.Key)+cap(data.span.EndKey))
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedSSTTableOverhead + int64(cap(sst)+cap(data.span.Key)+cap(data.span.EndKey))
	return ev, futureEvent, expectedMemUsage, expectedFutureMemUsage
}

func generateSyncEvent() (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
) {
	ev = event{
		sync: &syncEvent{c: make(chan struct{})},
	}
	expectedMemUsage += eventOverhead + syncEventOverhead
	return
}

func generateRandomizedEventAndSend(
	rand *rand.Rand,
) (ev event, expectedMemUsage int64, randomlyChosenEvent string) {
	// Opt out sst event since it requires testing.T to call
	// storageutils.MakeSST(t, st, data.kvs)
	typesOfEvents := []string{"logicalsOps", "ct", "initRTS", "sync"}
	randomlyChosenEvent = typesOfEvents[rand.Intn(len(typesOfEvents))]
	data := generateRandomTestData(rand)
	switch randomlyChosenEvent {
	case "logicalsOps":
		e, mem, _ := generateLogicalOpEvents(data)
		ev = e
		expectedMemUsage = mem
	case "ct":
		e, _, _, futureEvent := generateCtEvent(data)
		ev = e
		expectedMemUsage = futureEvent
	case "initRTS":
		e, _, _, futureEvent := generateInitRTSEvent()
		ev = e
		expectedMemUsage = futureEvent
	case "sync":
		e, _, mem, _ := generateSyncEvent()
		ev = e
		expectedMemUsage = mem
	}
	return ev, expectedMemUsage, randomlyChosenEvent
}

func generateStaticTestdata() testData {
	return testData{
		kvs:              testSSTKVs,
		span:             testSpan,
		key:              testKey,
		timestamp:        testTs,
		value:            testValue,
		startKey:         testStartKey,
		endKey:           testEndKey,
		txnID:            testTxnID,
		txnKey:           testKey,
		txnIsoLevel:      testIsolationLevel,
		txnMinTimestamp:  testTs,
		omitInRangefeeds: false,
	}
}

func TestBasicEventSizeCalculation(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	data := generateStaticTestdata()

	t.Run("write_value event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("write_value", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("delete_range event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("delete_range", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("write_intent event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("write_intent", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("update_intent event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("update_intent", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("commit_intent event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("commit_intent", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("abort_intent event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("abort_intent", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("abort_txn event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateOneLogicalOpEvent("abort_txn", data)
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})

	t.Run("ct event", func(t *testing.T) {
		ev, _, _, expectedFutureMemUsage := generateCtEvent(data)
		mem := ev.MemUsage()
		require.Equal(t, expectedFutureMemUsage, mem)
	})

	t.Run("initRTS event", func(t *testing.T) {
		generateOneLogicalOpEvent("write_intent", data)
		ev, _, _, expectedFutureMemUsage := generateInitRTSEvent()
		mem := ev.MemUsage()
		require.Equal(t, expectedFutureMemUsage, mem)
	})

	t.Run("sst event", func(t *testing.T) {
		ev, _, _, expectedFutureMemUsage := generateSSTEvent(t, data, st)
		mem := ev.MemUsage()
		require.Equal(t, expectedFutureMemUsage, mem)
	})

	t.Run("sync event", func(t *testing.T) {
		ev, _, expectedCurrMemUsage, _ := generateSyncEvent()
		mem := ev.MemUsage()
		require.Equal(t, expectedCurrMemUsage, mem)
	})
}

// BenchmarkMemoryAccounting benchmarks the memory accounting of the event
// struct.
func BenchmarkMemoryAccounting(b *testing.B) {
	skip.WithIssue(b, 121087)
	b.Run("memory_calculation", func(b *testing.B) {
		b.ReportAllocs()
		rand, _ := randutil.NewTestRand()
		events := []event{}
		type res struct {
			memUsage    int64
			chosenEvent string
		}
		expectedRes := []res{}
		for i := 0; i < 20; i++ {
			ev, mem, chosenEvent := generateRandomizedEventAndSend(rand)
			expectedRes = append(expectedRes, res{
				memUsage:    mem,
				chosenEvent: chosenEvent,
			})
			events = append(events, ev)
		}

		// Reset the timer without the cost of generating the events.
		b.ResetTimer()

		for _, ev := range events {
			ev.MemUsage()
		}

		b.StopTimer()

		totalMemUsageSum := int64(0)
		for i := 0; i < 20; i++ {
			b.Logf("event %d: %+v\n", i+1, events[i])
			b.Logf("chosen event: %s\n", expectedRes[i].chosenEvent)
			memUsage := events[i].MemUsage()
			require.Equal(b, expectedRes[i].memUsage, memUsage)
			totalMemUsageSum += memUsage
		}
		b.Logf("total memory usage: %d\n", totalMemUsageSum)
	})
}
