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
func generateRandomizedRawBytes(rand *rand.Rand) []byte {
	const valSize = 16 << 10
	return randutil.RandBytes(rand, valSize)
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
	expectedMemUsage += mvccLogicalOp + mvccWriteValueOp + int64(cap(key)) + int64(cap(value))
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
	expectedMemUsage += mvccLogicalOp + mvccDeleteRangeOp + int64(cap(startKey)) + int64(cap(endKey))
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
	expectedMemUsage += mvccLogicalOp + mvccWriteIntentOp + int64(cap(txnID)) + int64(cap(txnKey))
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
	expectedMemUsage += mvccLogicalOp + mvccUpdateIntentOp + int64(cap(txnID))
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

	expectedMemUsage += mvccLogicalOp + mvccCommitIntentOp + int64(cap(txnID)) + int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue))
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
	expectedMemUsage += mvccLogicalOp + mvccAbortIntentOp + int64(cap(txnID))
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
	expectedMemUsage += mvccLogicalOp + mvccAbortTxnOp + int64(cap(txnID))
	// No future event to publish.
	return op, futureEvent, expectedMemUsage, 0
}

func generateSampleCheckpointEvent() (checkPointEvent kvpb.RangeFeedEvent) {
	checkPointEvent.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       testSpan,
		ResolvedTS: testTs,
	})
	return
}
func generateLogicalOpEvent(
	typesOfOps string, data testData,
) (
	ev event,
	futureEvent kvpb.RangeFeedEvent,
	expectedMemUsage int64,
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
	expectedMemUsage += eventOverhead + mem
	expectedFutureMemUsage += futureMem
	// Publish checkpoint event.
	expectedFutureMemUsage += futureEventBaseOverhead + rangefeedCheckpointOverhead
	return ev, futureEvent, expectedMemUsage, expectedFutureMemUsage
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
) (
	ev event,
	expectedMemUsage int64,
	expectedFutureMemUsage int64,
	randomlyChosenEvent string,
	randomlyChosenLogicalOp string,
) {
	// Opt out sst event since it requires testing.T to call
	// storageutils.MakeSST(t, st, data.kvs)
	typesOfEvents := []string{"logicalsOps", "ct", "initRTS", "sync"}
	logicalOps := []string{"write_value", "delete_range", "write_intent", "update_intent", "commit_intent", "abort_intent", "abort_txn"}
	randomlyChosenEvent = typesOfEvents[rand.Intn(len(typesOfEvents))]
	randomlyChosenLogicalOp = logicalOps[rand.Intn(len(logicalOps))]
	data := generateRandomTestData(rand)
	switch randomlyChosenEvent {
	case "logicalsOps":
		ev, _, expectedMemUsage, expectedFutureMemUsage = generateLogicalOpEvent(randomlyChosenLogicalOp, data)
	case "ct":
		ev, _, expectedMemUsage, expectedFutureMemUsage = generateCtEvent(data)
	case "initRTS":
		ev, _, expectedMemUsage, expectedFutureMemUsage = generateInitRTSEvent()
	case "sync":
		ev, _, expectedMemUsage, expectedFutureMemUsage = generateSyncEvent()
	}
	return ev, expectedMemUsage, expectedFutureMemUsage, randomlyChosenEvent, randomlyChosenLogicalOp
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

	t.Run("empty_event", func(t *testing.T) {
		ev := event{}
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, eventOverhead, curr)
		require.Equal(t, int64(0), future)
		require.Equal(t, EventMemUsage(ev), max(eventOverhead, int64(0)))
	})

	t.Run("write_value event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("write_value", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("delete_range event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("delete_range", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("write_intent event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("write_intent", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, max(expectedMemUsage, expectedFutureMemUsage), EventMemUsage(ev))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("update_intent event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("update_intent", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("commit_intent event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("commit_intent", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("abort_intent event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("abort_intent", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("abort_txn event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateLogicalOpEvent("abort_txn", data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("ct event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateCtEvent(data)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("initRTS event", func(t *testing.T) {
		generateLogicalOpEvent("write_intent", data)
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateInitRTSEvent()
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		checkpointEvent := generateSampleCheckpointEvent()
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent)+RangefeedEventMemUsage(&checkpointEvent))
	})

	t.Run("sst event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateSSTEvent(t, data, st)
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent))
	})

	t.Run("sync event", func(t *testing.T) {
		ev, futureEvent, expectedMemUsage, expectedFutureMemUsage := generateSyncEvent()
		curr, future := ev.currAndFutureMemUsage()
		require.Equal(t, expectedMemUsage, curr)
		require.Equal(t, expectedFutureMemUsage, future)
		require.Equal(t, EventMemUsage(ev), max(expectedMemUsage, expectedFutureMemUsage))
		require.Equal(t, expectedFutureMemUsage, RangefeedEventMemUsage(&futureEvent))
	})
}

// BenchmarkMemoryAccounting benchmarks the memory accounting of the event
// struct.
func BenchmarkMemoryAccounting(b *testing.B) {
	b.Run("memory_calculation", func(b *testing.B) {
		rand, _ := randutil.NewTestRand()
		events := []event{}
		type res struct {
			currMemUsages       int64
			futureMemUsages     int64
			memUsagesToAllocate int64
			chosenEvent         string
			chosenOp            string
		}
		expectedRes := []res{}
		for i := 0; i < b.N; i++ {
			ev, mem, futureMem, chosenEvent, chosenOp := generateRandomizedEventAndSend(rand)
			expectedRes = append(expectedRes, res{
				currMemUsages:       mem,
				futureMemUsages:     futureMem,
				chosenEvent:         chosenEvent,
				chosenOp:            chosenOp,
				memUsagesToAllocate: max(mem, futureMem),
			})
			events = append(events, ev)
		}

		// Reset the timer without the cost of generating the events.
		b.ResetTimer()

		actualCurrMemUsage := int64(0)
		actualFutureMemUsage := int64(0)
		actualMaxUsage := int64(0)
		for _, ev := range events {
			curr, future := ev.currAndFutureMemUsage()
			actualCurrMemUsage += curr
			actualFutureMemUsage += future
		}

		b.StopTimer()

		sumOfCurr := 0
		sumOfFuture := 0
		diff := 0
		for i := 0; i < b.N; i++ {
			b.Logf("event %d: %+v\n", i+1, events[i])
			b.Logf("chosen event: %s, %s\n", expectedRes[i].chosenEvent, expectedRes[i].chosenOp)
			actualCurr, Future := events[i].currAndFutureMemUsage()
			actualMemUsage := EventMemUsage(events[i])
			actualMaxUsage += actualMemUsage
			require.Equal(b, expectedRes[i].currMemUsages, actualCurr)
			require.Equal(b, expectedRes[i].futureMemUsages, Future)
			require.Equal(b, expectedRes[i].memUsagesToAllocate, actualMemUsage)
			sumOfCurr += int(expectedRes[i].currMemUsages)
			sumOfFuture += int(expectedRes[i].futureMemUsages)
			diff += int(expectedRes[i].futureMemUsages - expectedRes[i].futureMemUsages)

			if expectedRes[i].currMemUsages < expectedRes[i].futureMemUsages {
				b.Logf("currMemUsage: %d is smaller than futureMemUsage: %d\n",
					expectedRes[i].currMemUsages, expectedRes[i].futureMemUsages)
			}
		}
		b.Logf("memory accounted: %d\n", actualMaxUsage)
		b.Logf("sum of curr: %d\n", sumOfCurr)
		b.Logf("sum of future: %d\n", sumOfFuture)
	})
}
