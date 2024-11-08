// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
	storage.ColumnarBlocksEnabled.Override(context.Background(), &st.SV, true)

	key := data.key
	timestamp := data.timestamp
	value := data.value
	prevValue := data.prevValue

	startKey := data.startKey
	endKey := data.endKey

	txnID := data.txnID
	txnKey := data.txnKey
	txnIsoLevel := data.txnIsoLevel
	txnMinTimestamp := data.txnMinTimestamp

	omitInRangefeeds := data.omitInRangefeeds

	span := data.span
	sst, _, _ := storageutils.MakeSST(t, st, data.kvs)

	for _, tc := range []struct {
		name                   string
		ev                     event
		actualCurrMemUsage     int64
		expectedCurrMemUsage   int64
		actualFutureMemUsage   int64
		expectedFutureMemUsage int64
	}{
		{
			name: "write_value event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				writeValueOpWithPrevValue(key, timestamp, value, prevValue),
			}},
			expectedCurrMemUsage: int64(241),
			actualCurrMemUsage: eventOverhead + mvccLogicalOp + mvccWriteValueOp +
				int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue)),
			expectedFutureMemUsage: int64(209),
			actualFutureMemUsage: futureEventBaseOverhead + rangefeedValueOverhead +
				int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue)),
		},
		{
			name: "delete_range event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				deleteRangeOp(startKey, endKey, timestamp),
			}},
			expectedCurrMemUsage: int64(202),
			actualCurrMemUsage: eventOverhead + mvccLogicalOp + mvccDeleteRangeOp +
				int64(cap(startKey)) + int64(cap(endKey)),
			expectedFutureMemUsage: int64(202),
			actualFutureMemUsage: futureEventBaseOverhead + rangefeedValueOverhead +
				int64(cap(startKey)) + int64(cap(endKey)),
		},
		{
			name: "write_intent event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				writeIntentOpWithDetails(txnID, txnKey, txnIsoLevel, txnMinTimestamp, timestamp),
			}},
			expectedCurrMemUsage: int64(236),
			actualCurrMemUsage: eventOverhead + mvccLogicalOp + mvccWriteIntentOp +
				int64(cap(txnID)) + int64(cap(txnKey)),
			expectedFutureMemUsage: int64(0),
			actualFutureMemUsage:   int64(0), // No future events to publish.
		},
		{
			name: "update_intent event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				updateIntentOp(txnID, timestamp),
			}},
			expectedCurrMemUsage:   int64(184),
			actualCurrMemUsage:     eventOverhead + mvccLogicalOp + mvccUpdateIntentOp + int64(cap(txnID)),
			expectedFutureMemUsage: int64(0),
			actualFutureMemUsage:   int64(0), // No future events to publish.
		},
		{
			name: "commit_intent event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				commitIntentOpWithPrevValue(txnID, key, timestamp, value, prevValue, omitInRangefeeds),
			}},
			expectedCurrMemUsage: int64(273),
			actualCurrMemUsage: eventOverhead + mvccLogicalOp + mvccCommitIntentOp +
				int64(cap(txnID)) + int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue)),
			expectedFutureMemUsage: int64(209),
			actualFutureMemUsage: futureEventBaseOverhead + rangefeedValueOverhead +
				int64(cap(key)) + int64(cap(value)) + int64(cap(prevValue)),
		},
		{
			name: "abort_intent event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				abortIntentOp(txnID),
			}},
			expectedCurrMemUsage:   int64(168),
			actualCurrMemUsage:     eventOverhead + mvccLogicalOp + mvccAbortIntentOp + int64(cap(txnID)),
			expectedFutureMemUsage: int64(0),
			actualFutureMemUsage:   int64(0), // No future events to publish.
		},
		{
			name: "abort_txn event",
			ev: event{ops: []enginepb.MVCCLogicalOp{
				abortTxnOp(txnID),
			}},
			expectedCurrMemUsage:   int64(168),
			actualCurrMemUsage:     eventOverhead + mvccLogicalOp + mvccAbortTxnOp + int64(cap(txnID)),
			expectedFutureMemUsage: int64(0),
			actualFutureMemUsage:   int64(0), // No future events to publish.
		},
		{
			name:                   "ct event",
			ev:                     event{ct: ctEvent{Timestamp: data.timestamp}},
			expectedCurrMemUsage:   int64(80),
			actualCurrMemUsage:     eventOverhead,
			expectedFutureMemUsage: int64(160),
			actualFutureMemUsage:   futureEventBaseOverhead + rangefeedCheckpointOverhead,
		},
		{
			name:                   "initRTS event",
			ev:                     event{initRTS: true},
			expectedCurrMemUsage:   int64(80),
			actualCurrMemUsage:     eventOverhead,
			expectedFutureMemUsage: int64(160),
			actualFutureMemUsage:   futureEventBaseOverhead + rangefeedCheckpointOverhead,
		},
		{
			name:                 "sstEvent event",
			ev:                   event{sst: &sstEvent{data: sst, span: span, ts: timestamp}},
			expectedCurrMemUsage: int64(2218),
			actualCurrMemUsage: eventOverhead + sstEventOverhead +
				int64(cap(sst)+cap(span.Key)+cap(span.EndKey)),
			expectedFutureMemUsage: int64(2234),
			actualFutureMemUsage: futureEventBaseOverhead + rangefeedSSTTableOverhead +
				int64(cap(sst)+cap(span.Key)+cap(span.EndKey)),
		},
		{
			name:                   "syncEvent event",
			ev:                     event{sync: &syncEvent{c: make(chan struct{})}},
			expectedCurrMemUsage:   int64(96),
			actualCurrMemUsage:     eventOverhead + syncEventOverhead,
			expectedFutureMemUsage: int64(0),
			actualFutureMemUsage:   int64(0), // No future events to publish.
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := MemUsage(tc.ev)
			require.Equal(t, tc.expectedCurrMemUsage, tc.actualCurrMemUsage)
			require.Equal(t, tc.expectedFutureMemUsage, tc.actualFutureMemUsage)
			if tc.actualFutureMemUsage > tc.actualCurrMemUsage {
				require.Equal(t, tc.actualFutureMemUsage, mem)
			} else {
				require.Equal(t, tc.actualCurrMemUsage, mem)
			}
		})
	}
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

func generateRandomizedTxnId(rand *rand.Rand) uuid.UUID {
	var txnID uuid.UUID
	n := rand.Intn(100)
	if n == 0 {
		// rand.Intn(0) panics
		n = 1
	}
	i := rand.Intn(n) // i must be in [0,n)
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
		numOfLogicalOps:  rand.Intn(100) + 1, // Avoid 0 (empty event)
		kvs:              testSSTKVs,
		span:             generateRandomizedSpan(rand).AsRawSpanWithNoLocals(),
		key:              generateRandomizedBytes(rand),
		timestamp:        GenerateRandomizedTs(rand, 100 /* maxTime */),
		value:            generateRandomizedBytes(rand),
		startKey:         startKey,
		endKey:           endkey,
		txnID:            generateRandomizedTxnId(rand),
		txnKey:           generateRandomizedBytes(rand),
		txnIsoLevel:      isolation.Levels()[rand.Intn(len(isolation.Levels()))],
		txnMinTimestamp:  GenerateRandomizedTs(rand, 100 /* maxTime */),
		omitInRangefeeds: rand.Intn(2) == 1,
	}
}

type exampleOp struct {
	op  enginepb.MVCCLogicalOp
	mem int64
}

func generateLogicalOpEvents(rand *rand.Rand, data testData) (ev event, expectedMemUsage int64) {
	var ops []enginepb.MVCCLogicalOp
	expectedMemUsage += eventOverhead
	exampleOps := [7]exampleOp{
		{
			op:  writeValueOpWithPrevValue(data.key, data.timestamp, data.value, data.prevValue),
			mem: mvccWriteValueOp + int64(cap(data.key)) + int64(cap(data.value)) + int64(cap(data.prevValue)),
		},
		{
			op:  deleteRangeOp(data.startKey, data.endKey, data.timestamp),
			mem: mvccDeleteRangeOp + int64(cap(data.startKey)) + int64(cap(data.endKey)),
		},
		{
			op:  writeIntentOpWithDetails(data.txnID, data.txnKey, data.txnIsoLevel, data.txnMinTimestamp, data.timestamp),
			mem: mvccWriteIntentOp + int64(cap(data.txnID)) + int64(cap(data.txnKey)),
		},
		{
			op:  updateIntentOp(data.txnID, data.timestamp),
			mem: mvccUpdateIntentOp + int64(cap(data.txnID)),
		},
		{
			op:  commitIntentOpWithPrevValue(data.txnID, data.key, data.timestamp, data.value, data.prevValue, data.omitInRangefeeds),
			mem: mvccCommitIntentOp + int64(cap(data.txnID)) + int64(cap(data.key)) + int64(cap(data.value)) + int64(cap(data.prevValue)),
		},
		{
			op:  abortIntentOp(data.txnID),
			mem: mvccAbortIntentOp + int64(cap(data.txnID)),
		},
		{
			op:  abortTxnOp(data.txnID),
			mem: mvccAbortTxnOp + int64(cap(data.txnID)),
		},
	}

	for i := 0; i < data.numOfLogicalOps; i++ {
		randomlyPickedIndex := rand.Intn(len(exampleOps))
		ops = append(ops, exampleOps[randomlyPickedIndex].op)
		expectedMemUsage += exampleOps[randomlyPickedIndex].mem
	}
	ev = event{ops: ops}
	expectedMemUsage += mvccLogicalOp * int64(cap(ev.ops))
	return ev, expectedMemUsage
}

func TestMultipleLogicalOpsEventsSizeCalculation(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	data := generateRandomTestData(rng)
	ev, mem := generateLogicalOpEvents(rng, data)
	t.Logf("chosen event: %v\n", &ev)
	require.Equal(t, MemUsage(ev), mem)
}
