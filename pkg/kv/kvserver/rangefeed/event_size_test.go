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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
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
)

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
			expectedCurrMemUsage: int64(1962),
			actualCurrMemUsage: eventOverhead + sstEventOverhead +
				int64(cap(sst)+cap(span.Key)+cap(span.EndKey)),
			expectedFutureMemUsage: int64(1978),
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

func TestMultipleLogicalOpsEventsSizeCalculation(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	data := generateRandomTestData(rng)
	ev, mem := generateLogicalOpEvents(rng, data)
	t.Logf("chosen event: %v\n", &ev)
	require.Equal(t, MemUsage(ev), mem)
}
