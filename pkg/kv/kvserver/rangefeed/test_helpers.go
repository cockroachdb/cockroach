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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

type kvs = storageutils.KVs

var (
	pointKV = storageutils.PointKV
	rangeKV = storageutils.RangeKV
)

var testSSTKVs = kvs{
	pointKV("a", 1, "1"),
	pointKV("b", 1, "2"),
	pointKV("c", 1, "3"),
	rangeKV("d", "e", 1, ""),
}

func generateRandomTestData(rand *rand.Rand) testData {
	startKey, endkey := generateStartAndEndKey(rand)
	return testData{
		numOfLogicalOps:  rand.Intn(100) + 1, // Avoid 0 (empty event)
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
