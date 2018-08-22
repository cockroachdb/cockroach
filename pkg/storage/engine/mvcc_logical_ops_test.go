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

package engine

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMVCCOpLogWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	engine := createTestEngine()
	defer engine.Close()

	batch := engine.NewBatch()
	ol := NewOpLoggerBatch(batch)
	defer ol.Close()

	// Write a value and an intent.
	if err := MVCCPut(ctx, ol, nil, testKey1, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, ol, nil, testKey1, hlc.Timestamp{Logical: 2}, value2, txn1); err != nil {
		t.Fatal(err)
	}

	// Write a value and an intent on local keys.
	localKey := keys.MakeRangeIDPrefix(1)
	if err := MVCCPut(ctx, ol, nil, localKey, hlc.Timestamp{Logical: 1}, value1, nil); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, ol, nil, localKey, hlc.Timestamp{Logical: 2}, value2, txn1); err != nil {
		t.Fatal(err)
	}

	// Update the intents and write another. Use a distinct batch.
	olDist := ol.Distinct()
	txn1Seq := *txn1
	txn1Seq.Sequence++
	if err := MVCCPut(ctx, olDist, nil, testKey1, hlc.Timestamp{Logical: 3}, value2, &txn1Seq); err != nil {
		t.Fatal(err)
	}
	if err := MVCCPut(ctx, olDist, nil, localKey, hlc.Timestamp{Logical: 3}, value2, &txn1Seq); err != nil {
		t.Fatal(err)
	}
	// Set the txn timestamp to a larger value than the intent.
	txn1LargerTS := *txn1
	txn1LargerTS.Timestamp = hlc.Timestamp{Logical: 4}
	if err := MVCCPut(ctx, olDist, nil, testKey2, hlc.Timestamp{Logical: 3}, value3, &txn1LargerTS); err != nil {
		t.Fatal(err)
	}
	olDist.Close()

	// Resolve all three intent.
	txn1CommitTS := *txn1Commit
	txn1CommitTS.Timestamp = hlc.Timestamp{Logical: 4}
	if _, _, err := MVCCResolveWriteIntentRange(ctx, ol, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: testKey1, EndKey: testKey2.Next()},
		Txn:    txn1CommitTS.TxnMeta,
		Status: txn1CommitTS.Status,
	}, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if _, _, err := MVCCResolveWriteIntentRange(ctx, ol, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: localKey, EndKey: localKey.Next()},
		Txn:    txn1CommitTS.TxnMeta,
		Status: txn1CommitTS.Status,
	}, math.MaxInt64); err != nil {
		t.Fatal(err)
	}

	// Write another intent, push it, then abort it.
	if err := MVCCPut(ctx, ol, nil, testKey3, hlc.Timestamp{Logical: 5}, value4, txn2); err != nil {
		t.Fatal(err)
	}
	txn2Pushed := *txn2
	txn2Pushed.Timestamp = hlc.Timestamp{Logical: 6}
	if err := MVCCResolveWriteIntent(ctx, ol, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: testKey3},
		Txn:    txn2Pushed.TxnMeta,
		Status: txn2Pushed.Status,
	}); err != nil {
		t.Fatal(err)
	}
	txn2Abort := txn2Pushed
	txn2Abort.Status = roachpb.ABORTED
	if err := MVCCResolveWriteIntent(ctx, ol, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: testKey3},
		Txn:    txn2Abort.TxnMeta,
		Status: txn2Abort.Status,
	}); err != nil {
		t.Fatal(err)
	}

	// Verify that the recorded logical ops match expectations.
	makeOp := func(val interface{}) enginepb.MVCCLogicalOp {
		var op enginepb.MVCCLogicalOp
		op.MustSetValue(val)
		return op
	}
	exp := []enginepb.MVCCLogicalOp{
		makeOp(&enginepb.MVCCWriteValueOp{
			Key:       testKey1,
			Timestamp: hlc.Timestamp{Logical: 1},
		}),
		makeOp(&enginepb.MVCCWriteIntentOp{
			TxnID:     txn1.ID,
			TxnKey:    txn1.Key,
			Timestamp: hlc.Timestamp{Logical: 2},
		}),
		makeOp(&enginepb.MVCCUpdateIntentOp{
			TxnID:     txn1.ID,
			Timestamp: hlc.Timestamp{Logical: 3},
		}),
		makeOp(&enginepb.MVCCWriteIntentOp{
			TxnID:     txn1.ID,
			TxnKey:    txn1.Key,
			Timestamp: hlc.Timestamp{Logical: 4},
		}),
		makeOp(&enginepb.MVCCCommitIntentOp{
			TxnID:     txn1.ID,
			Key:       testKey1,
			Timestamp: hlc.Timestamp{Logical: 4},
		}),
		makeOp(&enginepb.MVCCCommitIntentOp{
			TxnID:     txn1.ID,
			Key:       testKey2,
			Timestamp: hlc.Timestamp{Logical: 4},
		}),
		makeOp(&enginepb.MVCCWriteIntentOp{
			TxnID:     txn2.ID,
			TxnKey:    txn2.Key,
			Timestamp: hlc.Timestamp{Logical: 5},
		}),
		makeOp(&enginepb.MVCCUpdateIntentOp{
			TxnID:     txn2.ID,
			Timestamp: hlc.Timestamp{Logical: 6},
		}),
		makeOp(&enginepb.MVCCAbortIntentOp{
			TxnID: txn2.ID,
		}),
	}
	if ops := ol.LogicalOps(); !reflect.DeepEqual(exp, ops) {
		t.Errorf("expected logical ops %+v, found %+v", exp, ops)
	}
}
