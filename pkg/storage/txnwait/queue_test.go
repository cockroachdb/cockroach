// Copyright 2017 The Cockroach Authors.
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

package txnwait

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestShouldPushImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ        roachpb.PushTxnType
		pusherPri  int32
		pusheePri  int32
		shouldPush bool
	}{
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, roachpb.MinTxnPriority, false},
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, 1, false},
		{roachpb.PUSH_ABORT, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, 1, 1, false},
		{roachpb.PUSH_ABORT, 1, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_ABORT, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, roachpb.MinTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, 1, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, 1, 1, false},
		{roachpb.PUSH_TIMESTAMP, 1, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TIMESTAMP, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, false},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, roachpb.MinTxnPriority, roachpb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, 1, 1, true},
		{roachpb.PUSH_TOUCH, 1, roachpb.MaxTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, roachpb.MinTxnPriority, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, 1, true},
		{roachpb.PUSH_TOUCH, roachpb.MaxTxnPriority, roachpb.MaxTxnPriority, true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				PushType: test.typ,
				PusherTxn: roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						Priority: test.pusherPri,
					},
				},
				PusheeTxn: enginepb.TxnMeta{
					Priority: test.pusheePri,
				},
			}
			if shouldPush := ShouldPushImmediately(&req); shouldPush != test.shouldPush {
				t.Errorf("expected %t; got %t", test.shouldPush, shouldPush)
			}
		})
	}
}

func makeTS(w int64, l int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: w, Logical: l}
}

func TestIsPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		typ          roachpb.PushTxnType
		pushTo       hlc.Timestamp
		txnStatus    roachpb.TransactionStatus
		txnTimestamp hlc.Timestamp
		isPushed     bool
	}{
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_ABORT, hlc.Timestamp{}, roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, hlc.Timestamp{}, false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.ABORTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.COMMITTED, hlc.Timestamp{}, true},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 0), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 1), false},
		{roachpb.PUSH_TIMESTAMP, makeTS(10, 1), roachpb.PENDING, makeTS(10, 2), true},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			req := roachpb.PushTxnRequest{
				PushType: test.typ,
				PushTo:   test.pushTo,
			}
			txn := roachpb.Transaction{
				Status: test.txnStatus,
				TxnMeta: enginepb.TxnMeta{
					Timestamp: test.txnTimestamp,
				},
			}
			if isPushed := isPushed(&req, &txn); isPushed != test.isPushed {
				t.Errorf("expected %t; got %t", test.isPushed, isPushed)
			}
		})
	}
}
