// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestRequestsSerializeWithAllKeys ensures that no request can be evaluated
// concurrently with either a Subsume request or a TransferLease request, both
// of which declare latches using declareAllKeys to guarantee mutual exclusion
// over the leaseholder.
func TestRequestsSerializeWithAllKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var allLatchSpans spanset.SpanSet
	declareAllKeys(&allLatchSpans)

	for method, command := range cmds {
		t.Run(method.String(), func(t *testing.T) {
			var otherLatchSpans, otherLockSpans spanset.SpanSet

			startKey := []byte(`a`)
			endKey := []byte(`b`)
			desc := &roachpb.RangeDescriptor{
				RangeID:  0,
				StartKey: startKey,
				EndKey:   endKey,
			}
			testTxn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					ID:             uuid.FastMakeV4(),
					Key:            startKey,
					WriteTimestamp: hlc.Timestamp{WallTime: 1},
				},
				Name: "test txn",
			}
			header := roachpb.Header{Txn: testTxn}
			otherRequest := roachpb.CreateRequest(method)
			if queryTxnReq, ok := otherRequest.(*roachpb.QueryTxnRequest); ok {
				// QueryTxnRequest declares read-only access over the txn record of the txn
				// it is supposed to query and not the txn that sent it. We fill this Txn
				// field in here to prevent it from being nil and leading to the txn key
				// falling outside our test range's keyspace.
				queryTxnReq.Txn = testTxn.TxnMeta
			}
			otherRequest.SetHeader(roachpb.RequestHeader{
				Key:      startKey,
				EndKey:   endKey,
				Sequence: 0,
			})

			command.DeclareKeys(desc, header, otherRequest, &otherLatchSpans, &otherLockSpans)
			if !allLatchSpans.Intersects(&otherLatchSpans) {
				t.Errorf("%s does not serialize with declareAllKeys", method)
			}
		})
	}
}
