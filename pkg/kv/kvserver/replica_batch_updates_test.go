// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestMaybeStripInFlightWrites verifies that in-flight writes declared on an
// EndTxn request are stripped if the corresponding write or query intent is in
// the same batch as the EndTxn.
func TestMaybeStripInFlightWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	qi1 := &roachpb.QueryIntentRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}}
	qi1.Txn.Sequence = 1
	put2 := &roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyB}}
	put2.Sequence = 2
	put3 := &roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	put3.Sequence = 3
	delRng3 := &roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	delRng3.Sequence = 3
	scan3 := &roachpb.ScanRequest{RequestHeader: roachpb.RequestHeader{Key: keyC}}
	scan3.Sequence = 3
	et := &roachpb.EndTxnRequest{RequestHeader: roachpb.RequestHeader{Key: keyA}, Commit: true}
	et.Sequence = 4
	et.LockSpans = []roachpb.Span{{Key: keyC}}
	et.InFlightWrites = []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2}}
	testCases := []struct {
		reqs         []roachpb.Request
		expIFW       []roachpb.SequencedWrite
		expLockSpans []roachpb.Span
		expErr       string
	}{
		{
			reqs:         []roachpb.Request{et},
			expIFW:       []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2}},
			expLockSpans: []roachpb.Span{{Key: keyC}},
		},
		// QueryIntents aren't stripped from the in-flight writes set on the
		// slow-path of maybeStripInFlightWrites. This is intentional.
		{
			reqs:         []roachpb.Request{qi1, et},
			expIFW:       []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}, {Key: keyB, Sequence: 2}},
			expLockSpans: []roachpb.Span{{Key: keyC}},
		},
		{
			reqs:         []roachpb.Request{put2, et},
			expIFW:       []roachpb.SequencedWrite{{Key: keyA, Sequence: 1}},
			expLockSpans: []roachpb.Span{{Key: keyB}, {Key: keyC}},
		},
		{
			reqs:   []roachpb.Request{put3, et},
			expErr: "write in batch with EndTxn missing from in-flight writes",
		},
		{
			reqs:         []roachpb.Request{qi1, put2, et},
			expIFW:       nil,
			expLockSpans: []roachpb.Span{{Key: keyA}, {Key: keyB}, {Key: keyC}},
		},
		{
			reqs:         []roachpb.Request{qi1, put2, delRng3, et},
			expIFW:       nil,
			expLockSpans: []roachpb.Span{{Key: keyA}, {Key: keyB}, {Key: keyC}},
		},
		{
			reqs:         []roachpb.Request{qi1, put2, scan3, et},
			expIFW:       nil,
			expLockSpans: []roachpb.Span{{Key: keyA}, {Key: keyB}, {Key: keyC}},
		},
		{
			reqs:         []roachpb.Request{qi1, put2, delRng3, scan3, et},
			expIFW:       nil,
			expLockSpans: []roachpb.Span{{Key: keyA}, {Key: keyB}, {Key: keyC}},
		},
	}
	for _, c := range testCases {
		var ba roachpb.BatchRequest
		ba.Add(c.reqs...)
		t.Run(fmt.Sprint(ba), func(t *testing.T) {
			resBa, err := maybeStripInFlightWrites(&ba)
			if c.expErr == "" {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				resArgs, _ := resBa.GetArg(roachpb.EndTxn)
				resEt := resArgs.(*roachpb.EndTxnRequest)
				if !reflect.DeepEqual(resEt.InFlightWrites, c.expIFW) {
					t.Errorf("expected in-flight writes %v, got %v", c.expIFW, resEt.InFlightWrites)
				}
				if !reflect.DeepEqual(resEt.LockSpans, c.expLockSpans) {
					t.Errorf("expected lock spans %v, got %v", c.expLockSpans, resEt.LockSpans)
				}
			} else {
				if !testutils.IsError(err, c.expErr) {
					t.Errorf("expected error %q, got %v", c.expErr, err)
				}
			}
		})
	}
}
