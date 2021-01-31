// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestBatchIsCompleteTransaction(t *testing.T) {
	get := &GetRequest{}
	put := &PutRequest{}
	etA := &EndTxnRequest{Commit: false}
	etC := &EndTxnRequest{Commit: true}
	withSeq := func(r Request, s enginepb.TxnSeq) Request {
		c := r.ShallowCopy()
		h := c.Header()
		h.Sequence = s
		c.SetHeader(h)
		return c
	}
	testCases := []struct {
		reqs       []Request
		isComplete bool
	}{
		{[]Request{get, put}, false},
		{[]Request{put, get}, false},
		{[]Request{etA}, false},
		{[]Request{etC}, false},
		{[]Request{get, etA}, false},
		{[]Request{get, etC}, false},
		{[]Request{put, get, etA}, false},
		{[]Request{put, get, etC}, false},
		{[]Request{withSeq(etA, 1)}, false},
		{[]Request{withSeq(etC, 1)}, true},
		{[]Request{put, withSeq(etC, 3)}, false},
		{[]Request{withSeq(put, 1), withSeq(etC, 3)}, false},
		{[]Request{withSeq(put, 2), withSeq(etC, 3)}, false},
		{[]Request{withSeq(put, 1), withSeq(put, 2), withSeq(etA, 3)}, false},
		{[]Request{withSeq(put, 1), withSeq(put, 2), withSeq(etC, 3)}, true},
		{[]Request{withSeq(put, 1), withSeq(put, 2), withSeq(etC, 4)}, false},
		{[]Request{withSeq(put, 1), withSeq(put, 2), withSeq(put, 3), withSeq(etA, 4)}, false},
		{[]Request{withSeq(put, 1), withSeq(put, 2), withSeq(put, 3), withSeq(etC, 4)}, true},
		{[]Request{withSeq(get, 0), withSeq(put, 1), withSeq(get, 1), withSeq(etC, 3)}, false},
		{[]Request{withSeq(get, 0), withSeq(get, 1), withSeq(put, 2), withSeq(etC, 3)}, false},
		{[]Request{withSeq(get, 0), withSeq(put, 1), withSeq(put, 2), withSeq(get, 2), withSeq(etC, 3)}, true},
		{[]Request{withSeq(put, 1), withSeq(get, 1), withSeq(put, 2), withSeq(etC, 4)}, false},
		{[]Request{withSeq(get, 0), withSeq(put, 1), withSeq(put, 2), withSeq(put, 3), withSeq(get, 3), withSeq(etC, 4)}, true},
	}
	for i, test := range testCases {
		ba := BatchRequest{}
		for _, args := range test.reqs {
			ba.Add(args)
		}
		complete := ba.IsCompleteTransaction()
		if complete != test.isComplete {
			t.Errorf("%d: expected IsCompleteTransaction=%t, found %t", i, test.isComplete, complete)
		}
	}
}

func TestBatchSplit(t *testing.T) {
	get := &GetRequest{}
	scan := &ScanRequest{}
	put := &PutRequest{}
	spl := &AdminSplitRequest{}
	dr := &DeleteRangeRequest{}
	et := &EndTxnRequest{}
	qi := &QueryIntentRequest{}
	rv := &ReverseScanRequest{}
	testCases := []struct {
		reqs       []Request
		sizes      []int
		canSplitET bool
	}{
		{[]Request{get, put}, []int{1, 1}, true},
		{[]Request{put, et}, []int{1, 1}, true},
		{[]Request{get, get, get, put, put, get, get}, []int{3, 2, 2}, true},
		{[]Request{spl, get, scan, spl, get}, []int{1, 2, 1, 1}, true},
		{[]Request{spl, spl, get, spl}, []int{1, 1, 1, 1}, true},
		{[]Request{get, scan, get, dr, rv, put, et}, []int{3, 1, 1, 1, 1}, true},
		// Same one again, but this time don't allow EndTxn to be split.
		{[]Request{get, scan, get, dr, rv, put, et}, []int{3, 1, 1, 2}, false},
		// An invalid request in real life, but it demonstrates that we'll
		// always split **after** an EndTxn (because either the next request
		// wants to be alone, or its flags can't match the current flags, which
		// have isAlone set). Could be useful if we ever want to allow executing
		// multiple batches back-to-back.
		{[]Request{et, scan, et}, []int{1, 2}, false},
		{[]Request{et, et}, []int{1, 1}, false},
		// QueryIntents count as headers that are always compatible with the
		// request that follows.
		{[]Request{get, qi, put}, []int{1, 2}, true},
		{[]Request{get, qi, qi, qi, qi, put}, []int{1, 5}, true},
		{[]Request{qi, get, qi, get, qi, get, qi, put, qi, put, qi, get, qi, get}, []int{6, 4, 4}, true},
		{[]Request{qi, spl, qi, get, scan, qi, qi, spl, qi, get}, []int{1, 1, 5, 1, 2}, true},
		{[]Request{scan, qi, qi, qi, et}, []int{4, 1}, true},
		{[]Request{scan, qi, qi, qi, et}, []int{5}, false},
		{[]Request{put, qi, qi, qi, et}, []int{1, 3, 1}, true},
		{[]Request{put, qi, qi, qi, et}, []int{5}, false},
	}

	for i, test := range testCases {
		ba := BatchRequest{}
		for _, args := range test.reqs {
			ba.Add(args)
		}
		var partLen []int
		var recombined []RequestUnion
		for _, part := range ba.Split(test.canSplitET) {
			recombined = append(recombined, part...)
			partLen = append(partLen, len(part))
		}
		if !reflect.DeepEqual(partLen, test.sizes) {
			t.Errorf("%d: expected chunks %v, got %v", i, test.sizes, partLen)
		}
		if !reflect.DeepEqual(recombined, ba.Requests) {
			t.Errorf("%d: started with:\n%+v\ngot back:\n%+v", i, ba.Requests, recombined)
		}
	}
}

func TestBatchRequestGetArg(t *testing.T) {
	get := RequestUnion{
		Value: &RequestUnion_Get{Get: &GetRequest{}},
	}
	end := RequestUnion{
		Value: &RequestUnion_EndTxn{EndTxn: &EndTxnRequest{}},
	}
	testCases := []struct {
		bu         []RequestUnion
		expB, expG bool
	}{
		{[]RequestUnion{}, false, false},
		{[]RequestUnion{get}, false, true},
		{[]RequestUnion{end, get}, false, true},
		{[]RequestUnion{get, end}, true, true},
	}

	for i, c := range testCases {
		br := BatchRequest{Requests: c.bu}
		if _, r := br.GetArg(EndTxn); r != c.expB {
			t.Errorf("%d: unexpected batch request for %v: %v", i, c.bu, r)
		}
		if _, r := br.GetArg(Get); r != c.expG {
			t.Errorf("%d: unexpected get match for %v: %v", i, c.bu, r)
		}

	}
}

func TestBatchRequestSummary(t *testing.T) {
	// The Summary function is generated automatically, so the tests don't need to
	// be exhaustive.
	testCases := []struct {
		reqs     []Request
		expected string
	}{
		{
			reqs:     []Request{},
			expected: "empty batch",
		},
		{
			reqs:     []Request{&GetRequest{}},
			expected: "1 Get",
		},
		{
			reqs:     []Request{&PutRequest{}},
			expected: "1 Put",
		},
		{
			reqs:     []Request{&ConditionalPutRequest{}},
			expected: "1 CPut",
		},
		{
			reqs:     []Request{&ReverseScanRequest{}},
			expected: "1 RevScan",
		},
		{
			reqs: []Request{
				&GetRequest{}, &GetRequest{}, &PutRequest{}, &ScanRequest{}, &ScanRequest{},
			},
			expected: "2 Get, 1 Put, 2 Scan",
		},
		{
			reqs: []Request{
				&CheckConsistencyRequest{}, &InitPutRequest{}, &TruncateLogRequest{},
			},
			expected: "1 TruncLog, 1 ChkConsistency, 1 InitPut",
		},
	}
	for i, tc := range testCases {
		var br BatchRequest
		for _, v := range tc.reqs {
			var ru RequestUnion
			ru.MustSetInner(v)
			br.Requests = append(br.Requests, ru)
		}
		if str := br.Summary(); str != tc.expected {
			t.Errorf("%d: got '%s', expected '%s', batch: %+v", i, str, tc.expected, br)
		}
	}
}

func TestLockSpanIterate(t *testing.T) {
	type testReq struct {
		req    Request
		resp   Response
		span   Span
		resume Span
	}
	testReqs := []testReq{
		{&GetRequest{}, &GetResponse{}, sp("a", ""), sp("", "")},
		{&ScanRequest{}, &ScanResponse{}, sp("b", "d"), sp("c", "d")},
		{&ReverseScanRequest{}, &ReverseScanResponse{}, sp("e", "g"), sp("f", "g")},
		{&PutRequest{}, &PutResponse{}, sp("h", ""), sp("", "")},
		{&DeleteRangeRequest{}, &DeleteRangeResponse{}, sp("i", "k"), sp("j", "k")},
		{&GetRequest{KeyLocking: lock.Exclusive}, &GetResponse{}, sp("l", ""), sp("", "")},
		{&ScanRequest{KeyLocking: lock.Exclusive}, &ScanResponse{}, sp("m", "o"), sp("n", "o")},
		{&ReverseScanRequest{KeyLocking: lock.Exclusive}, &ReverseScanResponse{}, sp("p", "r"), sp("q", "r")},
	}

	// NB: can't import testutils for RunTrueAndFalse.
	for _, resume := range []bool{false, true} {
		t.Run(fmt.Sprintf("resume=%t", resume), func(t *testing.T) {
			// A batch request with a batch response with no ResumeSpan.
			ba := BatchRequest{}
			br := BatchResponse{}
			for i := range testReqs {
				tr := &testReqs[i]
				tr.req.SetHeader(RequestHeaderFromSpan(tr.span))
				ba.Add(tr.req)
				if resume {
					tr.resp.SetHeader(ResponseHeader{ResumeSpan: &tr.resume})
				}
				br.Add(tr.resp)
			}

			var spans [lock.MaxDurability + 1][]Span
			fn := func(span Span, dur lock.Durability) {
				spans[dur] = append(spans[dur], span)
			}
			ba.LockSpanIterate(&br, fn)

			toExpSpans := func(trs ...testReq) []Span {
				exp := make([]Span, len(trs))
				for i, tr := range trs {
					exp[i] = tr.span
					if resume {
						exp[i].EndKey = tr.resume.Key
					}
				}
				return exp
			}

			// The intent writes are replicated locking request.
			require.Equal(t, toExpSpans(testReqs[3], testReqs[4]), spans[lock.Replicated])

			// The scans with KeyLocking are unreplicated locking requests.
			require.Equal(t, toExpSpans(testReqs[5], testReqs[6], testReqs[7]), spans[lock.Unreplicated])
		})
	}
}

func TestRefreshSpanIterate(t *testing.T) {
	testCases := []struct {
		req    Request
		resp   Response
		span   Span
		resume Span
	}{
		{&ConditionalPutRequest{}, &ConditionalPutResponse{}, sp("a", ""), Span{}},
		{&PutRequest{}, &PutResponse{}, sp("a-put", ""), Span{}},
		{&InitPutRequest{}, &InitPutResponse{}, sp("a-initput", ""), Span{}},
		{&IncrementRequest{}, &IncrementResponse{}, sp("a-inc", ""), Span{}},
		{&ScanRequest{}, &ScanResponse{}, sp("a", "c"), sp("b", "c")},
		{&GetRequest{}, &GetResponse{}, sp("b", ""), Span{}},
		{&ReverseScanRequest{}, &ReverseScanResponse{}, sp("d", "f"), sp("d", "e")},
		{&DeleteRangeRequest{}, &DeleteRangeResponse{}, sp("g", "i"), sp("h", "i")},
	}

	// A batch request with a batch response with no ResumeSpan.
	ba := BatchRequest{}
	br := BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(RequestHeaderFromSpan(tc.span))
		ba.Add(tc.req)
		br.Add(tc.resp)
	}

	var readSpans []Span
	fn := func(span Span) {
		readSpans = append(readSpans, span)
	}
	ba.RefreshSpanIterate(&br, fn)
	// The conditional put and init put are not considered read spans.
	expReadSpans := []Span{testCases[4].span, testCases[5].span, testCases[6].span, testCases[7].span}
	require.Equal(t, expReadSpans, readSpans)

	// Batch responses with ResumeSpans.
	ba = BatchRequest{}
	br = BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(RequestHeaderFromSpan(tc.span))
		ba.Add(tc.req)
		if tc.resume.Key != nil {
			resume := tc.resume
			tc.resp.SetHeader(ResponseHeader{ResumeSpan: &resume})
		}
		br.Add(tc.resp)
	}

	readSpans = []Span{}
	ba.RefreshSpanIterate(&br, fn)
	expReadSpans = []Span{
		sp("a", "b"),
		sp("b", ""),
		sp("e", "f"),
		sp("g", "h"),
	}
	require.Equal(t, expReadSpans, readSpans)
}

func TestBatchResponseCombine(t *testing.T) {
	br := &BatchResponse{}
	{
		txn := MakeTransaction(
			"test", nil /* baseKey */, NormalUserPriority,
			hlc.Timestamp{WallTime: 123}, 0, /* maxOffsetNs */
		)
		brTxn := &BatchResponse{
			BatchResponse_Header: BatchResponse_Header{
				Txn: &txn,
			},
		}
		if err := br.Combine(brTxn, nil); err != nil {
			t.Fatal(err)
		}
		if br.Txn.Name != "test" {
			t.Fatal("Combine() did not update the header")
		}
	}

	br.Responses = make([]ResponseUnion, 1)

	singleScanBR := func() *BatchResponse {
		var union ResponseUnion
		union.MustSetInner(&ScanResponse{
			Rows: []KeyValue{{
				Key: Key("bar"),
			}},
			IntentRows: []KeyValue{{
				Key: Key("baz"),
			}},
		})
		return &BatchResponse{
			Responses: []ResponseUnion{union},
		}
	}
	// Combine twice with a single scan result: first one should simply copy over, second
	// one should add on top. Slightly different code paths internally, hence the distinction.
	for i := 0; i < 2; i++ {
		if err := br.Combine(singleScanBR(), []int{0}); err != nil {
			t.Fatal(err)
		}
		scan := br.Responses[0].GetInner().(*ScanResponse)
		if exp := i + 1; len(scan.Rows) != exp {
			t.Fatalf("expected %d rows, got %+v", exp, br)
		}
		if exp := i + 1; len(scan.IntentRows) != exp {
			t.Fatalf("expected %d intent rows, got %+v", exp, br)
		}
	}

	var union ResponseUnion
	union.MustSetInner(&PutResponse{})
	br.Responses = []ResponseUnion{union, br.Responses[0]}

	// Now we have br = [Put, Scan]. Combine should use the position to
	// combine singleScanBR on top of the Scan at index one.
	if err := br.Combine(singleScanBR(), []int{1}); err != nil {
		t.Fatal(err)
	}
	scan := br.Responses[1].GetInner().(*ScanResponse)
	expRows := 3
	if len(scan.Rows) != expRows {
		t.Fatalf("expected %d rows, got %s", expRows, pretty.Sprint(scan))
	}
	if len(scan.IntentRows) != expRows {
		t.Fatalf("expected %d intent rows, got %s", expRows, pretty.Sprint(scan))
	}
	if err := br.Combine(singleScanBR(), []int{0}); err.Error() !=
		`can not combine *roachpb.PutResponse and *roachpb.ScanResponse` {
		t.Fatal(err)
	}
}

func sp(start, end string) Span {
	res := Span{Key: Key(start)}
	if end != "" {
		res.EndKey = Key(end)
	}
	return res
}
