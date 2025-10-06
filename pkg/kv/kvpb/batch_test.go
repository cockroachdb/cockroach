// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	ulget := &GetRequest{KeyLockingStrength: lock.Exclusive, KeyLockingDurability: lock.Unreplicated}
	rlget := &GetRequest{KeyLockingStrength: lock.Exclusive, KeyLockingDurability: lock.Replicated}
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
		{[]Request{ulget, put}, []int{1, 1}, true},
		{[]Request{rlget, put}, []int{2}, true},
		{[]Request{put, et}, []int{1, 1}, true},
		{[]Request{get, get, get, put, put, get, get}, []int{3, 2, 2}, true},
		{[]Request{spl, get, scan, spl, get}, []int{1, 2, 1, 1}, true},
		{[]Request{spl, spl, get, spl}, []int{1, 1, 1, 1}, true},
		{[]Request{scan, get, scan, get}, []int{4}, true},
		{[]Request{rv, get, rv, get}, []int{4}, true},
		{[]Request{scan, get, rv, get}, []int{2, 2}, true},
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
		{[]Request{qi, get, qi, put}, []int{2, 2}, true},
		{[]Request{qi, ulget, qi, put}, []int{2, 2}, true},
		{[]Request{qi, rlget, qi, put}, []int{4}, true},
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
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}
	spanC := roachpb.Span{Key: keyC}
	spanAC := roachpb.Span{Key: keyA, EndKey: keyC}
	spanAD := roachpb.Span{Key: keyA, EndKey: keyD}

	pointHeader := RequestHeader{Key: keyA}
	rangeHeader := RequestHeader{Key: keyA, EndKey: keyD}

	getReqRepl := &GetRequest{
		RequestHeader:        pointHeader,
		KeyLockingStrength:   lock.Exclusive,
		KeyLockingDurability: lock.Replicated,
	}
	getReqUnrepl := &GetRequest{
		RequestHeader:        pointHeader,
		KeyLockingStrength:   lock.Exclusive,
		KeyLockingDurability: lock.Unreplicated,
	}
	scanReq := &ScanRequest{
		RequestHeader:        rangeHeader,
		ScanFormat:           KEY_VALUES,
		KeyLockingStrength:   lock.Exclusive,
		KeyLockingDurability: lock.Replicated,
	}
	revScanReq := &ReverseScanRequest{
		RequestHeader:        rangeHeader,
		ScanFormat:           KEY_VALUES,
		KeyLockingStrength:   lock.Exclusive,
		KeyLockingDurability: lock.Replicated,
	}

	testCases := []struct {
		name      string
		req       Request
		resp      Response
		expSpans  []roachpb.Span
		expUnrepl bool
	}{
		{
			name:     "get, missing",
			req:      getReqRepl,
			resp:     &GetResponse{},
			expSpans: nil,
		},
		{
			name:     "get, present",
			req:      getReqRepl,
			resp:     &GetResponse{Value: &roachpb.Value{}},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "get, no response",
			req:      getReqRepl,
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:      "get, missing, unreplicated",
			req:       getReqUnrepl,
			resp:      &GetResponse{},
			expSpans:  nil,
			expUnrepl: true,
		},
		{
			name:      "get, present, unreplicated",
			req:       getReqUnrepl,
			resp:      &GetResponse{Value: &roachpb.Value{}},
			expSpans:  []roachpb.Span{spanA},
			expUnrepl: true,
		},
		{
			name:      "get, no response, unreplicated",
			req:       getReqUnrepl,
			resp:      nil,
			expSpans:  []roachpb.Span{spanA},
			expUnrepl: true,
		},
		{
			name:     "non-locking get, present",
			req:      &GetRequest{RequestHeader: pointHeader},
			resp:     &GetResponse{Value: &roachpb.Value{}},
			expSpans: nil,
		},
		{
			name:     "scan, missing",
			req:      scanReq,
			resp:     &ScanResponse{},
			expSpans: nil,
		},
		{
			name: "scan, present",
			req:  scanReq,
			resp: &ScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyA}, {Key: keyB}, {Key: keyC},
			}},
			expSpans: []roachpb.Span{spanA, spanB, spanC},
		},
		{
			name:     "scan, no response",
			req:      scanReq,
			resp:     nil,
			expSpans: []roachpb.Span{spanAD},
		},
		{
			name: "non-locking scan, present",
			req:  &ScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp: &ScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyA}, {Key: keyB}, {Key: keyC},
			}},
			expSpans: nil,
		},
		{
			name:     "reverse-scan, missing",
			req:      revScanReq,
			resp:     &ReverseScanResponse{},
			expSpans: nil,
		},
		{
			name: "reverse-scan, present",
			req:  revScanReq,
			resp: &ReverseScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyC}, {Key: keyB}, {Key: keyA},
			}},
			expSpans: []roachpb.Span{spanC, spanB, spanA},
		},
		{
			name:     "reverse-scan, no response",
			req:      revScanReq,
			resp:     nil,
			expSpans: []roachpb.Span{spanAD},
		},
		{
			name: "non-locking reverse-scan, present",
			req:  &ReverseScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp: &ReverseScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyC}, {Key: keyB}, {Key: keyA},
			}},
			expSpans: nil,
		},
		{
			name:     "put",
			req:      &PutRequest{RequestHeader: pointHeader},
			resp:     &PutResponse{},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "cput",
			req:      &ConditionalPutRequest{RequestHeader: pointHeader},
			resp:     &ConditionalPutResponse{},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "initput",
			req:      &InitPutRequest{RequestHeader: pointHeader},
			resp:     &InitPutResponse{},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "increment",
			req:      &IncrementRequest{RequestHeader: pointHeader},
			resp:     &IncrementResponse{},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "delete",
			req:      &DeleteRequest{RequestHeader: pointHeader},
			resp:     &DeleteResponse{},
			expSpans: []roachpb.Span{spanA},
		},
		{
			name:     "delete-range, no response keys",
			req:      &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: true},
			resp:     &DeleteRangeResponse{Keys: nil},
			expSpans: nil,
		},
		{
			name:     "delete-range, some response keys",
			req:      &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: true},
			resp:     &DeleteRangeResponse{Keys: []roachpb.Key{keyB, keyC}},
			expSpans: []roachpb.Span{spanB, spanC},
		},
		{
			name:     "delete-range, return keys disabled",
			req:      &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: false},
			resp:     &DeleteRangeResponse{},
			expSpans: []roachpb.Span{spanAD},
		},
		{
			name: "delete-range, return keys disabled, resume span",
			req:  &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: false},
			resp: &DeleteRangeResponse{
				ResponseHeader: ResponseHeader{
					ResumeSpan: &roachpb.Span{Key: keyC, EndKey: keyD},
				},
				Keys: nil,
			},
			expSpans: []roachpb.Span{spanAC},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var spans []roachpb.Span
			var durs []lock.Durability
			err := LockSpanIterate(tc.req, tc.resp, func(span roachpb.Span, dur lock.Durability) {
				spans = append(spans, span)
				durs = append(durs, dur)
			})
			require.NoError(t, err)
			require.Equal(t, tc.expSpans, spans)
			if len(spans) > 0 {
				dur := durs[0]
				for _, otherDur := range durs[1:] {
					require.Equal(t, dur, otherDur)
				}
				expDur := lock.Replicated
				if tc.expUnrepl {
					expDur = lock.Unreplicated
				}
				require.Equal(t, expDur, dur)
			}
		})
	}
}

func TestRefreshSpanIterate(t *testing.T) {
	testCases := []struct {
		req    Request
		resp   Response
		span   roachpb.Span
		resume roachpb.Span
	}{
		{&ConditionalPutRequest{}, &ConditionalPutResponse{}, sp("a", ""), roachpb.Span{}},
		{&PutRequest{}, &PutResponse{}, sp("a-put", ""), roachpb.Span{}},
		{&InitPutRequest{}, &InitPutResponse{}, sp("a-initput", ""), roachpb.Span{}},
		{&IncrementRequest{}, &IncrementResponse{}, sp("a-inc", ""), roachpb.Span{}},
		{&ScanRequest{}, &ScanResponse{}, sp("a", "c"), sp("b", "c")},
		{&GetRequest{}, &GetResponse{}, sp("b", ""), roachpb.Span{}},
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

	var readSpans []roachpb.Span
	fn := func(span roachpb.Span) {
		readSpans = append(readSpans, span)
	}
	require.NoError(t, ba.RefreshSpanIterate(&br, fn))
	// The conditional put and init put are not considered read spans.
	expReadSpans := []roachpb.Span{testCases[4].span, testCases[5].span, testCases[6].span, testCases[7].span}
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

	readSpans = []roachpb.Span{}
	require.NoError(t, ba.RefreshSpanIterate(&br, fn))
	expReadSpans = []roachpb.Span{
		sp("a", "b"),
		sp("b", ""),
		sp("e", "f"),
		sp("g", "h"),
	}
	require.Equal(t, expReadSpans, readSpans)
}

func TestRefreshSpanIterateSkipLocked(t *testing.T) {
	ba := BatchRequest{}
	ba.Add(NewGet(roachpb.Key("a")))
	ba.Add(NewScan(roachpb.Key("b"), roachpb.Key("d")))
	ba.Add(NewReverseScan(roachpb.Key("e"), roachpb.Key("g")))
	br := ba.CreateReply()

	// Without a SkipLocked wait policy.
	var readSpans []roachpb.Span
	fn := func(span roachpb.Span) { readSpans = append(readSpans, span) }
	require.NoError(t, ba.RefreshSpanIterate(br, fn))
	expReadSpans := []roachpb.Span{
		sp("a", ""),
		sp("b", "d"),
		sp("e", "g"),
	}
	require.Equal(t, expReadSpans, readSpans)

	// With a SkipLocked wait policy and without any response keys.
	ba.WaitPolicy = lock.WaitPolicy_SkipLocked

	readSpans = nil
	require.NoError(t, ba.RefreshSpanIterate(br, fn))
	expReadSpans = []roachpb.Span(nil)
	require.Equal(t, expReadSpans, readSpans)

	// With a SkipLocked wait policy and with some response keys.
	br.Responses[0].GetGet().Value = &roachpb.Value{}
	br.Responses[1].GetScan().Rows = []roachpb.KeyValue{{Key: roachpb.Key("b")}, {Key: roachpb.Key("c")}}
	br.Responses[2].GetReverseScan().Rows = []roachpb.KeyValue{{Key: roachpb.Key("f")}}

	readSpans = nil
	require.NoError(t, ba.RefreshSpanIterate(br, fn))
	expReadSpans = []roachpb.Span{
		sp("a", ""),
		sp("b", ""),
		sp("c", ""),
		sp("f", ""),
	}
	require.Equal(t, expReadSpans, readSpans)
}

func TestResponseKeyIterate(t *testing.T) {
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	// Encoded keyB with value in the batch_response format.
	keyBBatchRespEnc := []byte{3, 0, 0, 0, 2, 0, 0, 0, 98, 0, 102, 111, 111}

	pointHeader := RequestHeader{Key: keyA}
	rangeHeader := RequestHeader{Key: keyA, EndKey: keyD}

	testCases := []struct {
		name    string
		req     Request
		resp    Response
		expKeys []roachpb.Key
		expErr  string
	}{
		{
			name:    "get, missing",
			req:     &GetRequest{RequestHeader: pointHeader},
			resp:    &GetResponse{},
			expKeys: nil,
		},
		{
			name:    "get, present",
			req:     &GetRequest{RequestHeader: pointHeader},
			resp:    &GetResponse{Value: &roachpb.Value{}},
			expKeys: []roachpb.Key{keyA},
		},
		{
			name:   "get, no response",
			req:    &GetRequest{RequestHeader: pointHeader},
			resp:   nil,
			expErr: "cannot iterate over response keys of Get request with nil response",
		},
		{
			name:    "scan, missing",
			req:     &ScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp:    &ScanResponse{},
			expKeys: nil,
		},
		{
			name: "scan, present, format=key-values",
			req:  &ScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp: &ScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyA}, {Key: keyB}, {Key: keyC},
			}},
			expKeys: []roachpb.Key{keyA, keyB, keyC},
		},
		{
			name:    "scan, present, format=batch-response",
			req:     &ScanRequest{RequestHeader: rangeHeader, ScanFormat: BATCH_RESPONSE},
			resp:    &ScanResponse{BatchResponses: [][]byte{keyBBatchRespEnc}},
			expKeys: []roachpb.Key{keyB},
		},
		{
			name:   "scan, present, format=col-batch-response",
			req:    &ScanRequest{RequestHeader: rangeHeader, ScanFormat: COL_BATCH_RESPONSE},
			resp:   &ScanResponse{},
			expErr: "unexpectedly called ResponseKeyIterate on a ScanRequest with COL_BATCH_RESPONSE scan format",
		},
		{
			name:    "reverse-scan, missing",
			req:     &ReverseScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp:    &ReverseScanResponse{},
			expKeys: nil,
		},
		{
			name: "reverse-scan, present, format=key-values",
			req:  &ReverseScanRequest{RequestHeader: rangeHeader, ScanFormat: KEY_VALUES},
			resp: &ReverseScanResponse{Rows: []roachpb.KeyValue{
				{Key: keyC}, {Key: keyB}, {Key: keyA},
			}},
			expKeys: []roachpb.Key{keyC, keyB, keyA},
		},
		{
			name:    "reverse-scan, present, format=batch-response",
			req:     &ReverseScanRequest{RequestHeader: rangeHeader, ScanFormat: BATCH_RESPONSE},
			resp:    &ReverseScanResponse{BatchResponses: [][]byte{keyBBatchRespEnc}},
			expKeys: []roachpb.Key{keyB},
		},
		{
			name:   "reverse-scan, present, format=col-batch-response",
			req:    &ReverseScanRequest{RequestHeader: rangeHeader, ScanFormat: COL_BATCH_RESPONSE},
			resp:   &ReverseScanResponse{},
			expErr: "unexpectedly called ResponseKeyIterate on a ReverseScanRequest with COL_BATCH_RESPONSE scan format",
		},
		{
			name:    "delete-range, no response keys",
			req:     &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: true},
			resp:    &DeleteRangeResponse{Keys: nil},
			expKeys: nil,
		},
		{
			name:    "delete-range, some response keys",
			req:     &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: true},
			resp:    &DeleteRangeResponse{Keys: []roachpb.Key{keyB, keyC}},
			expKeys: []roachpb.Key{keyB, keyC},
		},
		{
			name:   "delete-range, return keys disabled",
			req:    &DeleteRangeRequest{RequestHeader: rangeHeader, ReturnKeys: false},
			resp:   &DeleteRangeResponse{},
			expErr: "cannot iterate over response keys of DeleteRange request when ReturnKeys=false",
		},
		{
			name:   "put",
			req:    &PutRequest{},
			resp:   &PutResponse{},
			expErr: "cannot iterate over response keys of Put request",
		},
		{
			name:   "cput",
			req:    &ConditionalPutRequest{},
			resp:   &ConditionalPutResponse{},
			expErr: "cannot iterate over response keys of ConditionalPut request",
		},
		{
			name:   "initput",
			req:    &InitPutRequest{},
			resp:   &InitPutResponse{},
			expErr: "cannot iterate over response keys of InitPut request",
		},
		{
			name:   "increment",
			req:    &IncrementRequest{},
			resp:   &IncrementResponse{},
			expErr: "cannot iterate over response keys of Increment request",
		},
		{
			name:   "delete",
			req:    &DeleteRequest{},
			resp:   &DeleteResponse{},
			expErr: "cannot iterate over response keys of Delete request",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var keys []roachpb.Key
			err := ResponseKeyIterate(tc.req, tc.resp, func(key roachpb.Key) {
				keys = append(keys, key)
			})
			if tc.expErr == "" {
				require.Equal(t, tc.expKeys, keys)
				require.NoError(t, err)
			} else {
				require.Empty(t, keys)
				require.Error(t, err)
				require.Regexp(t, tc.expErr, err)
			}
			require.Equal(t, err == nil, canIterateResponseKeys(tc.req, tc.resp))
		})
	}
}

func TestBatchResponseCombine(t *testing.T) {
	br := &BatchResponse{}
	{
		txn := roachpb.MakeTransaction(
			"test", nil /* baseKey */, isolation.Serializable, roachpb.NormalUserPriority,
			hlc.Timestamp{WallTime: 123}, 0 /* baseKey */, 99 /* coordinatorNodeID */, 0, false, /* omitInRangefeeds */
		)
		brTxn := &BatchResponse{
			BatchResponse_Header: BatchResponse_Header{
				Txn: &txn,
			},
		}
		if err := br.Combine(context.Background(), brTxn, nil, &BatchRequest{}); err != nil {
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
			Rows: []roachpb.KeyValue{{
				Key: roachpb.Key("bar"),
			}},
			IntentRows: []roachpb.KeyValue{{
				Key: roachpb.Key("baz"),
			}},
		})
		return &BatchResponse{
			Responses: []ResponseUnion{union},
		}
	}
	// Combine twice with a single scan result: first one should simply copy over, second
	// one should add on top. Slightly different code paths internally, hence the distinction.
	for i := 0; i < 2; i++ {
		if err := br.Combine(context.Background(), singleScanBR(), []int{0}, &BatchRequest{}); err != nil {
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
	if err := br.Combine(context.Background(), singleScanBR(), []int{1}, &BatchRequest{}); err != nil {
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
	if err := br.Combine(context.Background(), singleScanBR(), []int{0}, &BatchRequest{}); err.Error() !=
		`can not combine *kvpb.PutResponse and *kvpb.ScanResponse` {
		t.Fatal(err)
	}
}

func sp(start, end string) roachpb.Span {
	res := roachpb.Span{Key: roachpb.Key(start)}
	if end != "" {
		res.EndKey = roachpb.Key(end)
	}
	return res
}
