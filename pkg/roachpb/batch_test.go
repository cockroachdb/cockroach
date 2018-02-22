// Copyright 2015 The Cockroach Authors.
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

package roachpb

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/kr/pretty"
)

func TestBatchSplit(t *testing.T) {
	get := &GetRequest{}
	scan := &ScanRequest{}
	put := &PutRequest{}
	spl := &AdminSplitRequest{}
	dr := &DeleteRangeRequest{}
	bt := &BeginTransactionRequest{}
	et := &EndTransactionRequest{}
	rv := &ReverseScanRequest{}
	np := &NoopRequest{}
	testCases := []struct {
		reqs       []Request
		sizes      []int
		canSplitET bool
	}{
		{[]Request{get, put}, []int{1, 1}, true},
		{[]Request{get, get, get, put, put, get, get}, []int{3, 2, 2}, true},
		{[]Request{spl, get, scan, spl, get}, []int{1, 2, 1, 1}, true},
		{[]Request{spl, spl, get, spl}, []int{1, 1, 1, 1}, true},
		{[]Request{bt, put, et}, []int{2, 1}, true},
		{[]Request{get, scan, get, dr, rv, put, et}, []int{3, 1, 1, 1, 1}, true},
		// Same one again, but this time don't allow EndTransaction to be split.
		{[]Request{get, scan, get, dr, rv, put, et}, []int{3, 1, 1, 2}, false},
		// An invalid request in real life, but it demonstrates that we'll always
		// split **after** an EndTransaction (because either the next request
		// wants to be alone, or its flags can't match the current flags, which
		// have isAlone set). Could be useful if we ever want to allow executing
		// multiple batches back-to-back.
		{[]Request{et, scan, et}, []int{1, 2}, false},
		// Check that Noop can mix with other requests regardless of flags.
		{[]Request{np, put, np}, []int{3}, true},
		{[]Request{np, spl, np}, []int{3}, true},
		{[]Request{np, rv, np}, []int{3}, true},
		{[]Request{np, np, et}, []int{3}, true}, // et does not split off
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
	testCases := []struct {
		bu         []RequestUnion
		expB, expG bool
	}{
		{[]RequestUnion{}, false, false},
		{[]RequestUnion{{Get: &GetRequest{}}}, false, true},
		{[]RequestUnion{{EndTransaction: &EndTransactionRequest{}}, {Get: &GetRequest{}}}, false, true},
		{[]RequestUnion{{EndTransaction: &EndTransactionRequest{}}}, true, false},
		{[]RequestUnion{{Get: &GetRequest{}}, {EndTransaction: &EndTransactionRequest{}}}, true, true},
	}

	for i, c := range testCases {
		br := BatchRequest{Requests: c.bu}
		if _, r := br.GetArg(EndTransaction); r != c.expB {
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
		reqs     []interface{}
		expected string
	}{
		{
			reqs:     []interface{}{},
			expected: "empty batch",
		},
		{
			reqs:     []interface{}{&GetRequest{}},
			expected: "1 Get",
		},
		{
			reqs:     []interface{}{&PutRequest{}},
			expected: "1 Put",
		},
		{
			reqs:     []interface{}{&ConditionalPutRequest{}},
			expected: "1 CPut",
		},
		{
			reqs:     []interface{}{&ReverseScanRequest{}},
			expected: "1 RevScan",
		},
		{
			reqs: []interface{}{
				&GetRequest{}, &GetRequest{}, &PutRequest{}, &ScanRequest{}, &ScanRequest{},
			},
			expected: "2 Get, 1 Put, 2 Scan",
		},
		{
			reqs: []interface{}{
				&CheckConsistencyRequest{}, &InitPutRequest{}, &TruncateLogRequest{},
			},
			expected: "1 TruncLog, 1 ChkConsistency, 1 InitPut",
		},
	}
	for i, tc := range testCases {
		var br BatchRequest
		for _, v := range tc.reqs {
			var ru RequestUnion
			ru.SetValue(v)
			br.Requests = append(br.Requests, ru)
		}
		if str := br.Summary(); str != tc.expected {
			t.Errorf("%d: got '%s', expected '%s', batch: %+v", i, str, tc.expected, br)
		}
	}
}

func TestIntentSpanIterate(t *testing.T) {
	testCases := []struct {
		req    Request
		resp   Response
		span   Span
		resume Span
	}{
		{&ScanRequest{}, &ScanResponse{},
			Span{Key("a"), Key("c")}, Span{Key("b"), Key("c")}},
		{&ReverseScanRequest{}, &ReverseScanResponse{},
			Span{Key("d"), Key("f")}, Span{Key("d"), Key("e")}},
		{&DeleteRangeRequest{}, &DeleteRangeResponse{},
			Span{Key("g"), Key("i")}, Span{Key("h"), Key("i")}},
	}

	// A batch request with a batch response with no ResumeSpan.
	ba := BatchRequest{}
	br := BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(tc.span)
		ba.Add(tc.req)
		br.Add(tc.resp)
	}

	var spans []Span
	fn := func(span Span) {
		spans = append(spans, span)
	}
	ba.IntentSpanIterate(&br, fn)
	// Only DeleteRangeResponse is a write request.
	if e := 1; len(spans) != e {
		t.Fatalf("unexpected number of spans: e = %d, found = %d", e, len(spans))
	}
	if e := testCases[2].span; !reflect.DeepEqual(e, spans[0]) {
		t.Fatalf("unexpected spans: e = %+v, found = %+v", e, spans[0])
	}

	// A batch request with a batch response with a ResumeSpan.
	ba = BatchRequest{}
	br = BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(tc.span)
		ba.Add(tc.req)
		tc.resp.SetHeader(ResponseHeader{ResumeSpan: &tc.resume})
		br.Add(tc.resp)
	}

	spans = []Span{}
	ba.IntentSpanIterate(&br, fn)
	// Only DeleteRangeResponse is a write request.
	if e := 1; len(spans) != e {
		t.Fatalf("unexpected number of spans: e = %d, found = %d", e, len(spans))
	}
	if e := (Span{Key("g"), Key("h")}); !reflect.DeepEqual(e, spans[0]) {
		t.Fatalf("unexpected spans: e = %+v, found = %+v", e, spans[0])
	}
}

func TestRefreshSpanIterate(t *testing.T) {
	testCases := []struct {
		req    Request
		resp   Response
		span   Span
		resume *Span
	}{
		{&ConditionalPutRequest{}, &ConditionalPutResponse{},
			Span{Key: Key("a")}, nil},
		{&PutRequest{}, &PutResponse{},
			Span{Key: Key("a-put")}, nil},
		{&InitPutRequest{}, &InitPutResponse{},
			Span{Key: Key("a-initput")}, nil},
		{&IncrementRequest{}, &IncrementResponse{},
			Span{Key: Key("a-inc")}, nil},
		{&ScanRequest{}, &ScanResponse{},
			Span{Key("a"), Key("c")}, &Span{Key("b"), Key("c")}},
		{&GetRequest{}, &GetResponse{},
			Span{Key: Key("b")}, nil},
		{&ReverseScanRequest{}, &ReverseScanResponse{},
			Span{Key("d"), Key("f")}, &Span{Key("d"), Key("e")}},
		{&DeleteRangeRequest{}, &DeleteRangeResponse{},
			Span{Key("g"), Key("i")}, &Span{Key("h"), Key("i")}},
	}

	// A batch request with a batch response with no ResumeSpan.
	ba := BatchRequest{}
	br := BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(tc.span)
		ba.Add(tc.req)
		br.Add(tc.resp)
	}

	var readSpans []Span
	var writeSpans []Span
	fn := func(span Span, write bool) {
		if write {
			writeSpans = append(writeSpans, span)
		} else {
			readSpans = append(readSpans, span)
		}
	}
	ba.RefreshSpanIterate(&br, fn)
	// Only the conditional put isn't considered a read span.
	expReadSpans := []Span{testCases[2].span, testCases[4].span, testCases[5].span, testCases[6].span}
	expWriteSpans := []Span{testCases[7].span}
	if !reflect.DeepEqual(expReadSpans, readSpans) {
		t.Fatalf("unexpected read spans: expected %+v, found = %+v", expReadSpans, readSpans)
	}
	if !reflect.DeepEqual(expWriteSpans, writeSpans) {
		t.Fatalf("unexpected write spans: expected %+v, found = %+v", expWriteSpans, writeSpans)
	}

	// Batch responses with ResumeSpans.
	ba = BatchRequest{}
	br = BatchResponse{}
	for _, tc := range testCases {
		tc.req.SetHeader(tc.span)
		ba.Add(tc.req)
		if tc.resume != nil {
			tc.resp.SetHeader(ResponseHeader{ResumeSpan: tc.resume})
		}
		br.Add(tc.resp)
	}

	readSpans = []Span{}
	writeSpans = []Span{}
	ba.RefreshSpanIterate(&br, fn)
	expReadSpans = []Span{
		{Key: Key("a-initput")},
		{Key("a"), Key("b")},
		{Key: Key("b")},
		{Key("e"), Key("f")},
	}
	expWriteSpans = []Span{
		{Key("g"), Key("h")},
	}
	if !reflect.DeepEqual(expReadSpans, readSpans) {
		t.Fatalf("unexpected read spans: expected %+v, found = %+v", expReadSpans, readSpans)
	}
	if !reflect.DeepEqual(expWriteSpans, writeSpans) {
		t.Fatalf("unexpected write spans: expected %+v, found = %+v", expWriteSpans, writeSpans)
	}
}

func TestBatchResponseCombine(t *testing.T) {
	br := &BatchResponse{}
	{
		txn := MakeTransaction(
			"test", nil /* baseKey */, NormalUserPriority,
			enginepb.SERIALIZABLE, hlc.Timestamp{WallTime: 123}, 0, /* maxOffsetNs */
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
