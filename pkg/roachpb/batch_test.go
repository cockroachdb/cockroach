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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Veteran Lu (23907238@qq.com)

package roachpb

import (
	"reflect"
	"testing"
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
	fn := func(key, endKey Key) {
		spans = append(spans, Span{Key: key, EndKey: endKey})
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
