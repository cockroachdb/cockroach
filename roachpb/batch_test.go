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

func TestBatchRequestString(t *testing.T) {
	br := BatchRequest{}
	for i := 0; i < 100; i++ {
		br.Requests = append(br.Requests, RequestUnion{Get: &GetRequest{}})
	}
	br.Requests = append(br.Requests, RequestUnion{EndTransaction: &EndTransactionRequest{}})

	e := `Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), Get ["",""), ... 76 skipped ..., Get ["",""), Get ["",""), Get ["",""), Get ["",""), EndTransaction ["","")`
	if e != br.String() {
		t.Fatalf("e = %s, v = %s", e, br.String())
	}
}
