// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"fmt"
	"reflect"
	"testing"
)

func TestClientCmdIDIsEmpty(t *testing.T) {
	if !(ClientCmdID{}).IsEmpty() {
		t.Error("expected cmd to be empty")
	}
	if (ClientCmdID{WallTime: 1}).IsEmpty() {
		t.Error("expected cmd to not be empty")
	}
	if (ClientCmdID{Random: 1}).IsEmpty() {
		t.Error("expected cmd to not be empty")
	}
}

type testError struct{}

func (t *testError) Error() string  { return "test" }
func (t *testError) CanRetry() bool { return true }

// TestResponseHeaderSetGoError verifies that a test error that
// implements retryable is converted properly into a generic error.
func TestResponseHeaderSetGoError(t *testing.T) {
	rh := ResponseHeader{}
	rh.SetGoError(&testError{})
	err := rh.GoError()
	if err.Error() != "test" {
		t.Fatalf("unexpected error: %s", err)
	}
	if !rh.Error.Retryable {
		t.Error("expected generic error to be retryable")
	}
}

// TestResponseHeaderNilError verifies that a nil error can be set
// and retrieved from a response header.
func TestResponseHeaderNilError(t *testing.T) {
	rh := ResponseHeader{}
	rh.SetGoError(nil)
	if err := rh.GoError(); err != nil {
		t.Errorf("expected nil error; got %s", err)
	}
}

type XX interface {
	Run()
}
type YY int

func (i YY) Run() {
	fmt.Println(i)
}

// TestCombinable tests the correct behaviour of some types that implement
// the Combinable interface, notably {Scan,DeleteRange}Response and
// ResponseHeader.
func TestCombinable(t *testing.T) {
	// Test that GetResponse doesn't have anything to do with Combinable.
	if _, ok := interface{}(&GetResponse{}).(Combinable); ok {
		t.Fatalf("GetResponse implements Combinable, so presumably all Response types will")
	}
	// Test that {Scan,DeleteRange}Response properly implement it.
	sr1 := &ScanResponse{
		ResponseHeader: ResponseHeader{Timestamp: MinTimestamp},
		Rows: []KeyValue{
			{Key: Key("A"), Value: Value{Bytes: []byte("V")}},
		},
	}

	if _, ok := interface{}(sr1).(Combinable); !ok {
		t.Fatalf("ScanResponse does not implement Combinable")
	}

	sr2 := &ScanResponse{
		ResponseHeader: ResponseHeader{Timestamp: MinTimestamp},
		Rows: []KeyValue{
			{Key: Key("B"), Value: Value{Bytes: []byte("W")}},
		},
	}
	sr2.Timestamp = MaxTimestamp

	wantedSR := &ScanResponse{
		ResponseHeader: ResponseHeader{Timestamp: MaxTimestamp},
		Rows:           append(append([]KeyValue(nil), sr1.Rows...), sr2.Rows...),
	}

	if err := sr1.Combine(sr2); err != nil {
		t.Fatal(err)
	}
	if err := sr1.Combine(&ScanResponse{}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sr1, wantedSR) {
		t.Errorf("wanted %v, got %v", wantedSR, sr1)
	}

	dr1 := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 100}},
		NumDeleted:     5,
	}
	if _, ok := interface{}(dr1).(Combinable); !ok {
		t.Fatalf("DeleteRangeResponse does not implement Combinable")
	}
	dr2 := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 1}},
		NumDeleted:     12,
	}
	dr3 := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 111}},
		NumDeleted:     3,
	}
	wantedDR := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 111}},
		NumDeleted:     20,
	}
	if err := dr2.Combine(dr3); err != nil {
		t.Fatal(err)
	}
	if err := dr1.Combine(dr2); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(dr1, wantedDR) {
		t.Errorf("wanted %v, got %v", wantedDR, dr1)
	}
}

func TestSetGoErrorCopy(t *testing.T) {
	rh := ResponseHeader{}
	oErr := &Error{Message: "test123"}
	rh.Error = oErr
	rh.SetGoError(&testError{})
	if oErr.Message != "test123" {
		t.Fatalf("SetGoError did not create a new error")
	}
}

func TestBatchKeyRange(t *testing.T) {
	verify := func(br *BatchRequest, start, end Key) {
		if !br.Key.Equal(start) {
			t.Errorf("expected batch start key %s; got %s", start, br.Key)
		}
		if !br.EndKey.Equal(end) {
			t.Errorf("expected batch end key %s; got %s", end, br.EndKey)
		}
	}

	br := &BatchRequest{}
	br.Add(&GetRequest{
		RequestHeader: RequestHeader{
			Key: Key("a"),
		},
	})
	verify(br, Key("a"), nil)
	// Add "a" again; shouldn't set end key.
	br.Add(&GetRequest{
		RequestHeader: RequestHeader{
			Key: Key("a"),
		},
	})
	verify(br, Key("a"), nil)

	br.Add(&GetRequest{
		RequestHeader: RequestHeader{
			Key: Key("b"),
		},
	})
	verify(br, Key("a"), Key("b"))

	br.Add(&ScanRequest{
		RequestHeader: RequestHeader{
			Key:    Key("c"),
			EndKey: Key("d"),
		},
	})
	verify(br, Key("a"), Key("d"))

	br.Add(&ScanRequest{
		RequestHeader: RequestHeader{
			Key:    Key("A"),
			EndKey: Key("d1"),
		},
	})
	verify(br, Key("A"), Key("d1"))
}

func TestBatchSplit(t *testing.T) {
	get := &GetRequest{}
	scan := &ScanRequest{}
	put := &PutRequest{}
	spl := &AdminSplitRequest{}
	dr := &DeleteRangeRequest{}
	et := &EndTransactionRequest{}
	rv := &ReverseScanRequest{}
	testCases := []struct {
		reqs  []Request
		sizes []int
	}{
		{[]Request{get, put}, []int{1, 1}},
		{[]Request{get, get, get, put, put, get, get}, []int{3, 2, 2}},
		{[]Request{get, scan, get, dr, rv, put, et}, []int{3, 1, 1, 1, 1}},
		{[]Request{spl, get, scan, spl, get}, []int{1, 2, 1, 1}},
		{[]Request{spl, spl, get, spl}, []int{1, 1, 1, 1}},
	}

	for i, test := range testCases {
		ba := BatchRequest{}
		for _, args := range test.reqs {
			ba.Add(args)
		}
		var partLen []int
		var recombined []RequestUnion
		for _, part := range ba.Split() {
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
