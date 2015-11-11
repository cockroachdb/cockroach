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

package roachpb

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/util/retry"
)

type testError struct{}

func (t *testError) Error() string             { return "test" }
func (t *testError) CanRetry() bool            { return true }
func (t *testError) ErrorIndex() (int32, bool) { return 99, true }
func (t *testError) SetErrorIndex(_ int32)     { panic("unsupported") }

// TestSetGoError verifies that a test error that
// implements retryable or indexed is converted properly into a generic error.
func TestSetGoErrorGeneric(t *testing.T) {
	br := &BatchResponse{}
	br.SetGoError(&testError{})
	err := br.GoError()
	if err.Error() != "test" {
		t.Fatalf("unexpected error: %s", err)
	}
	if !br.Error.Retryable {
		t.Error("expected generic error to be retryable")
	}
	if rErr, ok := br.Error.GoError().(retry.Retryable); !ok || !rErr.CanRetry() {
		t.Error("generated GoError is not retryable")
	}
}

// TestResponseHeaderNilError verifies that a nil error can be set
// and retrieved from a response header.
func TestSetGoErrorNil(t *testing.T) {
	br := &BatchResponse{}
	br.SetGoError(nil)
	if err := br.GoError(); err != nil {
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
			{Key: Key("A"), Value: MakeValueFromString("V")},
		},
	}

	if _, ok := interface{}(sr1).(Combinable); !ok {
		t.Fatalf("ScanResponse does not implement Combinable")
	}

	sr2 := &ScanResponse{
		ResponseHeader: ResponseHeader{Timestamp: MinTimestamp},
		Rows: []KeyValue{
			{Key: Key("B"), Value: MakeValueFromString("W")},
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
	br := &BatchResponse{}
	oErr := &Error{Message: "test123"}
	br.Error = oErr
	br.SetGoError(&testError{})
	if oErr.Message != "test123" {
		t.Fatalf("SetGoError did not create a new error")
	}
}

func TestBatchSplit(t *testing.T) {
	get := &GetRequest{}
	scan := &ScanRequest{}
	put := &PutRequest{}
	spl := &AdminSplitRequest{}
	dr := &DeleteRangeRequest{}
	bt := &BeginTransactionRequest{}
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
		{[]Request{bt, put, et}, []int{2, 1}},
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

func TestFlagsToStr(t *testing.T) {
	var ba BatchRequest
	ba.Add(&PutRequest{})
	ba.Add(&AdminSplitRequest{})
	exp := "AdWrAl"
	if act := flagsToStr(ba.flags()); act != exp {
		t.Fatalf("expected %s, got %s", exp, act)
	}
}

func TestBatchTraceID(t *testing.T) {
	var ba BatchRequest
	ba.Add(&ReverseScanRequest{})
	ba.Add(&IncrementRequest{})

	expID := "cRdWrRgRv@0.000000000,0"
	expName := expID[1:]
	if actID, actName := ba.TraceID(), ba.TraceName(); expID != actID || expName != actName {
		t.Fatalf("expected (%s,%s), got (%s, %s)", expID, expName, actID, actName)
	}

	uuidStr := "a53eef22-35f3-4c69-8e98-c563100e028c"
	bytes, err := hex.DecodeString(strings.Replace(uuidStr, "-", "", -1))
	if err != nil {
		t.Fatal(err)
	}
	ba.Txn = &Transaction{ID: bytes}
	expID = "t" + uuidStr
	expName = expID[:9]
	if actID, actName := ba.TraceID(), ba.TraceName(); expID != actID || expName != actName {
		t.Fatalf("expected (%s,%s), got (%s, %s)", expID, expName, actID, actName)
	}
}
