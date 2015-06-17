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
	rh := KVResponseHeader{}
	rh.SetGoError(&testError{})
	err := rh.GoError()
	if _, ok := err.(*Error); !ok {
		t.Errorf("expected set error to be type Error; got %T", err)
	}
	if !err.(*Error).Retryable {
		t.Error("expected generic error to be retryable")
	}
}

// TestResponseHeaderNilError verifies that a nil error can be set
// and retrieved from a response header.
func TestResponseHeaderNilError(t *testing.T) {
	rh := KVResponseHeader{}
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
		KVResponseHeader: KVResponseHeader{Timestamp: MinTimestamp},
		Rows: []KeyValue{
			{Key: Key("A"), Value: Value{Bytes: []byte("V")}},
		},
	}

	if _, ok := interface{}(sr1).(Combinable); !ok {
		t.Fatalf("ScanResponse does not implement Combinable")
	}

	sr2 := &ScanResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: MinTimestamp},
		Rows: []KeyValue{
			{Key: Key("B"), Value: Value{Bytes: []byte("W")}},
		},
	}
	sr2.Timestamp = MaxTimestamp

	wantedSR := &ScanResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: MaxTimestamp},
		Rows:             append(append([]KeyValue(nil), sr1.Rows...), sr2.Rows...),
	}

	sr1.Combine(sr2)
	sr1.Combine(&ScanResponse{})

	if !reflect.DeepEqual(sr1, wantedSR) {
		t.Errorf("wanted %v, got %v", wantedSR, sr1)
	}

	dr1 := &DeleteRangeResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: Timestamp{Logical: 100}},
		NumDeleted:       5,
	}
	if _, ok := interface{}(dr1).(Combinable); !ok {
		t.Fatalf("DeleteRangeResponse does not implement Combinable")
	}
	dr2 := &DeleteRangeResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: Timestamp{Logical: 1}},
		NumDeleted:       12,
	}
	dr3 := &DeleteRangeResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: Timestamp{Logical: 111}},
		NumDeleted:       3,
	}
	wantedDR := &DeleteRangeResponse{
		KVResponseHeader: KVResponseHeader{Timestamp: Timestamp{Logical: 111}},
		NumDeleted:       20,
	}
	dr2.Combine(dr3)
	dr1.Combine(dr2)

	if !reflect.DeepEqual(dr1, wantedDR) {
		t.Errorf("wanted %v, got %v", wantedDR, dr1)
	}
}

func TestSetGoErrorCopy(t *testing.T) {
	rh := KVResponseHeader{}
	err := &Error{Message: "test123"}
	rh.SetGoError(err)
	err.Message = "321tset"
	if rh.Error.Message != "test123" {
		t.Fatalf("SetGoError did not create a new error")
	}
}
