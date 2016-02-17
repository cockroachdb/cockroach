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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"reflect"
	"testing"
)

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
		Keys:           []Key{[]byte("1")},
	}
	if _, ok := interface{}(dr1).(Combinable); !ok {
		t.Fatalf("DeleteRangeResponse does not implement Combinable")
	}
	dr2 := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 1}},
		Keys:           []Key{[]byte("2")},
	}
	dr3 := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 111}},
		Keys:           nil,
	}
	wantedDR := &DeleteRangeResponse{
		ResponseHeader: ResponseHeader{Timestamp: Timestamp{Logical: 111}},
		Keys:           []Key{[]byte("1"), []byte("2")},
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
