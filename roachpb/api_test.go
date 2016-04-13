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
// the combinable interface, notably {Scan,DeleteRange}Response and
// ResponseHeader.
func TestCombinable(t *testing.T) {
	// Test that GetResponse doesn't have anything to do with combinable.
	if _, ok := interface{}(&GetResponse{}).(combinable); ok {
		t.Fatalf("GetResponse implements combinable, so presumably all Response types will")
	}
	// Test that {Scan,DeleteRange}Response properly implement it.
	sr1 := &ScanResponse{
		Rows: []KeyValue{
			{Key: Key("A"), Value: MakeValueFromString("V")},
		},
	}

	if _, ok := interface{}(sr1).(combinable); !ok {
		t.Fatalf("ScanResponse does not implement combinable")
	}

	sr2 := &ScanResponse{
		Rows: []KeyValue{
			{Key: Key("B"), Value: MakeValueFromString("W")},
		},
	}

	wantedSR := &ScanResponse{
		Rows: append(append([]KeyValue(nil), sr1.Rows...), sr2.Rows...),
	}

	if err := sr1.combine(sr2); err != nil {
		t.Fatal(err)
	}
	if err := sr1.combine(&ScanResponse{}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(sr1, wantedSR) {
		t.Errorf("wanted %v, got %v", wantedSR, sr1)
	}

	dr1 := &DeleteRangeResponse{
		Keys: []Key{[]byte("1")},
	}
	if _, ok := interface{}(dr1).(combinable); !ok {
		t.Fatalf("DeleteRangeResponse does not implement combinable")
	}
	dr2 := &DeleteRangeResponse{
		Keys: []Key{[]byte("2")},
	}
	dr3 := &DeleteRangeResponse{
		Keys: nil,
	}
	wantedDR := &DeleteRangeResponse{
		Keys: []Key{[]byte("1"), []byte("2")},
	}
	if err := dr2.combine(dr3); err != nil {
		t.Fatal(err)
	}
	if err := dr1.combine(dr2); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(dr1, wantedDR) {
		t.Errorf("wanted %v, got %v", wantedDR, dr1)
	}
}

// TestMustSetInner makes sure that calls to MustSetInner correctly reset the
// union before repopulating to avoid having more than one value set.
func TestMustSetInner(t *testing.T) {
	req := RequestUnion{}
	res := ResponseUnion{}

	// GetRequest is checked first in the generated code for SetValue.
	req.MustSetInner(&GetRequest{})
	res.MustSetInner(&GetResponse{})
	req.MustSetInner(&EndTransactionRequest{})
	res.MustSetInner(&EndTransactionResponse{})

	if m := req.GetInner().Method(); m != EndTransaction {
		t.Fatalf("unexpected request: %s in %+v", m, req)
	}
	if _, isET := res.GetValue().(*EndTransactionResponse); !isET {
		t.Fatalf("unexpected response union: %+v", res)
	}
}
