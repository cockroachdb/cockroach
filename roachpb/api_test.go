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
	"encoding/hex"
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

	cf1 := &ChangeFrozenResponse{RangesAffected: 3, MinStartKey: RKey("x"),
		Stores: map[StoreID]NodeID{1: 11, 2: 12, 4: 14},
	}
	cf2 := &ChangeFrozenResponse{RangesAffected: 1, MinStartKey: RKey("b"),
		Stores: map[StoreID]NodeID{3: 13},
	}
	cf3 := &ChangeFrozenResponse{RangesAffected: 0, MinStartKey: RKey("b"),
		Stores: map[StoreID]NodeID{8: 18, 1: 11, 3: 13},
	}

	wantedCF := &ChangeFrozenResponse{
		RangesAffected: 4, MinStartKey: RKey("b"),
		Stores: map[StoreID]NodeID{1: 11, 2: 12, 3: 13, 4: 14, 8: 18},
	}
	if err := cf1.combine(cf2); err != nil {
		t.Fatal(err)
	}
	if err := cf1.combine(cf3); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(cf1, wantedCF) {
		t.Errorf("wanted %v, got %v", wantedCF, cf1)
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

func TestDeprecatedVerifyChecksumRequest(t *testing.T) {
	// hexData was generated using the following code snippet. The batch contains
	// a VerifyChecksumRequest which is no longer part of RequestUnion.
	//
	// var ba BatchRequest
	// ba.Add(&VerifyChecksumRequest{})
	// var v Value
	// if err := v.SetProto(&ba); err != nil {
	// 	t.Fatal(err)
	// }
	// fmt.Printf("%s\n", hex.EncodeToString(v.RawBytes))

	hexData := `00000000030a1f0a0408001000120608001000180018002100000000000000003000400048001219ba01160a0010001a1000000000000000000000000000000000`
	data, err := hex.DecodeString(hexData)
	if err != nil {
		t.Fatal(err)
	}
	v := Value{RawBytes: data}
	var ba BatchRequest
	if err := v.GetProto(&ba); err != nil {
		t.Fatal(err)
	}
	// This previously failed with a nil-pointer conversion error in
	// BatchRequest.GetArg() because of the removal of
	// RequestUnion.VerifyChecksum. We've now re-added that member as
	// RequestUnion.DeprecatedVerifyChecksum.
	if ba.IsLeaseRequest() {
		t.Fatal("unexpected success")
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
			expected: "1 TruncateLog, 1 CheckConsistency, 1 InitPut",
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
