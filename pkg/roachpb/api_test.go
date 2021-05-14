// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestCombineResponses tests the behavior of the CombineResponses function,
// which attempts to combine two provided responses.
func TestCombineResponses(t *testing.T) {
	t.Run("both combinable", func(t *testing.T) {
		left := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("A"), Value: MakeValueFromString("V")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Ai"), Value: MakeValueFromString("X")},
			},
		}
		right := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("B"), Value: MakeValueFromString("W")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Bi"), Value: MakeValueFromString("Z")},
			},
		}
		expCombined := &ScanResponse{
			Rows:       append(append([]KeyValue(nil), left.Rows...), right.Rows...),
			IntentRows: append(append([]KeyValue(nil), left.IntentRows...), right.IntentRows...),
		}

		err := CombineResponses(left, right)
		require.NoError(t, err)
		require.Equal(t, expCombined, left)
	})

	t.Run("neither combinable", func(t *testing.T) {
		left := &GetResponse{
			Value: &Value{RawBytes: []byte("V")},
		}
		right := &GetResponse{
			Value: &Value{RawBytes: []byte("W")},
		}
		expCombined := &GetResponse{
			Value: left.Value.ShallowClone(),
		}

		err := CombineResponses(left, right)
		require.NoError(t, err)
		require.Equal(t, expCombined, left)
	})

	t.Run("left combinable", func(t *testing.T) {
		left := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("A"), Value: MakeValueFromString("V")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Ai"), Value: MakeValueFromString("X")},
			},
		}
		right := &GetResponse{
			Value: &Value{RawBytes: []byte("W")},
		}

		err := CombineResponses(left, right)
		require.Error(t, err)
		require.Regexp(t, "can not combine", err)
	})

	t.Run("right combinable", func(t *testing.T) {
		left := &GetResponse{
			Value: &Value{RawBytes: []byte("V")},
		}
		right := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("B"), Value: MakeValueFromString("W")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Bi"), Value: MakeValueFromString("Z")},
			},
		}

		err := CombineResponses(left, right)
		require.Error(t, err)
		require.Regexp(t, "can not combine", err)
	})
}

// TestCombinable tests the correct behavior of some types that implement
// the combinable interface, notably {Scan,DeleteRange}Response and
// ResponseHeader.
func TestCombinable(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		// Test that GetResponse doesn't have anything to do with combinable.
		getResp := GetResponse{}
		if _, ok := interface{}(&getResp).(combinable); ok {
			t.Fatalf("GetResponse implements combinable, so presumably all Response types will")
		}
	})

	t.Run("Scan", func(t *testing.T) {

		// Test that {Scan,DeleteRange}Response properly implement it.
		sr1 := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("A"), Value: MakeValueFromString("V")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Ai"), Value: MakeValueFromString("X")},
			},
		}

		if _, ok := interface{}(sr1).(combinable); !ok {
			t.Fatalf("ScanResponse does not implement combinable")
		}

		sr2 := &ScanResponse{
			Rows: []KeyValue{
				{Key: Key("B"), Value: MakeValueFromString("W")},
			},
			IntentRows: []KeyValue{
				{Key: Key("Bi"), Value: MakeValueFromString("Z")},
			},
		}

		wantedSR := &ScanResponse{
			Rows:       append(append([]KeyValue(nil), sr1.Rows...), sr2.Rows...),
			IntentRows: append(append([]KeyValue(nil), sr1.IntentRows...), sr2.IntentRows...),
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
	})

	t.Run("DeleteRange", func(t *testing.T) {
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
	})

	t.Run("AdminVerifyProtectedTimestamp", func(t *testing.T) {
		v1 := &AdminVerifyProtectedTimestampResponse{
			ResponseHeader: ResponseHeader{},
			Verified:       false,
			DeprecatedFailedRanges: []RangeDescriptor{
				{RangeID: 1},
			},
			VerificationFailedRanges: []AdminVerifyProtectedTimestampResponse_FailedRange{
				{RangeID: 1, StartKey: RKeyMin, EndKey: RKeyMax, Reason: "foo"},
			},
		}

		if _, ok := interface{}(v1).(combinable); !ok {
			t.Fatal("AdminVerifyProtectedTimestampResponse unexpectedly does not implement combinable")
		}
		v2 := &AdminVerifyProtectedTimestampResponse{
			ResponseHeader:         ResponseHeader{},
			Verified:               true,
			DeprecatedFailedRanges: nil,
		}
		v3 := &AdminVerifyProtectedTimestampResponse{
			ResponseHeader: ResponseHeader{},
			Verified:       false,
			DeprecatedFailedRanges: []RangeDescriptor{
				{RangeID: 2},
			},
			VerificationFailedRanges: []AdminVerifyProtectedTimestampResponse_FailedRange{
				{RangeID: 2, StartKey: RKeyMin, EndKey: RKeyMax, Reason: "bar"},
			},
		}
		require.NoError(t, v1.combine(v2))
		require.NoError(t, v1.combine(v3))
		require.EqualValues(t, &AdminVerifyProtectedTimestampResponse{
			Verified: false,
			DeprecatedFailedRanges: []RangeDescriptor{
				{RangeID: 1},
				{RangeID: 2},
			},
			VerificationFailedRanges: []AdminVerifyProtectedTimestampResponse_FailedRange{
				{RangeID: 1, StartKey: RKeyMin, EndKey: RKeyMax, Reason: "foo"},
				{RangeID: 2, StartKey: RKeyMin, EndKey: RKeyMax, Reason: "bar"},
			},
		}, v1)

	})
}

// TestMustSetInner makes sure that calls to MustSetInner correctly reset the
// union before repopulating to avoid having more than one value set.
func TestMustSetInner(t *testing.T) {
	req := RequestUnion{}
	res := ResponseUnion{}

	// GetRequest is checked first in the generated code for MustSetInner.
	req.MustSetInner(&GetRequest{})
	res.MustSetInner(&GetResponse{})
	req.MustSetInner(&EndTxnRequest{})
	res.MustSetInner(&EndTxnResponse{})

	if m := req.GetInner().Method(); m != EndTxn {
		t.Fatalf("unexpected request: %s in %+v", m, req)
	}
	if _, isET := res.GetInner().(*EndTxnResponse); !isET {
		t.Fatalf("unexpected response union: %+v", res)
	}
}

func TestContentionEvent_SafeFormat(t *testing.T) {
	ce := &ContentionEvent{
		Key:     Key("foo"),
		TxnMeta: enginepb.TxnMeta{ID: uuid.FromStringOrNil("51b5ef6a-f18f-4e85-bc3f-c44e33f2bb27")},
	}
	const exp = redact.RedactableString(`conflicted with ‹51b5ef6a-f18f-4e85-bc3f-c44e33f2bb27› on ‹"foo"› for 0.000s`)
	require.Equal(t, exp, redact.Sprint(ce))
}
