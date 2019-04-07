// Copyright 2019 The Cockroach Authors.
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

package types

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/lib/pq/oid"
)

func TestTypes(t *testing.T) {
	enCollate := "en"

	testCases := []struct {
		actual   *T
		expected *T
	}{
		// ARRAY
		{MakeArray(Any), AnyArray},
		{MakeArray(Any), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: Any, Oid: oid.T_anyarray, Locale: &emptyLocale}}},

		{MakeArray(Decimal), DecimalArray},
		{MakeArray(Decimal), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}},

		{MakeArray(Int), IntArray},
		{MakeArray(Int), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}},
		{MakeArray(MakeArray(Int)), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: IntArray, Oid: oid.T__int8, Locale: &emptyLocale}}},

		{Int2Vector, &T{InternalType: InternalType{
			SemanticType: ARRAY, Oid: oid.T_int2vector, ArrayContents: Int2, Locale: &emptyLocale}}},
		{MakeArray(Int2Vector), &T{InternalType: InternalType{
			SemanticType: ARRAY, Oid: oid.T__int2vector, ArrayContents: Int2Vector, Locale: &emptyLocale}}},

		{OidVector, &T{InternalType: InternalType{
			SemanticType: ARRAY, Oid: oid.T_oidvector, ArrayContents: Oid, Locale: &emptyLocale}}},
		{MakeArray(OidVector), &T{InternalType: InternalType{
			SemanticType: ARRAY, Oid: oid.T__oidvector, ArrayContents: OidVector, Locale: &emptyLocale}}},

		{MakeArray(String), StringArray},
		{MakeArray(String), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}},
		{MakeArray(String), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}},
		{MakeArray(MakeArray(String)), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: StringArray, Oid: oid.T__text, Locale: &emptyLocale}}},

		{MakeArray(AnyTuple), &T{InternalType: InternalType{
			SemanticType: ARRAY, ArrayContents: AnyTuple, Oid: oid.T__record, Locale: &emptyLocale}}},

		// BIT
		{MakeBit(0), typeBit},
		{MakeBit(0), &T{InternalType: InternalType{
			SemanticType: BIT, Oid: oid.T_bit, Locale: &emptyLocale}}},
		{MakeBit(100), &T{InternalType: InternalType{
			SemanticType: BIT, Oid: oid.T_bit, Width: 100, Locale: &emptyLocale}}},
		{MakeBit(100), MakeScalar(BIT, oid.T_bit, 0, 100, emptyLocale)},

		{MakeVarBit(0), VarBit},
		{MakeVarBit(0), &T{InternalType: InternalType{
			SemanticType: BIT, Oid: oid.T_varbit, Locale: &emptyLocale}}},
		{MakeVarBit(100), &T{InternalType: InternalType{
			SemanticType: BIT, Oid: oid.T_varbit, Width: 100, Locale: &emptyLocale}}},
		{MakeVarBit(100), MakeScalar(BIT, oid.T_varbit, 0, 100, emptyLocale)},

		// BOOL
		{Bool, &T{InternalType: InternalType{
			SemanticType: BOOL, Oid: oid.T_bool, Locale: &emptyLocale}}},
		{Bool, MakeScalar(BOOL, oid.T_bool, 0, 0, emptyLocale)},

		// BYTES
		{Bytes, &T{InternalType: InternalType{
			SemanticType: BYTES, Oid: oid.T_bytea, Locale: &emptyLocale}}},
		{Bytes, MakeScalar(BYTES, oid.T_bytea, 0, 0, emptyLocale)},

		// COLLATEDSTRING
		{MakeCollatedString(String, ""), AnyCollatedString},
		{MakeCollatedString(String, enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_text, Locale: &enCollate}}},
		{MakeCollatedString(MakeString(20), enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_text, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeString(20), enCollate),
			MakeScalar(COLLATEDSTRING, oid.T_text, 0, 20, enCollate)},

		{MakeCollatedString(VarChar, enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_varchar, Locale: &enCollate}}},
		{MakeCollatedString(MakeVarChar(20), enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_varchar, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeVarChar(20), enCollate),
			MakeScalar(COLLATEDSTRING, oid.T_varchar, 0, 20, enCollate)},

		{MakeCollatedString(typeBpChar, enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_bpchar, Locale: &enCollate}}},
		{MakeCollatedString(MakeChar(20), enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_bpchar, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeChar(20), enCollate),
			MakeScalar(COLLATEDSTRING, oid.T_bpchar, 0, 20, enCollate)},

		{MakeCollatedString(typeQChar, enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_char, Locale: &enCollate}}},
		{MakeCollatedString(MakeQChar(20), enCollate), &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: oid.T_char, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeQChar(20), enCollate),
			MakeScalar(COLLATEDSTRING, oid.T_char, 0, 20, enCollate)},

		// DATE
		{Date, &T{InternalType: InternalType{
			SemanticType: DATE, Oid: oid.T_date, Locale: &emptyLocale}}},
		{Date, MakeScalar(DATE, oid.T_date, 0, 0, emptyLocale)},

		// DECIMAL
		{MakeDecimal(0, 0), Decimal},
		{MakeDecimal(0, 0), &T{InternalType: InternalType{
			SemanticType: DECIMAL, Oid: oid.T_numeric, Locale: &emptyLocale}}},
		{MakeDecimal(10, 3), &T{InternalType: InternalType{
			SemanticType: DECIMAL, Oid: oid.T_numeric, Precision: 10, Width: 3, Locale: &emptyLocale}}},
		{MakeDecimal(10, 3), MakeScalar(DECIMAL, oid.T_numeric, 10, 3, emptyLocale)},

		// FLOAT
		{Float, &T{InternalType: InternalType{
			SemanticType: FLOAT, Width: 64, Oid: oid.T_float8, Locale: &emptyLocale}}},
		{Float4, &T{InternalType: InternalType{
			SemanticType: FLOAT, Width: 32, Oid: oid.T_float4, Locale: &emptyLocale}}},
		{Float4, MakeScalar(FLOAT, oid.T_float4, 0, 32, emptyLocale)},

		// INET
		{INet, &T{InternalType: InternalType{
			SemanticType: INET, Oid: oid.T_inet, Locale: &emptyLocale}}},
		{INet, MakeScalar(INET, oid.T_inet, 0, 0, emptyLocale)},

		// INT
		{Int, &T{InternalType: InternalType{
			SemanticType: INT, Width: 64, Oid: oid.T_int8, Locale: &emptyLocale}}},
		{Int4, &T{InternalType: InternalType{
			SemanticType: INT, Width: 32, Oid: oid.T_int4, Locale: &emptyLocale}}},
		{Int2, &T{InternalType: InternalType{
			SemanticType: INT, Width: 16, Oid: oid.T_int2, Locale: &emptyLocale}}},
		{Int2, MakeScalar(INT, oid.T_int2, 0, 16, emptyLocale)},

		// INTERVAL
		{Interval, &T{InternalType: InternalType{
			SemanticType: INTERVAL, Oid: oid.T_interval, Locale: &emptyLocale}}},
		{Interval, MakeScalar(INTERVAL, oid.T_interval, 0, 0, emptyLocale)},

		// JSON
		{Jsonb, &T{InternalType: InternalType{
			SemanticType: JSON, Oid: oid.T_jsonb, Locale: &emptyLocale}}},
		{Jsonb, MakeScalar(JSON, oid.T_jsonb, 0, 0, emptyLocale)},

		// OID
		{Oid, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_oid, Locale: &emptyLocale}}},
		{RegClass, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_regclass, Locale: &emptyLocale}}},
		{RegNamespace, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_regnamespace, Locale: &emptyLocale}}},
		{RegProc, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_regproc, Locale: &emptyLocale}}},
		{RegProcedure, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_regprocedure, Locale: &emptyLocale}}},
		{RegType, &T{InternalType: InternalType{
			SemanticType: OID, Oid: oid.T_regtype, Locale: &emptyLocale}}},
		{Oid, MakeScalar(OID, oid.T_oid, 0, 0, emptyLocale)},
		{RegClass, MakeScalar(OID, oid.T_regclass, 0, 0, emptyLocale)},

		// STRING
		{MakeString(0), String},
		{MakeString(0), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_text, Locale: &emptyLocale}}},
		{MakeString(20), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_text, Width: 20, Locale: &emptyLocale}}},
		{MakeString(20), MakeScalar(STRING, oid.T_text, 0, 20, emptyLocale)},

		{MakeVarChar(0), VarChar},
		{MakeVarChar(0), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_varchar, Locale: &emptyLocale}}},
		{MakeVarChar(20), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_varchar, Width: 20, Locale: &emptyLocale}}},
		{MakeVarChar(20), MakeScalar(STRING, oid.T_varchar, 0, 20, emptyLocale)},

		{MakeChar(0), typeBpChar},
		{MakeChar(0), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_bpchar, Locale: &emptyLocale}}},
		{MakeChar(20), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_bpchar, Width: 20, Locale: &emptyLocale}}},
		{MakeChar(20), MakeScalar(STRING, oid.T_bpchar, 0, 20, emptyLocale)},

		{MakeQChar(0), typeQChar},
		{MakeQChar(0), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_char, Locale: &emptyLocale}}},
		{MakeQChar(20), &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_char, Width: 20, Locale: &emptyLocale}}},
		{MakeQChar(20), MakeScalar(STRING, oid.T_char, 0, 20, emptyLocale)},

		{Name, &T{InternalType: InternalType{
			SemanticType: STRING, Oid: oid.T_name, Locale: &emptyLocale}}},
		{Name, MakeScalar(STRING, oid.T_name, 0, 0, emptyLocale)},

		// TIME
		{MakeTime(0), Time},
		{MakeTime(0), &T{InternalType: InternalType{
			SemanticType: TIME, Oid: oid.T_time, Locale: &emptyLocale}}},
		{MakeTime(6), &T{InternalType: InternalType{
			SemanticType: TIME, Oid: oid.T_time, Precision: 6, Locale: &emptyLocale}}},
		{MakeTime(6), MakeScalar(TIME, oid.T_time, 6, 0, emptyLocale)},

		// TIMESTAMP
		{MakeTimestamp(0), Timestamp},
		{MakeTimestamp(0), &T{InternalType: InternalType{
			SemanticType: TIMESTAMP, Oid: oid.T_timestamp, Locale: &emptyLocale}}},
		{MakeTimestamp(6), &T{InternalType: InternalType{
			SemanticType: TIMESTAMP, Oid: oid.T_timestamp, Precision: 6, Locale: &emptyLocale}}},
		{MakeTimestamp(6), MakeScalar(TIMESTAMP, oid.T_timestamp, 6, 0, emptyLocale)},

		// TIMESTAMPTZ
		{MakeTimestampTZ(0), TimestampTZ},
		{MakeTimestampTZ(0), &T{InternalType: InternalType{
			SemanticType: TIMESTAMPTZ, Oid: oid.T_timestamptz, Locale: &emptyLocale}}},
		{MakeTimestampTZ(6), &T{InternalType: InternalType{
			SemanticType: TIMESTAMPTZ, Oid: oid.T_timestamptz, Precision: 6, Locale: &emptyLocale}}},
		{MakeTimestampTZ(6), MakeScalar(TIMESTAMPTZ, oid.T_timestamptz, 6, 0, emptyLocale)},

		// TUPLE
		{MakeTuple(nil), EmptyTuple},
		{MakeTuple([]T{*Any}), AnyTuple},
		{MakeTuple([]T{*Int}), &T{InternalType: InternalType{
			SemanticType: TUPLE, Oid: oid.T_record, TupleContents: []T{*Int}, Locale: &emptyLocale}}},
		{MakeTuple([]T{*Int, *String}), &T{InternalType: InternalType{
			SemanticType: TUPLE, Oid: oid.T_record, TupleContents: []T{*Int, *String}, Locale: &emptyLocale}}},

		{MakeLabeledTuple([]T{*Int, *String}, []string{"foo", "bar"}), &T{InternalType: InternalType{
			SemanticType: TUPLE, Oid: oid.T_record, TupleContents: []T{*Int, *String},
			TupleLabels: []string{"foo", "bar"}, Locale: &emptyLocale}}},

		// UNKNOWN
		{Unknown, &T{InternalType: InternalType{
			SemanticType: UNKNOWN, Oid: oid.T_unknown, Locale: &emptyLocale}}},
		{Unknown, MakeScalar(UNKNOWN, oid.T_unknown, 0, 0, emptyLocale)},

		// UUID
		{Uuid, &T{InternalType: InternalType{
			SemanticType: UUID, Oid: oid.T_uuid, Locale: &emptyLocale}}},
		{Uuid, MakeScalar(UUID, oid.T_uuid, 0, 0, emptyLocale)},
	}

	for _, tc := range testCases {
		// Test that actual, expected types are identical.
		if !tc.actual.Identical(tc.expected) {
			t.Errorf("expected <%v>, got <%v>", tc.expected.DebugString(), tc.actual.DebugString())
		}
		if !reflect.DeepEqual(tc.actual, tc.expected) {
			t.Errorf("expected <%v>, got <%v>", tc.expected.DebugString(), tc.actual.DebugString())
		}

		// Roundtrip type by marshaling, then unmarshaling. Only do this for non-
		// nested array types (since we don't yet support marshaling/ummarshaling
		// nested arrays).
		if tc.actual.SemanticType() == ARRAY && tc.actual.ArrayContents().SemanticType() == ARRAY {
			continue
		}

		data, err := protoutil.Marshal(tc.actual)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.actual.DebugString(), err)
		}
		if len(data) != tc.actual.Size() {
			t.Errorf("expected %d bytes, got %d bytes", len(data), tc.actual.Size())
		}

		data2 := make([]byte, len(data))
		i, err := tc.actual.MarshalTo(data2)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.actual.DebugString(), err)
		}
		if i != len(data) {
			t.Errorf("expected %d bytes, got %d bytes", len(data), i)
		}
		if !bytes.Equal(data, data2) {
			t.Error("Marshal and MarshalTo bytes are not equal")
		}

		var roundtrip T
		err = protoutil.Unmarshal(data, &roundtrip)
		if err != nil {
			t.Errorf("error during unmarshal of type <%v>: %v", tc.actual.DebugString(), err)
		}
		if !tc.actual.Identical(&roundtrip) {
			t.Errorf("expected <%v>, got <%v>", tc.actual.DebugString(), roundtrip.DebugString())
		}
	}
}

func TestEquivalent(t *testing.T) {
	testCases := []struct {
		typ1  *T
		typ2  *T
		equiv bool
	}{
		// ARRAY
		{Int2Vector, IntArray, true},
		{OidVector, MakeArray(Oid), true},
		{MakeArray(Int), MakeArray(Int4), true},
		{MakeArray(String), MakeArray(MakeChar(10)), true},
		{IntArray, MakeArray(Float), false},
		{MakeArray(String), MakeArray(MakeArray(String)), false},
		{MakeArray(IntArray), IntArray, false},

		// BIT
		{MakeBit(1), MakeBit(2), true},
		{MakeBit(1), MakeVarBit(2), true},
		{MakeVarBit(10), Any, true},
		{VarBit, Bytes, false},

		// COLLATEDSTRING
		{MakeCollatedString(String, "en"), MakeCollatedString(MakeVarChar(10), "en"), true},
		{MakeCollatedString(String, "en"), AnyCollatedString, true},
		{AnyCollatedString, MakeCollatedString(String, "en"), true},
		{MakeCollatedString(String, "en"), MakeCollatedString(String, "de"), false},
		{MakeCollatedString(String, "en"), String, false},

		// DECIMAL
		{Decimal, MakeDecimal(3, 2), true},
		{MakeDecimal(3, 2), MakeDecimal(3, 0), true},
		{Any, MakeDecimal(10, 0), true},
		{Decimal, Float, false},

		// INT
		{Int2, Int4, true},
		{Int4, Int, true},
		{Int, Any, true},
		{Int, IntArray, false},

		// TUPLE
		{MakeTuple([]T{}), MakeTuple([]T{}), true},
		{MakeTuple([]T{*Int, *String}), MakeTuple([]T{*Int4, *VarChar}), true},
		{MakeTuple([]T{*Int, *String}), AnyTuple, true},
		{AnyTuple, MakeTuple([]T{*Int, *String}), true},
		{MakeTuple([]T{*Int, *String}),
			MakeLabeledTuple([]T{*Int4, *VarChar}, []string{"label2", "label1"}), true},
		{MakeLabeledTuple([]T{*Int, *String}, []string{"label1", "label2"}),
			MakeLabeledTuple([]T{*Int4, *VarChar}, []string{"label2", "label1"}), true},
		{MakeTuple([]T{*String, *Int}), MakeTuple([]T{*Int, *String}), false},

		// UNKNOWN
		{Unknown, &T{InternalType: InternalType{
			SemanticType: UNKNOWN, Oid: oid.T_unknown, Locale: &emptyLocale}}, true},
		{Any, Unknown, true},
		{Unknown, Int, false},
	}

	for _, tc := range testCases {
		if tc.equiv && !tc.typ1.Equivalent(tc.typ2) {
			t.Errorf("expected <%v> to be equivalent to <%v>",
				tc.typ1.DebugString(), tc.typ2.DebugString())
		}
		if !tc.equiv && tc.typ1.Equivalent(tc.typ2) {
			t.Errorf("expected <%v> to not be equivalent to <%v>",
				tc.typ1.DebugString(), tc.typ2.DebugString())
		}

		// Test equivalent values that are not identical.
		if !reflect.DeepEqual(tc.typ1, tc.typ2) {
			if tc.typ1.Identical(tc.typ2) {
				t.Errorf("expected <%v> to not be identical to <%v>",
					tc.typ1.DebugString(), tc.typ2.DebugString())
			}
		}
	}
}

// TestMarshalCompat tests backwards-compatibility during marshal.
func TestMarshalCompat(t *testing.T) {
	intElemType := INT
	oidElemType := OID
	strElemType := STRING
	collStrElemType := COLLATEDSTRING
	enLocale := "en"

	testCases := []struct {
		from *T
		to   InternalType
	}{
		// ARRAY
		{Int2Vector, InternalType{SemanticType: int2vector, Oid: oid.T_int2vector, Width: 16,
			ArrayElemType: &intElemType, ArrayContents: Int2}},
		{OidVector, InternalType{SemanticType: oidvector, Oid: oid.T_oidvector,
			ArrayElemType: &oidElemType, ArrayContents: Oid}},
		{IntArray, InternalType{SemanticType: ARRAY, Oid: oid.T__int8, Width: 64,
			ArrayElemType: &intElemType, ArrayContents: Int}},
		{MakeArray(MakeVarChar(10)), InternalType{SemanticType: ARRAY, Oid: oid.T__varchar, Width: 10, VisibleType: visibleVARCHAR,
			ArrayElemType: &strElemType, ArrayContents: MakeVarChar(10)}},
		{MakeArray(MakeCollatedString(String, enLocale)), InternalType{SemanticType: ARRAY, Oid: oid.T__text, Locale: &enLocale,
			ArrayElemType: &collStrElemType, ArrayContents: MakeCollatedString(String, enLocale)}},

		// BIT
		{typeBit, InternalType{SemanticType: BIT, Oid: oid.T_bit}},
		{MakeVarBit(10), InternalType{SemanticType: BIT, Oid: oid.T_varbit, Width: 10, VisibleType: visibleVARBIT}},

		// COLLATEDSTRING
		{MakeCollatedString(MakeVarChar(10), enLocale),
			InternalType{SemanticType: COLLATEDSTRING, Oid: oid.T_varchar, Width: 10, VisibleType: visibleVARCHAR, Locale: &enLocale}},

		// FLOAT
		{Float, InternalType{SemanticType: FLOAT, Oid: oid.T_float8, Width: 64}},
		{Float4, InternalType{SemanticType: FLOAT, Oid: oid.T_float4, Width: 32, VisibleType: visibleREAL}},

		// STRING
		{MakeString(10), InternalType{SemanticType: STRING, Oid: oid.T_text, Width: 10}},
		{VarChar, InternalType{SemanticType: STRING, Oid: oid.T_varchar, VisibleType: visibleVARCHAR}},
		{MakeChar(10), InternalType{SemanticType: STRING, Oid: oid.T_bpchar, Width: 10, VisibleType: visibleCHAR}},
		{MakeQChar(1), InternalType{SemanticType: STRING, Oid: oid.T_char, Width: 1, VisibleType: visibleQCHAR}},
		{Name, InternalType{SemanticType: name, Oid: oid.T_name}},
	}

	for _, tc := range testCases {
		data, err := protoutil.Marshal(tc.from)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.from.DebugString(), err)
		}

		var actual InternalType
		err = protoutil.Unmarshal(data, &actual)
		if err != nil {
			t.Errorf("error during unmarshal of type <%v>: %v", tc.from.DebugString(), err)
		}

		if !reflect.DeepEqual(actual, tc.to) {
			t.Errorf("expected <%v>, got <%v>", tc.to.String(), actual.String())
		}
	}
}

// TestMarshalCompat tests backwards-compatibility during unmarshal. Unmarshal
// needs to handle all formats ever used by CRDB in the past.
func TestUnmarshalCompat(t *testing.T) {
	intElemType := INT
	floatElemType := FLOAT

	testCases := []struct {
		from InternalType
		to   *T
	}{
		// ARRAY
		{InternalType{SemanticType: ARRAY, ArrayElemType: &intElemType, VisibleType: visibleSMALLINT},
			MakeArray(Int2)},
		{InternalType{SemanticType: ARRAY, ArrayElemType: &floatElemType, VisibleType: visibleDOUBLE},
			MakeArray(Float)},

		// BIT
		{InternalType{SemanticType: BIT, VisibleType: visibleVARBIT}, VarBit},
		{InternalType{SemanticType: BIT, VisibleType: visibleVARBIT, Width: 20}, MakeVarBit(20)},

		// FLOAT
		{InternalType{SemanticType: FLOAT}, Float},
		{InternalType{SemanticType: FLOAT, VisibleType: visibleREAL}, Float4},
		{InternalType{SemanticType: FLOAT, VisibleType: visibleDOUBLE}, Float},
		{InternalType{SemanticType: FLOAT, Precision: 1}, Float4},
		{InternalType{SemanticType: FLOAT, Precision: 24}, Float4},
		{InternalType{SemanticType: FLOAT, Precision: 25}, Float},
		{InternalType{SemanticType: FLOAT, Precision: 60}, Float},

		// INT
		{InternalType{SemanticType: INT, VisibleType: visibleSMALLINT}, Int2},
		{InternalType{SemanticType: INT, VisibleType: visibleINTEGER}, Int4},
		{InternalType{SemanticType: INT, VisibleType: visibleBIGINT}, Int},
		{InternalType{SemanticType: INT, VisibleType: visibleBIT}, Int},
		{InternalType{SemanticType: INT, Width: 20}, Int},
		{InternalType{SemanticType: INT}, Int},

		// STRING
		{InternalType{SemanticType: STRING}, String},
		{InternalType{SemanticType: STRING, VisibleType: visibleVARCHAR}, VarChar},
		{InternalType{SemanticType: STRING, VisibleType: visibleVARCHAR, Width: 20}, MakeVarChar(20)},
		{InternalType{SemanticType: STRING, VisibleType: visibleCHAR}, typeBpChar},
		{InternalType{SemanticType: STRING, VisibleType: visibleQCHAR}, typeQChar},
	}

	for _, tc := range testCases {
		data, err := protoutil.Marshal(&tc.from)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.from.String(), err)
		}

		var actual T
		err = protoutil.Unmarshal(data, &actual)
		if err != nil {
			t.Errorf("error during unmarshal of type <%v>: %v", tc.from.String(), err)
		}

		if !actual.Identical(tc.to) {
			t.Errorf("expected <%v>, got <%v>", tc.to.DebugString(), actual.DebugString())
		}
	}
}

func TestOids(t *testing.T) {
	for o, typ := range OidToType {
		if typ.Oid() != o {
			t.Errorf("expected OID %d, got %d", o, typ.Oid())
		}
	}

	for o := range ArrayOids {
		typ := OidToType[o]
		if typ.SemanticType() != ARRAY {
			t.Errorf("expected ARRAY type, got %s", typ.SemanticType())
		}
	}
}
