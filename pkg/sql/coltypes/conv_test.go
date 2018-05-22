// Copyright 2018 The Cockroach Authors.
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

package coltypes_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestTryCastTargetToDatumType(t *testing.T) {
	testcases := []struct {
		colTyp   coltypes.T
		datumTyp types.T
		tight    bool
	}{
		{coltypes.Bool, types.Bool, true},

		{coltypes.Int, types.Int, true},
		{coltypes.Serial, types.Int, true},
		{coltypes.Int8, types.Int, true},
		{coltypes.SmallInt, types.Int, false},

		{coltypes.Float, types.Float, true},
		{coltypes.Double, types.Float, true},
		{coltypes.Real, types.Float, false},
		{coltypes.NewFloat(4, true), types.Float, false},

		{coltypes.Numeric, types.Decimal, true},
		{&coltypes.TDecimal{Name: "DECIMAL", Prec: 20, Scale: 0}, types.Decimal, false},
		{&coltypes.TDecimal{Name: "DECIMAL", Scale: 2}, types.Decimal, false},

		{coltypes.VarChar, types.String, true},
		{coltypes.Text, types.String, true},
		{&coltypes.TString{Name: "STRING", N: 10}, types.String, false},
		{
			&coltypes.TCollatedString{Name: "VARCHAR", N: 0, Locale: "en_US"},
			types.TCollatedString{Locale: "en_US"},
			true,
		},
		{
			&coltypes.TCollatedString{Name: "TEXT", N: 0, Locale: "en_US"},
			types.TCollatedString{Locale: "en_US"},
			true,
		},
		{
			&coltypes.TCollatedString{Name: "STRING", N: 10, Locale: "en_US"},
			types.TCollatedString{Locale: "en_US"},
			false,
		},

		{coltypes.Name, types.Name, true},
		{coltypes.Bytes, types.Bytes, true},
		{coltypes.Date, types.Date, true},
		{coltypes.Time, types.Time, true},
		{coltypes.TimeTZ, types.TimeTZ, true},
		{coltypes.Timestamp, types.Timestamp, true},
		{coltypes.TimestampWithTZ, types.TimestampTZ, true},
		{coltypes.Interval, types.Interval, true},
		{coltypes.JSON, types.JSON, true},
		{coltypes.JSONB, types.JSON, true},
		{coltypes.UUID, types.UUID, true},
		{coltypes.INet, types.INet, true},

		{arrayOf(coltypes.Int, []int32{-1}), types.TArray{Typ: types.Int}, true},
		{arrayOf(coltypes.Int4, []int32{-1}), types.TArray{Typ: types.Int}, false},
		{arrayOf(coltypes.VarChar, []int32{1, 2}), types.TArray{Typ: types.String}, true},

		{coltypes.TTuple{coltypes.Int}, types.TTuple{Types: []types.T{types.Int}}, true},
		{
			coltypes.TTuple{coltypes.VarChar, coltypes.Numeric},
			types.TTuple{Types: []types.T{types.String, types.Decimal}},
			true,
		},
		{coltypes.TTuple{coltypes.Bit}, types.TTuple{Types: []types.T{types.Int}}, false},
		{
			coltypes.TTuple{coltypes.Integer, &coltypes.TString{Name: "String", N: 256}},
			types.TTuple{Types: []types.T{types.Int, types.String}},
			false,
		},
	}

	for _, tc := range testcases {
		typ, tight := coltypes.TryCastTargetToDatumType(tc.colTyp)
		if !typ.Equivalent(tc.datumTyp) {
			t.Errorf("expected: %v, actual: %v", tc.datumTyp, typ)
		} else if tight != tc.tight {
			t.Errorf("tight mismatch: %v => %v", tc.colTyp, typ)
		}
	}
}

func arrayOf(colType coltypes.T, bounds []int32) coltypes.T {
	typ, _ := coltypes.ArrayOf(colType, bounds)
	return typ
}
