// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

var alloc = sqlbase.DatumAlloc{}

func TestEncDatumRowsToColVecBool(t *testing.T) {
	// Test input: [[false, true], [true, false]]
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.DBoolFalse},
			sqlbase.EncDatum{Datum: tree.DBoolTrue},
		},
		sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.DBoolTrue},
			sqlbase.EncDatum{Datum: tree.DBoolFalse},
		},
	}
	vec := coldata.NewMemColumn(types.Bool, 2)
	ct := semtypes.Bool

	// Test converting column 0.
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := coldata.NewMemColumn(types.Bool, 2)
	expected.Bool()[0] = false
	expected.Bool()[1] = true
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}

	// Test converting column 1.
	if err := EncDatumRowsToColVec(rows, vec, 1 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected.Bool()[0] = true
	expected.Bool()[1] = false
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}

func TestEncDatumRowsToColVecInt16(t *testing.T) {
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(17)}},
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(42)}},
	}
	vec := coldata.NewMemColumn(types.Int16, 2)
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, semtypes.Int2, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := coldata.NewMemColumn(types.Int16, 2)
	expected.Int16()[0] = 17
	expected.Int16()[1] = 42
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}

func TestEncDatumRowsToColVecString(t *testing.T) {
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDString("foo")}},
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDString("bar")}},
	}
	vec := coldata.NewMemColumn(types.Bytes, 2)
	for _, width := range []int32{0, 25} {
		ct := semtypes.MakeString(width)
		if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
			t.Fatal(err)
		}
		expected := coldata.NewMemColumn(types.Bytes, 2)
		expected.Bytes()[0] = []byte("foo")
		expected.Bytes()[1] = []byte("bar")
		if !reflect.DeepEqual(vec, expected) {
			t.Errorf("expected vector %+v, got %+v", expected, vec)
		}
	}
}

func TestEncDatumRowsToColVecDecimal(t *testing.T) {
	nRows := 3
	rows := make(sqlbase.EncDatumRows, nRows)
	expected := coldata.NewMemColumn(types.Decimal, 3)
	for i, s := range []string{"1.0000", "-3.12", "NaN"} {
		var err error
		dec, err := tree.ParseDDecimal(s)
		if err != nil {
			t.Fatal(err)
		}
		rows[i] = sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: dec}}
		expected.Decimal()[i] = dec.Decimal
	}
	vec := coldata.NewMemColumn(types.Decimal, 3)
	ct := semtypes.Decimal
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, ct, &alloc); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}
