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

package exec

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	ct := semtypes.T{SemanticType: semtypes.BOOL}

	// Test converting column 0.
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := coldata.NewMemColumn(types.Bool, 2)
	expected.Bool()[0] = false
	expected.Bool()[1] = true
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}

	// Test converting column 1.
	if err := EncDatumRowsToColVec(rows, vec, 1 /* columnIdx */, &ct, &alloc); err != nil {
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
	ct := semtypes.T{SemanticType: semtypes.INT, Width: 16}
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
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
		ct := semtypes.T{SemanticType: semtypes.STRING, Width: width}
		if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
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
	ct := semtypes.T{SemanticType: semtypes.DECIMAL}
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}
