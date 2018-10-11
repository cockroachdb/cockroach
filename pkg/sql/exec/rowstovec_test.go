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

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	vec := newMemColumn(types.Bool, 2)
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}

	// Test converting column 0.
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := newMemColumn(types.Bool, 2)
	expected.Bool().Set(0, false)
	expected.Bool().Set(1, true)
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}

	// Test converting column 1.
	if err := EncDatumRowsToColVec(rows, vec, 1 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected.Bool().Set(0, true)
	expected.Bool().Set(1, false)
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}

func TestEncDatumRowsToColVecInt16(t *testing.T) {
	rows := sqlbase.EncDatumRows{
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(17)}},
		sqlbase.EncDatumRow{sqlbase.EncDatum{Datum: tree.NewDInt(42)}},
	}
	vec := newMemColumn(types.Int16, 2)
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, Width: 16}
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := newMemColumn(types.Int16, 2)
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
	vec := newMemColumn(types.Bytes, 2)
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}
	if err := EncDatumRowsToColVec(rows, vec, 0 /* columnIdx */, &ct, &alloc); err != nil {
		t.Fatal(err)
	}
	expected := newMemColumn(types.Bytes, 2)
	expected.Bytes().Set(0, []byte("foo"))
	expected.Bytes().Set(1, []byte("bar"))
	if !reflect.DeepEqual(vec, expected) {
		t.Errorf("expected vector %+v, got %+v", expected, vec)
	}
}
