// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var alloc tree.DatumAlloc

func TestEncDatumRowToColVecsBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Test input: [[false, true], [true, false]]
	rows := rowenc.EncDatumRows{
		rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.DBoolFalse},
			rowenc.EncDatum{Datum: tree.DBoolTrue},
		},
		rowenc.EncDatumRow{
			rowenc.EncDatum{Datum: tree.DBoolTrue},
			rowenc.EncDatum{Datum: tree.DBoolFalse},
		},
	}
	typs := []*types.T{types.Bool, types.Bool}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	var vecs coldata.TypedVecs
	vecs.SetBatch(batch)
	for rowIdx, row := range rows {
		EncDatumRowToColVecs(row, rowIdx, vecs, typs, &alloc)
	}
	expected := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	expected.ColVec(0).Bool()[0] = false
	expected.ColVec(0).Bool()[1] = true
	expected.ColVec(1).Bool()[0] = true
	expected.ColVec(1).Bool()[1] = false
	if !reflect.DeepEqual(expected, batch) {
		t.Errorf("expected batch %+v, got %+v", expected, batch)
	}
}

func TestEncDatumRowToColVecsInt16(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rows := rowenc.EncDatumRows{
		rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(17)}},
		rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(42)}},
	}
	typs := []*types.T{types.Int2}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	var vecs coldata.TypedVecs
	vecs.SetBatch(batch)
	for rowIdx, row := range rows {
		EncDatumRowToColVecs(row, rowIdx, vecs, typs, &alloc)
	}
	expected := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	expected.ColVec(0).Int16()[0] = 17
	expected.ColVec(0).Int16()[1] = 42
	if !reflect.DeepEqual(expected, batch) {
		t.Errorf("expected batch %+v, got %+v", expected, batch)
	}
}

func TestEncDatumRowToColVecsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rows := rowenc.EncDatumRows{
		rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDString("foo")}},
		rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDString("bar")}},
	}
	for _, width := range []int32{0, 25} {
		typs := []*types.T{types.MakeString(width)}
		batch := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
		var vecs coldata.TypedVecs
		vecs.SetBatch(batch)
		for rowIdx, row := range rows {
			EncDatumRowToColVecs(row, rowIdx, vecs, typs, &alloc)
		}
		expected := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
		expected.ColVec(0).Bytes().Set(0, []byte("foo"))
		expected.ColVec(0).Bytes().Set(1, []byte("bar"))
		if !reflect.DeepEqual(expected, batch) {
			t.Errorf("expected batch %+v, got %+v", expected, batch)
		}
	}
}

func TestEncDatumRowToColVecsDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	nRows := 3
	rows := make(rowenc.EncDatumRows, nRows)
	typs := []*types.T{types.Decimal}
	expected := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	for i, s := range []string{"1.0000", "-3.12", "NaN"} {
		var err error
		dec, err := tree.ParseDDecimal(s)
		if err != nil {
			t.Fatal(err)
		}
		rows[i] = rowenc.EncDatumRow{rowenc.EncDatum{Datum: dec}}
		expected.ColVec(0).Decimal()[i] = dec.Decimal
	}
	batch := testAllocator.NewMemBatchWithFixedCapacity(typs, len(rows))
	var vecs coldata.TypedVecs
	vecs.SetBatch(batch)
	for rowIdx, row := range rows {
		EncDatumRowToColVecs(row, rowIdx, vecs, typs, &alloc)
	}
	if !reflect.DeepEqual(expected, batch) {
		t.Errorf("expected batch %+v, got %+v", expected, batch)
	}
}
