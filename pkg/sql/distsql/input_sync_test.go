// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestOrderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	v := [6]sqlbase.EncDatum{}
	for i := range v {
		v[i].SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		sources  []sqlbase.EncDatumRows
		ordering sqlbase.ColumnOrdering
		expected sqlbase.EncDatumRows
	}{
		{
			sources: []sqlbase.EncDatumRows{
				{
					{v[0], v[1], v[4]},
					{v[0], v[1], v[2]},
					{v[0], v[2], v[3]},
					{v[1], v[1], v[3]},
				},
				{
					{v[1], v[0], v[4]},
				},
				{
					{v[0], v[0], v[0]},
					{v[4], v[4], v[4]},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: asc},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[2]},
				{v[0], v[2], v[3]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[3]},
				{v[4], v[4], v[4]},
			},
		},
		{
			sources: []sqlbase.EncDatumRows{
				{},
				{
					{v[1], v[0], v[4]},
				},
				{
					{v[3], v[4], v[1]},
					{v[4], v[4], v[4]},
					{v[3], v[2], v[0]},
				},
				{
					{v[4], v[4], v[5]},
					{v[3], v[3], v[0]},
					{v[0], v[0], v[0]},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 1, Direction: desc},
				{ColIdx: 0, Direction: asc},
				{ColIdx: 2, Direction: asc},
			},
			expected: sqlbase.EncDatumRows{
				{v[3], v[4], v[1]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
				{v[3], v[3], v[0]},
				{v[3], v[2], v[0]},
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
			},
		},
	}
	for testIdx, c := range testCases {
		var sources []RowSource
		for _, srcRows := range c.sources {
			rowBuf := &RowBuffer{rows: srcRows}
			sources = append(sources, rowBuf)
		}
		src, err := makeOrderedSync(c.ordering, sources)
		if err != nil {
			t.Fatal(err)
		}
		var retRows sqlbase.EncDatumRows
		for {
			row, err := src.NextRow()
			if err != nil {
				t.Fatal(err)
			}
			if row == nil {
				break
			}
			retRows = append(retRows, row)
		}
		expStr := c.expected.String()
		retStr := retRows.String()
		if expStr != retStr {
			t.Errorf("invalid results for case %d; expected:\n   %s\ngot:\n   %s",
				testIdx, expStr, retStr)
		}
	}
}

func TestUnorderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mrc := &MultiplexedRowChannel{}
	mrc.Init(5)
	for i := 1; i <= 5; i++ {
		go func(i int) {
			for j := 1; j <= 100; j++ {
				var a, b sqlbase.EncDatum
				a.SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(i)))
				b.SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(j)))
				row := sqlbase.EncDatumRow{a, b}
				mrc.PushRow(row)
			}
			mrc.Close(nil)
		}(i)
	}
	var retRows sqlbase.EncDatumRows
	for {
		row, err := mrc.NextRow()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		retRows = append(retRows, row)
	}
	// Verify all elements.
	for i := 1; i <= 5; i++ {
		j := 1
		for _, row := range retRows {
			if int(*(row[0].Datum.(*parser.DInt))) == i {
				if int(*(row[1].Datum.(*parser.DInt))) != j {
					t.Errorf("Expected [%d %d], got %s", i, j, row)
				}
				j++
			}
		}
		if j != 101 {
			t.Errorf("Missing [%d %d]", i, j)
		}
	}

	// Test case when one source closes with an error.
	mrc = &MultiplexedRowChannel{}
	mrc.Init(5)
	for i := 1; i <= 5; i++ {
		go func(i int) {
			for j := 1; j <= 100; j++ {
				var a, b sqlbase.EncDatum
				a.SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(i)))
				b.SetDatum(sqlbase.ColumnType_INT, parser.NewDInt(parser.DInt(j)))
				row := sqlbase.EncDatumRow{a, b}
				mrc.PushRow(row)
			}
			var err error
			if i == 3 {
				err = fmt.Errorf("Test error")
			}
			mrc.Close(err)
		}(i)
	}
	for {
		row, err := mrc.NextRow()
		if err != nil {
			if err.Error() != "Test error" {
				t.Error(err)
			}
			break
		}
		if row == nil {
			t.Error("Did not receive expected error")
		}
	}
}
