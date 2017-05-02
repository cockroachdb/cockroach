// Copyright 2017 The Cockroach Authors.
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
// Author: Arjun Narayan (arjun@cockroachlabs.com)

package distsqlrun

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type testCase struct {
	spec       AlgebraicSetOpSpec
	inputLeft  sqlbase.EncDatumRows
	inputRight sqlbase.EncDatumRows
	expected   sqlbase.EncDatumRows
}

type testInputs struct {
	v                [15]sqlbase.EncDatum
	inputUnordered   sqlbase.EncDatumRows
	inputIntsOrdered sqlbase.EncDatumRows
	inputOddsOrdered sqlbase.EncDatumRows
}

func initTestData() testInputs {
	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
			parser.NewDInt(parser.DInt(i)))
	}

	inputUnordered := sqlbase.EncDatumRows{
		{v[2], v[3]},
		{v[5], v[6]},
		{v[2], v[3]},
		{v[5], v[6]},
		{v[2], v[6]},
		{v[3], v[5]},
		{v[2], v[9]},
	}
	inputIntsOrdered := sqlbase.EncDatumRows{
		{v[2], v[3]},
		{v[3], v[6]},
		{v[4], v[3]},
		{v[5], v[6]},
		{v[6], v[6]},
		{v[7], v[5]},
		{v[8], v[9]},
	}
	inputOddsOrdered := sqlbase.EncDatumRows{
		{v[3], v[3]},
		{v[5], v[6]},
		{v[7], v[3]},
		{v[9], v[6]},
		{v[11], v[6]},
		{v[13], v[5]},
	}
	return testInputs{
		v:                v,
		inputUnordered:   inputUnordered,
		inputIntsOrdered: inputIntsOrdered,
		inputOddsOrdered: inputOddsOrdered,
	}
}

func runProcessors(tc testCase) (sqlbase.EncDatumRows, error) {
	inL := NewRowBuffer(nil, tc.inputLeft, RowBufferArgs{})
	inR := NewRowBuffer(nil, tc.inputRight, RowBufferArgs{})
	out := &RowBuffer{}

	flowCtx := FlowCtx{}

	s, err := newAlgebraicSetOp(&flowCtx, &tc.spec, inL, inR, &PostProcessSpec{}, out)
	if err != nil {
		return nil, err
	}

	s.Run(context.Background(), nil)
	if !out.ProducerClosed {
		return nil, errors.Errorf("output RowReceiver not closed")
	}

	var res sqlbase.EncDatumRows
	for {
		row, meta := out.Next()
		if !meta.Empty() {
			return nil, errors.Errorf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		res = append(res, row)
	}

	if result := res.String(); result != tc.expected.String() {
		return nil, errors.Errorf("invalid results: %s, expected %s'", result, tc.expected.String())
	}

	return res, nil
}

func TestExceptAll(t *testing.T) {
	defer leaktest.AfterTest(t)()

	td := initTestData()
	v := td.v
	setSpecFirstColumnOrderedAscending := AlgebraicSetOpSpec{
		OpType: AlgebraicSetOpSpec_Except_all,
		Ordering: Ordering{
			Columns: []Ordering_Column{
				{
					ColIdx:    0,
					Direction: Ordering_Column_ASC,
				},
			},
		},
	}
	testCases := []testCase{
		{
			spec: AlgebraicSetOpSpec{
				OpType: AlgebraicSetOpSpec_Except_all,
			},
			inputLeft: td.inputUnordered,
			inputRight: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[5], v[6]},
				{v[2], v[3]},
				{v[5], v[6]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[6]},
				{v[3], v[5]},
				{v[2], v[9]},
			},
		},
		{
			spec:       setSpecFirstColumnOrderedAscending,
			inputLeft:  td.inputIntsOrdered,
			inputRight: td.inputOddsOrdered,
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[6]},
				{v[4], v[3]},
				{v[6], v[6]},
				{v[7], v[5]},
				{v[8], v[9]},
			},
		},
		{
			spec: setSpecFirstColumnOrderedAscending,
			inputLeft: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[6]},
				{v[4], v[3]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[5]},
				{v[8], v[9]},
			},
			inputRight: sqlbase.EncDatumRows{
				{v[0], v[0]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3]},
				{v[3], v[6]},
				{v[4], v[3]},
				{v[5], v[6]},
				{v[6], v[6]},
				{v[7], v[5]},
				{v[8], v[9]},
			},
		},
	}
	for i, tc := range testCases {
		outRows, err := runProcessors(tc)
		if err != nil {
			t.Fatal(err)
		}
		if result := outRows.String(); result != tc.expected.String() {
			t.Errorf("invalid result index %d: %s, expected %s'", i, result, tc.expected.String())
		}
	}
}
