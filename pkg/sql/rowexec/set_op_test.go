// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type setOpTestCase struct {
	setOpType   descpb.JoinType
	columnTypes []*types.T
	ordering    colinfo.ColumnOrdering
	leftInput   rowenc.EncDatumRows
	rightInput  rowenc.EncDatumRows
	expected    rowenc.EncDatumRows
}

func setOpTestCaseToMergeJoinerTestCase(tc setOpTestCase) mergeJoinerTestCase {
	spec := execinfrapb.MergeJoinerSpec{Type: tc.setOpType, NullEquality: true}
	var ordering colinfo.ColumnOrdering
	if tc.ordering != nil {
		ordering = tc.ordering
	} else {
		ordering = make(colinfo.ColumnOrdering, 0, len(tc.columnTypes))
		for i := range tc.columnTypes {
			ordering = append(ordering, colinfo.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
		}
	}
	outCols := make([]uint32, 0, len(tc.columnTypes))
	for i := range tc.columnTypes {
		outCols = append(outCols, uint32(i))
	}
	spec.LeftOrdering = execinfrapb.ConvertToSpecOrdering(ordering)
	spec.RightOrdering = execinfrapb.ConvertToSpecOrdering(ordering)

	return mergeJoinerTestCase{
		spec:          spec,
		outCols:       outCols,
		leftTypes:     tc.columnTypes,
		leftInput:     tc.leftInput,
		rightTypes:    tc.columnTypes,
		rightInput:    tc.rightInput,
		expectedTypes: tc.columnTypes,
		expected:      tc.expected,
	}
}

func setOpTestCaseToHashJoinerTestCase(tc setOpTestCase) hashJoinerTestCase {
	outCols := make([]uint32, 0, len(tc.columnTypes))
	for i := range tc.columnTypes {
		outCols = append(outCols, uint32(i))
	}

	return hashJoinerTestCase{
		leftEqCols:  outCols,
		rightEqCols: outCols,
		joinType:    tc.setOpType,
		outCols:     outCols,
		leftTypes:   tc.columnTypes,
		leftInput:   tc.leftInput,
		rightTypes:  tc.columnTypes,
		rightInput:  tc.rightInput,
		expected:    tc.expected,
	}
}

func intersectAllTestCases() []setOpTestCase {
	null := rowenc.EncDatum{Datum: tree.DNull}
	var v = [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	return []setOpTestCase{
		{
			// Check that INTERSECT ALL only returns rows that are in both the left
			// and right side.
			setOpType:   descpb.IntersectAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[1]},
				{null, v[1]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[1], null},
				{v[1], null},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[1]},
				{null, v[1]},
				{null, v[1]},
				{null, v[2]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], null},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{null, null},
				{null, v[1]},
				{null, v[1]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], null},
				{v[5], v[0]},
				{v[5], v[1]},
			},
		},
		{
			// Check that INTERSECT ALL returns the correct number of duplicates when
			// the left side contains more duplicates of a row than the right side.
			setOpType:   descpb.IntersectAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[0]},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
		},
		{
			// Check that INTERSECT ALL returns the correct number of duplicates when
			// the right side contains more duplicates of a row than the left side.
			setOpType:   descpb.IntersectAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[0]},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
		},
	}
}

func exceptAllTestCases() []setOpTestCase {
	null := rowenc.EncDatum{Datum: tree.DNull}
	var v = [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	return []setOpTestCase{
		{
			// Check that EXCEPT ALL only returns rows that are on the left side
			// but not the right side.
			setOpType:   descpb.ExceptAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[1]},
				{null, v[1]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[1], null},
				{v[1], null},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[1]},
				{null, v[1]},
				{null, v[1]},
				{null, v[2]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[1], null},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[3]},
				{v[1], null},
			},
		},
		{
			// Check that EXCEPT ALL returns the correct number of duplicates when
			// the left side contains more duplicates of a row than the right side.
			setOpType:   descpb.ExceptAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[0]},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[3]},
			},
		},
		{
			// Check that EXCEPT ALL returns the correct number of duplicates when
			// the right side contains more duplicates of a row than the left side.
			setOpType:   descpb.ExceptAllJoin,
			columnTypes: types.TwoIntCols,
			leftInput: rowenc.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: rowenc.EncDatumRows{
				{null, null},
				{null, null},
				{null, v[0]},
				{null, v[0]},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[3]},
			},
		},
		{
			// Check that EXCEPT ALL handles mixed ordering correctly.
			setOpType:   descpb.ExceptAllJoin,
			columnTypes: types.TwoIntCols,
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Descending},
				{ColIdx: 1, Direction: encoding.Ascending},
			},
			leftInput: rowenc.EncDatumRows{
				{v[4], null},
				{v[4], v[1]},
				{v[1], null},
				{v[1], v[2]},
				{v[0], v[2]},
				{v[0], v[3]},
				{null, v[1]},
				{null, v[2]},
				{null, v[2]},
				{null, v[3]},
			},
			rightInput: rowenc.EncDatumRows{
				{v[3], v[2]},
				{v[2], v[1]},
				{v[2], v[2]},
				{v[2], v[3]},
				{v[1], null},
				{v[1], v[1]},
				{v[1], v[1]},
				{v[0], v[1]},
				{v[0], v[2]},
				{null, v[2]},
			},
			expected: rowenc.EncDatumRows{
				{v[4], null},
				{v[4], v[1]},
				{v[1], v[2]},
				{v[0], v[3]},
				{null, v[1]},
				{null, v[2]},
				{null, v[3]},
			},
		},
	}
}
