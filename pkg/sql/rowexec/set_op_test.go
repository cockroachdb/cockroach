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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type setOpTestCase struct {
	setOpType   sqlbase.JoinType
	columnTypes []*types.T
	ordering    sqlbase.ColumnOrdering
	leftInput   sqlbase.EncDatumRows
	rightInput  sqlbase.EncDatumRows
	expected    sqlbase.EncDatumRows
}

func setOpTestCaseToMergeJoinerTestCase(tc setOpTestCase) mergeJoinerTestCase {
	spec := execinfrapb.MergeJoinerSpec{Type: tc.setOpType, NullEquality: true}
	var ordering sqlbase.ColumnOrdering
	if tc.ordering != nil {
		ordering = tc.ordering
	} else {
		ordering = make(sqlbase.ColumnOrdering, 0, len(tc.columnTypes))
		for i := range tc.columnTypes {
			ordering = append(ordering, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
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

func setOpTestCaseToJoinerTestCase(tc setOpTestCase) joinerTestCase {
	outCols := make([]uint32, 0, len(tc.columnTypes))
	for i := range tc.columnTypes {
		outCols = append(outCols, uint32(i))
	}

	return joinerTestCase{
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
	null := sqlbase.EncDatum{Datum: tree.DNull}
	var v = [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	return []setOpTestCase{
		{
			// Check that INTERSECT ALL only returns rows that are in both the left
			// and right side.
			setOpType:   sqlbase.IntersectAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
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
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
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
			setOpType:   sqlbase.IntersectAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
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
			rightInput: sqlbase.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
			setOpType:   sqlbase.IntersectAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
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
	null := sqlbase.EncDatum{Datum: tree.DNull}
	var v = [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	return []setOpTestCase{
		{
			// Check that EXCEPT ALL only returns rows that are on the left side
			// but not the right side.
			setOpType:   sqlbase.ExceptAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
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
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[3]},
				{v[1], null},
			},
		},
		{
			// Check that EXCEPT ALL returns the correct number of duplicates when
			// the left side contains more duplicates of a row than the right side.
			setOpType:   sqlbase.ExceptAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
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
			rightInput: sqlbase.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
			setOpType:   sqlbase.ExceptAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{null, null},
				{null, v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
				{v[0], v[3]},
			},
		},
		{
			// Check that EXCEPT ALL handles mixed ordering correctly.
			setOpType:   sqlbase.ExceptAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Descending},
				{ColIdx: 1, Direction: encoding.Ascending},
			},
			leftInput: sqlbase.EncDatumRows{
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
			rightInput: sqlbase.EncDatumRows{
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
			expected: sqlbase.EncDatumRows{
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
