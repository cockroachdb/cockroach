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

package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type setOpTestCase struct {
	setOpType   sqlbase.JoinType
	columnTypes []types.T
	leftInput   sqlbase.EncDatumRows
	rightInput  sqlbase.EncDatumRows
	expected    sqlbase.EncDatumRows
}

func setOpTestCaseToMergeJoinerTestCase(tc setOpTestCase) mergeJoinerTestCase {
	spec := distsqlpb.MergeJoinerSpec{Type: tc.setOpType}
	ordering := make(sqlbase.ColumnOrdering, 0, len(tc.columnTypes))
	outCols := make([]uint32, 0, len(tc.columnTypes))
	for i := range tc.columnTypes {
		ordering = append(ordering, sqlbase.ColumnOrderInfo{ColIdx: i, Direction: encoding.Ascending})
		outCols = append(outCols, uint32(i))
	}
	spec.LeftOrdering = distsqlpb.ConvertToSpecOrdering(ordering)
	spec.RightOrdering = distsqlpb.ConvertToSpecOrdering(ordering)

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
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
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
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[3]},
			},
		},
		{
			// Check that EXCEPT ALL returns the correct number of duplicates when
			// the left side contains more duplicates of a row than the right side.
			setOpType:   sqlbase.ExceptAllJoin,
			columnTypes: sqlbase.TwoIntCols,
			leftInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			expected: sqlbase.EncDatumRows{
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
				{v[0], v[0]},
				{v[0], v[0]},
				{v[0], v[1]},
				{v[0], v[3]},
				{v[5], v[0]},
				{v[5], v[1]},
			},
			rightInput: sqlbase.EncDatumRows{
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
	}
}
