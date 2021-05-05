// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file doesn't live next to execinfra/joinreader.go in order to avoid
// the import cycle with distsqlutils.

package rowexec

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var threeIntColsAndBoolCol = []*types.T{
	types.Int, types.Int, types.Int, types.Bool}
var sixIntColsAndStringCol = []*types.T{
	types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.String}

func TestJoinReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	sumFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	// Insert a row for NULL testing.
	if _, err := sqlDB.Exec("INSERT INTO test.t VALUES (10, 0, NULL, NULL)"); err != nil {
		t.Fatal(err)
	}

	tdSecondary := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	tdFamily := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")

	sqlutils.CreateTable(t, sqlDB, "t3parent",
		"a INT PRIMARY KEY",
		0,
		sqlutils.ToRowFn(aFn))

	sqlutils.CreateTableInterleaved(t, sqlDB, "t3",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		"t3parent(a)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))
	tdInterleaved := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t3")

	testCases := []struct {
		description string
		indexIdx    uint32
		// The OutputColumns in post are the ones without continuation. For tests
		// that include the continuation column, the test adds the column position
		// using outputColumnForContinuation.
		post       execinfrapb.PostProcessSpec
		onExpr     string
		lookupExpr string
		input      [][]tree.Datum
		lookupCols []uint32
		joinType   descpb.JoinType
		inputTypes []*types.T
		// The output types for the case without continuation. The test adds the
		// bool type for the case with continuation.
		outputTypes            []*types.T
		secondJoinInPairedJoin bool
		// Without and with continuation output.
		expected                    string
		expectedWithContinuation    string
		outputColumnForContinuation uint32
	}{
		{
			description: "Test selecting columns from second table",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.ThreeIntCols,
			expected:                    "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
			expectedWithContinuation:    "[[0 2 2 false] [0 5 5 false] [1 0 1 false] [1 5 6 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test duplicates in the input of lookup joins",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.ThreeIntCols,
			expected:                    "[[0 2 2] [0 2 2] [0 5 5] [1 0 0] [1 5 5]]",
			expectedWithContinuation:    "[[0 2 2 false] [0 2 2 false] [0 5 5 false] [1 0 0 false] [1 5 5 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup join queries with separate families",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.FourIntCols,
			expected:                    "[[0 2 2 2] [0 5 5 5] [1 0 0 1] [1 5 5 6]]",
			expectedWithContinuation:    "[[0 2 2 2 false] [0 5 5 5 false] [1 0 0 1 false] [1 5 5 6 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup joins preserve order of left input",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 3},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(2), bFn(2)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.ThreeIntCols,
			expected:                    "[[0 2 2] [0 5 5] [0 2 2] [1 0 0] [1 5 5]]",
			expectedWithContinuation:    "[[0 2 2 false] [0 5 5 false] [0 2 2 false] [1 0 0 false] [1 5 5 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup join with onExpr",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				{aFn(5), bFn(5)},
				{aFn(10), bFn(10)},
				{aFn(15), bFn(15)},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.ThreeIntCols,
			onExpr:                      "@2 < @5",
			expected:                    "[[1 0 1] [1 5 6]]",
			expectedWithContinuation:    "[[1 0 1 false] [1 5 6 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test left outer lookup join on primary index",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 4},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				{aFn(2), bFn(2)},
			},
			lookupCols:                  []uint32{0, 1},
			joinType:                    descpb.LeftOuterJoin,
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.ThreeIntCols,
			expected:                    "[[10 0 NULL] [0 2 2]]",
			expectedWithContinuation:    "[[10 0 NULL false] [0 2 2 false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup join with multiple matches for a row",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				// No match for this row.
				{aFn(200), bFn(200)},
				{aFn(12), bFn(12)},
			},
			lookupCols:  []uint32{0},
			inputTypes:  types.TwoIntCols,
			outputTypes: types.FourIntCols,
			expected: "[[0 2 0 1] [0 2 0 2] [0 2 0 3] [0 2 0 4] [0 2 0 5] [0 2 0 6] [0 2 0 7] " +
				"[0 2 0 8] [0 2 0 9] " +
				"[1 2 1 1] [1 2 1 2] [1 2 1 3] [1 2 1 4] [1 2 1 5] [1 2 1 6] [1 2 1 7] [1 2 1 8] " +
				"[1 2 1 9] [1 2 1 10]",
			expectedWithContinuation: "[[0 2 0 1 false] [0 2 0 2 true] [0 2 0 3 true] [0 2 0 4 true] " +
				"[0 2 0 5 true] [0 2 0 6 true] [0 2 0 7 true] [0 2 0 8 true] [0 2 0 9 true] " +
				"[1 2 1 1 false] [1 2 1 2 true] [1 2 1 3 true] [1 2 1 4 true] [1 2 1 5 true] " +
				"[1 2 1 6 true] [1 2 1 7 true] [1 2 1 8 true] [1 2 1 9 true] [1 2 1 10 true]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test left outer lookup join with multiple matches for a row",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2, 4},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2)},
				// No match for this row.
				{aFn(200), bFn(200)},
				{aFn(12), bFn(12)},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftOuterJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.FourIntCols,
			expected: "[[0 2 0 1] [0 2 0 2] [0 2 0 3] [0 2 0 4] [0 2 0 5] [0 2 0 6] [0 2 0 7] " +
				"[0 2 0 8] [0 2 0 9] " +
				"[20 0 NULL NULL] " +
				"[1 2 1 1] [1 2 1 2] [1 2 1 3] [1 2 1 4] [1 2 1 5] [1 2 1 6] [1 2 1 7] [1 2 1 8] " +
				"[1 2 1 9] [1 2 1 10]",
			expectedWithContinuation: "[[0 2 0 1 false] [0 2 0 2 true] [0 2 0 3 true] [0 2 0 4 true] " +
				"[0 2 0 5 true] [0 2 0 6 true] [0 2 0 7 true] [0 2 0 8 true] [0 2 0 9 true] " +
				"[20 0 NULL NULL false] " +
				"[1 2 1 1 false] [1 2 1 2 true] [1 2 1 3 true] [1 2 1 4 true] [1 2 1 5 true] " +
				"[1 2 1 6 true] [1 2 1 7 true] [1 2 1 8 true] [1 2 1 9 true] [1 2 1 10 true]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:                  []uint32{0, 1},
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.OneIntCol,
			expected:                    "[]",
			expectedWithContinuation:    "[]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test left outer lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:                  []uint32{0, 1},
			joinType:                    descpb.LeftOuterJoin,
			inputTypes:                  types.TwoIntCols,
			outputTypes:                 types.TwoIntCols,
			expected:                    "[[0 NULL]]",
			expectedWithContinuation:    "[[0 NULL false]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test lookup join on secondary index with an implicit key column",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{2},
			},
			input: [][]tree.Datum{
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:                  []uint32{1, 2, 0},
			inputTypes:                  []*types.T{types.Int, types.Int, types.String},
			outputTypes:                 types.OneIntCol,
			expected:                    "[['two']]",
			expectedWithContinuation:    "[['two' false]]",
			outputColumnForContinuation: 7,
		},
		{
			description: "Test left semi lookup join",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1234)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(6)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(7)), sqlutils.RowEnglishFn(2)},
				{tree.NewDInt(tree.DInt(1)), sqlutils.RowEnglishFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			inputTypes:  []*types.T{types.Int, types.String},
			outputTypes: types.TwoIntCols,
			expected:    "[[1 'two'] [1 'two'] [6 'two'] [7 'two'] [1 'two']]",
		},
		{
			description: "Test left semi lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    descpb.LeftSemiJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left semi lookup join with onExpr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(1234)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  types.TwoIntCols,
			outputTypes: types.TwoIntCols,
			expected:    "[[1 3] [7 3]]",
		},
		{
			description: "Test left anti lookup join",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1234)), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.TwoIntCols,
			expected:    "[[1234 1234]]",
		},
		{
			description: "Test left anti lookup join with onExpr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(1)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
				{tree.NewDInt(tree.DInt(6)), bFn(2)},
				{tree.NewDInt(tree.DInt(7)), bFn(3)},
				{tree.NewDInt(tree.DInt(1)), bFn(2)},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			onExpr:      "@2 > 2",
			inputTypes:  types.TwoIntCols,
			outputTypes: types.TwoIntCols,
			expected:    "[[1 2] [6 2] [1 2]]",
		},
		{
			description: "Test left anti lookup join with match",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{aFn(10), tree.NewDInt(tree.DInt(1234))},
			},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.OneIntCol,
			expected:    "[]",
		},
		{
			description: "Test left anti lookup join on secondary index with NULL lookup value",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(0), tree.DNull},
			},
			lookupCols:  []uint32{0, 1},
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.TwoIntCols,
			expected:    "[[0 NULL]]",
		},
		{
			description: "Test second join in paired-joins with outer join",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2, 4, 5, 6, 7},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(5), bFn(5), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(23)), tree.DNull, tree.DNull, tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(26)), aFn(110), bFn(110), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(26)), aFn(7), bFn(7), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(105), bFn(105), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(34)), aFn(110), bFn(110), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(120), bFn(120), tree.DBoolTrue},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftOuterJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            sixIntColsAndStringCol,
			secondJoinInPairedJoin: true,
			expected: "[[12 0 2 0 2 2 'two'] [12 0 5 0 5 5 'five'] [23 NULL NULL NULL NULL NULL NULL] " +
				"[26 0 7 0 7 7 'seven'] [34 12 0 NULL NULL NULL NULL]]",
		},
		{
			description: "Test second join in paired-joins with semi join",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(5), bFn(5), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(26)), aFn(110), bFn(110), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(26)), aFn(7), bFn(7), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(105), bFn(105), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(34)), aFn(110), bFn(110), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(120), bFn(120), tree.DBoolTrue},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftSemiJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            types.ThreeIntCols,
			secondJoinInPairedJoin: true,
			expected:               "[[12 0 2] [26 0 7]]",
		},
		{
			description: "Test second join in paired-joins with anti join",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(5), bFn(5), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(23)), tree.DNull, tree.DNull, tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(26)), aFn(110), bFn(110), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(26)), aFn(7), bFn(7), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(105), bFn(105), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(34)), aFn(110), bFn(110), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(120), bFn(120), tree.DBoolTrue},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftAntiJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            types.ThreeIntCols,
			secondJoinInPairedJoin: true,
			expected:               "[[23 NULL NULL] [34 12 0]]",
		},
		{
			// Group will span batches when we SetBatchSizeBytes to ~2 rows below.
			description: "Test second join in paired-joins with outer join with group spanning batches",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2, 4, 5, 6, 7},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(106), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(107), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(108), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(109), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(110), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(111), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(112), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(113), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(114), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(5), bFn(5), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(43)), aFn(105), bFn(105), tree.DBoolFalse},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftOuterJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            sixIntColsAndStringCol,
			secondJoinInPairedJoin: true,
			expected:               "[[12 0 2 0 2 2 'two'] [34 0 5 0 5 5 'five'] [43 10 5 NULL NULL NULL NULL]]",
		},
		{
			// Group will span batches when we SetBatchSizeBytes to ~2 rows below.
			description: "Test second join in paired-joins with semi join with group spanning batches",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(106), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(107), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(108), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(109), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(110), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(111), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(112), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(113), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(114), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(5), bFn(5), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(43)), aFn(105), bFn(105), tree.DBoolFalse},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftSemiJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            types.ThreeIntCols,
			secondJoinInPairedJoin: true,
			expected:               "[[12 0 2] [34 0 5]]",
		},
		{
			// Group will span batches since we SetBatchSizeBytes to ~2 rows below.
			description: "Test second join in paired-joins with anti join with group spanning batches",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(12)), aFn(2), bFn(2), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(12)), aFn(105), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(106), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(107), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(108), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(109), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(110), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(111), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(112), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(113), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(12)), aFn(114), bFn(105), tree.DBoolTrue},
				{tree.NewDInt(tree.DInt(34)), aFn(5), bFn(5), tree.DBoolFalse},
				{tree.NewDInt(tree.DInt(43)), aFn(105), bFn(105), tree.DBoolFalse},
			},
			lookupCols:             []uint32{1, 2},
			joinType:               descpb.LeftAntiJoin,
			inputTypes:             threeIntColsAndBoolCol,
			outputTypes:            types.ThreeIntCols,
			secondJoinInPairedJoin: true,
			expected:               "[[43 10 5]]",
		},
		{
			description: "Test left outer lookup join on primary index with lookup expr",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 2, 3},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				// No match for this row.
				{tree.NewDInt(tree.DInt(11)), tree.NewDInt(tree.DInt(11))},
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
			},
			lookupExpr:  "@3 IN (1, 2) AND @2 = @4",
			joinType:    descpb.LeftOuterJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 0] [0 2 0] [11 NULL NULL] [2 1 2] [2 2 2] [2 1 2] [2 2 2]]",
			expectedWithContinuation: "[[0 1 0 false] [0 2 0 true] [11 NULL NULL false] [2 1 2 false] " +
				"[2 2 2 true] [2 1 2 false] [2 2 2 true]]",
			outputColumnForContinuation: 6,
		},
		{
			description: "Test left anti lookup join on primary index with lookup expr",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
				// No match for this row.
				{tree.NewDInt(tree.DInt(11)), tree.NewDInt(tree.DInt(11))},
			},
			lookupExpr:  "@4 = @2 AND @3 IN (1, 2)",
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.OneIntCol,
			expected:    "[[11]]",
		},
		{
			description: "Test left outer lookup join on secondary index with lookup expr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 6},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1), sqlutils.RowEnglishFn(1)},
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
				{aFn(10), tree.DNull, tree.DNull},
				// No match for this row.
				{aFn(20), bFn(20), sqlutils.RowEnglishFn(20)},
				// No match for this row since it's null.
				{tree.DNull, bFn(1), sqlutils.RowEnglishFn(1)},
			},
			lookupExpr:  "@5 IN (1, 2, 5) AND @4 = @1 AND @7 IN ('one', 'two', 'one-two')",
			joinType:    descpb.LeftOuterJoin,
			inputTypes:  []*types.T{types.Int, types.Int, types.String},
			outputTypes: []*types.T{types.Int, types.Int, types.String},
			expected: "[[0 1 'one'] [0 1 'two'] [0 2 'one'] [0 2 'two'] [1 NULL 'one-two'] " +
				"[2 0 NULL] [NULL 1 NULL]]",
			expectedWithContinuation: "[[0 1 'one' false] [0 1 'two' true] [0 2 'one' false] " +
				"[0 2 'two' true] [1 NULL 'one-two' false] [2 0 NULL false] [NULL 1 NULL false]]",
			outputColumnForContinuation: 7,
		},
		{
			description: "Test left anti lookup join on secondary index with lookup expr",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1), sqlutils.RowEnglishFn(1)},
				// No match for this row.
				{aFn(20), bFn(20), sqlutils.RowEnglishFn(20)},
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
				{aFn(10), tree.DNull, tree.DNull},
				// No match for this row since it's null.
				{tree.DNull, bFn(1), sqlutils.RowEnglishFn(1)},
			},
			lookupExpr:  "@5 IN (1, 2, 5) AND @7 IN ('one', 'two', 'one-two') AND @1 = @4",
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  []*types.T{types.Int, types.Int, types.String},
			outputTypes: types.TwoIntCols,
			expected:    "[[2 0] [NULL 1]]",
		},
	}
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	for i, td := range []catalog.TableDescriptor{tdSecondary, tdFamily, tdInterleaved} {
		for _, c := range testCases {
			for _, reqOrdering := range []bool{true, false} {
				// Small and large batches exercise different paths of interest for
				// paired joins, so do both.
				for _, smallBatch := range []bool{true, false} {
					for _, outputContinuation := range []bool{false, true} {
						if outputContinuation && c.secondJoinInPairedJoin {
							// outputContinuation is for the first join in paired-joins, so
							// can't do that when this test case is for the second join in
							// paired-joins.
							continue
						}
						if outputContinuation && !reqOrdering {
							// The first join in paired-joins must preserve ordering.
							continue
						}
						if outputContinuation && len(c.expectedWithContinuation) == 0 {
							continue
						}
						t.Run(fmt.Sprintf("%d/reqOrdering=%t/%s/smallBatch=%t/cont=%t",
							i, reqOrdering, c.description, smallBatch, outputContinuation), func(t *testing.T) {
							evalCtx := tree.MakeTestingEvalContext(st)
							defer evalCtx.Stop(ctx)
							flowCtx := execinfra.FlowCtx{
								EvalCtx: &evalCtx,
								Cfg: &execinfra.ServerConfig{
									Settings:    st,
									TempStorage: tempEngine,
								},
								Txn:         kv.NewTxn(ctx, s.DB(), s.NodeID()),
								DiskMonitor: diskMonitor,
							}
							encRows := make(rowenc.EncDatumRows, len(c.input))
							for rowIdx, row := range c.input {
								encRow := make(rowenc.EncDatumRow, len(row))
								for i, d := range row {
									encRow[i] = rowenc.DatumToEncDatum(c.inputTypes[i], d)
								}
								encRows[rowIdx] = encRow
							}
							in := distsqlutils.NewRowBuffer(c.inputTypes, encRows, distsqlutils.RowBufferArgs{})

							out := &distsqlutils.RowBuffer{}
							post := c.post
							if outputContinuation {
								post.OutputColumns = append(post.OutputColumns, c.outputColumnForContinuation)
							}
							jr, err := newJoinReader(
								&flowCtx,
								0, /* processorID */
								&execinfrapb.JoinReaderSpec{
									Table:                             *td.TableDesc(),
									IndexIdx:                          c.indexIdx,
									LookupColumns:                     c.lookupCols,
									LookupExpr:                        execinfrapb.Expression{Expr: c.lookupExpr},
									OnExpr:                            execinfrapb.Expression{Expr: c.onExpr},
									Type:                              c.joinType,
									MaintainOrdering:                  reqOrdering,
									LeftJoinWithPairedJoiner:          c.secondJoinInPairedJoin,
									OutputGroupContinuationForLeftRow: outputContinuation,
								},
								in,
								&post,
								out,
								lookupJoinReaderType,
							)
							if err != nil {
								t.Fatal(err)
							}

							if smallBatch {
								// Set a lower batch size to force multiple batches.
								jr.(*joinReader).SetBatchSizeBytes(int64(encRows[0].Size() * 2))
							}
							// Else, use the default.

							jr.Run(ctx)

							if !in.Done {
								t.Fatal("joinReader didn't consume all the rows")
							}
							if !out.ProducerClosed() {
								t.Fatalf("output RowReceiver not closed")
							}

							var res rowenc.EncDatumRows
							for {
								row, meta := out.Next()
								if meta != nil && meta.Metrics == nil {
									t.Fatalf("unexpected metadata %+v", meta)
								}
								if row == nil {
									break
								}
								res = append(res, row)
							}

							// processOutputRows is a helper function that takes a stringified
							// EncDatumRows output (e.g. [[1 2] [3 1]]) and returns a slice of
							// stringified rows without brackets (e.g. []string{"1 2", "3 1"}).
							processOutputRows := func(output string) []string {
								// Comma-separate the rows.
								output = strings.ReplaceAll(output, "] [", ",")
								// Remove leading and trailing bracket.
								output = strings.Trim(output, "[]")
								// Split on the commas that were introduced and return that.
								return strings.Split(output, ",")
							}

							outputTypes := c.outputTypes
							if outputContinuation {
								outputTypes = append(outputTypes, types.Bool)
							}
							result := processOutputRows(res.String(outputTypes))
							var expected []string
							if outputContinuation {
								expected = processOutputRows(c.expectedWithContinuation)
							} else {
								expected = processOutputRows(c.expected)
							}

							if !reqOrdering {
								// An ordering was not required, so sort both the result and
								// expected slice to reuse equality comparison.
								sort.Strings(result)
								sort.Strings(expected)
							}

							require.Equal(t, expected, result)
						})
					}
				}
			}
		}
	}
}

func TestJoinReaderDiskSpill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create our lookup table, with a single key which has enough rows to trigger
	// a disk spill.
	key := 0
	stringColVal := "0123456789"
	numRows := 100
	if _, err := sqlDB.Exec(`
CREATE DATABASE test;
CREATE TABLE test.t (a INT, s STRING, INDEX (a, s))`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(
		`INSERT INTO test.t SELECT $1, $2 FROM generate_series(1, $3)`,
		key, stringColVal, numRows); err != nil {
		t.Fatal(err)
	}
	td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		Txn:         kv.NewTxn(ctx, s.DB(), s.NodeID()),
		DiskMonitor: diskMonitor,
	}
	// Set the memory limit to the minimum allocation size so that the row
	// container can buffer some rows in memory before spilling to disk. This
	// also means we don't need to disable caching in the
	// DiskBackedIndexedRowContainer.
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = mon.DefaultPoolAllocationSize

	// Input row is just a single 0.
	inputRows := rowenc.EncDatumRows{
		rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(key))}},
	}

	out := &distsqlutils.RowBuffer{}
	jr, err := newJoinReader(
		&flowCtx,
		0, /* processorID */
		&execinfrapb.JoinReaderSpec{
			Table:         *td.TableDesc(),
			IndexIdx:      1,
			LookupColumns: []uint32{0},
			Type:          descpb.InnerJoin,
			// Disk storage is only used when the input ordering must be maintained.
			MaintainOrdering: true,
		},
		distsqlutils.NewRowBuffer(types.OneIntCol, inputRows, distsqlutils.RowBufferArgs{}),
		&execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: []uint32{2},
		},
		out,
		lookupJoinReaderType,
	)
	if err != nil {
		t.Fatal(err)
	}
	jr.Run(ctx)

	count := 0
	for {
		row, meta := out.Next()
		if meta != nil && meta.Metrics == nil {
			t.Fatalf("unexpected metadata %+v", meta)
		}
		if row == nil {
			break
		}
		expected := fmt.Sprintf("['%s']", stringColVal)
		actual := row.String([]*types.T{types.String})
		require.Equal(t, expected, actual)
		count++
	}
	require.Equal(t, numRows, count)
	require.True(t, jr.(*joinReader).Spilled())
}

// TestJoinReaderDrain tests various scenarios in which a joinReader's consumer
// is closed.
func TestJoinReaderDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, PRIMARY KEY (a)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn),
	)
	td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := s.ClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(context.Background(), base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// Run the flow in a verbose trace so that we can test for tracing info.
	tracer := tracing.NewTracer()
	ctx, sp := tracing.StartVerboseTrace(context.Background(), tracer, "test flow ctx")
	defer sp.Finish()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), &leafInputState)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		Txn:         leafTxn,
		DiskMonitor: diskMonitor,
	}

	encRow := make(rowenc.EncDatumRow, 1)
	encRow[0] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1))

	testReaderProcessorDrain(ctx, t, func(out execinfra.RowReceiver) (execinfra.Processor, error) {
		return newJoinReader(
			&flowCtx,
			0, /* processorID */
			&execinfrapb.JoinReaderSpec{Table: *td.TableDesc()},
			distsqlutils.NewRowBuffer(types.OneIntCol, nil /* rows */, distsqlutils.RowBufferArgs{}),
			&execinfrapb.PostProcessSpec{},
			out,
			lookupJoinReaderType,
		)
	})

	// ConsumerDone verifies that the producer drains properly by checking that
	// metadata coming from the producer is still read when ConsumerDone is
	// called on the consumer.
	t.Run("ConsumerDone", func(t *testing.T) {
		expectedMetaErr := errors.New("dummy")
		in := distsqlutils.NewRowBuffer(types.OneIntCol, nil /* rows */, distsqlutils.RowBufferArgs{})
		if status := in.Push(encRow, &execinfrapb.ProducerMetadata{Err: expectedMetaErr}); status != execinfra.NeedMoreRows {
			t.Fatalf("unexpected response: %d", status)
		}

		out := &distsqlutils.RowBuffer{}
		out.ConsumerDone()
		jr, err := newJoinReader(
			&flowCtx, 0 /* processorID */, &execinfrapb.JoinReaderSpec{
				Table: *td.TableDesc(),
			}, in, &execinfrapb.PostProcessSpec{},
			out, lookupJoinReaderType)
		if err != nil {
			t.Fatal(err)
		}
		jr.Run(ctx)
		row, meta := out.Next()
		if row != nil {
			t.Fatalf("row was pushed unexpectedly: %s", row.String(types.OneIntCol))
		}
		if !errors.Is(meta.Err, expectedMetaErr) {
			t.Fatalf("unexpected error in metadata: %v", meta.Err)
		}

		// Check for trailing metadata.
		var traceSeen, txnFinalStateSeen bool
		for {
			row, meta = out.Next()
			if row != nil {
				t.Fatalf("row was pushed unexpectedly: %s", row.String(types.OneIntCol))
			}
			if meta == nil {
				break
			}
			if meta.TraceData != nil {
				traceSeen = true
			}
			if meta.LeafTxnFinalState != nil {
				txnFinalStateSeen = true
			}
		}
		if !traceSeen {
			t.Fatal("missing tracing trailing metadata")
		}
		if !txnFinalStateSeen {
			t.Fatal("missing txn final state")
		}
	})
}

func TestIndexJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	sumFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tdf := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = randgen.IntEncDatum(i)
	}

	testCases := []struct {
		description string
		desc        *descpb.TableDescriptor
		post        execinfrapb.PostProcessSpec
		input       rowenc.EncDatumRows
		outputTypes []*types.T
		expected    rowenc.EncDatumRows
	}{
		{
			description: "Test selecting rows using the primary index",
			desc:        td.TableDesc(),
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: rowenc.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
		{
			description: "Test selecting rows using the primary index with multiple family spans",
			desc:        tdf.TableDesc(),
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: rowenc.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: types.ThreeIntCols,
			expected: rowenc.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			spec := execinfrapb.JoinReaderSpec{
				Table:    *c.desc,
				IndexIdx: 0,
			}
			txn := kv.NewTxn(context.Background(), s.DB(), s.NodeID())
			runProcessorTest(
				t,
				execinfrapb.ProcessorCoreUnion{JoinReader: &spec},
				c.post,
				types.TwoIntCols,
				c.input,
				c.outputTypes,
				c.expected,
				txn,
			)
		})
	}
}

// BenchmarkJoinReader benchmarks different lookup join match ratios against a
// table with half a million rows. A match ratio specifies how many rows are
// returned for a single lookup row. Some cases will cause the join reader to
// spill to disk, in which case the benchmark logs that the join spilled.
func BenchmarkJoinReader(b *testing.B) {
	skip.UnderShort(b)

	// Create an *on-disk* store spec for the primary store and temp engine to
	// reflect the real costs of lookups and spilling.
	primaryStoragePath, cleanupPrimaryDir := testutils.TempDir(b)
	defer cleanupPrimaryDir()
	storeSpec, err := base.NewStoreSpec(fmt.Sprintf("path=%s", primaryStoragePath))
	require.NoError(b, err)

	var (
		logScope       = log.Scope(b)
		ctx            = context.Background()
		s, sqlDB, kvDB = serverutils.StartServer(b, base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{storeSpec},
		})
		st          = s.ClusterSettings()
		evalCtx     = tree.MakeTestingEvalContext(st)
		diskMonitor = execinfra.NewTestDiskMonitor(ctx, st)
		flowCtx     = execinfra.FlowCtx{
			EvalCtx: &evalCtx,
			Cfg: &execinfra.ServerConfig{
				Settings: st,
			},
			DiskMonitor: diskMonitor,
		}
	)
	defer logScope.Close(b)
	defer s.Stopper().Stop(ctx)
	defer evalCtx.Stop(ctx)
	defer diskMonitor.Stop(ctx)

	tempStoragePath, cleanupTempDir := testutils.TempDir(b)
	defer cleanupTempDir()
	tempStoreSpec, err := base.NewStoreSpec(fmt.Sprintf("path=%s", tempStoragePath))
	require.NoError(b, err)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.TempStorageConfig{Path: tempStoragePath, Mon: diskMonitor}, tempStoreSpec)
	require.NoError(b, err)
	defer tempEngine.Close()
	flowCtx.Cfg.TempStorage = tempEngine

	// rightSideColumnDef is the definition of a column in the table that is being
	// looked up.
	type rightSideColumnDef struct {
		// name is the name of the column.
		name string
		// matchesPerLookupRow is the number of rows with the same column value.
		matchesPerLookupRow int
	}
	rightSideColumnDefs := []rightSideColumnDef{
		{name: "one", matchesPerLookupRow: 1},
		{name: "four", matchesPerLookupRow: 4},
		{name: "sixteen", matchesPerLookupRow: 16},
		{name: "thirtytwo", matchesPerLookupRow: 32},
		{name: "sixtyfour", matchesPerLookupRow: 64},
	}
	tableSizeToName := func(sz int) string {
		return fmt.Sprintf("t%d", sz)
	}

	createRightSideTable := func(sz int) {
		colDefs := make([]string, 0, len(rightSideColumnDefs))
		indexDefs := make([]string, 0, len(rightSideColumnDefs))
		genValueFns := make([]sqlutils.GenValueFn, 0, len(rightSideColumnDefs))
		for _, columnDef := range rightSideColumnDefs {
			if columnDef.matchesPerLookupRow > sz {
				continue
			}
			colDefs = append(colDefs, fmt.Sprintf("%s INT", columnDef.name))
			indexDefs = append(indexDefs, fmt.Sprintf("INDEX (%s)", columnDef.name))

			curValue := -1
			// Capture matchesPerLookupRow for use in the generating function later
			// on.
			matchesPerLookupRow := columnDef.matchesPerLookupRow
			genValueFns = append(genValueFns, func(row int) tree.Datum {
				idx := row - 1
				if idx%matchesPerLookupRow == 0 {
					// Increment curValue every columnDef.matchesPerLookupRow values. The
					// first value will be 0.
					curValue++
				}
				return tree.NewDInt(tree.DInt(curValue))
			})
		}
		tableName := tableSizeToName(sz)

		sqlutils.CreateTable(
			b, sqlDB, tableName, strings.Join(append(colDefs, indexDefs...), ", "), sz,
			sqlutils.ToRowFn(genValueFns...),
		)
	}

	rightSz := 1 << 19 /* 524,288 rows */
	createRightSideTable(rightSz)
	// Create a new txn after the table has been created.
	flowCtx.Txn = kv.NewTxn(ctx, s.DB(), s.NodeID())
	for _, reqOrdering := range []bool{true, false} {
		for columnIdx, columnDef := range rightSideColumnDefs {
			for _, numLookupRows := range []int{1, 1 << 4 /* 16 */, 1 << 8 /* 256 */, 1 << 10 /* 1024 */, 1 << 12 /* 4096 */, 1 << 13 /* 8192 */, 1 << 14 /* 16384 */, 1 << 15 /* 32768 */, 1 << 16 /* 65,536 */, 1 << 19 /* 524,288 */} {
				for _, memoryLimit := range []int64{100 << 10, math.MaxInt64} {
					memoryLimitStr := "mem=unlimited"
					if memoryLimit != math.MaxInt64 {
						if !reqOrdering {
							// Smaller memory limit is not relevant when there is no ordering.
							continue
						}
						memoryLimitStr = fmt.Sprintf("mem=%dKB", memoryLimit/(1<<10))
						// The benchmark workloads are such that each right row never joins
						// with more than one left row. And the access pattern of right rows
						// accessed across all the left rows is monotonically increasing. So
						// once spilled to disk, the reads will always need to get from disk
						// (caching cannot improve performance).
						//
						// TODO(sumeer): add workload that can benefit from caching.
					}
					if rightSz/columnDef.matchesPerLookupRow < numLookupRows {
						// This case does not make sense since we won't have distinct lookup
						// rows. We don't currently merge spans which could make this an
						// interesting case to benchmark, but we probably should.
						continue
					}

					eqColsAreKey := []bool{false}
					if numLookupRows == 1 {
						// For this case, execute the parallel lookup case as well.
						eqColsAreKey = []bool{true, false}
					}
					for _, parallel := range eqColsAreKey {
						benchmarkName := fmt.Sprintf("reqOrdering=%t/matchratio=oneto%s/lookuprows=%d/%s",
							reqOrdering, columnDef.name, numLookupRows, memoryLimitStr)
						if parallel {
							benchmarkName += "/parallel=true"
						}
						b.Run(benchmarkName, func(b *testing.B) {
							tableName := tableSizeToName(rightSz)

							// Get the table descriptor and find the index that will provide us with
							// the expected match ratio.
							tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)
							foundIndex := catalog.FindPublicNonPrimaryIndex(tableDesc, func(idx catalog.Index) bool {
								require.Equal(b, 1, idx.NumColumns(), "all indexes created in this benchmark should only contain one column")
								return idx.GetColumnName(0) == columnDef.name
							})
							if foundIndex == nil {
								b.Fatalf("failed to find secondary index for column %s", columnDef.name)
							}
							indexIdx := uint32(foundIndex.Ordinal())
							input := newRowGeneratingSource(types.OneIntCol, sqlutils.ToRowFn(func(rowIdx int) tree.Datum {
								// Convert to 0-based.
								return tree.NewDInt(tree.DInt(rowIdx - 1))
							}), numLookupRows)
							output := rowDisposer{}

							spec := execinfrapb.JoinReaderSpec{
								Table:               *tableDesc.TableDesc(),
								LookupColumns:       []uint32{0},
								LookupColumnsAreKey: parallel,
								IndexIdx:            indexIdx,
								MaintainOrdering:    reqOrdering,
							}
							// Post specifies that only the columns contained in the secondary index
							// need to be output.
							post := execinfrapb.PostProcessSpec{
								Projection:    true,
								OutputColumns: []uint32{uint32(columnIdx + 1)},
							}

							expectedNumOutputRows := numLookupRows * columnDef.matchesPerLookupRow
							b.ResetTimer()
							// The number of bytes processed in this benchmark is the number of
							// lookup bytes processed + the number of result bytes. We only look
							// up using a single int column and the request only a single int column
							// contained in the index.
							b.SetBytes(int64((numLookupRows * 8) + (expectedNumOutputRows * 8)))

							spilled := false
							for i := 0; i < b.N; i++ {
								flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
								jr, err := newJoinReader(&flowCtx, 0 /* processorID */, &spec, input, &post, &output, lookupJoinReaderType)
								if err != nil {
									b.Fatal(err)
								}
								jr.Run(ctx)
								if !spilled && jr.(*joinReader).Spilled() {
									spilled = true
								}
								meta := output.bufferedMeta
								if len(meta) != 1 || meta[0].Metrics == nil {
									// Expect a single metadata payload with Metrics set.
									b.Fatalf("unexpected metadata: %v", meta)
								}
								if output.NumRowsDisposed() != expectedNumOutputRows {
									b.Fatalf("got %d output rows, expected %d", output.NumRowsDisposed(), expectedNumOutputRows)
								}
								output.ResetNumRowsDisposed()
								input.Reset()
							}
							if spilled {
								b.Log("joinReader spilled to disk in at least one of the benchmark iterations")
							}
						})
					}
				}
			}
		}
	}
}
