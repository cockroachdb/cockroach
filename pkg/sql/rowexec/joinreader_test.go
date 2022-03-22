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
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

var threeIntColsAndBoolCol = []*types.T{
	types.Int, types.Int, types.Int, types.Bool}
var sixIntColsAndStringCol = []*types.T{
	types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.String}

func TestJoinReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	tdSecondary := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	tdFamily := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")

	sqlutils.CreateTable(t, sqlDB, "t3parent",
		"a INT PRIMARY KEY",
		0,
		sqlutils.ToRowFn(aFn))

	testCases := []struct {
		description string
		indexIdx    uint32
		// The OutputColumns in post are the ones without continuation. For tests
		// that include the continuation column, the test adds the continuation
		// column.
		post             execinfrapb.PostProcessSpec
		onExpr           string
		lookupExpr       string
		remoteLookupExpr string
		input            [][]tree.Datum
		fetchCols        []uint32
		lookupCols       []uint32
		joinType         descpb.JoinType
		inputTypes       []*types.T
		// The output types for the case without continuation. The test adds the
		// bool type for the case with continuation.
		outputTypes            []*types.T
		secondJoinInPairedJoin bool
		// Without and with continuation output.
		expected                 string
		expectedWithContinuation string
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.ThreeIntCols,
			expected:                 "[[0 2 2] [0 5 5] [1 0 1] [1 5 6]]",
			expectedWithContinuation: "[[0 2 2 false] [0 5 5 false] [1 0 1 false] [1 5 6 false]]",
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.ThreeIntCols,
			expected:                 "[[0 2 2] [0 2 2] [0 5 5] [1 0 0] [1 5 5]]",
			expectedWithContinuation: "[[0 2 2 false] [0 2 2 false] [0 5 5 false] [1 0 0 false] [1 5 5 false]]",
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.FourIntCols,
			expected:                 "[[0 2 2 2] [0 5 5 5] [1 0 0 1] [1 5 5 6]]",
			expectedWithContinuation: "[[0 2 2 2 false] [0 5 5 5 false] [1 0 0 1 false] [1 5 5 6 false]]",
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.ThreeIntCols,
			expected:                 "[[0 2 2] [0 5 5] [0 2 2] [1 0 0] [1 5 5]]",
			expectedWithContinuation: "[[0 2 2 false] [0 5 5 false] [0 2 2 false] [1 0 0 false] [1 5 5 false]]",
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.ThreeIntCols,
			onExpr:                   "@2 < @5",
			expected:                 "[[1 0 1] [1 5 6]]",
			expectedWithContinuation: "[[1 0 1 false] [1 5 6 false]]",
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
			fetchCols:                []uint32{0, 1, 2},
			lookupCols:               []uint32{0, 1},
			joinType:                 descpb.LeftOuterJoin,
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.ThreeIntCols,
			expected:                 "[[10 0 NULL] [0 2 2]]",
			expectedWithContinuation: "[[10 0 NULL false] [0 2 2 false]]",
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
			fetchCols:   []uint32{0, 1, 2},
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
			fetchCols:   []uint32{0, 1, 2},
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
			fetchCols:                []uint32{0, 1},
			lookupCols:               []uint32{0, 1},
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.OneIntCol,
			expected:                 "[]",
			expectedWithContinuation: "[]",
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
			fetchCols:                []uint32{0, 1},
			lookupCols:               []uint32{0, 1},
			joinType:                 descpb.LeftOuterJoin,
			inputTypes:               types.TwoIntCols,
			outputTypes:              types.TwoIntCols,
			expected:                 "[[0 NULL]]",
			expectedWithContinuation: "[[0 NULL false]]",
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
			fetchCols:                []uint32{0, 1, 3},
			lookupCols:               []uint32{1, 2, 0},
			inputTypes:               []*types.T{types.Int, types.Int, types.String},
			outputTypes:              types.OneIntCol,
			expected:                 "[['two']]",
			expectedWithContinuation: "[['two' false]]",
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
			fetchCols:   []uint32{0},
			lookupCols:  []uint32{0},
			joinType:    descpb.LeftSemiJoin,
			inputTypes:  []*types.T{types.Int, types.String},
			outputTypes: []*types.T{types.Int, types.String},
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
			fetchCols:   []uint32{0, 1},
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
			fetchCols:   []uint32{0},
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
			fetchCols:   []uint32{0},
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
			fetchCols:   []uint32{0},
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
			fetchCols:   []uint32{0},
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
			fetchCols:   []uint32{0, 1},
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
			fetchCols:              []uint32{0, 1, 2, 3},
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
			fetchCols:              []uint32{0, 1, 2, 3},
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
			fetchCols:              []uint32{0, 1, 2, 3},
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
			fetchCols:              []uint32{0, 1, 2, 3},
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
			fetchCols:              []uint32{1, 2},
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
			fetchCols:              []uint32{1, 2},
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
			fetchCols:   []uint32{0, 1},
			lookupExpr:  "@3 IN (1, 2) AND @2 = @4",
			joinType:    descpb.LeftOuterJoin,
			inputTypes:  types.TwoIntCols,
			outputTypes: types.ThreeIntCols,
			expected:    "[[0 1 0] [0 2 0] [11 NULL NULL] [2 1 2] [2 2 2] [2 1 2] [2 2 2]]",
			expectedWithContinuation: "[[0 1 0 false] [0 2 0 true] [11 NULL NULL false] [2 1 2 false] " +
				"[2 2 2 true] [2 1 2 false] [2 2 2 true]]",
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
			fetchCols:   []uint32{0, 1},
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
				OutputColumns: []uint32{0, 1, 5},
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
			fetchCols:   []uint32{0, 1, 3},
			lookupExpr:  "@5 IN (1, 2, 5) AND @4 = @1 AND @6 IN ('one', 'two', 'one-two')",
			joinType:    descpb.LeftOuterJoin,
			inputTypes:  []*types.T{types.Int, types.Int, types.String},
			outputTypes: []*types.T{types.Int, types.Int, types.String},
			expected: "[[0 1 'one'] [0 1 'two'] [0 2 'one'] [0 2 'two'] [1 NULL 'one-two'] " +
				"[2 0 NULL] [NULL 1 NULL]]",
			expectedWithContinuation: "[[0 1 'one' false] [0 1 'two' true] [0 2 'one' false] " +
				"[0 2 'two' true] [1 NULL 'one-two' false] [2 0 NULL false] [NULL 1 NULL false]]",
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
			fetchCols:   []uint32{0, 1, 3},
			lookupExpr:  "@5 IN (1, 2, 5) AND @6 IN ('one', 'two', 'one-two') AND @1 = @4",
			joinType:    descpb.LeftAntiJoin,
			inputTypes:  []*types.T{types.Int, types.Int, types.String},
			outputTypes: types.TwoIntCols,
			expected:    "[[2 0] [NULL 1]]",
		},
		{
			description: "Test locality optimized left semi lookup join on primary index",
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1},
			},
			input: [][]tree.Datum{
				{aFn(100), bFn(100)},
				// No match for this row.
				{tree.NewDInt(tree.DInt(11)), tree.NewDInt(tree.DInt(11))},
				{aFn(2), bFn(2)},
				{aFn(2), bFn(2)},
			},
			fetchCols:        []uint32{0, 1},
			lookupExpr:       "@3 = 1 AND @2 = @4",
			remoteLookupExpr: "@3 = 2 AND @2 = @4",
			joinType:         descpb.LeftSemiJoin,
			inputTypes:       types.TwoIntCols,
			outputTypes:      types.OneIntCol,
			expected:         "[[0] [2] [2]]",
		},
		{
			description: "Test locality optimized inner lookup join on secondary index",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 5},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1), sqlutils.RowEnglishFn(1)},
				{aFn(11), bFn(11), sqlutils.RowEnglishFn(11)},
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
				{aFn(22), bFn(22), sqlutils.RowEnglishFn(22)},
				// No match for this row since it's null.
				{aFn(10), tree.DNull, tree.DNull},
				// No match for this row.
				{aFn(20), bFn(20), sqlutils.RowEnglishFn(20)},
			},
			fetchCols:        []uint32{0, 1, 3},
			lookupExpr:       "@5 = 1 AND @3 = @6",
			remoteLookupExpr: "@5 IN (2, 5) AND @3 = @6",
			joinType:         descpb.InnerJoin,
			inputTypes:       []*types.T{types.Int, types.Int, types.String},
			outputTypes:      []*types.T{types.Int, types.Int, types.String},
			expected:         "[[0 1 'one'] [1 1 'one-one'] [0 2 'two'] [2 2 'two-two']]",
			expectedWithContinuation: "[[0 1 'one' false] [1 1 'one-one' false] [0 2 'two' false] " +
				"[2 2 'two-two' false]]",
		},
		{
			description: "Test locality optimized left outer lookup join on secondary index",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 5},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1), sqlutils.RowEnglishFn(1)},
				{aFn(11), bFn(11), sqlutils.RowEnglishFn(11)},
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
				{aFn(22), bFn(22), sqlutils.RowEnglishFn(22)},
				// No match for this row since it's null.
				{aFn(10), tree.DNull, tree.DNull},
				// No match for this row.
				{aFn(20), bFn(20), sqlutils.RowEnglishFn(20)},
			},
			fetchCols:        []uint32{0, 1, 3},
			lookupExpr:       "@5 = 1 AND @3 = @6",
			remoteLookupExpr: "@5 IN (2, 5) AND @3 = @6",
			joinType:         descpb.LeftOuterJoin,
			inputTypes:       []*types.T{types.Int, types.Int, types.String},
			outputTypes:      []*types.T{types.Int, types.Int, types.String},
			expected:         "[[0 1 'one'] [1 1 'one-one'] [0 2 'two'] [2 2 'two-two'] [1 NULL NULL] [2 0 NULL]]",
			expectedWithContinuation: "[[0 1 'one' false] [1 1 'one-one' false] [0 2 'two' false] " +
				"[2 2 'two-two' false] [1 NULL NULL false] [2 0 NULL false]]",
		},
		{
			description: "Test locality optimized left semi lookup join on secondary index",
			indexIdx:    1,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{aFn(1), bFn(1), sqlutils.RowEnglishFn(1)},
				{aFn(11), bFn(11), sqlutils.RowEnglishFn(11)},
				{aFn(2), bFn(2), sqlutils.RowEnglishFn(2)},
				{aFn(22), bFn(22), sqlutils.RowEnglishFn(22)},
				// No match for this row since it's null.
				{aFn(10), tree.DNull, tree.DNull},
				// No match for this row.
				{aFn(20), bFn(20), sqlutils.RowEnglishFn(20)},
			},
			fetchCols:        []uint32{0, 1, 3},
			lookupExpr:       "@5 = 1 AND @3 = @6",
			remoteLookupExpr: "@5 IN (2, 5) AND @3 = @6",
			joinType:         descpb.LeftSemiJoin,
			inputTypes:       []*types.T{types.Int, types.Int, types.String},
			outputTypes:      types.TwoIntCols,
			expected:         "[[0 1] [1 1] [0 2] [2 2]]",
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
	for i, td := range []catalog.TableDescriptor{tdSecondary, tdFamily} {
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
								post.OutputColumns = append(post.OutputColumns, uint32(len(c.fetchCols)+len(c.inputTypes)))
							}

							index := td.ActiveIndexes()[c.indexIdx]
							var fetchColIDs []descpb.ColumnID
							var neededOrds util.FastIntSet
							for _, ord := range c.fetchCols {
								neededOrds.Add(int(ord))
								fetchColIDs = append(fetchColIDs, td.PublicColumns()[ord].GetID())
							}
							var fetchSpec descpb.IndexFetchSpec
							if err := rowenc.InitIndexFetchSpec(
								&fetchSpec,
								keys.SystemSQLCodec,
								td, index, fetchColIDs,
							); err != nil {
								t.Fatal(err)
							}
							splitter := span.MakeSplitter(td, index, neededOrds)

							jr, err := newJoinReader(
								&flowCtx,
								0, /* processorID */
								&execinfrapb.JoinReaderSpec{
									FetchSpec:                         fetchSpec,
									SplitFamilyIDs:                    splitter.FamilyIDs(),
									LookupColumns:                     c.lookupCols,
									LookupExpr:                        execinfrapb.Expression{Expr: c.lookupExpr},
									RemoteLookupExpr:                  execinfrapb.Expression{Expr: c.remoteLookupExpr},
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
	defer log.Scope(t).Close(t)

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
	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

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
	var fetchSpec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&fetchSpec,
		keys.SystemSQLCodec,
		td,
		td.ActiveIndexes()[1],
		[]descpb.ColumnID{1, 2, 3},
	); err != nil {
		t.Fatal(err)
	}

	out := &distsqlutils.RowBuffer{}
	jr, err := newJoinReader(
		&flowCtx,
		0, /* processorID */
		&execinfrapb.JoinReaderSpec{
			FetchSpec:     fetchSpec,
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
	defer log.Scope(t).Close(t)

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
	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	st := s.ClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(context.Background(), base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// Run the flow in a verbose trace so that we can test for tracing info.
	tracer := s.TracerI().(*tracing.Tracer)
	ctx, sp := tracer.StartSpanCtx(context.Background(), "test flow ctx", tracing.WithRecording(tracing.RecordingVerbose))
	defer sp.Finish()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), leafInputState)

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

	var fetchSpec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&fetchSpec,
		keys.SystemSQLCodec,
		td, td.GetPrimaryIndex(), []descpb.ColumnID{1},
	); err != nil {
		t.Fatal(err)
	}

	testReaderProcessorDrain(ctx, t, func(out execinfra.RowReceiver) (execinfra.Processor, error) {
		return newJoinReader(
			&flowCtx,
			0, /* processorID */
			&execinfrapb.JoinReaderSpec{FetchSpec: fetchSpec},
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
				FetchSpec: fetchSpec,
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
	defer log.Scope(t).Close(t)

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

	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tdf := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")

	v := [10]rowenc.EncDatum{}
	for i := range v {
		v[i] = randgen.IntEncDatum(i)
	}

	testCases := []struct {
		description string
		desc        catalog.TableDescriptor
		fetchCols   []int
		post        execinfrapb.PostProcessSpec
		input       rowenc.EncDatumRows
		outputTypes []*types.T
		expected    rowenc.EncDatumRows
	}{
		{
			description: "Test selecting rows using the primary index",
			desc:        td,
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
			desc:        tdf,
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
			var fetchSpec descpb.IndexFetchSpec
			if err := rowenc.InitIndexFetchSpec(
				&fetchSpec,
				keys.SystemSQLCodec,
				c.desc, c.desc.GetPrimaryIndex(),
				[]descpb.ColumnID{1, 2, 3, 4},
			); err != nil {
				t.Fatal(err)
			}
			splitter := span.MakeSplitter(c.desc, c.desc.GetPrimaryIndex(), util.MakeFastIntSet(0, 1, 2, 3))

			spec := execinfrapb.JoinReaderSpec{
				FetchSpec:      fetchSpec,
				SplitFamilyIDs: splitter.FamilyIDs(),
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
				s.Stopper(),
				s.DistSenderI().(*kvcoord.DistSender),
			)
		})
	}
}

type JRBenchConfig struct {
	rightSz                int
	lookupExprs            []bool
	ordering               []bool
	numLookupRows          []int
	memoryLimits           []int64
	hideLookupExprFromName bool // omit /lookupExpr from benchmark name, see below
}

// BenchmarkJoinReader runs a comprehensive combination of lookup join scenarios
// and can take a long time.   See BenchmarkJoinReaderShortExprs and
// BenchmarkJoinReaderShortCols below for quicker versions.
func BenchmarkJoinReader(b *testing.B) {
	skip.UnderShort(b)
	config := JRBenchConfig{
		rightSz:       1 << 19, /* 524,288 rows */
		lookupExprs:   []bool{false},
		ordering:      []bool{false, true},
		numLookupRows: []int{1, 1 << 4 /* 16 */, 1 << 8 /* 256 */, 1 << 10 /* 1024 */, 1 << 12 /* 4096 */, 1 << 13 /* 8192 */, 1 << 14 /* 16384 */, 1 << 15 /* 32768 */, 1 << 16 /* 65,536 */, 1 << 19 /* 524,288 */},
		memoryLimits:  []int64{1000 << 10, math.MaxInt64},
	}
	benchmarkJoinReader(b, config)
}

// benchmarkJoinReader benchmarks different lookup join match ratios against a
// table with a configuratable number of rows. A match ratio specifies how many
// rows are returned for a single lookup row. Some cases will cause the join
// reader to spill to disk, in which case the benchmark logs that the join
// spilled.
//
// The input table is a 1 column row source where each value is the row number
// (0 based). This is joined using a number of different rows per lookup on a
// each column of the table. For example:
//
// input: 0,1,2,3,4 (size of input is 'numLookupRows')
// table: one | four | sixteen |
//          0 |    0 |       0
//          1 |    0 |       0
//          2 |    0 |       0
//          3 |    0 |       0
//          4 |    1 |       0
//          5 |    1 |       0
//  ...
// SELECT one FROM input INNER LOOKUP JOIN t64 ON i = one;
//    -> 0,1,2,3,4
// SELECT four FROM input INNER LOOKUP JOIN t64 ON i = four;
//    -> 0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3
func benchmarkJoinReader(b *testing.B, bc JRBenchConfig) {

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
			sqlutils.ToRowFn(genValueFns...))
	}

	createRightSideTable(bc.rightSz)
	// Create a new txn after the table has been created.
	flowCtx.Txn = kv.NewTxn(ctx, s.DB(), s.NodeID())
	for _, reqOrdering := range bc.ordering {
		for columnIdx, columnDef := range rightSideColumnDefs {
			for _, numLookupRows := range bc.numLookupRows {
				for _, memoryLimit := range bc.memoryLimits {
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
					if bc.rightSz/columnDef.matchesPerLookupRow < numLookupRows {
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
						for _, lookupExpr := range bc.lookupExprs {
							benchmarkName := fmt.Sprintf("reqOrdering=%t/matchratio=oneto%s/lookuprows=%d/%s",
								reqOrdering, columnDef.name, numLookupRows, memoryLimitStr)
							if parallel {
								benchmarkName += "/parallel=true"
							}
							if lookupExpr && !bc.hideLookupExprFromName {
								benchmarkName += "/lookupexpr=true"
							}
							b.Run(benchmarkName, func(b *testing.B) {
								tableName := tableSizeToName(bc.rightSz)

								// Get the table descriptor and find the index that will provide us with
								// the expected match ratio.
								tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)
								foundIndex := catalog.FindPublicNonPrimaryIndex(tableDesc, func(idx catalog.Index) bool {
									require.Equal(b, 1, idx.NumKeyColumns(), "all indexes created in this benchmark should only contain one column")
									return idx.GetKeyColumnName(0) == columnDef.name
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

								var fetchSpec descpb.IndexFetchSpec
								if err := rowenc.InitIndexFetchSpec(
									&fetchSpec,
									keys.SystemSQLCodec,
									tableDesc, tableDesc.ActiveIndexes()[indexIdx],
									[]descpb.ColumnID{descpb.ColumnID(columnIdx + 1)},
								); err != nil {
									b.Fatal(err)
								}

								spec := execinfrapb.JoinReaderSpec{
									FetchSpec:           fetchSpec,
									LookupColumnsAreKey: parallel,
									MaintainOrdering:    reqOrdering,
								}
								if lookupExpr {
									// @1 is the column in the input, @2 is the only fetched column.
									spec.LookupExpr = execinfrapb.Expression{Expr: "@1 = @2"}
								} else {
									// This is always zero because the input has one column
									spec.LookupColumns = []uint32{0}
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
									jr, err := newJoinReader(
										&flowCtx, 0 /* processorID */, &spec, input, &execinfrapb.PostProcessSpec{}, &output, lookupJoinReaderType,
									)
									if err != nil {
										b.Fatal(err)
									}
									jr.Run(ctx)
									if !spilled && jr.(*joinReader).Spilled() {
										spilled = true
									}

									meta := output.bufferedMeta
									// Expect at least one metadata payload with Metrics set.
									if len(meta) == 0 {
										b.Fatal("expected metadata but none was found")
									}
									if meta[0].Metrics == nil {
										b.Fatalf("expected metadata to contain Metrics but it did not. Err: %v", meta[0].Err)
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
}

var jrBenchConfigShort = JRBenchConfig{
	rightSz:       1 << 16, /* 65,536 rows */
	lookupExprs:   []bool{false},
	ordering:      []bool{false},
	numLookupRows: []int{1, 1 << 4 /* 16 */, 1 << 7 /* 128 */, 1 << 10 /* 1024 */, 1 << 13 /* 4096 */},
	memoryLimits:  []int64{math.MaxInt64},
}

// BenchmarkJoinReaderShortCols runs the short config above, it takes about 1.5m
// where the BenchmarkJoinReader can take over 10m.
func BenchmarkJoinReaderShortCols(b *testing.B) {
	benchmarkJoinReader(b, jrBenchConfigShort)
}

// BenchmarkJoinReaderShortExprs is identical to BenchmarkJoinReaderShortCols
// but it uses LookupExprs and sets hideLookupExprFromName to allow A/B
// comparisons of LookupExprs vs LookupColumns. The idea is to run
// BenchmarkJoinReaderShortCols then run BenchmarkJoinReaderShortExprs and
// compare to see what if any overhead LookupExprs imposes. See:
// https://github.com/cockroachdb/cockroach/pull/66726#issuecomment-866433635
func BenchmarkJoinReaderShortExprs(b *testing.B) {
	config := jrBenchConfigShort
	config.lookupExprs = []bool{true}
	config.hideLookupExprFromName = true
	benchmarkJoinReader(b, config)
}

// BenchmarkJoinReaderLookupStress is a variation on the join reader benchmark
// that tests successively larger lookupExprs to get an idea what the cost is.
func BenchmarkJoinReaderLookupStress(b *testing.B) {

	// parameters
	numCols := 65
	tableSize := 1024

	// this isn't configurable, the dataset is chosen to only ever return 1 row
	numLookupRows := 1

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

	tableSizeToName := func(sz int) string {
		return fmt.Sprintf("t%d", sz)
	}

	createRightSideTable := func(sz, numCols int) {
		colDefs := make([]string, 0, numCols)
		genValueFns := make([]sqlutils.GenValueFn, 0, numCols)
		indexCreateString := "INDEX ("

		for i := 0; i < numCols; i++ {
			colName := fmt.Sprintf("c%d", i)
			colDefs = append(colDefs, colName+" INT")
			genValueFns = append(genValueFns, func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row - 1))
			})
			indexCreateString += colName
			if i+1 < numCols {
				indexCreateString += ","
			} else {
				indexCreateString += ")"
			}
		}
		tableName := tableSizeToName(sz)
		indexDefs := []string{indexCreateString}
		sqlutils.CreateTable(
			b, sqlDB, tableName, strings.Join(append(colDefs, indexDefs...), ", "), sz,
			sqlutils.ToRowFn(genValueFns...),
		)
	}

	createRightSideTable(tableSize, numCols)
	// Create a new txn after the table has been created.
	flowCtx.Txn = kv.NewTxn(ctx, s.DB(), s.NodeID())
	for numExprs := 1; numExprs < numCols; numExprs = numExprs << 1 {
		benchmarkName := fmt.Sprintf("exprs=%d", numExprs)

		b.Run(benchmarkName, func(b *testing.B) {
			tableName := tableSizeToName(tableSize)

			// Get the table descriptor and find the index that will provide us with
			// the expected match ratio.
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", tableName)
			foundIndex := catalog.FindPublicNonPrimaryIndex(tableDesc, func(idx catalog.Index) bool {
				return idx.NumKeyColumns() == numCols
			})
			if foundIndex == nil {
				b.Fatalf("failed to find secondary index!")
			}
			indexIdx := uint32(foundIndex.Ordinal())
			input := newRowGeneratingSource(types.OneIntCol, sqlutils.ToRowFn(func(rowIdx int) tree.Datum {
				// Convert to 0-based.
				return tree.NewDInt(tree.DInt(rowIdx - 1))
			}), numLookupRows)
			output := rowDisposer{}

			var fetchColumnIDs []descpb.ColumnID
			for i := 1; i <= numCols; i++ {
				fetchColumnIDs = append(fetchColumnIDs, descpb.ColumnID(i))
			}

			var fetchSpec descpb.IndexFetchSpec
			if err := rowenc.InitIndexFetchSpec(
				&fetchSpec,
				keys.SystemSQLCodec,
				tableDesc, tableDesc.ActiveIndexes()[indexIdx],
				fetchColumnIDs,
			); err != nil {
				b.Fatal(err)
			}
			spec := execinfrapb.JoinReaderSpec{
				FetchSpec: fetchSpec,
				LookupColumnsAreKey:/*parallel=*/ true,
				MaintainOrdering:/*reqOrdering=*/ false,
			}
			lookupExprString := "@1 = @2"
			for i := 0; i < numExprs; i++ {
				lookupExprString += fmt.Sprintf(" AND @%d = 0", 2+i+1)
			}
			spec.LookupExpr = execinfrapb.Expression{Expr: lookupExprString}

			// Post specifies that only the columns contained in the secondary index
			// need to be output.
			post := execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{uint32(1)},
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				jr, err := newJoinReader(&flowCtx, 0 /* processorID */, &spec, input, &post, &output, lookupJoinReaderType)
				if err != nil {
					b.Fatal(err)
				}
				jr.Run(ctx)

				if output.NumRowsDisposed() != numLookupRows {
					b.Fatalf("got %d output rows, expected %d", output.NumRowsDisposed(), numLookupRows)
				}
				output.ResetNumRowsDisposed()
				input.Reset()
			}
		})
	}
}
