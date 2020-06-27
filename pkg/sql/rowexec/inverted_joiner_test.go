// Copyright 2020 The Cockroach Authors.
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
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// The table has 99 rows, where each row is the row index 1..99 (since row
// numbers start from 1 in CreateTable). This row index is the primary key,
// and is followed by an array and a JSON column. Each array has two elements,
// defined using the row index: {row/10, row%10}. Each JSON element has two
// paths: {"c1": row/10, "c2": row%10}. Note that the ordering of the two
// array elements is not utilized in the inverted index, while the unique key
// of each of JSON paths is part of the inverted index.
//
// Join expression: The left side of the join is simply integers. This is not
// realistic since the left side should be the same type as the corresponding
// column on the right side. But the DatumToInvertedExpr hook allows us to
// convert any left side value into a SpanExpression for the join, so we
// utilize that to simplify the test. The SpanExpressions are defined below.
const numRows = 99

// For each integer d provided by the left side, this expression converter
// constructs an intersection of d/10 and d%10. As mentioned earlier, the
// array inverted index is position agnostic. So say d = 5, which means we are
// looking for rows on the right side that have both 5/10 and 5%10 as array
// elements. This set, {0, 5} is satisfied by two right side rows: the row
// index 5 (obviously), since the test used the same transformation to
// generate the array for each row. And this is also the set for row index
// 50, since 50%10 = 0, 50/10 = 5.
type arrayIntersectionExpr struct{}

var _ invertedexpr.DatumToInvertedExpr = &arrayIntersectionExpr{}

func (arrayIntersectionExpr) Convert(
	ctx context.Context, datum sqlbase.EncDatum,
) (*invertedexpr.SpanExpressionProto, error) {
	d := int64(*(datum.Datum.(*tree.DInt)))
	d1Span := invertedexpr.MakeSingleInvertedValSpan(intToEncodedInvertedVal(d / 10))
	d2Span := invertedexpr.MakeSingleInvertedValSpan(intToEncodedInvertedVal(d % 10))
	// The tightness only affects the optimizer, so arbitrarily use true.
	expr := invertedexpr.And(invertedexpr.ExprForInvertedSpan(d1Span, true),
		invertedexpr.ExprForInvertedSpan(d2Span, true))
	return expr.(*invertedexpr.SpanExpression).ToProto(), nil
}

// This expression converter is similar to the arrayIntersectionExpr, but for
// JSON. Since the keys "c1" and "c2" distinguish the application of the / and
// % operator here (unlike the array case), a left side integer d will only
// match a right side row with row index d.
type jsonIntersectionExpr struct{}

var _ invertedexpr.DatumToInvertedExpr = &jsonIntersectionExpr{}

func (jsonIntersectionExpr) Convert(
	ctx context.Context, datum sqlbase.EncDatum,
) (*invertedexpr.SpanExpressionProto, error) {
	d := int64(*(datum.Datum.(*tree.DInt)))
	d1 := d / 10
	d2 := d % 10
	j, err := json.ParseJSON(fmt.Sprintf(`{"c1": %d, "c2": %d}`, d1, d2))
	if err != nil {
		panic(err)
	}
	keys, err := json.EncodeInvertedIndexKeys(nil, j)
	if err != nil {
		panic(err)
	}
	if len(keys) != 2 {
		panic(fmt.Sprintf("unexpected length: %d", len(keys)))
	}
	d1Span := invertedexpr.MakeSingleInvertedValSpan(keys[0])
	d2Span := invertedexpr.MakeSingleInvertedValSpan(keys[1])
	// The tightness only affects the optimizer, so arbitrarily use false.
	expr := invertedexpr.And(invertedexpr.ExprForInvertedSpan(d1Span, false),
		invertedexpr.ExprForInvertedSpan(d2Span, false))
	return expr.(*invertedexpr.SpanExpression).ToProto(), nil
}

// For each integer d provided by the left side, this expression converter
// constructs a union of {"c1": d/10} and {"c2": d%10}. So if d = 5, we will
// find all right side rows containing {"c1": 0} or {"c2": 5}, which is
// {1..9, 15, 25, 35, ..., 95}.
type jsonUnionExpr struct{}

var _ invertedexpr.DatumToInvertedExpr = &jsonUnionExpr{}

func (jsonUnionExpr) Convert(
	ctx context.Context, datum sqlbase.EncDatum,
) (*invertedexpr.SpanExpressionProto, error) {
	d := int64(*(datum.Datum.(*tree.DInt)))
	d1 := d / 10
	d2 := d % 10
	j, err := json.ParseJSON(fmt.Sprintf(`{"c1": %d, "c2": %d}`, d1, d2))
	if err != nil {
		panic(err)
	}
	keys, err := json.EncodeInvertedIndexKeys(nil, j)
	if err != nil {
		panic(err)
	}
	if len(keys) != 2 {
		panic(fmt.Sprintf("unexpected length: %d", len(keys)))
	}
	d1Span := invertedexpr.MakeSingleInvertedValSpan(keys[0])
	d2Span := invertedexpr.MakeSingleInvertedValSpan(keys[1])
	// The tightness only affects the optimizer, so arbitrarily use true.
	expr := invertedexpr.Or(invertedexpr.ExprForInvertedSpan(d1Span, true),
		invertedexpr.ExprForInvertedSpan(d2Span, true))
	return expr.(*invertedexpr.SpanExpression).ToProto(), nil
}

func TestInvertedJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row))
	}
	bFn := func(row int) tree.Datum {
		arr := tree.NewDArray(types.Int)
		arr.Array = tree.Datums{tree.NewDInt(tree.DInt(row / 10)), tree.NewDInt(tree.DInt(row % 10))}
		return arr
	}
	cFn := func(row int) tree.Datum {
		j, err := json.ParseJSON(fmt.Sprintf(`{"c1": %d, "c2": %d}`, row/10, row%10))
		require.NoError(t, err)
		return tree.NewDJSON(j)
	}
	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT ARRAY, c JSONB, PRIMARY KEY (a), INVERTED INDEX bi (b), INVERTED INDEX ci(c)",
		numRows,
		sqlutils.ToRowFn(aFn, bFn, cFn))
	const biIndex = 1
	const ciIndex = 2

	td := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	type testCase struct {
		description string
		indexIdx    uint32
		post        execinfrapb.PostProcessSpec
		onExpr      string
		input       [][]tree.Datum
		lookupCol   uint32
		datumToExpr invertedexpr.DatumToInvertedExpr
		joinType    sqlbase.JoinType
		inputTypes  []*types.T
		outputTypes []*types.T
		expected    string
	}
	// The current test cases don't use the full diversity of possibilities,
	// so can share initialization of some fields.
	initCommonFields := func(c testCase) testCase {
		c.post.Projection = true
		c.lookupCol = 0
		c.inputTypes = sqlbase.OneIntCol
		return c
	}
	testCases := []testCase{
		{
			description: "array intersection",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0, 1},
			},
			// As discussed in the arrayIntersectionExpr comment, 5 will match rows 5 and 50.
			// Input 20 will match any rows that have array elements {2, 0}, which is rows
			// 2 and 20. Input 42 will match any rows that have array elements {4, 2} which
			// are rows 24 and 42.
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[5 5] [5 50] [20 2] [20 20] [42 24] [42 42]]",
		},
		{
			// This case is similar to the "array intersection" case, and uses the
			// same input, but additionally filters to only include right rows with
			// index > 20. So the output is a subset. Note that the input 20 has its
			// joined rows completely eliminated. We use the same OnExpr for the
			// LeftOuterJoin, LeftSemiJoin and LeftAntiJoin cases below.
			description: "array intersection and onExpr",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0, 1},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[5 50] [42 24] [42 42]]",
		},
		{
			// Same as previous except that the join is a LeftOuterJoin. So the
			// input 20, which was completely filtered out, now reappears with a
			// NULL for the right side.
			description: "array intersection and onExpr and LeftOuterJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0, 1},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftOuterJoin,
			outputTypes: sqlbase.TwoIntCols,
			// Similar to previous, but the left side input failing the onExpr is emitted.
			expected: "[[5 50] [20 NULL] [42 24] [42 42]]",
		},
		{
			// Same as previous, except a LeftSemiJoin. So input 20 is absent from
			// the output.
			description: "array intersection and onExpr and LeftSemiJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftSemiJoin,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[[5] [42]]",
		},
		{
			// Same as previous, except a LeftAntiJoin. So only row 20 from the
			// input, which does not match, is output.
			description: "array intersection and onExpr and LeftAntiJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftAntiJoin,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[[20]]",
		},
		{
			// JSON intersection. The left side and right side rows have the same
			// index as described in the comment in jsonIntersectionExpr. There is
			// no row 101 in the right so 101 is not output.
			description: "json intersection",
			indexIdx:    ciIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			datumToExpr: jsonIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[5 5] [42 42] [20 20]]",
		},
		{
			// JSON union. See the comment in jsonUnionExpr that describes what
			// is matched by each input row. 101 is not a row on the right, so
			// it cannot match 100..109, but it will match all rows which have
			// d % 10 == 101 % 10.
			description: "json union",
			indexIdx:    ciIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			datumToExpr: jsonUnionExpr{},
			joinType:    sqlbase.InnerJoin,
			outputTypes: sqlbase.TwoIntCols,
			// The ordering looks odd because of how the JSON values were ordered in the
			// inverted index. The spans we are reading for the first batch of two inputs
			// 5, 42 are (c1, 0), (c1, 4), (c2, 2), (c2, 5). (c1, 0) causes
			// [5 1] [5 2] ... [5 9] to be output first. Since 45 was indexed also under
			// (c1, 4), which is needed by the second input in the batch, it gets read
			// next and is assigned a smaller number in the row container than the rows
			// read for (c2, 5) (which are needed by first input, 5). Which is why
			// [5 45] precedes [5 15] ... (the set operators output the result in the
			// order of the numbering in the row container).
			expected: "[[5 1] [5 2] [5 3] [5 4] [5 5] [5 6] [5 7] [5 8] [5 9] [5 45] [5 15] [5 25] " +
				"[5 35] [5 55] [5 65] [5 75] [5 85] [5 95] [42 2] [42 40] [42 41] [42 42] [42 43] " +
				"[42 44] [42 45] [42 46] [42 47] [42 48] [42 49] [42 12] [42 22] [42 32] [42 52] [42 62] " +
				"[42 72] [42 82] [42 92] [101 21] [101 1] [101 11] [101 31] [101 41] [101 51] [101 61] " +
				"[101 71] [101 81] [101 91] [20 20] [20 21] [20 22] [20 23] [20 24] [20 25] [20 26] " +
				"[20 27] [20 28] [20 29] [20 10] [20 30] [20 40] [20 50] [20 60] [20 70] [20 80] [20 90]]",
		},
		{
			// JSON union with LeftSemiJoin. Everything on the left side is output.
			description: "json union and LeftSemiJoin",
			indexIdx:    ciIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			datumToExpr: jsonUnionExpr{},
			joinType:    sqlbase.LeftSemiJoin,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[[5] [42] [101] [20]]",
		},
		{
			// JSON union with LeftAntiJoin. There is no output.
			description: "json union with LeftAntiJoin",
			indexIdx:    ciIndex,
			post: execinfrapb.PostProcessSpec{
				OutputColumns: []uint32{0},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			datumToExpr: jsonUnionExpr{},
			joinType:    sqlbase.LeftAntiJoin,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[]",
		},
	}
	for i, c := range testCases {
		testCases[i] = initCommonFields(c)
	}

	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
			DiskMonitor: diskMonitor,
		},
		Txn: kv.NewTxn(ctx, s.DB(), s.NodeID()),
	}
	for _, c := range testCases {
		encRows := make(sqlbase.EncDatumRows, len(c.input))
		for rowIdx, row := range c.input {
			encRow := make(sqlbase.EncDatumRow, len(row))
			for i, d := range row {
				encRow[i] = sqlbase.DatumToEncDatum(c.inputTypes[i], d)
			}
			encRows[rowIdx] = encRow
		}
		in := distsqlutils.NewRowBuffer(c.inputTypes, encRows, distsqlutils.RowBufferArgs{})
		out := &distsqlutils.RowBuffer{}
		ij, err := newInvertedJoiner(
			&flowCtx,
			0, /* processorID */
			&execinfrapb.InvertedJoinerSpec{
				Table:        *td,
				IndexIdx:     c.indexIdx,
				LookupColumn: c.lookupCol,
				// The invertedJoiner does not look at InvertedExpr since that information
				// is encapsulated in the DatumToInvertedExpr parameter.
				InvertedExpr: execinfrapb.Expression{},
				OnExpr:       execinfrapb.Expression{Expr: c.onExpr},
				Type:         c.joinType,
			},
			c.datumToExpr,
			in,
			&c.post,
			out,
		)
		require.NoError(t, err)
		// Small batch size to exercise multiple batches.
		ij.(*invertedJoiner).SetBatchSize(2)
		ij.Run(ctx)
		require.True(t, in.Done)
		require.True(t, out.ProducerClosed())

		var result sqlbase.EncDatumRows
		for {
			row := out.NextNoMeta(t)
			if row == nil {
				break
			}
			result = append(result, row)
		}

		require.Equal(t, c.expected, result.String(c.outputTypes), c.description)
	}
}

// TODO(sumeer):
// - add geospatial test cases
// - add benchmark
