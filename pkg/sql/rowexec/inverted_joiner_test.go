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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
// column on the right side. But the DatumsToInvertedExpr hook allows us to
// convert any left side value into a SpanExpression for the join, so we
// utilize that to simplify the test. The SpanExpressions are defined below.
const invertedJoinerNumRows = 99

// For each integer d provided by the left side, this expression converter
// constructs an intersection of d/10 and d%10. As mentioned earlier, the
// array inverted index is position agnostic. So say d = 5, which means we are
// looking for rows on the right side that have both 5/10 and 5%10 as array
// elements. This set, {0, 5} is satisfied by two right side rows: the row
// index 5 (obviously), since the test used the same transformation to
// generate the array for each row. And this is also the set for row index
// 50, since 50%10 = 0, 50/10 = 5.
type arrayIntersectionExpr struct {
	t         *testing.T
	toExclude *struct {
		left, right int64
	}
}

var _ invertedexpr.DatumsToInvertedExpr = &arrayIntersectionExpr{}

func decodeInvertedValToInt(b []byte) int64 {
	b, val, err := encoding.DecodeVarintAscending(b)
	if err != nil {
		panic(err)
	}
	if len(b) > 0 {
		panic("leftover bytes")
	}
	return val
}

func (a arrayIntersectionExpr) Convert(
	_ context.Context, datums rowenc.EncDatumRow,
) (*inverted.SpanExpressionProto, interface{}, error) {
	d := int64(*(datums[0].Datum.(*tree.DInt)))
	d1Span := inverted.MakeSingleValSpan(intToEncodedInvertedVal(d / 10))
	d2Span := inverted.MakeSingleValSpan(intToEncodedInvertedVal(d % 10))
	// The tightness only affects the optimizer, so arbitrarily use true.
	expr := inverted.And(inverted.ExprForSpan(d1Span, true),
		inverted.ExprForSpan(d2Span, true))
	return expr.(*inverted.SpanExpression).ToProto(), d, nil
}

func (a arrayIntersectionExpr) CanPreFilter() bool {
	return a.toExclude != nil
}

func (a arrayIntersectionExpr) PreFilter(
	enc inverted.EncVal, preFilters []interface{}, result []bool,
) (bool, error) {
	require.True(a.t, a.CanPreFilter())
	right := decodeInvertedValToInt(enc)
	rv := false
	for i := range preFilters {
		left := preFilters[i].(int64)
		result[i] = !(a.toExclude != nil && a.toExclude.left == left && a.toExclude.right == right)
		rv = rv || result[i]
	}
	return rv, nil
}

// This expression converter is similar to the arrayIntersectionExpr, but for
// JSON. Since the keys "c1" and "c2" distinguish the application of the / and
// % operator here (unlike the array case), a left side integer d will only
// match a right side row with row index d.
type jsonIntersectionExpr struct{}

var _ invertedexpr.DatumsToInvertedExpr = &jsonIntersectionExpr{}

func (jsonIntersectionExpr) Convert(
	_ context.Context, datums rowenc.EncDatumRow,
) (*inverted.SpanExpressionProto, interface{}, error) {
	d := int64(*(datums[0].Datum.(*tree.DInt)))
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
		panic(errors.AssertionFailedf("unexpected length: %d", len(keys)))
	}
	d1Span := inverted.MakeSingleValSpan(keys[0])
	d2Span := inverted.MakeSingleValSpan(keys[1])
	// The tightness only affects the optimizer, so arbitrarily use false.
	expr := inverted.And(inverted.ExprForSpan(d1Span, false),
		inverted.ExprForSpan(d2Span, false))
	return expr.(*inverted.SpanExpression).ToProto(), nil, nil
}

func (jsonIntersectionExpr) CanPreFilter() bool {
	return false
}
func (jsonIntersectionExpr) PreFilter(_ inverted.EncVal, _ []interface{}, _ []bool) (bool, error) {
	return false, errors.Errorf("unsupported")
}

// For each integer d provided by the left side, this expression converter
// constructs a union of {"c1": d/10} and {"c2": d%10}. So if d = 5, we will
// find all right side rows containing {"c1": 0} or {"c2": 5}, which is
// {1..9, 15, 25, 35, ..., 95}.
type jsonUnionExpr struct{}

var _ invertedexpr.DatumsToInvertedExpr = &jsonUnionExpr{}

func (jsonUnionExpr) Convert(
	_ context.Context, datums rowenc.EncDatumRow,
) (*inverted.SpanExpressionProto, interface{}, error) {
	d := int64(*(datums[0].Datum.(*tree.DInt)))
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
		panic(errors.AssertionFailedf("unexpected length: %d", len(keys)))
	}
	d1Span := inverted.MakeSingleValSpan(keys[0])
	d2Span := inverted.MakeSingleValSpan(keys[1])
	// The tightness only affects the optimizer, so arbitrarily use true.
	expr := inverted.Or(inverted.ExprForSpan(d1Span, true),
		inverted.ExprForSpan(d2Span, true))
	return expr.(*inverted.SpanExpression).ToProto(), nil, nil
}

func (jsonUnionExpr) CanPreFilter() bool {
	return false
}
func (jsonUnionExpr) PreFilter(_ inverted.EncVal, _ []interface{}, _ []bool) (bool, error) {
	return false, errors.Errorf("unsupported")
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
	dFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row * 10))
	}
	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		`
			a INT,
			b INT ARRAY,
			c JSONB,
			d INT,
			PRIMARY KEY (a),
			INVERTED INDEX bi (b),
			INVERTED INDEX ci(c),
			INVERTED INDEX dbi(d, b),
			INVERTED INDEX dci(d, c),
			INVERTED INDEX daci(d, a, c)
		`,
		invertedJoinerNumRows,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn))
	const biIndex = 1
	const ciIndex = 2
	const dbiIndex = 3
	const dciIndex = 4
	const daciIndex = 5

	td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	type testCase struct {
		description           string
		indexIdx              uint32
		post                  execinfrapb.PostProcessSpec
		onExpr                string
		input                 [][]tree.Datum
		prefixEqualityColumns []uint32
		datumsToExpr          invertedexpr.DatumsToInvertedExpr
		joinType              descpb.JoinType
		inputTypes            []*types.T
		// The output types for the case without continuation. The test adds the
		// bool type for the case with continuation.
		outputTypes []*types.T
		// The output columns for the case without continuation. The test adds
		// the bool column index for the case with continuation.
		outputColumns []uint32
		// Without and with continuation output.
		expected                 string
		expectedWithContinuation string
	}
	// The current test cases don't use the full diversity of possibilities,
	// so can share initialization of some fields.
	initCommonFields := func(c testCase) testCase {
		c.post.Projection = true
		return c
	}
	testCases := []testCase{
		{
			description: "array intersection",
			indexIdx:    biIndex,
			// As discussed in the arrayIntersectionExpr comment, 5 will match rows 5 and 50.
			// Input 20 will match any rows that have array elements {2, 0}, which is rows
			// 2 and 20. Input 42 will match any rows that have array elements {4, 2} which
			// are rows 24 and 42.
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes:               types.OneIntCol,
			datumsToExpr:             arrayIntersectionExpr{t: t},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.TwoIntCols,
			outputColumns:            []uint32{0, 1},
			expected:                 "[[5 5] [5 50] [20 2] [20 20] [42 24] [42 42]]",
			expectedWithContinuation: "[[5 5 false] [5 50 true] [20 2 false] [20 20 true] [42 24 false] [42 42 true]]",
		},
		{
			description: "array intersection with pre-filter",
			indexIdx:    biIndex,
			// Similar to above, but with a pre-filter that prevents a left row 5
			// from matching right rows with inverted key 0. Note that a right row 0
			// is also used for the left row 20, due to 20 % 10 = 0, but that won't
			// be excluded. This causes left row 5 to not match anything.
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes: types.OneIntCol,
			datumsToExpr: arrayIntersectionExpr{
				t: t, toExclude: &struct{ left, right int64 }{left: 5, right: 0},
			},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.TwoIntCols,
			outputColumns:            []uint32{0, 1},
			expected:                 "[[20 2] [20 20] [42 24] [42 42]]",
			expectedWithContinuation: "[[20 2 false] [20 20 true] [42 24 false] [42 42 true]]",
		},
		{
			// This case is similar to the "array intersection" case, and uses the
			// same input, but additionally filters to only include right rows with
			// index > 20. So the output is a subset. Note that the input 20 has its
			// joined rows completely eliminated. We use the same OnExpr for the
			// LeftOuterJoin, LeftSemiJoin and LeftAntiJoin cases below.
			description: "array intersection and onExpr",
			indexIdx:    biIndex,
			onExpr:      "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes:               types.OneIntCol,
			datumsToExpr:             arrayIntersectionExpr{t: t},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.TwoIntCols,
			outputColumns:            []uint32{0, 1},
			expected:                 "[[5 50] [42 24] [42 42]]",
			expectedWithContinuation: "[[5 50 false] [42 24 false] [42 42 true]]",
		},
		{
			// Same as previous except that the join is a LeftOuterJoin. So the
			// input 20, which was completely filtered out, now reappears with a
			// NULL for the right side.
			description: "array intersection and onExpr and LeftOuterJoin",
			indexIdx:    biIndex,
			onExpr:      "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  arrayIntersectionExpr{t: t},
			joinType:      descpb.LeftOuterJoin,
			outputTypes:   types.TwoIntCols,
			outputColumns: []uint32{0, 1},
			// Similar to previous, but the left side input failing the onExpr is emitted.
			expected:                 "[[5 50] [20 NULL] [42 24] [42 42]]",
			expectedWithContinuation: "[[5 50 false] [20 NULL false] [42 24 false] [42 42 true]]",
		},
		{
			// Same as previous, except a LeftSemiJoin. So input 20 is absent from
			// the output.
			description: "array intersection and onExpr and LeftSemiJoin",
			indexIdx:    biIndex,
			onExpr:      "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  arrayIntersectionExpr{t: t},
			joinType:      descpb.LeftSemiJoin,
			outputTypes:   types.OneIntCol,
			outputColumns: []uint32{0},
			expected:      "[[5] [42]]",
		},
		{
			// Same as previous, except a LeftAntiJoin. So only row 20 from the
			// input, which does not match, is output.
			description: "array intersection and onExpr and LeftAntiJoin",
			indexIdx:    biIndex,
			onExpr:      "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  arrayIntersectionExpr{t: t},
			joinType:      descpb.LeftAntiJoin,
			outputTypes:   types.OneIntCol,
			outputColumns: []uint32{0},
			expected:      "[[20]]",
		},
		{
			// JSON intersection. The left side and right side rows have the same
			// index as described in the comment in jsonIntersectionExpr. There is
			// no row 101 in the right so 101 is not output.
			description: "json intersection",
			indexIdx:    ciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			inputTypes:               types.OneIntCol,
			datumsToExpr:             jsonIntersectionExpr{},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.TwoIntCols,
			outputColumns:            []uint32{0, 1},
			expected:                 "[[5 5] [42 42] [20 20]]",
			expectedWithContinuation: "[[5 5 false] [42 42 false] [20 20 false]]",
		},
		{
			// JSON union. See the comment in jsonUnionExpr that describes what
			// is matched by each input row. 101 is not a row on the right, so
			// it cannot match 100..109, but it will match all rows which have
			// d % 10 == 101 % 10.
			description: "json union",
			indexIdx:    ciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  jsonUnionExpr{},
			joinType:      descpb.InnerJoin,
			outputTypes:   types.TwoIntCols,
			outputColumns: []uint32{0, 1},
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
			expectedWithContinuation: "[[5 1 false] [5 2 true] [5 3 true] [5 4 true] [5 5 true] [5 6 true] [5 7 true] " +
				"[5 8 true] [5 9 true] [5 45 true] [5 15 true] [5 25 true] [5 35 true] [5 55 true] " +
				"[5 65 true] [5 75 true] [5 85 true] [5 95 true] " +
				"[42 2 false] [42 40 true] [42 41 true] [42 42 true] [42 43 true] [42 44 true] " +
				"[42 45 true] [42 46 true] [42 47 true] [42 48 true] [42 49 true] [42 12 true] " +
				"[42 22 true] [42 32 true] [42 52 true] [42 62 true] [42 72 true] [42 82 true] " +
				"[42 92 true] " +
				"[101 21 false] [101 1 true] [101 11 true] [101 31 true] [101 41 true] [101 51 true] " +
				"[101 61 true] [101 71 true] [101 81 true] [101 91 true] " +
				"[20 20 false] [20 21 true] [20 22 true] [20 23 true] [20 24 true] [20 25 true] " +
				"[20 26 true] [20 27 true] [20 28 true] [20 29 true] [20 10 true] [20 30 true] " +
				"[20 40 true] [20 50 true] [20 60 true] [20 70 true] [20 80 true] [20 90 true]]",
		},
		{
			// JSON union with LeftSemiJoin. Everything on the left side is output.
			description: "json union and LeftSemiJoin",
			indexIdx:    ciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  jsonUnionExpr{},
			joinType:      descpb.LeftSemiJoin,
			outputTypes:   types.OneIntCol,
			outputColumns: []uint32{0},
			expected:      "[[5] [42] [101] [20]]",
		},
		{
			// JSON union with LeftAntiJoin. There is no output.
			description: "json union with LeftAntiJoin",
			indexIdx:    ciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))},
				{tree.NewDInt(tree.DInt(42))},
				{tree.NewDInt(tree.DInt(101))},
				{tree.NewDInt(tree.DInt(20))},
			},
			inputTypes:    types.OneIntCol,
			datumsToExpr:  jsonUnionExpr{},
			joinType:      descpb.LeftAntiJoin,
			outputTypes:   types.OneIntCol,
			outputColumns: []uint32{0},
			expected:      "[]",
		},
		{
			description: "array intersection two-column index",
			indexIdx:    dbiIndex,
			// (5, 50) will match any rows have have array elements {5} and
			// d=50, which is only row 5. Row 50 satisfies the {5} requirement
			// but not d=50.
			// (20, 0) will match no rows because row 20 has d=200.
			// (42, 420) will match any rows that have array elements {2, 4} and
			// d=420, which is only row 42. Row 24 satisfies the {2, 4}
			// requirement but not d=420.
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(0))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(420))},
			},
			inputTypes:               types.TwoIntCols,
			prefixEqualityColumns:    []uint32{1},
			datumsToExpr:             arrayIntersectionExpr{t: t},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[5 50 5] [42 420 42]]",
			expectedWithContinuation: "[[5 50 5 false] [42 420 42 false]]",
		},
		{
			description: "array intersection with pre-filter two-column index",
			indexIdx:    dbiIndex,
			// Similar to above, but with a pre-filter that prevents a left row
			// 5 from matching right rows with inverted key 0.
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(200))},
				{tree.NewDInt(tree.DInt(21)), tree.NewDInt(tree.DInt(0))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(420))},
			},
			inputTypes:            types.TwoIntCols,
			prefixEqualityColumns: []uint32{1},
			datumsToExpr: arrayIntersectionExpr{
				t: t, toExclude: &struct{ left, right int64 }{left: 5, right: 0},
			},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[20 200 20] [42 420 42]]",
			expectedWithContinuation: "[[20 200 20 false] [42 420 42 false]]",
		},
		{
			// This case is similar to the "array intersection" case, and uses
			// the same input, but additionally filters to only include right
			// rows with index > 20. So the output is a subset. Note that the
			// input 20 has its joined rows completely eliminated.
			description: "array intersection and onExpr two-column index",
			indexIdx:    dbiIndex,
			onExpr:      "@3 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(200))},
				{tree.NewDInt(tree.DInt(21)), tree.NewDInt(tree.DInt(0))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(420))},
			},
			inputTypes:               types.TwoIntCols,
			prefixEqualityColumns:    []uint32{1},
			datumsToExpr:             arrayIntersectionExpr{t: t},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[42 420 42]]",
			expectedWithContinuation: "[[42 420 42 false]]",
		},
		{
			// JSON intersection; two-column index. The left side and right
			// side rows have the same index as described in the comment in
			// jsonIntersectionExpr. There is no row 101 in the right so 101 is
			// not output. There is a row 42 in the right but the value for d is
			// 420, so 42 is not output.
			description: "json intersection two-column index",
			indexIdx:    dciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(0))},
				{tree.NewDInt(tree.DInt(101)), tree.NewDInt(tree.DInt(1010))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(200))},
			},
			inputTypes:               types.TwoIntCols,
			prefixEqualityColumns:    []uint32{1},
			datumsToExpr:             jsonIntersectionExpr{},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[5 50 5] [20 200 20]]",
			expectedWithContinuation: "[[5 50 5 false] [20 200 20 false]]",
		},
		{
			// JSON union; two-column index. See the test with description "json
			// union" for what is matched solely using the jsonUnionExpr. Those
			// matches are pared down using the second columns in the input
			// which are used for prefix equality.
			description: "json union two-column index",
			indexIdx:    dciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(420))},
				{tree.NewDInt(tree.DInt(101)), tree.NewDInt(tree.DInt(1010))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(200))},
			},
			inputTypes:               types.TwoIntCols,
			prefixEqualityColumns:    []uint32{1},
			datumsToExpr:             jsonUnionExpr{},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[5 50 5] [42 420 42] [20 200 20]]",
			expectedWithContinuation: "[[5 50 5 false] [42 420 42 false] [20 200 20 false]]",
		},
		{
			// JSON intersection; three-column index. The left side and right
			// side rows have the same index as described in the comment in
			// jsonIntersectionExpr. There is no row 101 in the right so 101 is
			// not output.
			description: "json intersection three-column index",
			indexIdx:    daciIndex,
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5)), tree.NewDInt(tree.DInt(50))},
				{tree.NewDInt(tree.DInt(42)), tree.NewDInt(tree.DInt(420))},
				{tree.NewDInt(tree.DInt(101)), tree.NewDInt(tree.DInt(1010))},
				{tree.NewDInt(tree.DInt(20)), tree.NewDInt(tree.DInt(200))},
			},
			inputTypes:               types.TwoIntCols,
			prefixEqualityColumns:    []uint32{1, 0},
			datumsToExpr:             jsonIntersectionExpr{},
			joinType:                 descpb.InnerJoin,
			outputTypes:              types.ThreeIntCols,
			outputColumns:            []uint32{0, 1, 2},
			expected:                 "[[5 50 5] [42 420 42] [20 200 20]]",
			expectedWithContinuation: "[[5 50 5 false] [42 420 42 false] [20 200 20 false]]",
		},
	}
	for i, c := range testCases {
		testCases[i] = initCommonFields(c)
	}

	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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
		},
		Txn:         kv.NewTxn(ctx, s.DB(), s.NodeID()),
		DiskMonitor: diskMonitor,
	}
	for _, c := range testCases {
		for _, outputGroupContinuation := range []bool{false, true} {
			if outputGroupContinuation && len(c.expectedWithContinuation) == 0 {
				continue
			}
			t.Run(fmt.Sprintf("%s/cont=%t", c.description, outputGroupContinuation), func(t *testing.T) {
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
				if outputGroupContinuation {
					// Append the continuation column which is the last column.
					// The total number of columns is the number of left columns
					// + 4 columns from the right (a, b, c, d) + 1 continuation
					// column, so the index of the continuation column is the
					// number of left columns + 4.
					continuationColumnIdx := uint32(len(c.inputTypes) + 4)
					post.OutputColumns = append(c.outputColumns, continuationColumnIdx)
				} else {
					post.OutputColumns = c.outputColumns
				}
				ij, err := newInvertedJoiner(
					&flowCtx,
					0, /* processorID */
					&execinfrapb.InvertedJoinerSpec{
						Table:    *td.TableDesc(),
						IndexIdx: c.indexIdx,
						// The invertedJoiner does not look at InvertedExpr since that information
						// is encapsulated in the DatumsToInvertedExpr parameter.
						InvertedExpr:                      execinfrapb.Expression{},
						OnExpr:                            execinfrapb.Expression{Expr: c.onExpr},
						Type:                              c.joinType,
						OutputGroupContinuationForLeftRow: outputGroupContinuation,
						PrefixEqualityColumns:             c.prefixEqualityColumns,
					},
					c.datumsToExpr,
					in,
					&post,
					out,
				)
				require.NoError(t, err)
				// Small batch size to exercise multiple batches.
				ij.(*invertedJoiner).SetBatchSize(2)
				ij.Run(ctx)
				require.True(t, in.Done)
				require.True(t, out.ProducerClosed())

				var result rowenc.EncDatumRows
				for {
					row, meta := out.Next()
					if meta != nil && meta.Metrics == nil {
						t.Fatalf("unexpected metadata %+v", meta)
					}
					if row == nil {
						break
					}
					result = append(result, row)
				}

				expected := c.expected
				outputTypes := c.outputTypes
				if outputGroupContinuation {
					expected = c.expectedWithContinuation
					outputTypes = append(outputTypes, types.Bool)
				}
				require.Equal(t, expected, result.String(outputTypes), c.description)
			})
		}
	}
}

func TestInvertedJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row))
	}
	bFn := func(row int) tree.Datum {
		arr := tree.NewDArray(types.Int)
		arr.Array = tree.Datums{tree.NewDInt(tree.DInt(row / 10)), tree.NewDInt(tree.DInt(row % 10))}
		return arr
	}
	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT ARRAY, INVERTED INDEX bi (b)",
		invertedJoinerNumRows,
		sqlutils.ToRowFn(aFn, bFn))
	const biIndex = 1
	td := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	tracer := tracing.NewTracer()
	ctx, sp := tracing.StartVerboseTrace(context.Background(), tracer, "test flow ctx")
	defer sp.Finish()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
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

	testReaderProcessorDrain(ctx, t, func(out execinfra.RowReceiver) (execinfra.Processor, error) {
		return newInvertedJoiner(
			&flowCtx,
			0, /* processorID */
			&execinfrapb.InvertedJoinerSpec{
				Table:        *td.TableDesc(),
				IndexIdx:     biIndex,
				InvertedExpr: execinfrapb.Expression{},
				Type:         descpb.InnerJoin,
			},
			arrayIntersectionExpr{t: t},
			distsqlutils.NewRowBuffer(types.TwoIntCols, nil /* rows */, distsqlutils.RowBufferArgs{}),
			&execinfrapb.PostProcessSpec{},
			out,
		)
	})
}

// TODO(sumeer): add geospatial test cases
// TODO(sumeer): add geospatial benchmark
// TODO(mgartner): add JSON/ARRAY benchmark for single and multi-column inverted
// indexes.
