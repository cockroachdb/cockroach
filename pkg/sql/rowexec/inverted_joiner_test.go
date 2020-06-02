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

type arrayIntersectionExpr struct{}

func (arrayIntersectionExpr) Convert(
	datum sqlbase.EncDatum,
) (*invertedexpr.SpanExpressionProto, error) {
	d := int64(*(datum.Datum.(*tree.DInt)))
	d1Span := invertedexpr.MakeSingleInvertedValSpan(intToEncodedInvertedVal(d / 10))
	d2Span := invertedexpr.MakeSingleInvertedValSpan(intToEncodedInvertedVal(d % 10))
	expr := invertedexpr.And(invertedexpr.ExprForInvertedSpan(d1Span, true),
		invertedexpr.ExprForInvertedSpan(d2Span, true))
	return expr.(*invertedexpr.SpanExpression).ToProto(), nil
}

type jsonIntersectionExpr struct{}

func (jsonIntersectionExpr) Convert(
	datum sqlbase.EncDatum,
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
	expr := invertedexpr.And(invertedexpr.ExprForInvertedSpan(d1Span, true),
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
		99,
		sqlutils.ToRowFn(aFn, bFn, cFn))
	const biIndex = 1
	const ciIndex = 2

	td := sqlbase.GetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	type testCase struct {
		description string
		indexIdx    uint32
		post        execinfrapb.PostProcessSpec
		onExpr      string
		input       [][]tree.Datum
		lookupCol   uint32
		datumToExpr DatumToInvertedExpr
		joinType    sqlbase.JoinType
		inputTypes  []*types.T
		outputTypes []*types.T
		expected    string
	}
	testCases := []testCase{
		{
			description: "array intersection",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.TwoIntCols,
			// 5/10=0 and 5%10=5. So the inverted join is looking for an array
			// containing {0, 5}, which happens for both row 5, and row 50 (row 50
			// has 50/10=5 and 50%10=0. Similar reason for 20.
			expected: "[[5 5] [5 50] [20 2] [20 20] [42 24] [42 42]]",
		},
		{
			description: "array intersection and onExpr",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.TwoIntCols,
			// Similar to "array intersection" but omitted some due to onExpr.
			expected: "[[5 50] [42 24] [42 42]]",
		},
		{
			description: "array intersection and onExpr and LeftOuterJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftOuterJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.TwoIntCols,
			// Similar to previous, but the left input failing the onExpr is emitted.
			expected: "[[5 50] [20 NULL] [42 24] [42 42]]",
		},
		{
			description: "array intersection and onExpr and LeftSemiJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftSemiJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[[5] [42]]",
		},
		{
			description: "array intersection and onExpr and LeftAntiJoin",
			indexIdx:    biIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0},
			},
			onExpr: "@2 > 20",
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: arrayIntersectionExpr{},
			joinType:    sqlbase.LeftAntiJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.OneIntCol,
			expected:    "[[20]]",
		},
		{
			description: "json intersection",
			indexIdx:    ciIndex,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
			input: [][]tree.Datum{
				{tree.NewDInt(tree.DInt(5))}, {tree.NewDInt(tree.DInt(20))}, {tree.NewDInt(tree.DInt(42))},
			},
			lookupCol:   0,
			datumToExpr: jsonIntersectionExpr{},
			joinType:    sqlbase.InnerJoin,
			inputTypes:  sqlbase.OneIntCol,
			outputTypes: sqlbase.TwoIntCols,
			expected:    "[[5 5] [20 20] [42 42]]",
		},
	}
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, storage.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	for _, c := range testCases {
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
