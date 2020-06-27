// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physicalplan

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// diffCtx is a decimal context used to perform subtractions between
	// local and non-local decimal results to check if they are within
	// 1ulp. Decimals within 1ulp is acceptable for high-precision
	// decimal calculations.
	diffCtx = tree.DecimalCtx.WithPrecision(0)
	// Use to check for 1ulp.
	bigOne = big.NewInt(1)
	// floatPrecFmt is the format string with a precision of 3 (after
	// decimal point) specified for float comparisons. Float aggregation
	// operations involve unavoidable off-by-last-few-digits errors, which
	// is expected.
	floatPrecFmt = "%.3f"
)

// runTestFlow runs a flow with the given processors and returns the results.
// Any errors stop the current test.
func runTestFlow(
	t *testing.T,
	srv serverutils.TestServerInterface,
	txn *kv.Txn,
	procs ...execinfrapb.ProcessorSpec,
) sqlbase.EncDatumRows {
	distSQLSrv := srv.DistSQLServer().(*distsql.ServerImpl)

	leafInputState := txn.GetLeafTxnInputState(context.Background())
	req := execinfrapb.SetupFlowRequest{
		Version:           execinfra.Version,
		LeafTxnInputState: &leafInputState,
		Flow: execinfrapb.FlowSpec{
			FlowID:     execinfrapb.FlowID{UUID: uuid.MakeV4()},
			Processors: procs,
		},
	}

	var rowBuf distsqlutils.RowBuffer

	ctx, flow, err := distSQLSrv.SetupSyncFlow(context.Background(), distSQLSrv.ParentMemoryMonitor, &req, &rowBuf)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.Start(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	flow.Wait()
	flow.Cleanup(ctx)

	if !rowBuf.ProducerClosed() {
		t.Errorf("output not closed")
	}

	var res sqlbase.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if meta != nil {
			if meta.LeafTxnFinalState != nil || meta.Metrics != nil {
				continue
			}
			t.Fatalf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		res = append(res, row)
	}

	return res
}

// checkDistAggregationInfo tests that a flow with multiple local stages and a
// final stage (in accordance with per DistAggregationInfo) gets the same result
// with a naive aggregation flow that has a single non-distributed stage.
//
// Both types of flows are set up and ran against the first numRows of the given
// table. We assume the table's first column is the primary key, with values
// from 1 to numRows. A non-PK column that works with the function is chosen.
func checkDistAggregationInfo(
	ctx context.Context,
	t *testing.T,
	srv serverutils.TestServerInterface,
	tableDesc *sqlbase.TableDescriptor,
	colIdx int,
	numRows int,
	fn execinfrapb.AggregatorSpec_Func,
	info DistAggregationInfo,
) {
	colType := tableDesc.Columns[colIdx].Type

	makeTableReader := func(startPK, endPK int, streamID int) execinfrapb.ProcessorSpec {
		tr := execinfrapb.TableReaderSpec{
			Table: *tableDesc,
			Spans: make([]execinfrapb.TableReaderSpan, 1),
		}

		var err error
		tr.Spans[0].Span.Key, err = sqlbase.TestingMakePrimaryIndexKey(tableDesc, startPK)
		if err != nil {
			t.Fatal(err)
		}
		tr.Spans[0].Span.EndKey, err = sqlbase.TestingMakePrimaryIndexKey(tableDesc, endPK)
		if err != nil {
			t.Fatal(err)
		}

		return execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{TableReader: &tr},
			Post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{uint32(colIdx)},
			},
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{
					{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: execinfrapb.StreamID(streamID)},
				},
			}},
		}
	}

	txn := kv.NewTxn(ctx, srv.DB(), srv.NodeID())

	// First run a flow that aggregates all the rows without any local stages.

	rowsNonDist := runTestFlow(
		t, srv, txn,
		makeTableReader(1, numRows+1, 0),
		execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				Type:        execinfrapb.InputSyncSpec_UNORDERED,
				ColumnTypes: []*types.T{colType},
				Streams: []execinfrapb.StreamEndpointSpec{
					{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: 0},
				},
			}},
			Core: execinfrapb.ProcessorCoreUnion{Aggregator: &execinfrapb.AggregatorSpec{
				Aggregations: []execinfrapb.AggregatorSpec_Aggregation{{Func: fn, ColIdx: []uint32{0}}},
			}},
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{
					{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE},
				},
			}},
		},
	)

	numIntermediary := len(info.LocalStage)
	numFinal := len(info.FinalStage)
	for _, finalInfo := range info.FinalStage {
		if len(finalInfo.LocalIdxs) == 0 {
			t.Fatalf("final stage must specify input local indices: %#v", info)
		}
		for _, localIdx := range finalInfo.LocalIdxs {
			if localIdx >= uint32(numIntermediary) {
				t.Fatalf("local index %d out of bounds of local stages: %#v", localIdx, info)
			}
		}
	}

	// Now run a flow with 4 separate table readers, each with its own local
	// stage, all feeding into a single final stage.

	numParallel := 4

	// The type(s) outputted by the local stage can be different than the input type
	// (e.g. DECIMAL instead of INT).
	intermediaryTypes := make([]*types.T, numIntermediary)
	for i, fn := range info.LocalStage {
		var err error
		_, returnTyp, err := execinfrapb.GetAggregateInfo(fn, colType)
		if err != nil {
			t.Fatal(err)
		}
		intermediaryTypes[i] = returnTyp
	}

	localAggregations := make([]execinfrapb.AggregatorSpec_Aggregation, numIntermediary)
	for i, fn := range info.LocalStage {
		// Local aggregations have the same input.
		localAggregations[i] = execinfrapb.AggregatorSpec_Aggregation{Func: fn, ColIdx: []uint32{0}}
	}
	finalAggregations := make([]execinfrapb.AggregatorSpec_Aggregation, numFinal)
	for i, finalInfo := range info.FinalStage {
		// Each local aggregation feeds into a final aggregation.
		finalAggregations[i] = execinfrapb.AggregatorSpec_Aggregation{
			Func:   finalInfo.Fn,
			ColIdx: finalInfo.LocalIdxs,
		}
	}

	if numParallel < numRows {
		numParallel = numRows
	}
	finalProc := execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{
			Type:        execinfrapb.InputSyncSpec_UNORDERED,
			ColumnTypes: intermediaryTypes,
		}},
		Core: execinfrapb.ProcessorCoreUnion{Aggregator: &execinfrapb.AggregatorSpec{
			Aggregations: finalAggregations,
		}},
		Output: []execinfrapb.OutputRouterSpec{{
			Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			Streams: []execinfrapb.StreamEndpointSpec{
				{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE},
			},
		}},
	}

	// The type(s) outputted by the final stage can be different than the
	// input type (e.g. DECIMAL instead of INT).
	finalOutputTypes := make([]*types.T, numFinal)
	// Passed into FinalIndexing as the indices for the IndexedVars inputs
	// to the post processor.
	varIdxs := make([]int, numFinal)
	for i, finalInfo := range info.FinalStage {
		inputTypes := make([]*types.T, len(finalInfo.LocalIdxs))
		for i, localIdx := range finalInfo.LocalIdxs {
			inputTypes[i] = intermediaryTypes[localIdx]
		}
		var err error
		_, finalOutputTypes[i], err = execinfrapb.GetAggregateInfo(finalInfo.Fn, inputTypes...)
		if err != nil {
			t.Fatal(err)
		}
		varIdxs[i] = i
	}

	var procs []execinfrapb.ProcessorSpec
	for i := 0; i < numParallel; i++ {
		tr := makeTableReader(1+i*numRows/numParallel, 1+(i+1)*numRows/numParallel, 2*i)
		agg := execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				Type:        execinfrapb.InputSyncSpec_UNORDERED,
				ColumnTypes: []*types.T{colType},
				Streams: []execinfrapb.StreamEndpointSpec{
					{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: execinfrapb.StreamID(2 * i)},
				},
			}},
			Core: execinfrapb.ProcessorCoreUnion{Aggregator: &execinfrapb.AggregatorSpec{
				Aggregations: localAggregations,
			}},
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{
					{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: execinfrapb.StreamID(2*i + 1)},
				},
			}},
		}
		procs = append(procs, tr, agg)
		finalProc.Input[0].Streams = append(finalProc.Input[0].Streams, execinfrapb.StreamEndpointSpec{
			Type:     execinfrapb.StreamEndpointSpec_LOCAL,
			StreamID: execinfrapb.StreamID(2*i + 1),
		})
	}

	if info.FinalRendering != nil {
		h := tree.MakeTypesOnlyIndexedVarHelper(finalOutputTypes)
		renderExpr, err := info.FinalRendering(&h, varIdxs)
		if err != nil {
			t.Fatal(err)
		}
		var expr execinfrapb.Expression
		expr, err = MakeExpression(renderExpr, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		finalProc.Post.RenderExprs = []execinfrapb.Expression{expr}

	}

	procs = append(procs, finalProc)
	rowsDist := runTestFlow(t, srv, txn, procs...)

	if len(rowsDist[0]) != len(rowsNonDist[0]) {
		t.Errorf("different row lengths (dist: %d non-dist: %d)", len(rowsDist[0]), len(rowsNonDist[0]))
	} else {
		for i := range rowsDist[0] {
			rowDist := rowsDist[0][i]
			rowNonDist := rowsNonDist[0][i]
			if rowDist.Datum.ResolvedType().Family() != rowNonDist.Datum.ResolvedType().Family() {
				t.Fatalf("different type for column %d (dist: %s non-dist: %s)", i, rowDist.Datum.ResolvedType(), rowNonDist.Datum.ResolvedType())
			}

			var equiv bool
			var strDist, strNonDist string
			switch typedDist := rowDist.Datum.(type) {
			case *tree.DDecimal:
				// For some decimal operations, non-local and
				// local computations may differ by the last
				// digit (by 1 ulp).
				decDist := &typedDist.Decimal
				decNonDist := &rowNonDist.Datum.(*tree.DDecimal).Decimal
				strDist = decDist.String()
				strNonDist = decNonDist.String()
				// We first check if they're equivalent, and if
				// not, we check if they're within 1ulp.
				equiv = decDist.Cmp(decNonDist) == 0
				if !equiv {
					if _, err := diffCtx.Sub(decNonDist, decNonDist, decDist); err != nil {
						t.Fatal(err)
					}
					equiv = decNonDist.Coeff.Cmp(bigOne) == 0
				}
			case *tree.DFloat:
				// Float results are highly variable and loss
				// of precision between non-local and local is
				// expected. We reduce the precision specified
				// by floatPrecFmt and compare their string
				// representations.
				floatDist := float64(*typedDist)
				floatNonDist := float64(*rowNonDist.Datum.(*tree.DFloat))
				strDist = fmt.Sprintf(floatPrecFmt, floatDist)
				strNonDist = fmt.Sprintf(floatPrecFmt, floatNonDist)
				// Compare using a relative equality
				// func that isn't dependent on the scale
				// of the number. In addition, ignore any
				// NaNs. Sometimes due to the non-deterministic
				// ordering of distsql, we get a +Inf and
				// Nan result. Both of these changes started
				// happening with the float rand datums
				// were taught about some more adversarial
				// inputs. Since floats by nature have equality
				// problems and I think our algorithms are
				// correct, we need to be slightly more lenient
				// in our float comparisons.
				equiv = almostEqualRelative(floatDist, floatNonDist) || math.IsNaN(floatNonDist) || math.IsNaN(floatDist)
			default:
				// For all other types, a simple string
				// representation comparison will suffice.
				strDist = rowDist.Datum.String()
				strNonDist = rowNonDist.Datum.String()
				equiv = strDist == strNonDist
			}
			if !equiv {
				t.Errorf("different results for column %d\nw/o local stage:   %s\nwith local stage:  %s", i, strDist, strNonDist)
			}
		}
	}
}

// almostEqualRelative returns whether a and b are close-enough to equal. It
// checks if the two numbers are within a certain relative percentage of
// each other (maxRelDiff), which avoids problems when using "%.3f" as a
// comparison string. This is the "Relative epsilon comparisons" method from:
// https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
func almostEqualRelative(a, b float64) bool {
	if a == b {
		return true
	}
	// Calculate the difference.
	diff := math.Abs(a - b)
	A := math.Abs(a)
	B := math.Abs(b)
	// Find the largest
	largest := A
	if B > A {
		largest = B
	}
	const maxRelDiff = 1e-10
	return diff <= largest*maxRelDiff
}

// Test that distributing agg functions according to DistAggregationTable
// yields correct results. We're going to run each aggregation as either the
// two-stage process described by the DistAggregationTable or as a single global
// process, and verify that the results are the same.
func TestDistAggregationTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numRows = 100

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	// Create a table with a few columns:
	//  - random integer values from 0 to numRows
	//  - random integer values (with some NULLs)
	//  - random bool value (mostly false)
	//  - random bool value (mostly true)
	//  - random decimals
	//  - random decimals (with some NULLs)
	rng, _ := randutil.NewPseudoRand()
	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"k INT PRIMARY KEY, int1 INT, int2 INT, bool1 BOOL, bool2 BOOL, dec1 DECIMAL, dec2 DECIMAL, float1 FLOAT, float2 FLOAT, b BYTES",
		numRows,
		func(row int) []tree.Datum {
			return []tree.Datum{
				tree.NewDInt(tree.DInt(row)),
				tree.NewDInt(tree.DInt(rng.Intn(numRows))),
				sqlbase.RandDatum(rng, types.Int, true),
				tree.MakeDBool(tree.DBool(rng.Intn(10) == 0)),
				tree.MakeDBool(tree.DBool(rng.Intn(10) != 0)),
				sqlbase.RandDatum(rng, types.Decimal, false),
				sqlbase.RandDatum(rng, types.Decimal, true),
				sqlbase.RandDatum(rng, types.Float, false),
				sqlbase.RandDatum(rng, types.Float, true),
				tree.NewDBytes(tree.DBytes(randutil.RandBytes(rng, 10))),
			}
		},
	)

	kvDB := tc.Server(0).DB()
	desc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	for fn, info := range DistAggregationTable {
		if fn == execinfrapb.AggregatorSpec_ANY_NOT_NULL {
			// ANY_NOT_NULL only has a definite result if all rows have the same value
			// on the relevant column; skip testing this trivial case.
			continue
		}
		if fn == execinfrapb.AggregatorSpec_COUNT_ROWS {
			// COUNT_ROWS takes no arguments; skip it in this test.
			continue
		}
		// We're going to test each aggregation function on every column that can be
		// used as input for it.
		foundCol := false
		for colIdx := 1; colIdx < len(desc.Columns); colIdx++ {
			// See if this column works with this function.
			_, _, err := execinfrapb.GetAggregateInfo(fn, desc.Columns[colIdx].Type)
			if err != nil {
				continue
			}
			foundCol = true
			for _, numRows := range []int{5, numRows / 10, numRows / 2, numRows} {
				name := fmt.Sprintf("%s/%s/%d", fn, desc.Columns[colIdx].Name, numRows)
				t.Run(name, func(t *testing.T) {
					checkDistAggregationInfo(
						context.Background(), t, tc.Server(0), desc, colIdx, numRows, fn, info)
				})
			}
		}
		if !foundCol {
			t.Errorf("aggregation function %s was not tested (no suitable column)", fn)
		}
	}
}
