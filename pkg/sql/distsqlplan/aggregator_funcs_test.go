// Copyright 2016 The Cockroach Authors.
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

package distsqlplan

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	txn *client.Txn,
	procs ...distsqlrun.ProcessorSpec,
) sqlbase.EncDatumRows {
	distSQLSrv := srv.DistSQLServer().(*distsqlrun.ServerImpl)

	req := distsqlrun.SetupFlowRequest{
		Version: distsqlrun.Version,
		Txn:     txn.Proto(),
		Flow: distsqlrun.FlowSpec{
			FlowID:     distsqlrun.FlowID{UUID: uuid.MakeV4()},
			Processors: procs,
		},
	}

	var rowBuf distsqlrun.RowBuffer

	ctx, flow, err := distSQLSrv.SetupSyncFlow(context.TODO(), &req, &rowBuf)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.Start(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	flow.Wait()
	flow.Cleanup(ctx)

	if !rowBuf.ProducerClosed {
		t.Errorf("output not closed")
	}

	var res sqlbase.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if meta != nil {
			if meta.TxnMeta != nil {
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
	t *testing.T,
	srv serverutils.TestServerInterface,
	tableDesc *sqlbase.TableDescriptor,
	colIdx int,
	numRows int,
	fn distsqlrun.AggregatorSpec_Func,
	info DistAggregationInfo,
) {
	colType := tableDesc.Columns[colIdx].Type

	makeTableReader := func(startPK, endPK int, streamID int) distsqlrun.ProcessorSpec {
		tr := distsqlrun.TableReaderSpec{
			Table: *tableDesc,
			Spans: make([]distsqlrun.TableReaderSpan, 1),
		}

		var err error
		tr.Spans[0].Span.Key, err = sqlbase.MakePrimaryIndexKey(tableDesc, startPK)
		if err != nil {
			t.Fatal(err)
		}
		tr.Spans[0].Span.EndKey, err = sqlbase.MakePrimaryIndexKey(tableDesc, endPK)
		if err != nil {
			t.Fatal(err)
		}

		return distsqlrun.ProcessorSpec{
			Core: distsqlrun.ProcessorCoreUnion{TableReader: &tr},
			Post: distsqlrun.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{uint32(colIdx)},
			},
			Output: []distsqlrun.OutputRouterSpec{{
				Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlrun.StreamEndpointSpec{
					{Type: distsqlrun.StreamEndpointSpec_LOCAL, StreamID: distsqlrun.StreamID(streamID)},
				},
			}},
		}
	}

	txn := client.NewTxn(srv.DB(), srv.NodeID(), client.RootTxn)

	// First run a flow that aggregates all the rows without any local stages.

	rowsNonDist := runTestFlow(
		t, srv, txn,
		makeTableReader(1, numRows+1, 0),
		distsqlrun.ProcessorSpec{
			Input: []distsqlrun.InputSyncSpec{{
				Type:        distsqlrun.InputSyncSpec_UNORDERED,
				ColumnTypes: []sqlbase.ColumnType{colType},
				Streams: []distsqlrun.StreamEndpointSpec{
					{Type: distsqlrun.StreamEndpointSpec_LOCAL, StreamID: 0},
				},
			}},
			Core: distsqlrun.ProcessorCoreUnion{Aggregator: &distsqlrun.AggregatorSpec{
				Aggregations: []distsqlrun.AggregatorSpec_Aggregation{{Func: fn, ColIdx: []uint32{0}}},
			}},
			Output: []distsqlrun.OutputRouterSpec{{
				Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlrun.StreamEndpointSpec{
					{Type: distsqlrun.StreamEndpointSpec_SYNC_RESPONSE},
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
	intermediaryTypes := make([]sqlbase.ColumnType, numIntermediary)
	for i, fn := range info.LocalStage {
		var err error
		_, intermediaryTypes[i], err = distsqlrun.GetAggregateInfo(fn, colType)
		if err != nil {
			t.Fatal(err)
		}
	}

	localAggregations := make([]distsqlrun.AggregatorSpec_Aggregation, numIntermediary)
	for i, fn := range info.LocalStage {
		// Local aggregations have the same input.
		localAggregations[i] = distsqlrun.AggregatorSpec_Aggregation{Func: fn, ColIdx: []uint32{0}}
	}
	finalAggregations := make([]distsqlrun.AggregatorSpec_Aggregation, numFinal)
	for i, finalInfo := range info.FinalStage {
		// Each local aggregation feeds into a final aggregation.
		finalAggregations[i] = distsqlrun.AggregatorSpec_Aggregation{
			Func:   finalInfo.Fn,
			ColIdx: finalInfo.LocalIdxs,
		}
	}

	if numParallel < numRows {
		numParallel = numRows
	}
	finalProc := distsqlrun.ProcessorSpec{
		Input: []distsqlrun.InputSyncSpec{{
			Type:        distsqlrun.InputSyncSpec_UNORDERED,
			ColumnTypes: intermediaryTypes,
		}},
		Core: distsqlrun.ProcessorCoreUnion{Aggregator: &distsqlrun.AggregatorSpec{
			Aggregations: finalAggregations,
		}},
		Output: []distsqlrun.OutputRouterSpec{{
			Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
			Streams: []distsqlrun.StreamEndpointSpec{
				{Type: distsqlrun.StreamEndpointSpec_SYNC_RESPONSE},
			},
		}},
	}

	// The type(s) outputted by the final stage can be different than the
	// input type (e.g. DECIMAL instead of INT).
	finalOutputTypes := make([]sqlbase.ColumnType, numFinal)
	// Passed into FinalIndexing as the indices for the IndexedVars inputs
	// to the post processor.
	varIdxs := make([]int, numFinal)
	for i, finalInfo := range info.FinalStage {
		inputTypes := make([]sqlbase.ColumnType, len(finalInfo.LocalIdxs))
		for i, localIdx := range finalInfo.LocalIdxs {
			inputTypes[i] = intermediaryTypes[localIdx]
		}
		var err error
		_, finalOutputTypes[i], err = distsqlrun.GetAggregateInfo(finalInfo.Fn, inputTypes...)
		if err != nil {
			t.Fatal(err)
		}
		varIdxs[i] = i
	}

	var procs []distsqlrun.ProcessorSpec
	for i := 0; i < numParallel; i++ {
		tr := makeTableReader(1+i*numRows/numParallel, 1+(i+1)*numRows/numParallel, 2*i)
		agg := distsqlrun.ProcessorSpec{
			Input: []distsqlrun.InputSyncSpec{{
				Type:        distsqlrun.InputSyncSpec_UNORDERED,
				ColumnTypes: []sqlbase.ColumnType{colType},
				Streams: []distsqlrun.StreamEndpointSpec{
					{Type: distsqlrun.StreamEndpointSpec_LOCAL, StreamID: distsqlrun.StreamID(2 * i)},
				},
			}},
			Core: distsqlrun.ProcessorCoreUnion{Aggregator: &distsqlrun.AggregatorSpec{
				Aggregations: localAggregations,
			}},
			Output: []distsqlrun.OutputRouterSpec{{
				Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
				Streams: []distsqlrun.StreamEndpointSpec{
					{Type: distsqlrun.StreamEndpointSpec_LOCAL, StreamID: distsqlrun.StreamID(2*i + 1)},
				},
			}},
		}
		procs = append(procs, tr, agg)
		finalProc.Input[0].Streams = append(finalProc.Input[0].Streams, distsqlrun.StreamEndpointSpec{
			Type:     distsqlrun.StreamEndpointSpec_LOCAL,
			StreamID: distsqlrun.StreamID(2*i + 1),
		})
	}

	if info.FinalRendering != nil {
		h := tree.MakeTypesOnlyIndexedVarHelper(sqlbase.ColumnTypesToDatumTypes(finalOutputTypes))
		expr, err := info.FinalRendering(&h, varIdxs)
		if err != nil {
			t.Fatal(err)
		}
		finalProc.Post.RenderExprs = []distsqlrun.Expression{MakeExpression(expr, nil, nil)}
	}

	procs = append(procs, finalProc)
	rowsDist := runTestFlow(t, srv, txn, procs...)

	if len(rowsDist[0]) != len(rowsNonDist[0]) {
		t.Errorf("different row lengths (dist: %d non-dist: %d)", len(rowsDist[0]), len(rowsNonDist[0]))
	} else {
		for i := range rowsDist[0] {
			rowDist := rowsDist[0][i]
			rowNonDist := rowsNonDist[0][i]
			if !rowDist.Datum.ResolvedType().FamilyEqual(rowNonDist.Datum.ResolvedType()) {
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
				// Float results are highly variable and
				// loss of precision between non-local and
				// local is expected. We reduce the precision
				// specified by floatPrecFmt and compare
				// their string representations.
				floatDist := float64(*typedDist)
				floatNonDist := float64(*rowNonDist.Datum.(*tree.DFloat))
				strDist = fmt.Sprintf(floatPrecFmt, floatDist)
				strNonDist = fmt.Sprintf(floatPrecFmt, floatNonDist)
				equiv = strDist == strNonDist
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

// Test that distributing agg functions according to DistAggregationTable
// yields correct results. We're going to run each aggregation as either the
// two-stage process described by the DistAggregationTable or as a single global
// process, and verify that the results are the same.
func TestDistAggregationTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numRows = 100

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

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
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}, true),
				tree.MakeDBool(tree.DBool(rng.Intn(10) == 0)),
				tree.MakeDBool(tree.DBool(rng.Intn(10) != 0)),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}, false),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}, true),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}, false),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}, true),
				tree.NewDBytes(tree.DBytes(randutil.RandBytes(rng, 10))),
			}
		},
	)

	kvDB := tc.Server(0).DB()
	desc := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	for fn, info := range DistAggregationTable {
		if fn == distsqlrun.AggregatorSpec_IDENT {
			// IDENT only works as expected if all rows have the same value on the
			// relevant column; skip testing this trivial case.
			continue
		}
		if fn == distsqlrun.AggregatorSpec_COUNT_ROWS {
			// COUNT_ROWS takes no arguments; skip it in this test.
			continue
		}
		// We're going to test each aggregation function on every column that can be
		// used as input for it.
		foundCol := false
		for colIdx := 1; colIdx < len(desc.Columns); colIdx++ {
			// See if this column works with this function.
			_, _, err := distsqlrun.GetAggregateInfo(fn, desc.Columns[colIdx].Type)
			if err != nil {
				continue
			}
			foundCol = true
			for _, numRows := range []int{5, numRows / 10, numRows / 2, numRows} {
				name := fmt.Sprintf("%s/%s/%d", fn, desc.Columns[colIdx].Name, numRows)
				t.Run(name, func(t *testing.T) {
					checkDistAggregationInfo(t, tc.Server(0), desc, colIdx, numRows, fn, info)
				})
			}
		}
		if !foundCol {
			t.Errorf("aggregation function %s was not tested (no suitable column)", fn)
		}
	}
}
