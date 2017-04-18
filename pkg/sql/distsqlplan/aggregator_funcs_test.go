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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlplan

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// runTestFlow runs a flow with the given processors and returns the results.
// Any errors stop the current test.
func runTestFlow(
	t *testing.T, srv serverutils.TestServerInterface, procs ...distsqlrun.ProcessorSpec,
) sqlbase.EncDatumRows {
	distSQLSrv := srv.DistSQLServer().(*distsqlrun.ServerImpl)

	req := distsqlrun.SetupFlowRequest{
		Version: distsqlrun.Version,
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
	flow.Start(ctx, func() {})
	flow.Wait()
	flow.Cleanup(ctx)

	if !rowBuf.ProducerClosed {
		t.Errorf("output not closed")
	}

	var res sqlbase.EncDatumRows
	for {
		row, meta := rowBuf.Next()
		if !meta.Empty() {
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

	// First run a flow that aggregates all the rows without any local stages.

	rowsNonDist := runTestFlow(
		t, srv,
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
				Aggregations: []distsqlrun.AggregatorSpec_Aggregation{{Func: fn, ColIdx: 0}},
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
	if len(info.FinalStage) != numIntermediary {
		t.Fatalf("local and final stages have different lengths: %#v", info)
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
		localAggregations[i] = distsqlrun.AggregatorSpec_Aggregation{Func: fn, ColIdx: 0}
	}
	finalAggregations := make([]distsqlrun.AggregatorSpec_Aggregation, numIntermediary)
	for i, fn := range info.FinalStage {
		// Each local aggregation feeds into a final aggregation.
		finalAggregations[i] = distsqlrun.AggregatorSpec_Aggregation{Func: fn, ColIdx: uint32(i)}
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
		h := MakeTypeIndexedVarHelper(intermediaryTypes)
		expr, err := info.FinalRendering(&h, 0 /* varIdxOffset */)
		if err != nil {
			t.Fatal(err)
		}
		finalProc.Post.RenderExprs = []distsqlrun.Expression{MakeExpression(expr, nil)}
	}

	procs = append(procs, finalProc)
	rowsDist := runTestFlow(t, srv, procs...)

	if len(rowsDist[0]) != len(rowsNonDist[0]) {
		t.Errorf("different row lengths (dist: %d non-dist: %d)", len(rowsDist[0]), len(rowsNonDist[0]))
	} else {
		for i := range rowsDist[0] {
			tDist := rowsDist[0][i].Type.String()
			tNonDist := rowsNonDist[0][i].Type.String()
			if tDist != tNonDist {
				t.Errorf("different type for column %d (dist: %s non-dist: %s)", i, tDist, tNonDist)
			}
		}
	}
	if rowsDist.String() != rowsNonDist.String() {
		t.Errorf("different results\nw/o local stage:   %s\nwith local stage:  %s", rowsNonDist, rowsDist)
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
		"k INT PRIMARY KEY, int1 INT, int2 INT, bool1 BOOL, bool2 BOOL, dec1 DECIMAL, dec2 DECIMAL",
		numRows,
		func(row int) []parser.Datum {
			return []parser.Datum{
				parser.NewDInt(parser.DInt(row)),
				parser.NewDInt(parser.DInt(rng.Intn(numRows))),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT}, true),
				parser.MakeDBool(parser.DBool(rng.Intn(10) == 0)),
				parser.MakeDBool(parser.DBool(rng.Intn(10) != 0)),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{Kind: sqlbase.ColumnType_DECIMAL}, false),
				sqlbase.RandDatum(rng, sqlbase.ColumnType{Kind: sqlbase.ColumnType_DECIMAL}, true),
			}
		},
	)

	kvDB := tc.Server(0).KVClient().(*client.DB)
	desc := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	for fn, info := range DistAggregationTable {
		if info.LocalStage[0] == distsqlrun.AggregatorSpec_IDENT &&
			info.FinalStage[0] == distsqlrun.AggregatorSpec_IDENT {
			// IDENT only works as expected if all rows have the same value on the
			// relevant column; skip testing this trivial case.
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
