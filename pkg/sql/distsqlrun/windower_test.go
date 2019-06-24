// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type windowTestSpec struct {
	// The column indices of PARTITION BY clause.
	partitionBy []uint32
	// Window function to be computed.
	windowFn windowFnTestSpec
}

type windowFnTestSpec struct {
	funcName       string
	argsIdxs       []uint32
	columnOrdering sqlbase.ColumnOrdering
}

func windows(windowTestSpecs []windowTestSpec) ([]distsqlpb.WindowerSpec, error) {
	windows := make([]distsqlpb.WindowerSpec, len(windowTestSpecs))
	for i, spec := range windowTestSpecs {
		windows[i].PartitionBy = spec.partitionBy
		windows[i].WindowFns = make([]distsqlpb.WindowerSpec_WindowFn, 1)
		windowFnSpec := distsqlpb.WindowerSpec_WindowFn{}
		fnSpec, err := CreateWindowerSpecFunc(spec.windowFn.funcName)
		if err != nil {
			return nil, err
		}
		windowFnSpec.Func = fnSpec
		windowFnSpec.ArgsIdxs = spec.windowFn.argsIdxs
		if spec.windowFn.columnOrdering != nil {
			ordCols := make([]distsqlpb.Ordering_Column, 0, len(spec.windowFn.columnOrdering))
			for _, column := range spec.windowFn.columnOrdering {
				ordCols = append(ordCols, distsqlpb.Ordering_Column{
					ColIdx: uint32(column.ColIdx),
					// We need this -1 because encoding.Direction has extra value "_"
					// as zeroth "entry" which its proto equivalent doesn't have.
					Direction: distsqlpb.Ordering_Column_Direction(column.Direction - 1),
				})
			}
			windowFnSpec.Ordering = distsqlpb.Ordering{Columns: ordCols}
		}
		windows[i].WindowFns[0] = windowFnSpec
	}
	return windows, nil
}

func BenchmarkWindower(b *testing.B) {
	const numCols = 3
	const numRows = 100000

	specs, err := windows([]windowTestSpec{
		{ // sum(@1) OVER ()
			partitionBy: []uint32{},
			windowFn: windowFnTestSpec{
				funcName: "SUM",
				argsIdxs: []uint32{0},
			},
		},
		{ // sum(@1) OVER (ORDER BY @3)
			windowFn: windowFnTestSpec{
				funcName:       "SUM",
				argsIdxs:       []uint32{0},
				columnOrdering: sqlbase.ColumnOrdering{{ColIdx: 2, Direction: encoding.Ascending}},
			},
		},
		{ // sum(@1) OVER (PARTITION BY @2)
			partitionBy: []uint32{1},
			windowFn: windowFnTestSpec{
				funcName: "SUM",
				argsIdxs: []uint32{0},
			},
		},
		{ // sum(@1) OVER (PARTITION BY @2 ORDER BY @3)
			partitionBy: []uint32{1},
			windowFn: windowFnTestSpec{
				funcName:       "SUM",
				argsIdxs:       []uint32{0},
				columnOrdering: sqlbase.ColumnOrdering{{ColIdx: 2, Direction: encoding.Ascending}},
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := makeTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	flowCtx := &FlowCtx{
		Settings:    st,
		EvalCtx:     &evalCtx,
		diskMonitor: diskMonitor,
	}

	rowsGenerators := []func(int, int) sqlbase.EncDatumRows{
		sqlbase.MakeIntRows,
		func(numRows, numCols int) sqlbase.EncDatumRows {
			return sqlbase.MakeRepeatedIntRows(numRows/100, numRows, numCols)
		},
	}
	skipRepeatedSpecs := map[int]bool{0: true, 1: true}

	for j, rowsGenerator := range rowsGenerators {
		for i, spec := range specs {
			if skipRepeatedSpecs[i] && j == 1 {
				continue
			}
			runName := fmt.Sprintf("%s() OVER (", spec.WindowFns[0].Func.AggregateFunc.String())
			if len(spec.PartitionBy) > 0 {
				runName = runName + "PARTITION BY"
				if j == 0 {
					runName = runName + "/* SINGLE ROW PARTITIONS */"
				} else {
					runName = runName + "/* MULTIPLE ROWS PARTITIONS */"
				}
			}
			if len(spec.PartitionBy) > 0 && spec.WindowFns[0].Ordering.Columns != nil {
				runName = runName + " "
			}
			if spec.WindowFns[0].Ordering.Columns != nil {
				runName = runName + "ORDER BY"
			}
			runName = runName + ")"

			b.Run(runName, func(b *testing.B) {
				post := &distsqlpb.PostProcessSpec{}
				disposer := &RowDisposer{}
				input := NewRepeatableRowSource(sqlbase.ThreeIntCols, rowsGenerator(numRows, numCols))

				b.SetBytes(int64(8 * numRows * numCols))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					d, err := newWindower(flowCtx, 0 /* processorID */, &spec, input, post, disposer)
					if err != nil {
						b.Fatal(err)
					}
					d.Run(context.TODO())
					input.Reset()
				}
				b.StopTimer()
			})
		}
	}
}
