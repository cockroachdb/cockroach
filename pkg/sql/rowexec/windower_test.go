// Copyright 2019 The Cockroach Authors.
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
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

func TestWindowerAccountingForResults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	monitor := mon.NewMonitorWithLimit(
		"test-monitor",
		mon.MemoryResource,
		100000,        /* limit */
		nil,           /* curCount */
		nil,           /* maxHist */
		5000,          /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	evalCtx := tree.MakeTestingEvalContextWithMon(st, monitor)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
		},
		DiskMonitor: diskMonitor,
	}

	post := &execinfrapb.PostProcessSpec{}
	input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(1000, 1))
	aggSpec := execinfrapb.ArrayAgg
	spec := execinfrapb.WindowerSpec{
		PartitionBy: []uint32{},
		WindowFns: []execinfrapb.WindowerSpec_WindowFn{{
			Func:         execinfrapb.WindowerSpec_Func{AggregateFunc: &aggSpec},
			ArgsIdxs:     []uint32{0},
			Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
			OutputColIdx: 1,
			FilterColIdx: tree.NoColumnIdx,
			Frame: &execinfrapb.WindowerSpec_Frame{
				Mode: execinfrapb.WindowerSpec_Frame_ROWS,
				Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
					Start: execinfrapb.WindowerSpec_Frame_Bound{
						BoundType: execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING,
						IntOffset: 100,
					},
				},
			},
		}},
	}
	output := distsqlutils.NewRowBuffer(
		types.OneIntCol, nil, distsqlutils.RowBufferArgs{},
	)

	d, err := newWindower(flowCtx, 0 /* processorID */, &spec, input, post, output)
	if err != nil {
		t.Fatal(err)
	}
	d.Run(ctx)
	for {
		row, meta := output.Next()
		if row != nil {
			t.Fatalf("unexpectedly received row %+v", row)
		}
		if meta == nil {
			t.Fatalf("unexpectedly didn't receive an OOM error")
		}
		if meta.Err != nil {
			if !strings.Contains(meta.Err.Error(), "memory budget exceeded") {
				t.Fatalf("unexpectedly received an error other than OOM")
			}
			break
		}
	}
}

type windowTestSpec struct {
	// The column indices of PARTITION BY clause.
	partitionBy []uint32
	// Window function to be computed.
	windowFn windowFnTestSpec
}

type windowFnTestSpec struct {
	funcName       string
	argsIdxs       []uint32
	columnOrdering colinfo.ColumnOrdering
}

func windows(windowTestSpecs []windowTestSpec) ([]execinfrapb.WindowerSpec, error) {
	windows := make([]execinfrapb.WindowerSpec, len(windowTestSpecs))
	for i, spec := range windowTestSpecs {
		windows[i].PartitionBy = spec.partitionBy
		windows[i].WindowFns = make([]execinfrapb.WindowerSpec_WindowFn, 1)
		windowFnSpec := execinfrapb.WindowerSpec_WindowFn{}
		fnSpec, err := CreateWindowerSpecFunc(spec.windowFn.funcName)
		if err != nil {
			return nil, err
		}
		windowFnSpec.Func = fnSpec
		windowFnSpec.ArgsIdxs = spec.windowFn.argsIdxs
		if spec.windowFn.columnOrdering != nil {
			ordCols := make([]execinfrapb.Ordering_Column, 0, len(spec.windowFn.columnOrdering))
			for _, column := range spec.windowFn.columnOrdering {
				ordCols = append(ordCols, execinfrapb.Ordering_Column{
					ColIdx: uint32(column.ColIdx),
					// We need this -1 because encoding.Direction has extra value "_"
					// as zeroth "entry" which its proto equivalent doesn't have.
					Direction: execinfrapb.Ordering_Column_Direction(column.Direction - 1),
				})
			}
			windowFnSpec.Ordering = execinfrapb.Ordering{Columns: ordCols}
		}
		windowFnSpec.FilterColIdx = tree.NoColumnIdx
		windows[i].WindowFns[0] = windowFnSpec
	}
	return windows, nil
}

func BenchmarkWindower(b *testing.B) {
	defer log.Scope(b).Close(b)
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
				columnOrdering: colinfo.ColumnOrdering{{ColIdx: 2, Direction: encoding.Ascending}},
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
				columnOrdering: colinfo.ColumnOrdering{{ColIdx: 2, Direction: encoding.Ascending}},
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
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: diskMonitor,
	}

	rowsGenerators := []func(int, int) rowenc.EncDatumRows{
		randgen.MakeIntRows,
		func(numRows, numCols int) rowenc.EncDatumRows {
			return randgen.MakeRepeatedIntRows(numRows/100, numRows, numCols)
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
			spec.WindowFns[0].OutputColIdx = 3

			b.Run(runName, func(b *testing.B) {
				post := &execinfrapb.PostProcessSpec{}
				disposer := &rowDisposer{}
				input := execinfra.NewRepeatableRowSource(types.ThreeIntCols, rowsGenerator(numRows, numCols))

				b.SetBytes(int64(8 * numRows * numCols))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					d, err := newWindower(flowCtx, 0 /* processorID */, &spec, input, post, disposer)
					if err != nil {
						b.Fatal(err)
					}
					d.Run(context.Background())
					input.Reset()
				}
				b.StopTimer()
			})
		}
	}
}
