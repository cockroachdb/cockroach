// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/pkg/errors"
)

// verifyColOperator passes inputs through both the processor defined by pspec
// and the corresponding columnar operator and verifies that the results match.
//
// anyOrder determines whether the results should be matched in order (when
// anyOrder is false) or as sets (when anyOrder is true).
func verifyColOperator(
	anyOrder bool,
	inputTypes [][]types.T,
	inputs []sqlbase.EncDatumRows,
	outputTypes []types.T,
	pspec *execinfrapb.ProcessorSpec,
) error {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(engine.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		return err
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			TempStorage: tempEngine,
			DiskMonitor: diskMonitor,
		},
	}

	inputsProc := make([]execinfra.RowSource, len(inputs))
	inputsColOp := make([]execinfra.RowSource, len(inputs))
	for i, input := range inputs {
		inputsProc[i] = execinfra.NewRepeatableRowSource(inputTypes[i], input)
		inputsColOp[i] = execinfra.NewRepeatableRowSource(inputTypes[i], input)
	}

	proc, err := rowexec.NewProcessor(ctx, flowCtx, 0, &pspec.Core, &pspec.Post, inputsProc, []execinfra.RowReceiver{nil}, nil)
	if err != nil {
		return err
	}
	outProc, ok := proc.(execinfra.RowSource)
	if !ok {
		return errors.New("processor is unexpectedly not a RowSource")
	}

	acc := evalCtx.Mon.MakeBoundAccount()
	defer acc.Close(ctx)
	testAllocator := colexec.NewAllocator(ctx, &acc)
	columnarizers := make([]colexec.Operator, len(inputs))
	for i, input := range inputsColOp {
		c, err := colexec.NewColumnarizer(ctx, testAllocator, flowCtx, int32(i)+1, input)
		if err != nil {
			return err
		}
		columnarizers[i] = c
	}

	result, err := colexec.NewColOperator(
		ctx, flowCtx, pspec, columnarizers, &acc,
		true, /* useStreamingMemAccountForBuffering */
	)
	if err != nil {
		return err
	}
	if result.BufferingOpMemMonitor != nil {
		defer result.BufferingOpMemMonitor.Stop(ctx)
		defer result.BufferingOpMemAccount.Close(ctx)
	}

	outColOp, err := colexec.NewMaterializer(
		flowCtx,
		int32(len(inputs))+2,
		result.Op,
		outputTypes,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		return err
	}

	outProc.Start(ctx)
	outColOp.Start(ctx)
	defer outProc.ConsumerClosed()
	defer outColOp.ConsumerClosed()

	var procRows, colOpRows []string
	rowCount := 0
	for {
		rowProc, meta := outProc.Next()
		if meta != nil {
			return errors.Errorf("unexpected meta %+v from processor", meta)
		}
		rowColOp, meta := outColOp.Next()
		if meta != nil {
			return errors.Errorf("unexpected meta %+v from columnar operator", meta)
		}

		if rowProc != nil && rowColOp == nil {
			return errors.Errorf("different results: processor produced a row %s while columnar operator is done", rowProc.String(outputTypes))
		}
		if rowColOp != nil && rowProc == nil {
			return errors.Errorf("different results: columnar operator produced a row %s while processor is done", rowColOp.String(outputTypes))
		}
		if rowProc == nil && rowColOp == nil {
			break
		}

		expStr := rowProc.String(outputTypes)
		retStr := rowColOp.String(outputTypes)
		if anyOrder {
			// We accumulate all the rows to be matched using set comparison when
			// both "producers" are done.
			procRows = append(procRows, expStr)
			colOpRows = append(colOpRows, retStr)
		} else {
			// anyOrder is false, so the result rows must match in the same order.
			if expStr != retStr {
				return errors.Errorf("different results on row %d;\nexpected:\n   %s\ngot:\n   %s", rowCount, expStr, retStr)
			}
		}
		rowCount++
	}

	if anyOrder {
		used := make([]bool, len(colOpRows))
		for i, expStr := range procRows {
			rowMatched := false
			for j, retStr := range colOpRows {
				if used[j] {
					continue
				}
				if expStr == retStr {
					rowMatched = true
					used[j] = true
					break
				}
			}
			if !rowMatched {
				return errors.Errorf("different results: no match found for row %d of processor output\n"+
					"processor output:\n		%v\ncolumnar operator output:\n		%v", i, procRows, colOpRows)
			}
		}
		// Note: we do not check whether used is all true here because procRows and
		// colOpRows, at this point, must have equal number of rows - if it weren't
		// true, an error would have been returned that either of the "producers"
		// outputted a row while the other one didn't.
	}
	return nil
}
