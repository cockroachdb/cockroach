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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
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
	pspec *distsqlpb.ProcessorSpec,
) error {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		return err
	}
	defer tempEngine.Close()

	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	diskMonitor := makeTestDiskMonitor(ctx, st)
	defer diskMonitor.Stop(ctx)
	flowCtx := &FlowCtx{
		EvalCtx:     &evalCtx,
		Settings:    cluster.MakeTestingClusterSettings(),
		TempStorage: tempEngine,
		diskMonitor: diskMonitor,
	}

	inputsProc := make([]RowSource, len(inputs))
	inputsColOp := make([]RowSource, len(inputs))
	for i, input := range inputs {
		inputsProc[i] = NewRepeatableRowSource(inputTypes[i], input)
		inputsColOp[i] = NewRepeatableRowSource(inputTypes[i], input)
	}

	proc, err := newProcessor(ctx, flowCtx, 0, &pspec.Core, &pspec.Post, inputsProc, []RowReceiver{nil}, nil)
	if err != nil {
		return err
	}
	outProc, ok := proc.(RowSource)
	if !ok {
		return errors.New("processor is unexpectedly not a RowSource")
	}

	columnarizers := make([]exec.Operator, len(inputs))
	for i, input := range inputsColOp {
		c, err := newColumnarizer(ctx, flowCtx, int32(i)+1, input)
		if err != nil {
			return err
		}
		columnarizers[i] = c
	}

	result, err := newColOperator(ctx, flowCtx, pspec, columnarizers)
	if err != nil {
		return err
	}

	outputToInputColIdx := make([]int, len(outputTypes))
	for i := range outputTypes {
		outputToInputColIdx[i] = i
	}
	outColOp, err := newMaterializer(
		flowCtx,
		int32(len(inputs))+2,
		result.op,
		outputTypes,
		outputToInputColIdx,
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* outputStatsToTrace */
	)
	if err != nil {
		return err
	}

	outProc.Start(ctx)
	outColOp.Start(ctx)
	defer outProc.ConsumerClosed()
	defer outColOp.ConsumerClosed()

	var procRows, colOpRows sqlbase.EncDatumRows
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

		if anyOrder {
			// We accumulate all the rows to be matched using set comparison when
			// both "producers" are done.
			procRows = append(procRows, rowProc.Copy())
			colOpRows = append(colOpRows, rowColOp.Copy())
		} else {
			// anyOrder is false, so the result rows must match in the same order.
			expStr := rowProc.String(outputTypes)
			retStr := rowColOp.String(outputTypes)
			if expStr != retStr {
				return errors.Errorf("different results on row %d;\nexpected:\n   %s\ngot:\n   %s", rowCount, expStr, retStr)
			}
		}
		rowCount++
	}

	if anyOrder {
		used := make([]bool, len(colOpRows))
		for i, procRow := range procRows {
			rowMatched := false
			for j, colOpRow := range colOpRows {
				if used[j] {
					continue
				}
				expStr := procRow.String(outputTypes)
				retStr := colOpRow.String(outputTypes)
				if expStr == retStr {
					rowMatched = true
					used[j] = true
					break
				}
			}
			if !rowMatched {
				return errors.Errorf("different results: no match found for row %d of processor output\n"+
					"processor output:\n		%s\ncolumnar operator output:\n		%s", i, procRows.String(outputTypes), colOpRows.String(outputTypes))
			}
		}
		// Note: we do not check whether used is all true here because procRows and
		// colOpRows, at this point, must have equal number of rows - if it weren't
		// true, an error would have been returned that either of the "producers"
		// outputted a row while the other one didn't.
	}
	return nil
}
