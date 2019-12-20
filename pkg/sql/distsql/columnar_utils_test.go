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

	args := colexec.NewColOperatorArgs{
		Spec:                               pspec,
		Inputs:                             columnarizers,
		StreamingMemAccount:                &acc,
		UseStreamingMemAccountForBuffering: true,
		ProcessorConstructor:               rowexec.NewProcessor,
	}
	result, err := colexec.NewColOperator(ctx, flowCtx, args)
	if err != nil {
		return err
	}
	defer func() {
		for _, memAccount := range result.BufferingOpMemAccounts {
			memAccount.Close(ctx)
		}
		for _, memMonitor := range result.BufferingOpMemMonitors {
			memMonitor.Stop(ctx)
		}
	}()

	outColOp, err := colexec.NewMaterializer(
		flowCtx,
		int32(len(inputs))+2,
		result.Op,
		outputTypes,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		result.MetadataSources,
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
	var procMetas, colOpMetas []execinfrapb.ProducerMetadata
	for {
		rowProc, metaProc := outProc.Next()
		if rowProc != nil {
			procRows = append(procRows, rowProc.String(outputTypes))
		}
		if metaProc != nil {
			if metaProc.Err == nil {
				return errors.Errorf("unexpectedly processor returned non-error "+
					"meta\n%+v", metaProc)
			}
			procMetas = append(procMetas, *metaProc)
		}
		rowColOp, metaColOp := outColOp.Next()
		if rowColOp != nil {
			colOpRows = append(colOpRows, rowColOp.String(outputTypes))
		}
		if metaColOp != nil {
			if metaColOp.Err == nil {
				return errors.Errorf("unexpectedly columnar operator returned "+
					"non-error meta\n%+v", metaColOp)
			}
			colOpMetas = append(colOpMetas, *metaColOp)
		}

		if rowProc == nil && metaProc == nil &&
			rowColOp == nil && metaColOp == nil {
			break
		}
	}

	if len(procMetas) != len(colOpMetas) {
		return errors.Errorf("different number of metas returned:\n"+
			"processor returned\n%+v\n\ncolumnar operator returned\n%+v",
			procMetas, colOpMetas)
	}
	// It is possible that a query will hit an error (for example, integer out of
	// range). We then expect that both the processor and the operator returned
	// such error.
	if len(procMetas) > 1 {
		return errors.Errorf("unexpectedly multiple metas returned:\n"+
			"processor returned\n%+v\n\ncolumnar operator returned\n%+v",
			procMetas, colOpMetas)
	} else if len(procMetas) == 1 {
		procErr := procMetas[0].Err.Error()
		colOpErr := colOpMetas[0].Err.Error()
		if procErr != colOpErr {
			return errors.Errorf("different errors returned:\n"+
				"processor return\n%+v\ncolumnar operator returned\n%+v",
				procMetas[0].Err, colOpMetas[0].Err)
		}
		// The errors are the same, so the rows that were returned do not matter.
		return nil
	}

	if len(procRows) != len(colOpRows) {
		return errors.Errorf("different number of rows returned:\n"+
			"processor returned\n%+v\n\ncolumnar operator returned\n%+v\n"+
			"processor metas\n%+v\ncolumnar operator metas\n%+v\n",
			procRows, colOpRows, procMetas, colOpMetas)
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
	} else {
		for i, expStr := range procRows {
			retStr := colOpRows[i]
			// anyOrder is false, so the result rows must match in the same order.
			if expStr != retStr {
				return errors.Errorf(
					"different results on row %d;\nexpected:\n%s\ngot:\n%s",
					i, expStr, retStr,
				)
			}
		}
	}
	return nil
}
