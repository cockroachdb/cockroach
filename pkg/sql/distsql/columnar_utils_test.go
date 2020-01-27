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
	"fmt"
	"math"
	"strconv"

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

type verifyColOperatorArgs struct {
	// anyOrder determines whether the results should be matched in order (when
	// anyOrder is false) or as sets (when anyOrder is true).
	anyOrder bool
	// colsForEqCheck (when non-nil) specifies the column indices that should be
	// used for equality check. If it is nil, then the whole rows are compared.
	colsForEqCheck []uint32
	inputTypes     [][]types.T
	inputs         []sqlbase.EncDatumRows
	outputTypes    []types.T
	pspec          *execinfrapb.ProcessorSpec
	// memoryLimit specifies the desired memory limit on the testing knob (in
	// bytes). If it is 0, then the default limit of 64MiB will be used.
	memoryLimit int64
}

// verifyColOperator passes inputs through both the processor defined by pspec
// and the corresponding columnar operator and verifies that the results match.
func verifyColOperator(args verifyColOperatorArgs) error {
	const floatPrecision = 0.0000001
	if args.colsForEqCheck == nil {
		args.colsForEqCheck = make([]uint32, len(args.outputTypes))
		for i := range args.colsForEqCheck {
			args.colsForEqCheck[i] = uint32(i)
		}
	}

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
	flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = args.memoryLimit

	inputsProc := make([]execinfra.RowSource, len(args.inputs))
	inputsColOp := make([]execinfra.RowSource, len(args.inputs))
	for i, input := range args.inputs {
		inputsProc[i] = execinfra.NewRepeatableRowSource(args.inputTypes[i], input)
		inputsColOp[i] = execinfra.NewRepeatableRowSource(args.inputTypes[i], input)
	}

	proc, err := rowexec.NewProcessor(
		ctx, flowCtx, 0, &args.pspec.Core, &args.pspec.Post,
		inputsProc, []execinfra.RowReceiver{nil}, nil,
	)
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
	columnarizers := make([]colexec.Operator, len(args.inputs))
	for i, input := range inputsColOp {
		c, err := colexec.NewColumnarizer(ctx, testAllocator, flowCtx, int32(i)+1, input)
		if err != nil {
			return err
		}
		columnarizers[i] = c
	}

	constructorArgs := colexec.NewColOperatorArgs{
		Spec:                 args.pspec,
		Inputs:               columnarizers,
		StreamingMemAccount:  &acc,
		ProcessorConstructor: rowexec.NewProcessor,
	}
	var spilled bool
	if args.memoryLimit > 0 {
		constructorArgs.TestingKnobs.SpillingCallbackFn = func() { spilled = true }
	}
	result, err := colexec.NewColOperator(ctx, flowCtx, constructorArgs)
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
		int32(len(args.inputs))+2,
		result.Op,
		args.outputTypes,
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

	printRowForChecking := func(r sqlbase.EncDatumRow) []string {
		res := make([]string, len(args.colsForEqCheck))
		for i, col := range args.colsForEqCheck {
			res[i] = r[col].String(&args.outputTypes[col])
		}
		return res
	}
	var procRows, colOpRows [][]string
	var procMetas, colOpMetas []execinfrapb.ProducerMetadata
	for {
		rowProc, metaProc := outProc.Next()
		if rowProc != nil {
			row := printRowForChecking(rowProc)
			if len(row) != len(args.colsForEqCheck) {
				return errors.Errorf("unexpectedly processor returned a row of"+
					"different length\n%s", row)
			}
			procRows = append(procRows, row)
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
			row := printRowForChecking(rowColOp)
			if len(row) != len(args.colsForEqCheck) {
				return errors.Errorf("unexpectedly columnar operator returned a row of "+
					"different length\n%s", row)
			}
			colOpRows = append(colOpRows, printRowForChecking(rowColOp))
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

	printRowsOutput := func(rows [][]string) string {
		res := ""
		for i, row := range rows {
			res = fmt.Sprintf("%s\n%d: %v", res, i, row)
		}
		return res
	}

	datumsMatch := func(expected, actual string, typ *types.T) (bool, error) {
		switch typ.Family() {
		case types.FloatFamily:
			// Some operations on floats (for example, aggregation) can produce
			// slightly different results in the row-by-row and vectorized engines.
			// That's why we handle them separately.

			// We first try direct string matching. If that succeeds, then great!
			if expected == actual {
				return true, nil
			}
			// If only one of the values is NULL, then the datums do not match.
			if expected == `NULL` || actual == `NULL` {
				return false, nil
			}
			// Now we will try parsing both strings as floats and check whether they
			// are within allowed precision from each other.
			expFloat, err := strconv.ParseFloat(expected, 64)
			if err != nil {
				return false, err
			}
			actualFloat, err := strconv.ParseFloat(actual, 64)
			if err != nil {
				return false, err
			}
			return math.Abs(expFloat-actualFloat) < floatPrecision, nil
		default:
			return expected == actual, nil
		}
	}

	if args.anyOrder {
		used := make([]bool, len(colOpRows))
		for i, expStrRow := range procRows {
			rowMatched := false
			for j, retStrRow := range colOpRows {
				if used[j] {
					continue
				}
				foundDifference := false
				for k, col := range args.colsForEqCheck {
					match, err := datumsMatch(expStrRow[k], retStrRow[k], &args.outputTypes[col])
					if err != nil {
						return errors.Errorf("error while parsing datum in rows\n%v\n%v\n%s",
							expStrRow, retStrRow, err.Error())
					}
					if !match {
						foundDifference = true
						break
					}
				}
				if !foundDifference {
					rowMatched = true
					used[j] = true
					break
				}
			}
			if !rowMatched {
				return errors.Errorf("different results: no match found for row %d of processor output\n"+
					"processor output:%s\n\ncolumnar operator output:%s",
					i, printRowsOutput(procRows), printRowsOutput(colOpRows))
			}
		}
	} else {
		for i, expStrRow := range procRows {
			retStrRow := colOpRows[i]
			// anyOrder is false, so the result rows must match in the same order.
			for k, col := range args.colsForEqCheck {
				match, err := datumsMatch(expStrRow[k], retStrRow[k], &args.outputTypes[col])
				if err != nil {
					return errors.Errorf("error while parsing datum in rows\n%v\n%v\n%s",
						expStrRow, retStrRow, err.Error())
				}
				if !match {
					return errors.Errorf(
						"different results on row %d;\nexpected:\n%s\ngot:\n%s",
						i, expStrRow, retStrRow,
					)
				}
			}
		}
	}

	if args.memoryLimit > 0 {
		// Check that the spilling did occur.
		if !spilled {
			return errors.Errorf("expected spilling to disk but it did *not* occur")
		}
	}
	return nil
}
