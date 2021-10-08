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
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

type verifyColOperatorArgs struct {
	// anyOrder determines whether the results should be matched in order (when
	// anyOrder is false) or as sets (when anyOrder is true).
	anyOrder bool
	// colIdxsToCheckForEquality determines which columns of the rows to use
	// for equality check. If left unset, full rows are compared. Use this
	// with caution and leave a comment that justifies using this knob.
	colIdxsToCheckForEquality []int
	inputTypes                [][]*types.T
	inputs                    []rowenc.EncDatumRows
	pspec                     *execinfrapb.ProcessorSpec
	// forceDiskSpill, if set, will force the operator to spill to disk.
	forceDiskSpill bool
	// forcedDiskSpillMightNotOccur determines whether we error out if
	// forceDiskSpill is true but the spilling doesn't occur. Please leave an
	// explanation for why that could be the case.
	forcedDiskSpillMightNotOccur bool
	// numForcedRepartitions specifies a number of "repartitions" that a
	// disk-backed operator should be forced to perform. "Repartition" can mean
	// different things depending on the operator (for example, for hash joiner
	// it is dividing original partition into multiple new partitions; for sorter
	// it is merging already created partitions into new one before proceeding
	// to the next partition from the input).
	numForcedRepartitions int
	// rng (if set) will be used to randomize batch size.
	rng *rand.Rand
}

// verifyColOperator passes inputs through both the processor defined by pspec
// and the corresponding columnar operator and verifies that the results match.
func verifyColOperator(t *testing.T, args verifyColOperatorArgs) error {
	const floatPrecision = 0.0000001
	rng := args.rng
	if rng == nil {
		rng, _ = randutil.NewPseudoRand()
	}
	if rng.Float64() < 0.5 {
		randomBatchSize := 1 + rng.Intn(3)
		if err := coldata.SetBatchSizeForTests(randomBatchSize); err != nil {
			return err
		}
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, tempFS, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
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
		},
		DiskMonitor: diskMonitor,
	}
	flowCtx.Cfg.TestingKnobs.ForceDiskSpill = args.forceDiskSpill

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
	testAllocator := colmem.NewAllocator(ctx, &acc, coldataext.NewExtendedColumnFactory(&evalCtx))
	columnarizers := make([]colexecop.Operator, len(args.inputs))
	for i, input := range inputsColOp {
		columnarizers[i] = colexec.NewBufferingColumnarizer(testAllocator, flowCtx, int32(i)+1, input)
	}

	constructorArgs := &colexecargs.NewColOperatorArgs{
		Spec:                args.pspec,
		Inputs:              colexectestutils.MakeInputs(columnarizers),
		StreamingMemAccount: &acc,
		DiskQueueCfg: colcontainer.DiskQueueCfg{
			FS:        tempFS,
			GetPather: colcontainer.GetPatherFunc(func(context.Context) string { return "" }),
		},
		FDSemaphore: colexecop.NewTestingSemaphore(256),

		// TODO(yuzefovich): adjust expression generator to not produce
		// mixed-type timestamp-related expressions and then disallow the
		// fallback again.
		ProcessorConstructor: rowexec.NewProcessor,
	}
	var spilled bool
	if args.forceDiskSpill {
		constructorArgs.TestingKnobs.SpillingCallbackFn = func() { spilled = true }
	}
	constructorArgs.TestingKnobs.NumForcedRepartitions = args.numForcedRepartitions
	result, err := colbuilder.NewColOperator(ctx, flowCtx, constructorArgs)
	if err != nil {
		return err
	}
	defer func() {
		for _, memAccount := range result.OpAccounts {
			memAccount.Close(ctx)
		}
		for _, memMonitor := range result.OpMonitors {
			memMonitor.Stop(ctx)
		}
	}()

	outColOp := colexec.NewMaterializer(
		flowCtx,
		int32(len(args.inputs))+2,
		result.OpWithMetaInfo,
		args.pspec.ResultTypes,
	)

	outProc.Start(ctx)
	outColOp.Start(ctx)
	defer outProc.ConsumerClosed()
	defer outColOp.ConsumerClosed()

	printRowForChecking := func(r rowenc.EncDatumRow) []string {
		res := make([]string, len(args.pspec.ResultTypes))
		for i, col := range r {
			res[i] = col.String(args.pspec.ResultTypes[i])
		}
		return res
	}
	var procRows, colOpRows [][]string
	var procMetas, colOpMetas []execinfrapb.ProducerMetadata
	for {
		rowProc, metaProc := outProc.Next()
		if rowProc != nil {
			procRows = append(procRows, printRowForChecking(rowProc))
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
				"processor returned\n%+v\ncolumnar operator returned\n%+v",
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

	colIdxsToCheckForEquality := args.colIdxsToCheckForEquality
	if len(colIdxsToCheckForEquality) == 0 {
		colIdxsToCheckForEquality = make([]int, len(args.pspec.ResultTypes))
		for i := range colIdxsToCheckForEquality {
			colIdxsToCheckForEquality[i] = i
		}
	}

	if args.anyOrder {
		// The rows are allowed to be in any order, so in order to use the
		// ordered comparison below we will sort rows from both the processor
		// and the operator lexicographically.
		getLessFn := func(rows [][]string) func(int, int) bool {
			return func(i, j int) bool {
				for _, colIdx := range colIdxsToCheckForEquality {
					if cmp := strings.Compare(rows[i][colIdx], rows[j][colIdx]); cmp != 0 {
						return cmp < 0
					}
				}
				return false
			}
		}
		sort.Slice(procRows, getLessFn(procRows))
		sort.Slice(colOpRows, getLessFn(colOpRows))
	}

	for i, expStrRow := range procRows {
		retStrRow := colOpRows[i]
		for _, colIdx := range colIdxsToCheckForEquality {
			match, err := datumsMatch(expStrRow[colIdx], retStrRow[colIdx], args.pspec.ResultTypes[colIdx])
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

	if args.forceDiskSpill {
		// Check that the spilling did occur.
		if !spilled && !args.forcedDiskSpillMightNotOccur {
			return errors.Errorf("expected spilling to disk but it did *not* occur")
		}
	}
	return nil
}
