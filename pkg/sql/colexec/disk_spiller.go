// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// oneInputDiskSpiller is an Operator that manages the fallback from a one
// input in-memory buffering operator to a disk-backed one when the former hits
// the memory limit.
//
// NOTE: if an out of memory error occurs during initialization, this operator
// simply propagates the error further.
//
// The diagram of the components involved is as follows:
//
//        -------------  input  -----------
//       |                ||                | (2nd src)
//       |                ||   (1st src)    ↓
//       |            ----||---> bufferExportingOperator
//       ↓           |    ||                |
//    inMemoryOp ----     ||                ↓
//       |                ||           diskBackedOp
//       |                ||                |
//       |                ↓↓                |
//        ---------> disk spiller <--------
//                        ||
//                        ||
//                        ↓↓
//                      output
//
// Here is the explanation:
// - the main chain of Operators is input -> disk spiller -> output.
// - the disk spiller will first try running everything through the left side
//   chain of input -> inMemoryOp. If that succeeds, great! The disk spiller
//   will simply propagate the batch to the output. If that fails with an OOM
//   error, the disk spiller will then initialize the right side chain and will
//   proceed to emit from there.
// - the right side chain is bufferExportingOperator -> diskBackedOp. The
//   former will first export all the buffered tuples from inMemoryOp and then
//   will proceed on emitting from input.

// NewOneInputDiskSpiller returns a new oneInputDiskSpiller. It takes the
// following arguments:
// - inMemoryOp - the in-memory operator that will be consuming input and doing
//   computations until it either successfully processes the whole input or
//   reaches its memory limit.
// - inMemoryMemMonitorName - the name of the memory monitor of the in-memory
//   operator. diskSpiller will catch an OOM error only if this name is
//   contained within the error message.
// - diskBackedOpConstructor - the function to construct the disk-backed
//   operator when given an input operator. We take in a constructor rather
//   than an already created operator in order to hide the complexity of buffer
//   exporting operator that serves as the input to the disk-backed operator.
// - spillingCallbackFn will be called when the spilling from in-memory to disk
//   backed operator occurs. It should only be set in tests.
func NewOneInputDiskSpiller(
	input colexecbase.Operator,
	inMemoryOp colexecbase.BufferingInMemoryOperator,
	inMemoryMemMonitorName string,
	diskBackedOpConstructor func(input colexecbase.Operator) colexecbase.Operator,
	spillingCallbackFn func(),
) colexecbase.Operator {
	diskBackedOpInput := newBufferExportingOperator(inMemoryOp, input)
	return &diskSpillerBase{
		inputs:                 []colexecbase.Operator{input},
		inMemoryOp:             inMemoryOp,
		inMemoryMemMonitorName: inMemoryMemMonitorName,
		diskBackedOp:           diskBackedOpConstructor(diskBackedOpInput),
		spillingCallbackFn:     spillingCallbackFn,
	}
}

// twoInputDiskSpiller is an Operator that manages the fallback from a two
// input in-memory buffering operator to a disk-backed one when the former hits
// the memory limit.
//
// NOTE: if an out of memory error occurs during initialization, this operator
// simply propagates the error further.
//
// The diagram of the components involved is as follows:
//
//   ----- input1                                                  input2 ----------
// ||     /   |       _____________________________________________|  |             ||
// ||    /    ↓      /                                                |             ||
// ||    |  inMemoryOp  ------------------------------                |             ||
// ||    |  /  |                                      |               |             ||
// ||    | /    ------------------                    |               |             ||
// ||    |/       (2nd src)       ↓ (1st src)         ↓ (1st src)     ↓ (2nd src)   ||
// ||    / ----------> bufferExportingOperator1   bufferExportingOperator2          ||
// ||   /                         |                          |                      ||
// ||   |                         |                          |                      ||
// ||   |                          -----> diskBackedOp <-----                       ||
// ||   |                                    |                                      ||
// ||    ------------------------------      |                                      ||
// ||                                  ↓     ↓                                      ||
//   ---------------------------->   disk spiller   <-------------------------------
//
// Here is the explanation:
// - the main chain of Operators is inputs -> disk spiller -> output.
// - the disk spiller will first try running everything through the left side
//   chain of inputs -> inMemoryOp. If that succeeds, great! The disk spiller
//   will simply propagate the batch to the output. If that fails with an OOM
//   error, the disk spiller will then initialize the right side chain and will
//   proceed to emit from there.
// - the right side chain is bufferExportingOperators -> diskBackedOp. The
//   former will first export all the buffered tuples from inMemoryOp and then
//   will proceed on emitting from input.

// NewTwoInputDiskSpiller returns a new twoInputDiskSpiller. It takes the
// following arguments:
// - inMemoryOp - the in-memory operator that will be consuming inputs and
//   doing computations until it either successfully processes the whole inputs
//   or reaches its memory limit.
// - inMemoryMemMonitorName - the name of the memory monitor of the in-memory
//   operator. diskSpiller will catch an OOM error only if this name is
//   contained within the error message.
// - diskBackedOpConstructor - the function to construct the disk-backed
//   operator when given two input operators. We take in a constructor rather
//   than an already created operator in order to hide the complexity of buffer
//   exporting operators that serves as inputs to the disk-backed operator.
// - spillingCallbackFn will be called when the spilling from in-memory to disk
//   backed operator occurs. It should only be set in tests.
func NewTwoInputDiskSpiller(
	inputOne, inputTwo colexecbase.Operator,
	inMemoryOp colexecbase.BufferingInMemoryOperator,
	inMemoryMemMonitorName string,
	diskBackedOpConstructor func(inputOne, inputTwo colexecbase.Operator) colexecbase.Operator,
	spillingCallbackFn func(),
) colexecbase.Operator {
	diskBackedOpInputOne := newBufferExportingOperator(inMemoryOp, inputOne)
	diskBackedOpInputTwo := newBufferExportingOperator(inMemoryOp, inputTwo)
	return &diskSpillerBase{
		inputs:                 []colexecbase.Operator{inputOne, inputTwo},
		inMemoryOp:             inMemoryOp,
		inMemoryOpInitStatus:   OperatorNotInitialized,
		inMemoryMemMonitorName: inMemoryMemMonitorName,
		diskBackedOp:           diskBackedOpConstructor(diskBackedOpInputOne, diskBackedOpInputTwo),
		distBackedOpInitStatus: OperatorNotInitialized,
		spillingCallbackFn:     spillingCallbackFn,
	}
}

// diskSpillerBase is the common base for the one-input and two-input disk
// spillers.
type diskSpillerBase struct {
	NonExplainable

	closerHelper

	inputs  []colexecbase.Operator
	spilled bool

	inMemoryOp             colexecbase.BufferingInMemoryOperator
	inMemoryOpInitStatus   OperatorInitStatus
	inMemoryMemMonitorName string
	diskBackedOp           colexecbase.Operator
	distBackedOpInitStatus OperatorInitStatus
	spillingCallbackFn     func()
}

var _ ResettableOperator = &diskSpillerBase{}

func (d *diskSpillerBase) Init() {
	if d.inMemoryOpInitStatus == OperatorInitialized {
		return
	}
	// It is possible that Init() call below will hit an out of memory error,
	// but we decide to bail on this query, so we do not catch internal panics.
	//
	// Also note that d.input is the input to d.inMemoryOp, so calling Init()
	// only on the latter is sufficient.
	d.inMemoryOp.Init()
	d.inMemoryOpInitStatus = OperatorInitialized
}

func (d *diskSpillerBase) Next(ctx context.Context) coldata.Batch {
	if d.spilled {
		return d.diskBackedOp.Next(ctx)
	}
	var batch coldata.Batch
	if err := colexecerror.CatchVectorizedRuntimeError(
		func() {
			batch = d.inMemoryOp.Next(ctx)
		},
	); err != nil {
		if sqlbase.IsOutOfMemoryError(err) &&
			strings.Contains(err.Error(), d.inMemoryMemMonitorName) {
			d.spilled = true
			if d.spillingCallbackFn != nil {
				d.spillingCallbackFn()
			}
			d.diskBackedOp.Init()
			d.distBackedOpInitStatus = OperatorInitialized
			return d.diskBackedOp.Next(ctx)
		}
		// Either not an out of memory error or an OOM error coming from a
		// different operator, so we propagate it further.
		colexecerror.InternalError(err)
	}
	return batch
}

func (d *diskSpillerBase) reset(ctx context.Context) {
	for _, input := range d.inputs {
		if r, ok := input.(resetter); ok {
			r.reset(ctx)
		}
	}
	if d.inMemoryOpInitStatus == OperatorInitialized {
		if r, ok := d.inMemoryOp.(resetter); ok {
			r.reset(ctx)
		}
	}
	if d.distBackedOpInitStatus == OperatorInitialized {
		if r, ok := d.diskBackedOp.(resetter); ok {
			r.reset(ctx)
		}
	}
	d.spilled = false
}

// Close closes the diskSpillerBase's input.
// TODO(asubiotto): Remove this method. It only exists so that we can call Close
//  from some runTests subtests when not draining the input fully. The test
//  should pass in the testing.T object used so that the caller can decide to
//  explicitly close the input after checking the test.
func (d *diskSpillerBase) IdempotentClose(ctx context.Context) error {
	if !d.close() {
		return nil
	}
	if c, ok := d.diskBackedOp.(IdempotentCloser); ok {
		return c.IdempotentClose(ctx)
	}
	return nil
}

func (d *diskSpillerBase) ChildCount(verbose bool) int {
	if verbose {
		return len(d.inputs) + 2
	}
	return 1
}

func (d *diskSpillerBase) Child(nth int, verbose bool) execinfra.OpNode {
	// Note: although the main chain is d.inputs -> diskSpiller -> output (and
	// the main chain should be under nth == 0), in order to make the output of
	// EXPLAIN (VEC) less confusing we return the in-memory operator as being on
	// the main chain.
	if verbose {
		switch nth {
		case 0:
			return d.inMemoryOp
		case len(d.inputs) + 1:
			return d.diskBackedOp
		default:
			return d.inputs[nth-1]
		}
	}
	switch nth {
	case 0:
		return d.inMemoryOp
	default:
		colexecerror.InternalError(fmt.Sprintf("invalid index %d", nth))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

// bufferExportingOperator is an Operator that first returns all batches from
// firstSource, and once firstSource is exhausted, it proceeds on returning all
// batches from the second source.
//
// NOTE: bufferExportingOperator assumes that both sources will have been
// initialized when bufferExportingOperator.Init() is called.
// NOTE: it is assumed that secondSource is the input to firstSource.
type bufferExportingOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable

	firstSource     colexecbase.BufferingInMemoryOperator
	secondSource    colexecbase.Operator
	firstSourceDone bool
}

var _ ResettableOperator = &bufferExportingOperator{}

func newBufferExportingOperator(
	firstSource colexecbase.BufferingInMemoryOperator, secondSource colexecbase.Operator,
) colexecbase.Operator {
	return &bufferExportingOperator{
		firstSource:  firstSource,
		secondSource: secondSource,
	}
}

func (b *bufferExportingOperator) Init() {
	// Init here is a noop because the operator assumes that both sources have
	// already been initialized.
}

func (b *bufferExportingOperator) Next(ctx context.Context) coldata.Batch {
	if b.firstSourceDone {
		return b.secondSource.Next(ctx)
	}
	batch := b.firstSource.ExportBuffered(b.secondSource)
	if batch.Length() == 0 {
		b.firstSourceDone = true
		return b.secondSource.Next(ctx)
	}
	return batch
}

func (b *bufferExportingOperator) reset(ctx context.Context) {
	if r, ok := b.firstSource.(resetter); ok {
		r.reset(ctx)
	}
	if r, ok := b.secondSource.(resetter); ok {
		r.reset(ctx)
	}
	b.firstSourceDone = false
}
