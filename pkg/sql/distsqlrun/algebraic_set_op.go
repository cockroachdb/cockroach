// Copyright 2017 The Cockroach Authors.
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

package distsqlrun

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// algebraicSetOp is a processor for the algebraic set operations,
// currently just EXCEPT ALL.
type algebraicSetOp struct {
	processorBase

	evalCtx *tree.EvalContext

	leftSource, rightSource RowSource
	opType                  AlgebraicSetOpSpec_SetOpType
	ordering                Ordering
	types                   []sqlbase.ColumnType
	allCols                 columns
	leftAccum, rightAccum   streamGroupAccumulator

	// The following variables retain the state of the processor between
	// successive calls to Next().
	leftGroup, rightGroup []sqlbase.EncDatumRow
	// rightMap stores counts of rightGroup, decrementing as rows are encountered
	// in leftGroup.
	rightMap map[string]int
	// leftIdx is the index into leftGroup where we resume iteration.
	leftIdx int
	// cmp stores whether leftGroup < rightGroup.
	cmp int

	err        error
	datumAlloc sqlbase.DatumAlloc
	scratch    []byte
}

var _ RowSource = &algebraicSetOp{}
var _ Processor = &algebraicSetOp{}

func newAlgebraicSetOp(
	flowCtx *FlowCtx,
	spec *AlgebraicSetOpSpec,
	leftSource, rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*algebraicSetOp, error) {
	e := &algebraicSetOp{
		leftSource:  leftSource,
		rightSource: rightSource,
		ordering:    spec.Ordering,
		opType:      spec.OpType,
	}

	switch spec.OpType {
	case AlgebraicSetOpSpec_Except_all:
		break
	default:
		return nil, errors.Errorf("cannot create algebraicSetOp for unsupported algebraicSetOpType %v", e.opType)
	}

	lt := leftSource.OutputTypes()
	rt := rightSource.OutputTypes()
	if len(lt) != len(rt) {
		return nil, errors.Errorf(
			"Non union compatible: left and right have different numbers of columns %d and %d",
			len(lt), len(rt))
	}
	for i := 0; i < len(lt); i++ {
		if lt[i].SemanticType != rt[i].SemanticType {
			return nil, errors.Errorf(
				"Left column index %d (%s) is not the same as right column index %d (%s)",
				i, lt[i].SemanticType, i, rt[i].SemanticType)
		}
	}

	e.types = lt
	evalCtx := flowCtx.NewEvalCtx()
	err := e.init(post, e.types, flowCtx, evalCtx, output)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *algebraicSetOp) Run(wg *sync.WaitGroup) {
	if e.out.output == nil {
		panic("algebraicSetOp output not initialized for emitting rows")
	}
	Run(e.flowCtx.Ctx, e, e.out.output)
	if wg != nil {
		wg.Done()
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (e *algebraicSetOp) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !e.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(e.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		e.internalClose()
	}
	return meta
}

func (e *algebraicSetOp) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if e.closed {
		return nil, e.producerMeta(nil /* err */)
	}

	for {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch e.opType {
		case AlgebraicSetOpSpec_Except_all:
			row, meta = e.nextExceptAll()
		default:
			panic(fmt.Sprintf("cannot run unsupported algebraicSetOp %v", e.opType))
		}

		if e.closed || meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, e.producerMeta(nil /* err */)
		}

		outRow, status, err := e.out.ProcessRow(e.ctx, row)
		if err != nil {
			return nil, e.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil {
				continue
			}
		case DrainRequested:
			e.leftSource.ConsumerDone()
			e.rightSource.ConsumerDone()
			continue
		}
		return outRow, nil
	}
}

// nextExceptAll returns the next row in the left stream that is not present in the
// right stream. It does not remove duplicates.
func (e *algebraicSetOp) nextExceptAll() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if e.maybeStart("exceptAll", "ExceptAll") {
		e.evalCtx = e.flowCtx.NewEvalCtx()

		e.leftAccum = makeStreamGroupAccumulator(
			e.leftSource, convertToColumnOrdering(e.ordering),
		)
		e.rightAccum = makeStreamGroupAccumulator(
			e.rightSource, convertToColumnOrdering(e.ordering),
		)

		e.leftGroup, e.err = e.leftAccum.advanceGroup(e.evalCtx, e.out.output)
		if e.err != nil {
			return nil, e.producerMeta(e.err)
		}
		e.rightGroup, e.err = e.rightAccum.advanceGroup(e.evalCtx, e.out.output)
		if e.err != nil {
			return nil, e.producerMeta(e.err)
		}
		e.resetIndexes()
		if e.err != nil {
			return nil, e.producerMeta(e.err)
		}

		e.allCols = make(columns, len(e.types))
		for i := range e.allCols {
			e.allCols[i] = uint32(i)
		}
	}

	// The loop below forms a restartable state machine that iterates over a
	// batch of rows from the left and right side. The state machine
	// returns a result for every row that should be output.
	for {
		if e.rightGroup == nil {
			// We have no more right rows, so emit all left rows.

			if !e.maybeAdvanceLeft() {
				if e.leftGroup == nil {
					return nil, e.producerMeta(nil /* err */)
				}

				row := e.leftGroup[e.leftIdx]
				e.leftIdx++

				return row, nil
			}
			if e.err != nil {
				return nil, e.producerMeta(e.err)
			}
		}

		if e.cmp > 0 {
			e.advanceRight()
			if e.err != nil {
				return nil, e.producerMeta(e.err)
			}
		} else if e.cmp == 0 {
			if e.maybeAdvanceLeft() {
				if e.leftGroup == nil {
					return nil, e.producerMeta(nil /* err */)
				}
				continue
			}

			for e.leftIdx < len(e.leftGroup) {
				encDatumRow := e.leftGroup[e.leftIdx]
				e.leftIdx++

				encoded, _, err := encodeColumnsOfRow(
					&e.datumAlloc, e.scratch, encDatumRow, e.allCols, e.types, true, /* encodeNull */
				)
				e.scratch = encoded[:0]
				if err != nil {
					return nil, e.producerMeta(err)
				}

				e.maybeRefreshRightMap()

				if count := e.rightMap[string(encoded)]; count == 0 {
					return encDatumRow, nil
				}
				e.rightMap[string(encoded)]--
			}
		}
		// emit the left group until exhaustion.
		if !e.maybeAdvanceLeft() {
			if e.leftGroup == nil {
				return nil, e.producerMeta(e.err)
			}

			row := e.leftGroup[e.leftIdx]
			e.leftIdx++
			return row, nil
		}
		if e.err != nil {
			return nil, e.producerMeta(e.err)
		}
	}

}

// maybeAdvanceLeft advances the left group if the left pointer is at the end
// of the current group. It returns true if it advanced the group. If it returns
// true the error field must be checked.
func (e *algebraicSetOp) maybeAdvanceLeft() bool {
	if e.leftIdx == len(e.leftGroup) {
		e.advanceLeft()
		return true
	}
	return false
}

// advanceLeft advances the left group. The error field must be checked.
func (e *algebraicSetOp) advanceLeft() {
	e.leftGroup, e.err = e.leftAccum.advanceGroup(e.evalCtx, e.out.output)
	if e.leftGroup != nil {
		e.resetIndexes()
	}
}

// advanceRight advances the right group. The error field must be checked.
func (e *algebraicSetOp) advanceRight() {
	e.rightGroup, e.err = e.rightAccum.advanceGroup(e.evalCtx, e.out.output)
	e.rightMap = nil
	if e.rightGroup != nil {
		e.resetIndexes()
	}
}

// resetIndexes resets the struct state. The error field must be checked.
func (e *algebraicSetOp) resetIndexes() {
	e.leftIdx = 0
	if e.leftGroup != nil && e.rightGroup != nil {
		e.cmp, e.err = CompareEncDatumRowForMerge(
			e.types,
			e.leftGroup[0], e.rightGroup[0],
			convertToColumnOrdering(e.ordering), convertToColumnOrdering(e.ordering),
			false, /* nullEquality */
			&e.datumAlloc, e.evalCtx,
		)
	}
}

// refreshRightMap resets the right map. The error field must be checked.
func (e *algebraicSetOp) maybeRefreshRightMap() {
	if e.rightMap == nil {
		e.rightMap = make(map[string]int, len(e.rightGroup))

		var encoded []byte
		for _, encDatumRow := range e.rightGroup {
			encoded, _, e.err = encodeColumnsOfRow(
				&e.datumAlloc, nil, encDatumRow, e.allCols,
				e.types, true, /* encodeNull */
			)
			if e.err != nil {
				return
			}
			e.rightMap[string(encoded)]++
		}
	}
}

func (e *algebraicSetOp) ConsumerDone() {
	e.leftSource.ConsumerDone()
	e.rightSource.ConsumerDone()
}

func (e *algebraicSetOp) ConsumerClosed() {
	e.leftSource.ConsumerClosed()
	e.rightSource.ConsumerClosed()
}
