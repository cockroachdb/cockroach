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
//
// Author: Arjun Narayan (arjun@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// algebraicSetOp is a processor for the algebraic set operations,
// currently just EXCEPT ALL.
type algebraicSetOp struct {
	leftSource, rightSource RowSource
	opType                  AlgebraicSetOpSpec_SetOpType
	ordering                Ordering
	datumAlloc              *sqlbase.DatumAlloc
	out                     procOutputHelper
}

var _ processor = &algebraicSetOp{}

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

	lt := leftSource.Types()
	rt := rightSource.Types()
	if len(lt) != len(rt) {
		return nil, errors.Errorf(
			"Non union compatible: left and right have different numbers of columns %d and %d",
			len(lt), len(rt))
	}
	for i := 0; i < len(lt); i++ {
		if lt[i].Kind != rt[i].Kind {
			return nil, errors.Errorf(
				"Left column index %d (%s) is not the same as right column index %d (%s)",
				i, lt[i].Kind, i, rt[i].Kind)
		}
	}

	err := e.out.init(post, leftSource.Types(), &flowCtx.evalCtx, output)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *algebraicSetOp) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "ExceptAll", nil)
	ctx, span := tracing.ChildSpan(ctx, "exceptAll")
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 2, "starting exceptAll set process")
	defer log.VEventf(ctx, 2, "exiting exceptAll")

	defer e.leftSource.ConsumerDone()
	defer e.rightSource.ConsumerDone()

	switch e.opType {
	case AlgebraicSetOpSpec_Except_all:
		if err := e.exceptAll(ctx); err != nil {
			e.out.output.Push(nil, ProducerMetadata{Err: err})
		}
	}
	e.leftSource.ConsumerClosed()
	e.rightSource.ConsumerClosed()
	e.out.close()
}

// exceptAll pushes all rows in the left stream that are not present in the
// right stream. It does not remove duplicates.
func (e *algebraicSetOp) exceptAll(ctx context.Context) error {
	leftCacher := streamCacher{
		src:        MakeNoMetadataRowSource(e.leftSource, e.out.output),
		ordering:   convertToColumnOrdering(e.ordering),
		datumAlloc: e.datumAlloc,
	}
	rightCacher := streamCacher{
		src:        MakeNoMetadataRowSource(e.rightSource, e.out.output),
		ordering:   convertToColumnOrdering(e.ordering),
		datumAlloc: e.datumAlloc,
	}
	_, err := leftCacher.nextRow()
	if err != nil {
		return err
	}
	_, err = rightCacher.nextRow()
	if err != nil {
		return err
	}
	err = leftCacher.accumulateGroup()
	if err != nil {
		return err
	}
	err = rightCacher.accumulateGroup()
	if err != nil {
		return err
	}
	for {
		leftRow := leftCacher.currentGroupRow()
		rightRow := rightCacher.currentGroupRow()
		if leftRow == nil {
			return nil
		}

		cmp, err := CompareEncDatumRowForMerge(leftRow, rightRow,
			convertToColumnOrdering(e.ordering), convertToColumnOrdering(e.ordering),
			e.datumAlloc,
		)
		if err != nil {
			return err
		}
		if cmp == 0 {
			var scratch []byte
			rightSlice := rightCacher.currentGroup()
			rightHash := make(map[string]struct{}, len(rightSlice))
			allRightCols := make(columns, len(e.rightSource.Types()))
			for i := range e.rightSource.Types() {
				allRightCols[i] = uint32(i)
			}
			for _, encDatumRow := range rightSlice {
				encoded, _, err := encodeColumnsOfRow(e.datumAlloc, scratch, encDatumRow, allRightCols, true)
				if err != nil {
					return err
				}
				scratch = encoded[:0]
				rightHash[string(encoded)] = struct{}{}
			}
			for _, encDatumRow := range leftCacher.currentGroup() {
				encoded, _, err := encodeColumnsOfRow(e.datumAlloc, scratch, encDatumRow, allRightCols, true)
				if err != nil {
					return err
				}
				scratch = encoded[:0]
				if _, ok := rightHash[string(encoded)]; !ok {
					status, err := e.out.emitRow(ctx, encDatumRow)
					if status == ConsumerClosed {
						return nil
					}
					if err != nil {
						return err
					}
				}
			}
			leftCacher.advanceGroup()
			rightCacher.advanceGroup()
			err = leftCacher.accumulateGroup()
			if err != nil {
				return err
			}
			err = rightCacher.accumulateGroup()
			if err != nil {
				return err
			}
		}
		if cmp < 0 {
			for _, encDatumRow := range leftCacher.currentGroup() {
				status, err := e.out.emitRow(ctx, encDatumRow)
				if status == ConsumerClosed {
					return nil
				}
				if err != nil {
					return err
				}
			}
			leftCacher.advanceGroup()
			if err := leftCacher.accumulateGroup(); err != nil {
				return err
			}
		}
		if cmp > 0 {
			if rightCacher.advanceGroup() == nil {
				break
			}
			if err := rightCacher.accumulateGroup(); err != nil {
				return err
			}
		}
	}

	if rightCacher.currentGroupRow() == nil {
		// Emit all accumulated left rows.
		for _, encDatumRow := range leftCacher.allAccumulatedRows() {
			status, err := e.out.emitRow(ctx, encDatumRow)
			if status == ConsumerClosed {
				return nil
			}
			if err != nil {
				return err
			}
		}

		// Emit all remaining rows.
		for {
			leftRow, err := leftCacher.nextRow()
			// Emit all left rows until completion/error.
			if err != nil || leftRow == nil {
				return err
			}
			status, err := e.out.emitRow(ctx, leftRow)
			if status == ConsumerClosed {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
