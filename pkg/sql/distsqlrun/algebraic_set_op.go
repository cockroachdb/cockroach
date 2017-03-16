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
	"fmt"
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

	switch spec.OpType {
	case AlgebraicSetOpSpec_Except_all:
		break
	default:
		return nil, errors.Errorf("cannot create algebraicSetOp for unsupported algebraicSetOpType %v", e.opType)
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

	default:
		panic(fmt.Sprintf("cannot run unsupported algebraicSetOp %v", e.opType))
	}
	e.leftSource.ConsumerClosed()
	e.rightSource.ConsumerClosed()
	e.out.close()
}

// exceptAll pushes all rows in the left stream that are not present in the
// right stream. It does not remove duplicates.
func (e *algebraicSetOp) exceptAll(ctx context.Context) error {
	leftGroup := makeStreamGroupAccumulator(
		MakeNoMetadataRowSource(e.leftSource, e.out.output),
		convertToColumnOrdering(e.ordering),
	)

	rightGroup := makeStreamGroupAccumulator(
		MakeNoMetadataRowSource(e.rightSource, e.out.output),
		convertToColumnOrdering(e.ordering),
	)

	leftRows, err := leftGroup.advanceGroup()
	if err != nil {
		return err
	}
	rightRows, err := rightGroup.advanceGroup()
	if err != nil {
		return err
	}

	// We iterate in lockstep through the groups of rows given equalilty under
	// the common source ordering. Whenever we find a left group without a match
	// on the right, it's easy - we output the full group. Whenever we find a
	// group on the right without a match on the left, we ignore it. Whenever
	// we find matching groups, we generate a hashMap of the right group and
	// check the left group against the hashMap.
	// TODO(arjun): if groups are large and we have a limit, we might want to
	// stream through the leftGroup instead of accumulating it all.
	for {
		// If we exhause all left rows, we are done.
		if len(leftRows) == 0 {
			return nil
		}
		// If we exhause all right rows, we can emit all left rows.
		if len(rightRows) == 0 {
			break
		}

		cmp, err := CompareEncDatumRowForMerge(leftRows[0], rightRows[0],
			convertToColumnOrdering(e.ordering), convertToColumnOrdering(e.ordering),
			e.datumAlloc,
		)
		if err != nil {
			return err
		}
		if cmp == 0 {
			var scratch []byte
			rightMap := make(map[string]struct{}, len(rightRows))
			allRightCols := make(columns, len(e.rightSource.Types()))
			for i := range e.rightSource.Types() {
				allRightCols[i] = uint32(i)
			}
			for _, encDatumRow := range rightRows {
				encoded, _, err := encodeColumnsOfRow(e.datumAlloc, scratch, encDatumRow, allRightCols, true /* encodeNull */)
				if err != nil {
					return err
				}
				scratch = encoded[:0]
				rightMap[string(encoded)] = struct{}{}
			}
			for _, encDatumRow := range leftRows {
				encoded, _, err := encodeColumnsOfRow(e.datumAlloc, scratch, encDatumRow, allRightCols, true /* encodeNull */)
				if err != nil {
					return err
				}
				scratch = encoded[:0]
				if _, ok := rightMap[string(encoded)]; !ok {
					status, err := e.out.emitRow(ctx, encDatumRow)
					if status == ConsumerClosed {
						return nil
					}
					if err != nil {
						return err
					}
				}
			}
			leftRows, err = leftGroup.advanceGroup()
			if err != nil {
				return err
			}
			rightRows, err = rightGroup.advanceGroup()
			if err != nil {
				return err
			}
		}
		if cmp < 0 {
			for _, encDatumRow := range leftRows {
				status, err := e.out.emitRow(ctx, encDatumRow)
				if status == ConsumerClosed {
					return nil
				}
				if err != nil {
					return err
				}
			}
			leftRows, err = leftGroup.advanceGroup()
			if err != nil {
				return err
			}
		}
		if cmp > 0 {
			rightRows, err = rightGroup.advanceGroup()
			if len(rightRows) == 0 {
				break
			}
			if err != nil {
				return err
			}
		}
	}

	if len(rightRows) == 0 {
		// Emit all accumulated left rows.
		for _, encDatumRow := range leftRows {
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
			leftRows, err = leftGroup.advanceGroup()
			// Emit all left rows until completion/error.
			if err != nil || len(leftRows) == 0 {
				return err
			}
			for _, row := range leftRows {
				status, err := e.out.emitRow(ctx, row)
				if status == ConsumerClosed {
					return nil
				}
				if err != nil {
					return err
				}
			}
		}
	}
	if !leftGroup.srcConsumed {
		return errors.Errorf("exceptAll finished but leftGroup not consumed")
	}
	return nil
}
