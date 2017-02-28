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

// algebraicSet is a processor for the algebraic set operations: UNION,
// INTERSECT, EXCEPT, UNION ALL, INTERSECT ALL, EXCEPT ALL. There are
// processors for UNION ALL and EXCEPT ALL, the rest can be implemented
// using these two building blocks.
type algebraicSet struct {
	leftSource, rightSource RowSource
	ordering                Ordering

	opType     AlgebraicSetSpec_Type
	datumAlloc *sqlbase.DatumAlloc

	out procOutputHelper
}

var _ processor = &algebraicSet{}

func newAlgebraicSet(
	flowCtx *FlowCtx,
	spec *AlgebraicSetSpec,
	leftSource, rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*algebraicSet, error) {
	s := &algebraicSet{
		leftSource:  leftSource,
		rightSource: rightSource,
		ordering:    spec.Ordering,
		opType:      spec.Type,
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

	err := s.out.init(post, leftSource.Types(), flowCtx.evalCtx, output)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *algebraicSet) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "AlgebraicSet", nil)
	ctx, span := tracing.ChildSpan(ctx, "algebraicSet")
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 2, "starting algebraic set process")
	defer log.VEventf(ctx, 2, "exiting algebraic set")

	defer s.leftSource.ConsumerDone()
	defer s.rightSource.ConsumerDone()

	switch s.opType {
	case AlgebraicSetSpec_UnionAll:
		s.unionAll(ctx)
	case AlgebraicSetSpec_ExceptAll:
		if err := s.exceptAll(ctx); err != nil {
			s.out.close(err)
			return
		}
	}
	s.out.close(nil)
}

func (s *algebraicSet) unionAll(ctx context.Context) {
	rowSources := []RowSource{s.leftSource, s.rightSource}
	sync, err := makeOrderedSync(convertToColumnOrdering(s.ordering), rowSources)
	if err != nil {
		s.out.close(err)
	}
	for {
		nextRow, err := sync.NextRow()
		if err != nil {
			s.out.close(err)
			return
		}
		if nextRow == nil {
			return
		}
		if !s.out.emitRow(ctx, nextRow) {
			return
		}
	}
}

// exceptAll pushes all rows in the left stream that are not present in the
// right stream. It does not remove duplicates.
func (s *algebraicSet) exceptAll(ctx context.Context) error {
	leftCacher := streamCacher{
		src:        s.leftSource,
		ordering:   convertToColumnOrdering(s.ordering),
		datumAlloc: s.datumAlloc,
	}
	rightCacher := streamCacher{
		src:        s.rightSource,
		ordering:   convertToColumnOrdering(s.ordering),
		datumAlloc: s.datumAlloc,
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
			convertToColumnOrdering(s.ordering), convertToColumnOrdering(s.ordering),
			s.datumAlloc,
		)
		if cmp == 0 {
			if err != nil {
				return err
			}
			rightSlice := rightCacher.currentGroup()
			rightHash := make(map[string]struct{}, len(rightSlice))
			for _, encDatumRow := range rightSlice {
				rightHash[encDatumRow.String()] = struct{}{}
			}
			for _, encDatumRow := range leftCacher.currentGroup() {
				if _, ok := rightHash[encDatumRow.String()]; !ok {
					if !s.out.emitRow(ctx, encDatumRow) {
						return nil
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
				if !s.out.emitRow(ctx, encDatumRow) {
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
			if !s.out.emitRow(ctx, encDatumRow) {
				return nil
			}
		}

		// Emit all remaining rows.
		for {
			leftRow, err := leftCacher.nextRow()
			// Emit all left rows until completion/error.
			if err != nil || leftRow == nil {
				return err
			}
			if !s.out.emitRow(ctx, leftRow) {
				return nil
			}
		}
	}
	return nil
}
