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

type set struct {
	leftSource, rightSource RowSource
	ordering                Ordering

	opType     SetOpSpec_Type
	datumAlloc *sqlbase.DatumAlloc

	out procOutputHelper
}

var _ processor = &set{}

func newSet(
	flowCtx *FlowCtx,
	spec *SetOpSpec,
	leftSource, rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*set, error) {
	s := &set{
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

func (s *set) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "SetOp", nil)
	ctx, span := tracing.ChildSpan(ctx, "set")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting set process")
		defer log.Infof(ctx, "exiting set")
	}

	defer s.leftSource.ConsumerDone()
	defer s.rightSource.ConsumerDone()

	switch s.opType {
	case SetOpSpec_UnionAll:
		s.unionAll(ctx)
	case SetOpSpec_ExceptAll:
		s.exceptAll(ctx)
	}
	s.out.close(nil)
}

func (s *set) unionAll(ctx context.Context) {
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

func (s *set) exceptAll(ctx context.Context) {
	fmt.Println("starting exceptAll")
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
		s.out.close(err)
		return
	}
	_, err = rightCacher.nextRow()
	if err != nil {
		s.out.close(err)
		return
	}
	err = leftCacher.accumulateGroup()
	if err != nil {
		s.out.close(err)
		return
	}
	err = rightCacher.accumulateGroup()
	if err != nil {
		s.out.close(err)
		return
	}
	fmt.Printf("right cache: %v\n", rightCacher.currentGroup())
	for {
		fmt.Println("loop start")
		leftRow := leftCacher.currentGroupRow()
		rightRow := rightCacher.currentGroupRow()
		fmt.Printf("left row: %s\n", leftRow.String())
		fmt.Printf("right row: %s\n", rightRow.String())
		if leftRow == nil {
			fmt.Println("leftrow == nil")
			return
		}

		cmp, err := sqlbase.CompareEncDatumRow(leftRow, rightRow,
			convertToColumnOrdering(s.ordering), convertToColumnOrdering(s.ordering),
			s.datumAlloc,
		)
		if cmp == 0 {
			fmt.Println("cmp == 0")
			if err != nil {
				s.out.close(err)
				return
			}
			rightSlice := rightCacher.currentGroup()
			fmt.Printf("right slice: %v\n", rightSlice)
			rightHash := make(map[string]struct{}, len(rightSlice))
			for _, encDatumRow := range rightSlice {
				rightHash[encDatumRow.String()] = struct{}{}
			}
			for _, encDatumRow := range leftCacher.currentGroup() {
				if _, ok := rightHash[encDatumRow.String()]; !ok {
					if !s.out.emitRow(ctx, encDatumRow) {
						s.out.close(nil)
						return
					}
				}
			}
			fmt.Printf("Done with emitting rows for slice: %v\n", rightSlice)
			leftCacher.advanceGroup()
			rightCacher.advanceGroup()
			err = leftCacher.accumulateGroup()
			if err != nil {
				s.out.close(err)
				return
			}
			err = rightCacher.accumulateGroup()
			if err != nil {
				s.out.close(err)
				return
			}
		}
		if cmp < 0 {
			fmt.Println("cmp < 0")
			for _, encDatumRow := range leftCacher.currentGroup() {
				if !s.out.emitRow(ctx, encDatumRow) {
					s.out.close(nil)
					return
				}
			}
			leftCacher.advanceGroup()
			if err := leftCacher.accumulateGroup(); err != nil {
				s.out.close(err)
				return
			}
		}
		if cmp > 0 {
			fmt.Println("cmp > 0")
			if rightCacher.advanceGroup() != nil {
				if err := rightCacher.accumulateGroup(); err != nil {
					s.out.close(err)
					return
				}
			}
			break
		}
	}
	fmt.Printf("Advancing... leftRow: %v, rightRow: %v\n", leftCacher.currentGroup(), rightCacher.currentGroup())
	if rightCacher.currentGroupRow() == nil {
		fmt.Println("rightrow == nil")

		// Emit all accumulated left rows.
		for _, encDatumRow := range leftCacher.allAccumulatedRows() {
			if !s.out.emitRow(ctx, encDatumRow) {
				s.out.close(nil)
				return
			}
		}

		// Emit all remaining rows.
		for {
			leftRow, err := leftCacher.nextRow()
			// Emit all left rows until completion/error.
			if err != nil || leftRow == nil {
				s.out.close(err)
				return
			}
			if !s.out.emitRow(ctx, leftRow) {
				s.out.close(nil)
				return
			}
		}
	}

}
