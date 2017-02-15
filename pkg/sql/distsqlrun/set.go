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

type set struct {
	leftSource, rightSource RowSource
	ordering                sqlbase.ColumnOrdering

	opType     SetSpec_Type
	datumAlloc sqlbase.DatumAlloc

	out procOutputHelper
}

var _ processor = &set{}

func newSet(
	flowCtx *FlowCtx, spec *SetSpec, leftSource, rightSource RowSource,
	post *PostProcessSpec, output RowReceiver,
) (*set, error) {
	s := &set{
		leftSource:  leftSource,
		rightSource: rightSource,
		ordering:    convertToColumnOrdering(spec.Ordering),
		opType:      spec.Type,
	}

	lt := leftSource.Types()
	rt := rightSource.Types()
	if len(lt) != len(rt) {
		return nil, errors.Errorf("Non union compatible: left and right have different numbers of columns %d and %d",
			len(lt), len(rt))
	}
	for i := 0; i < len(lt); i++ {
		if lt[i].Kind != rt[i].Kind {
			return nil, errors.Errorf("Left column index %d (%s) is not the same as right column index %d (%s)",
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

	ctx = log.WithLogTag(ctx, "Evaluator", nil)
	ctx, span := tracing.ChildSpan(ctx, "set")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting set process")
		defer log.Infof(ctx, "exiting set")
	}

	defer s.leftSource.ConsumerDone()
	defer s.rightSource.ConsumerDone()

	switch s.opType {
	case SetSpec_UnionAll:
		if err := s.unionAll(ctx); err != nil {
			s.out.close(err)
		}
	case SetSpec_Union:
		panic("TODO(arjun): not implemented yet")
	case SetSpec_IntersectAll:
		panic("TODO(arjun): not implemented yet")
	case SetSpec_Intersect:
		panic("TODO(arjun): not implemented yet")
	case SetSpec_ExceptAll:
		panic("TODO(arjun): not implemented yet")
	case SetSpec_Except:
		panic("TODO(arjun): not implemented yet")
	}
	s.out.close(nil)
}

// UnionAll processes rows from the the two sources in tandem.
func (s *set) unionAll(ctx context.Context) error {
	var leftRow, rightRow sqlbase.EncDatumRow
	var err error
	leftRow, err = s.leftSource.NextRow()
	if err != nil {
		return err
	}
	rightRow, err = s.rightSource.NextRow()
	if err != nil {
		return err
	}

	for {
		if leftRow == nil && rightRow == nil {
			break
		}
		cmp, err := sqlbase.CompareEncDatumRow(leftRow, rightRow,
			s.ordering, s.ordering, &s.datumAlloc,
		)
		if err != nil {
			return err
		}

		// leftRow < rightRow
		if cmp < 0 {
			if !s.out.emitRow(ctx, leftRow) {
				return nil
			}
			if leftRow, err = s.leftSource.NextRow(); err != nil {
				return err
			}
		} else {
			if !s.out.emitRow(ctx, rightRow) {
				return nil
			}
			if rightRow, err = s.rightSource.NextRow(); err != nil {
				return err
			}
		}
	}
	return nil
}
