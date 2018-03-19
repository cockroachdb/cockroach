// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// streamGroupAccumulator groups input rows coming from src into groups dictated
// by equality according to the ordering columns.
type streamGroupAccumulator struct {
	src   RowSource
	types []sqlbase.ColumnType

	// srcConsumed is set once src has been exhausted.
	srcConsumed bool
	ordering    sqlbase.ColumnOrdering

	// curGroup maintains the rows accumulated in the current group.
	curGroup   []sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc
}

func makeStreamGroupAccumulator(
	src RowSource, ordering sqlbase.ColumnOrdering,
) streamGroupAccumulator {
	return streamGroupAccumulator{
		src:      src,
		types:    src.OutputTypes(),
		ordering: ordering,
	}
}

func (s *streamGroupAccumulator) nextGroup(
	evalCtx *tree.EvalContext,
) ([]sqlbase.EncDatumRow, *ProducerMetadata) {
	if s.srcConsumed {
		// If src has been exhausted, then we also must have advanced away from the
		// last group.
		return nil, nil
	}

	for {
		row, meta := s.src.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			s.srcConsumed = true
			return s.curGroup, nil
		}

		if len(s.curGroup) == 0 {
			if s.curGroup == nil {
				s.curGroup = make([]sqlbase.EncDatumRow, 0, 64)
			}
			s.curGroup = append(s.curGroup, row)
			continue
		}

		cmp, err := s.curGroup[0].Compare(s.types, &s.datumAlloc, s.ordering, evalCtx, row)
		if err != nil {
			return nil, &ProducerMetadata{Err: err}
		}
		if cmp == 0 {
			s.curGroup = append(s.curGroup, row)
		} else if cmp == 1 {
			return nil, &ProducerMetadata{
				Err: errors.Errorf(
					"detected badly ordered input: %s > %s, but expected '<'",
					s.curGroup[0].String(s.types), row.String(s.types)),
			}
		} else {
			n := len(s.curGroup)
			ret := s.curGroup[:n:n]
			// The curGroup slice possibly has additional space at the end of it. Use
			// it if possible to avoid an allocation.
			s.curGroup = s.curGroup[n:]
			if cap(s.curGroup) == 0 {
				s.curGroup = make([]sqlbase.EncDatumRow, 0, 64)
			}
			s.curGroup = append(s.curGroup, row)
			return ret, nil
		}
	}
}
