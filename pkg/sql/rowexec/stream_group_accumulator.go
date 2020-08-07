// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// streamGroupAccumulator groups input rows coming from src into groups dictated
// by equality according to the ordering columns.
type streamGroupAccumulator struct {
	src   execinfra.RowSource
	types []*types.T

	// srcConsumed is set once src has been exhausted.
	srcConsumed bool
	ordering    colinfo.ColumnOrdering

	// curGroup maintains the rows accumulated in the current group.
	curGroup   []rowenc.EncDatumRow
	datumAlloc rowenc.DatumAlloc

	// leftoverRow is the first row of the next group. It's saved in the
	// accumulator after the current group is returned, so the accumulator can
	// resume later.
	leftoverRow rowenc.EncDatumRow

	rowAlloc rowenc.EncDatumRowAlloc

	memAcc mon.BoundAccount
}

func makeStreamGroupAccumulator(
	src execinfra.RowSource, ordering colinfo.ColumnOrdering, memMonitor *mon.BytesMonitor,
) streamGroupAccumulator {
	return streamGroupAccumulator{
		src:      src,
		types:    src.OutputTypes(),
		ordering: ordering,
		memAcc:   memMonitor.MakeBoundAccount(),
	}
}

func (s *streamGroupAccumulator) start(ctx context.Context) {
	s.src.Start(ctx)
}

// nextGroup returns the next group from the inputs. The returned slice is not safe
// to use after the next call to nextGroup.
func (s *streamGroupAccumulator) nextGroup(
	ctx context.Context, evalCtx *tree.EvalContext,
) ([]rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if s.srcConsumed {
		// If src has been exhausted, then we also must have advanced away from the
		// last group.
		return nil, nil
	}

	if s.leftoverRow != nil {
		s.curGroup = append(s.curGroup, s.leftoverRow)
		s.leftoverRow = nil
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

		if err := s.memAcc.Grow(ctx, int64(row.Size())); err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		row = s.rowAlloc.CopyRow(row)

		if len(s.curGroup) == 0 {
			if s.curGroup == nil {
				s.curGroup = make([]rowenc.EncDatumRow, 0, 64)
			}
			s.curGroup = append(s.curGroup, row)
			continue
		}

		cmp, err := s.curGroup[0].Compare(s.types, &s.datumAlloc, s.ordering, evalCtx, row)
		if err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		if cmp == 0 {
			s.curGroup = append(s.curGroup, row)
		} else if cmp == 1 {
			return nil, &execinfrapb.ProducerMetadata{
				Err: errors.Errorf(
					"detected badly ordered input: %s > %s, but expected '<'",
					s.curGroup[0].String(s.types), row.String(s.types)),
			}
		} else {
			n := len(s.curGroup)
			ret := s.curGroup[:n:n]
			s.curGroup = s.curGroup[:0]
			s.memAcc.Empty(ctx)
			s.leftoverRow = row
			return ret, nil
		}
	}
}

func (s *streamGroupAccumulator) close(ctx context.Context) {
	s.memAcc.Close(ctx)
}
