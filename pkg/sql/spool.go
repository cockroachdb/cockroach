// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// spoolNode ensures that a child planNode is executed to completion
// during the start phase. The results, if any, are collected. The
// child node is guaranteed to run to completion.
// If hardLimit is set, only that number of rows is collected, but
// the child node is still run to completion.
type spoolNode struct {
	singleInputPlanNode
	rows      *rowcontainer.RowContainer
	hardLimit int64
	curRowIdx int
}

// spoolNode is not a mutationPlanNode itself, but it might wrap one.
var _ mutationPlanNode = &spoolNode{}

func (s *spoolNode) startExec(params runParams) error {
	// If FastPathResults() on the source indicates that the results are
	// already available (2nd value true), then the computation is
	// already done at start time and spooling is unnecessary.
	if f, ok := s.input.(planNodeFastPath); ok {
		_, done := f.FastPathResults()
		if done {
			return nil
		}
	}

	s.rows = rowcontainer.NewRowContainer(
		params.p.Mon().MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(planColumns(s.input)),
	)

	// Accumulate all the rows up to the hardLimit, if any.
	// This also guarantees execution of the child node to completion,
	// even if Next() on the spool itself is not called for every row.
	for {
		next, err := s.input.Next(params)
		if err != nil {
			return err
		}
		if !next {
			break
		}
		if s.hardLimit == 0 || int64(s.rows.Len()) < s.hardLimit {
			if _, err := s.rows.AddRow(params.ctx, s.input.Values()); err != nil {
				return err
			}
		}
	}
	s.curRowIdx = -1
	return nil
}

// FastPathResults implements the planNodeFastPath interface.
func (s *spoolNode) FastPathResults() (int, bool) {
	// If the source implements the fast path interface, let it report
	// its status through. This lets e.g. the fast path of a DELETE or
	// an UPSERT report that they have finished its computing already,
	// so the calls to Next() on the spool itself can also be elided.
	// If FastPathResults() on the source says the fast path is unavailable,
	// then startExec() on the spool will also notice that and
	// spooling will occur as expected.
	if f, ok := s.input.(planNodeFastPath); ok {
		return f.FastPathResults()
	}
	return 0, false
}

// spooled implements the planNodeSpooled interface.
func (s *spoolNode) spooled() {}

// Next is part of the planNode interface.
func (s *spoolNode) Next(params runParams) (bool, error) {
	s.curRowIdx++
	return s.curRowIdx < s.rows.Len(), nil
}

// Values is part of the planNode interface.
func (s *spoolNode) Values() tree.Datums {
	return s.rows.At(s.curRowIdx)
}

// Close is part of the planNode interface.
func (s *spoolNode) Close(ctx context.Context) {
	s.input.Close(ctx)
	if s.rows != nil {
		s.rows.Close(ctx)
		s.rows = nil
	}
}

func (s *spoolNode) rowsWritten() int64 {
	m, ok := s.input.(mutationPlanNode)
	if !ok {
		return 0
	}
	return m.rowsWritten()
}
