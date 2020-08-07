// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	source    planNode
	rows      *rowcontainer.RowContainer
	hardLimit int64
	curRowIdx int
}

func (s *spoolNode) startExec(params runParams) error {
	// If FastPathResults() on the source indicates that the results are
	// already available (2nd value true), then the computation is
	// already done at start time and spooling is unnecessary.
	if f, ok := s.source.(planNodeFastPath); ok {
		_, done := f.FastPathResults()
		if done {
			return nil
		}
	}

	s.rows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		colinfo.ColTypeInfoFromResCols(planColumns(s.source)),
	)

	// Accumulate all the rows up to the hardLimit, if any.
	// This also guarantees execution of the child node to completion,
	// even if Next() on the spool itself is not called for every row.
	for {
		next, err := s.source.Next(params)
		if err != nil {
			return err
		}
		if !next {
			break
		}
		if s.hardLimit == 0 || int64(s.rows.Len()) < s.hardLimit {
			if _, err := s.rows.AddRow(params.ctx, s.source.Values()); err != nil {
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
	if f, ok := s.source.(planNodeFastPath); ok {
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
	s.source.Close(ctx)
	if s.rows != nil {
		s.rows.Close(ctx)
		s.rows = nil
	}
}
