// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// spoolNode ensures that a child planNode is executed to completion
// during the start phase. The results, if any, are collected. The
// child node is guaranteed to run to completion.
// If hardLimit is set, only that number of rows is collected, but
// the child node is still run to completion.
type spoolNode struct {
	source    planNode
	rows      *sqlbase.RowContainer
	hardLimit int64
	curRowIdx int
}

func (p *planner) makeSpool(source planNode) planNode {
	return &spoolNode{source: source}
}

// startExec implements the execStartable interface.
func (s *spoolNode) startExec(params runParams) error {
	// If the source node is done computing already, there's no need for a spool.
	if f, ok := s.source.(planNodeFastPath); ok {
		_, done := f.FastPathResults()
		if done {
			return nil
		}
	}

	s.rows = sqlbase.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(planColumns(s.source)),
		0,
	)

	// Accumulate all the rows. This also guarantees execution of the
	// child node to completion, even if Next() is not called for every
	// row.
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
