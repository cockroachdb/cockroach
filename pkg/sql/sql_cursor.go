// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// DeclareCursor implements the DECLARE statement.
// See https://www.postgresql.org/docs/current/sql-declare.html for details.
func (p *planner) DeclareCursor(ctx context.Context, s *tree.DeclareCursor) (planNode, error) {
	if p.autoCommit {
		return nil, pgerror.Newf(pgcode.NoActiveSQLTransaction, "DECLARE CURSOR can only be used in transaction blocks")
	}

	if s.Hold {
		return nil, unimplemented.NewWithIssue(41412, "DECLARE CURSOR WITH HOLD")
	}
	if s.Binary {
		return nil, unimplemented.NewWithIssue(41412, "DECLARE BINARY CURSOR")
	}
	if s.Scroll == tree.Scroll {
		return nil, unimplemented.NewWithIssue(41412, "DECLARE SCROLL CURSOR")
	}
	if s.Sensitivity != tree.UnspecifiedSensitivity {
		return nil, unimplemented.NewWithIssue(41412, "DECLARE INSENSITIVE CURSOR")
	}

	ie := p.ExecCfg().InternalExecutorFactory(ctx, p.SessionData())
	cursorName := s.Name.String()
	if _, ok := p.sqlCursors.cursorMap[cursorName]; ok {
		return nil, pgerror.Newf(pgcode.DuplicateCursor, "cursor %q already exists", cursorName)
	}
	rows, err := ie.QueryIterator(ctx, "sql-cursor", p.txn, s.Select.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to DECLARE CURSOR")
	}
	if p.sqlCursors.cursorMap == nil {
		p.sqlCursors.cursorMap = make(map[string]*sqlCursor)
	}
	p.sqlCursors.cursorMap[cursorName] = &sqlCursor{InternalRows: rows}
	return newZeroNode(nil /* columns */), nil
}

var errBackwardScan = pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cursor can only scan forward")

// FetchCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-fetch.html for details.
func (p *planner) FetchCursor(_ context.Context, s *tree.FetchCursor) (planNode, error) {
	cursorName := s.Name.String()
	rows, ok := p.sqlCursors.cursorMap[cursorName]
	if !ok {
		return nil, pgerror.Newf(pgcode.InvalidCursorName, "cursor %q does not exist", cursorName)
	}
	if s.Count < 0 || s.FetchType == tree.FetchBackwardAll {
		return nil, errBackwardScan
	}
	node := &fetchNode{
		n:         s.Count,
		fetchType: s.FetchType,
		rows:      rows,
	}
	if s.FetchType != tree.FetchNormal {
		node.n = 0
		node.offset = s.Count
	}
	return node, nil
}

type fetchNode struct {
	rows *sqlCursor
	// n is the number of rows requested.
	n int64
	// offset is the number of rows to read first, when in relative or absolute
	// mode.
	offset    int64
	fetchType tree.FetchType

	seeked bool
}

func (f fetchNode) startExec(_ runParams) error { return nil }

func (f *fetchNode) Next(params runParams) (bool, error) {
	if f.fetchType == tree.FetchAll {
		return f.rows.Next(params.ctx)
	}

	if !f.seeked {
		// FIRST, LAST, ABSOLUTE, and RELATIVE require seeking before returning
		// values. Do that first.
		f.seeked = true
		switch f.fetchType {
		case tree.FetchFirst:
			switch f.rows.curRow {
			case 0:
				_, err := f.rows.Next(params.ctx)
				return true, err
			case 1:
				return true, nil
			}
			return false, errBackwardScan
		case tree.FetchLast:
			return false, errBackwardScan
		case tree.FetchAbsolute:
			if f.rows.curRow > f.offset {
				return false, errBackwardScan
			}
			for f.rows.curRow < f.offset {
				more, err := f.rows.Next(params.ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		case tree.FetchRelative:
			for i := int64(0); i < f.offset; i++ {
				more, err := f.rows.Next(params.ctx)
				if !more || err != nil {
					return more, err
				}
			}
			return true, nil
		}
	}
	if f.n <= 0 {
		return false, nil
	}
	f.n--
	return f.rows.Next(params.ctx)
}

func (f fetchNode) Values() tree.Datums {
	return f.rows.Cur()
}

func (f fetchNode) Close(ctx context.Context) {
	// We explicitly do not pass through the Close to our InternalRows, because
	// running FETCH on a CURSOR does not close it.
}

// CloseCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-close.html for details.
func (p *planner) CloseCursor(ctx context.Context, n *tree.CloseCursor) (planNode, error) {
	cursorName := n.Name.String()
	_, ok := p.sqlCursors.cursorMap[cursorName]
	if !ok {
		return nil, pgerror.Newf(pgcode.InvalidCursorName, "cursor %q does not exist", cursorName)
	}
	err := p.sqlCursors.cursorMap[cursorName].Close()
	delete(p.sqlCursors.cursorMap, cursorName)
	return newZeroNode(nil /* columns */), err
}

type sqlCursor struct {
	sqlutil.InternalRows
	curRow int64
}

func (s *sqlCursor) Next(ctx context.Context) (bool, error) {
	more, err := s.InternalRows.Next(ctx)
	if err == nil {
		s.curRow++
	}
	return more, err
}
