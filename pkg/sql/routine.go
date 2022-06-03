// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

func (p *planner) EvalRoutine(expr *tree.Routine) (result tree.Datum, err error) {
	// TODO(mgartner): Why does the query get canceled when I use EvalContext's
	// ctx?
	// ctx := p.EvalContext().Ctx()
	ctx := p.ex.Ctx()

	var res routineCommandResult
	resCols := colinfo.ResultColumns{{
		Name: "udf",
		Typ:  expr.Typ,
	}}
	res.SetColumns(ctx, resCols)

	for i := range expr.Statements {
		stmt, err := parser.ParseOne(expr.Statements[i])
		if err != nil {
			return nil, err
		}

		_, _, err = p.ex.execStmtInOpenState(ctx, stmt, nil, nil, &res, false /* canAutoCommit */)
		if err != nil {
			return nil, err
		}
	}
	if res.err != nil {
		return nil, res.err
	}
	return res.rows[0][0], nil
}

type routineCommandResult struct {
	rows         []tree.Datums
	err          error
	rowsAffected int
}

var _ RestrictedCommandResult = &routineCommandResult{}

// SetColumns is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) SetColumns(ctx context.Context, cols colinfo.ResultColumns) {
	// No-op.
}

// BufferParamStatusUpdate is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) BufferParamStatusUpdate(key string, val string) {
	panic("unimplemented")
}

// BufferNotice is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) BufferNotice(notice pgnotice.Notice) {
	panic("unimplemented")
}

// ResetStmtType is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) ResetStmtType(stmt tree.Statement) {
	panic("unimplemented")
}

// AddRow is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) AddRow(ctx context.Context, row tree.Datums) error {
	// AddRow() and IncrementRowsAffected() are never called on the same command
	// result, so we will not double count the affected rows by an increment
	// here.
	r.rowsAffected++
	rowCopy := make(tree.Datums, len(row))
	copy(rowCopy, row)
	r.rows = append(r.rows, rowCopy)
	return nil
}

// AddBatch is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) AddBatch(context.Context, coldata.Batch) error {
	panic("unimplemented")
}

// SupportsAddBatch is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) SupportsAddBatch() bool {
	return false
}

func (r *routineCommandResult) DisableBuffering() {
	panic("cannot disable buffering here")
}

// SetError is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) SetError(err error) {
	r.err = err
}

// Err is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) Err() error {
	return r.err
}

// IncrementRowsAffected is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) IncrementRowsAffected(ctx context.Context, n int) {
	r.rowsAffected += n
}

// RowsAffected is part of the RestrictedCommandResult interface.
func (r *routineCommandResult) RowsAffected() int {
	return r.rowsAffected
}

// Close is part of the CommandResultClose interface.
func (r *routineCommandResult) Close(context.Context, TransactionStatusIndicator) {}

// Discard is part of the CommandResult interface.
func (r *routineCommandResult) Discard() {}

// SetInferredTypes is part of the DescribeResult interface.
func (r *routineCommandResult) SetInferredTypes([]oid.Oid) {}

// SetNoDataRowDescription is part of the DescribeResult interface.
func (r *routineCommandResult) SetNoDataRowDescription() {}

// SetPrepStmtOutput is part of the DescribeResult interface.
func (r *routineCommandResult) SetPrepStmtOutput(context.Context, colinfo.ResultColumns) {}

// SetPortalOutput is part of the DescribeResult interface.
func (r *routineCommandResult) SetPortalOutput(
	context.Context, colinfo.ResultColumns, []pgwirebase.FormatCode,
) {
}
