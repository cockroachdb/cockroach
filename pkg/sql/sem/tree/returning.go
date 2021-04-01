// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ReturningClause represents the returning clause on a statement.
type ReturningClause interface {
	NodeFormatter
	// statementReturnType returns the StatementReturnType of statements that include
	// the implementors variant of a RETURNING clause.
	statementReturnType() StatementReturnType
	returningClause()
}

var _ ReturningClause = &ReturningExprs{}
var _ ReturningClause = &ReturningNothing{}
var _ ReturningClause = &NoReturningClause{}

// ReturningExprs represents RETURNING expressions.
type ReturningExprs SelectExprs

// Format implements the NodeFormatter interface.
func (r *ReturningExprs) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURNING ")
	ctx.FormatNode((*SelectExprs)(r))
}

// ReturningNothingClause is a shared instance to avoid unnecessary allocations.
var ReturningNothingClause = &ReturningNothing{}

// ReturningNothing represents RETURNING NOTHING.
type ReturningNothing struct{}

// Format implements the NodeFormatter interface.
func (*ReturningNothing) Format(ctx *FmtCtx) {
	ctx.WriteString("RETURNING NOTHING")
}

// AbsentReturningClause is a ReturningClause variant representing the absence of
// a RETURNING clause.
var AbsentReturningClause = &NoReturningClause{}

// NoReturningClause represents the absence of a RETURNING clause.
type NoReturningClause struct{}

// Format implements the NodeFormatter interface.
func (*NoReturningClause) Format(_ *FmtCtx) {}

// used by parent statements to determine their own StatementReturnType.
func (*ReturningExprs) statementReturnType() StatementReturnType    { return Rows }
func (*ReturningNothing) statementReturnType() StatementReturnType  { return RowsAffected }
func (*NoReturningClause) statementReturnType() StatementReturnType { return RowsAffected }

func (*ReturningExprs) returningClause()    {}
func (*ReturningNothing) returningClause()  {}
func (*NoReturningClause) returningClause() {}

// HasReturningClause determines if a ReturningClause is present, given a
// variant of the ReturningClause interface.
func HasReturningClause(clause ReturningClause) bool {
	_, ok := clause.(*NoReturningClause)
	return !ok
}
