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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package parser

import "bytes"

// ReturningClause represents the returning clause on a statement.
type ReturningClause interface {
	NodeFormatter
	// statementType returns the StatementType of statements that include
	// the implementors variant of a RETURNING clause.
	statementType() StatementType
	returningClause()
}

var _ ReturningClause = &ReturningExprs{}
var _ ReturningClause = &ReturningNothing{}
var _ ReturningClause = &NoReturningClause{}

// ReturningExprs represents RETURNING expressions.
type ReturningExprs SelectExprs

// Format implements the NodeFormatter interface.
func (r *ReturningExprs) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(" RETURNING ")
	FormatNode(buf, f, SelectExprs(*r))
}

// Shared instance to avoid unnecessary allocations.
var returningNothingClause = &ReturningNothing{}

// ReturningNothing represents RETURNING NOTHING.
type ReturningNothing struct{}

// Format implements the NodeFormatter interface.
func (*ReturningNothing) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(" RETURNING NOTHING")
}

// AbsentReturningClause is a ReturningClause variant representing the absence of
// a RETURNING clause.
var AbsentReturningClause = &NoReturningClause{}

// NoReturningClause represents the absence of a RETURNING clause.
type NoReturningClause struct{}

// Format implements the NodeFormatter interface.
func (*NoReturningClause) Format(buf *bytes.Buffer, f FmtFlags) {}

// used by parent statements to determine their own StatementType.
func (*ReturningExprs) statementType() StatementType    { return Rows }
func (*ReturningNothing) statementType() StatementType  { return RowsAffected }
func (*NoReturningClause) statementType() StatementType { return RowsAffected }

func (*ReturningExprs) returningClause()    {}
func (*ReturningNothing) returningClause()  {}
func (*NoReturningClause) returningClause() {}

// HasReturningClause determines if a ReturningClause is present, given a
// variant of the ReturningClause interface.
func HasReturningClause(clause ReturningClause) bool {
	_, ok := clause.(*NoReturningClause)
	return !ok
}
