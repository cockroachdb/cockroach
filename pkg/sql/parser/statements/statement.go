// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package statements

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

// Statement is the result of parsing a single statement. It contains the AST
// node along with other information.
type Statement[T any] struct {
	// AST is the root of the AST tree for the parsed statement.
	// Note that it is NOT SAFE to access this currently with statement execution,
	// as unfortunately the AST is not immutable.
	// See issue https://github.com/cockroachdb/cockroach/issues/22847 for more
	// details on this problem.
	AST T

	// Comments is the list of parsed SQL comments.
	Comments []string

	// SQL is the original SQL from which the statement was parsed. Note that this
	// is not appropriate for use in logging, as it may contain passwords and
	// other sensitive data.
	SQL string

	// NumPlaceholders indicates the number of arguments to the statement (which
	// are referenced through placeholders). This corresponds to the highest
	// argument position (i.e. the x in "$x") that appears in the query.
	//
	// Note: where there are "gaps" in the placeholder positions, this number is
	// based on the highest position encountered. For example, for `SELECT $3`,
	// NumPlaceholders is 3. These cases are malformed and will result in a
	// type-check error.
	NumPlaceholders int

	// NumAnnotations indicates the number of annotations in the tree. It is equal
	// to the maximum annotation index.
	NumAnnotations tree.AnnotationIdx
}

// IsANSIDML returns true if the AST is one of the 4 DML statements,
// SELECT, UPDATE, INSERT, DELETE, or an EXPLAIN of one of these statements.
func IsANSIDML(stmt tree.Statement) bool {
	switch t := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.Delete, *tree.Insert, *tree.Update:
		return true
	case *tree.Explain:
		return IsANSIDML(t.Statement)
	}
	return false
}

// Statements is a list of parsed statements.
type Statements []Statement[tree.Statement]

type PLpgStatement Statement[*plpgsqltree.Block]

type JsonpathStatement Statement[*jsonpath.Jsonpath]

// String returns the AST formatted as a string.
func (stmts Statements) String() string {
	return stmts.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmts Statements) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	for i, s := range stmts {
		if i > 0 {
			ctx.WriteString("; ")
		}
		ctx.FormatNode(s.AST)
	}
	return ctx.CloseAndGetString()
}

func (stmt PLpgStatement) String() string {
	return stmt.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmt PLpgStatement) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	stmt.AST.Format(ctx)
	return ctx.CloseAndGetString()
}

func (stmt JsonpathStatement) String() string {
	return stmt.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmt JsonpathStatement) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	stmt.AST.Format(ctx)
	return ctx.CloseAndGetString()
}

type ParsedStmts interface {
	String() string
	StringWithFlags(flags tree.FmtFlags) string
}

var _ ParsedStmts = Statements{}
var _ ParsedStmts = PLpgStatement{}
var _ ParsedStmts = JsonpathStatement{}
