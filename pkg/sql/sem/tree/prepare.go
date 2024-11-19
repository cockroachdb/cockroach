// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lexbase"

// Prepare represents a PREPARE statement.
type Prepare struct {
	Name      Name
	Types     []ResolvableTypeReference
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *Prepare) Format(ctx *FmtCtx) {
	ctx.WriteString("PREPARE ")
	ctx.FormatNode(&node.Name)
	if len(node.Types) > 0 {
		ctx.WriteString(" (")
		for i, t := range node.Types {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatTypeReference(t)
		}
		ctx.WriteRune(')')
	}
	ctx.WriteString(" AS ")
	ctx.FormatNode(node.Statement)
}

// CannedOptPlan is used as the AST for a PREPARE .. AS OPT PLAN statement.
// This is a testing facility that allows execution (and benchmarking) of
// specific plans. See exprgen package for more information on the syntax.
type CannedOptPlan struct {
	Plan string
}

// Format implements the NodeFormatter interface.
func (node *CannedOptPlan) Format(ctx *FmtCtx) {
	// This node can only be used as the AST for a Prepare statement of the form:
	//   PREPARE name AS OPT PLAN '...').
	ctx.WriteString("OPT PLAN ")
	ctx.WriteString(lexbase.EscapeSQLString(node.Plan))
}

// Execute represents an EXECUTE statement.
type Execute struct {
	Name   Name
	Params Exprs
	// DiscardRows is set when we want to throw away all the rows rather than
	// returning for client (used for testing and benchmarking).
	DiscardRows bool
}

// Format implements the NodeFormatter interface.
func (node *Execute) Format(ctx *FmtCtx) {
	ctx.WriteString("EXECUTE ")
	ctx.FormatNode(&node.Name)
	if len(node.Params) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Params)
		ctx.WriteByte(')')
	}
	if node.DiscardRows {
		ctx.WriteString(" DISCARD ROWS")
	}
}

// Deallocate represents a DEALLOCATE statement.
type Deallocate struct {
	Name Name // empty for ALL
}

// Format implements the NodeFormatter interface.
func (node *Deallocate) Format(ctx *FmtCtx) {
	ctx.WriteString("DEALLOCATE ")
	if node.Name == "" {
		ctx.WriteString("ALL")
	} else {
		// Special case for names in DEALLOCATE: the names are redacted in
		// FmtHideConstants mode so that DEALLOCATE statements all show up together
		// in the statement stats UI. The reason is that unlike other statements
		// where the name being referenced is useful for observability, the name of
		// a prepared statement doesn't matter that much. Also, it's extremely cheap
		// to run DEALLOCATE, which can lead to thousands or more DEALLOCATE
		// statements appearing in the UI; other statements that refer to things by
		// name are too expensive for that to be a real problem.
		if ctx.HasFlags(FmtHideConstants) {
			ctx.WriteByte('_')
		} else {
			ctx.FormatNode(&node.Name)
		}
	}
}
