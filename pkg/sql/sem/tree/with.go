// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// With represents a WITH statement.
type With struct {
	Recursive bool
	CTEList   []*CTE
}

// CTE represents a common table expression inside of a WITH clause.
type CTE struct {
	Name AliasClause
	Mtr  MaterializeClause
	Stmt Statement
}

// MaterializeClause represents a materialize clause inside of a WITH clause.
type MaterializeClause struct {
	// Set controls whether to use the Materialize bool instead of the default.
	Set bool

	// Materialize overrides the default materialization behavior.
	Materialize bool
}

// Format implements the NodeFormatter interface.
func (node *With) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	ctx.WriteString("WITH ")
	if node.Recursive {
		ctx.WriteString("RECURSIVE ")
	}
	for i, cte := range node.CTEList {
		if i != 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&cte.Name)
		ctx.WriteString(" AS ")
		if cte.Mtr.Set {
			if !cte.Mtr.Materialize {
				ctx.WriteString("NOT ")
			}
			ctx.WriteString("MATERIALIZED ")
		}
		ctx.WriteString("(")
		ctx.FormatNode(cte.Stmt)
		ctx.WriteString(")")
	}
	ctx.WriteByte(' ')
}
