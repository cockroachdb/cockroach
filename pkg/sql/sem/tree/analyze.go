// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Analyze represents an ANALYZE statement.
type Analyze struct {
	Table TableExpr
}

// Format implements the NodeFormatter interface.
func (node *Analyze) Format(ctx *FmtCtx) {
	ctx.WriteString("ANALYZE ")
	ctx.FormatNode(node.Table)
}
