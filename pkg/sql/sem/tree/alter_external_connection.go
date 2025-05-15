// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterExternalConnection represents a ALTER EXTERNAL CONNECTION statement.
type AlterExternalConnection struct {
	ConnectionLabelSpec LabelSpec
	As                  Expr
}

var _ Statement = &AlterExternalConnection{}

// Format implements the Statement interface.
func (node *AlterExternalConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER EXTERNAL CONNECTION")
	ctx.FormatNode(&node.ConnectionLabelSpec)
	ctx.WriteString(" AS ")
	ctx.FormatURI(node.As)
}
