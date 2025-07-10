// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

type AlterExternalConnection struct {
	IfExists            bool
	ConnectionLabelSpec LabelSpec
	As                  Expr
}

func (node *AlterExternalConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER EXTERNAL CONNECTION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.ConnectionLabelSpec)
	ctx.WriteString(" AS ")
	ctx.FormatURI(node.As)
}
