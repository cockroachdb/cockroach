// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Export represents a EXPORT statement.
type Export struct {
	Query      *Select
	FileFormat string
	File       Expr
	Options    KVOptions
}

var _ Statement = &Export{}

// Format implements the NodeFormatter interface.
func (node *Export) Format(ctx *FmtCtx) {
	ctx.WriteString("EXPORT INTO ")
	ctx.WriteString(node.FileFormat)
	ctx.WriteString(" ")
	ctx.FormatURI(node.File)
	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS(")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
	ctx.WriteString(" FROM ")
	ctx.FormatNode(node.Query)
}
