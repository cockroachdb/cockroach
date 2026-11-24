// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Import represents a IMPORT statement.
type Import struct {
	Table      *TableName
	IntoCols   NameList
	FileFormat string
	Files      Exprs
	Options    KVOptions
}

var _ Statement = &Import{}

// Format implements the NodeFormatter interface.
func (node *Import) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT ")

	ctx.WriteString("INTO ")
	ctx.FormatNode(node.Table)
	if node.IntoCols != nil {
		ctx.WriteByte('(')
		ctx.FormatNode(&node.IntoCols)
		ctx.WriteString(") ")
	} else {
		ctx.WriteString(" ")
	}
	ctx.WriteString(node.FileFormat)
	ctx.WriteString(" DATA ")
	if len(node.Files) == 1 {
		ctx.WriteString("(")
	}
	ctx.FormatURIs(node.Files)
	if len(node.Files) == 1 {
		ctx.WriteString(")")
	}

	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}
