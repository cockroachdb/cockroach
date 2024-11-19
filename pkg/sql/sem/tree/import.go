// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Import represents a IMPORT statement.
type Import struct {
	Table      *TableName
	Into       bool
	IntoCols   NameList
	FileFormat string
	Files      Exprs
	Bundle     bool
	Options    KVOptions
}

var _ Statement = &Import{}

// Format implements the NodeFormatter interface.
func (node *Import) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT ")

	if node.Bundle {
		if node.Table != nil {
			ctx.WriteString("TABLE ")
			ctx.FormatNode(node.Table)
			ctx.WriteString(" FROM ")
		}
		ctx.WriteString(node.FileFormat)
		ctx.WriteByte(' ')
		ctx.FormatURIs(node.Files)
	} else {
		if node.Into {
			ctx.WriteString("INTO ")
			ctx.FormatNode(node.Table)
			if node.IntoCols != nil {
				ctx.WriteByte('(')
				ctx.FormatNode(&node.IntoCols)
				ctx.WriteString(") ")
			} else {
				ctx.WriteString(" ")
			}
		} else {
			ctx.WriteString("TABLE ")
			ctx.FormatNode(node.Table)
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
	}

	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}
