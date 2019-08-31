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

// Import represents a IMPORT statement.
type Import struct {
	Table      *TableName
	Into       bool
	IntoCols   NameList
	CreateFile Expr
	CreateDefs TableDefs
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
		ctx.FormatNode(&node.Files)
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

			if node.CreateFile != nil {
				ctx.WriteString(" CREATE USING ")
				ctx.FormatNode(node.CreateFile)
				ctx.WriteString(" ")
			} else {
				ctx.WriteString(" (")
				ctx.FormatNode(&node.CreateDefs)
				ctx.WriteString(") ")
			}
		}
		ctx.WriteString(node.FileFormat)
		ctx.WriteString(" DATA (")
		ctx.FormatNode(&node.Files)
		ctx.WriteString(")")
	}

	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}
