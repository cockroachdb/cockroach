// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// CopyFrom represents a COPY FROM statement.
type CopyFrom struct {
	Table   TableName
	Columns NameList
	Stdin   bool
	Options KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CopyFrom) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	ctx.FormatNode(&node.Table)
	if len(node.Columns) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Columns)
		ctx.WriteString(")")
	}
	ctx.WriteString(" FROM ")
	if node.Stdin {
		ctx.WriteString("STDIN")
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}
