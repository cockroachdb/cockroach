// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
	ctx.FormatNode(node.File)
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
	ctx.WriteString(" FROM ")
	ctx.FormatNode(node.Query)
}
