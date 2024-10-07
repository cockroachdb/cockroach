// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

package tree

// Delete represents a DELETE statement.
type Delete struct {
	Batch     *Batch
	With      *With
	Table     TableExpr
	Where     *Where
	OrderBy   OrderBy
	Using     TableExprs
	Limit     *Limit
	Returning ReturningClause
}

// Format implements the NodeFormatter interface.
func (node *Delete) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	ctx.WriteString("DELETE ")
	ctx.FormatNode(node.Batch)
	ctx.WriteString("FROM ")
	ctx.FormatNode(node.Table)
	if len(node.Using) > 0 {
		ctx.WriteString(" USING ")
		ctx.FormatNode(&node.Using)
	}
	if node.Where != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Where)
	}
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.OrderBy)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Limit)
	}
	if HasReturningClause(node.Returning) {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Returning)
	}
}
