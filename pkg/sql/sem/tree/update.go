// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

// Update represents an UPDATE statement.
type Update struct {
	With      *With
	Table     TableExpr
	Exprs     UpdateExprs
	From      TableExprs
	Where     *Where
	OrderBy   OrderBy
	Limit     *Limit
	Returning ReturningClause
}

// Format implements the NodeFormatter interface.
func (node *Update) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	ctx.WriteString("UPDATE ")
	ctx.FormatNode(node.Table)
	ctx.WriteString(" SET ")
	ctx.FormatNode(&node.Exprs)
	if len(node.From) > 0 {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.From)
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

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

// Format implements the NodeFormatter interface.
func (node *UpdateExprs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Tuple bool
	Names NameList
	Expr  Expr
}

// Format implements the NodeFormatter interface.
func (node *UpdateExpr) Format(ctx *FmtCtx) {
	open, close := "", ""
	if node.Tuple {
		open, close = "(", ")"
	}
	ctx.WriteString(open)
	ctx.FormatNode(&node.Names)
	ctx.WriteString(close)
	ctx.WriteString(" = ")
	ctx.FormatNode(node.Expr)
}
