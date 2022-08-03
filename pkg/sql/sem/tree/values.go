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

// ValuesClause represents a VALUES clause.
type ValuesClause struct {
	Rows []Exprs
}

// ExprContainer represents an abstract container of Exprs
type ExprContainer interface {
	// NumRows returns number of rows.
	NumRows() int
	// NumCols returns number of columns.
	NumCols() int
	//Get returns the Expr at row i column j.
	Get(i, j int) Expr
}

// RawRows exposes a [][]TypedExpr as an ExprContainer.
type RawRows struct {
	Rows [][]TypedExpr
}

// NumRows implements the ExprContainer interface.
func (r *RawRows) NumRows() int {
	return len(r.Rows)
}

// NumCols implements the ExprContainer interface.
func (r *RawRows) NumCols() int {
	return len(r.Rows[0])
}

// Get implements the ExprContainer interface.
func (r *RawRows) Get(i, j int) Expr {
	return r.Rows[i][j]
}

// LiteralValuesClause is like ValuesClause but values have been typed checked
// and evaluated and are assumed to be ready to use Datums.
type LiteralValuesClause struct {
	Rows ExprContainer
}

// Format implements the NodeFormatter interface.
func (node *ValuesClause) Format(ctx *FmtCtx) {
	ctx.WriteString("VALUES ")
	comma := ""
	for i := range node.Rows {
		ctx.WriteString(comma)
		ctx.WriteByte('(')
		ctx.FormatNode(&node.Rows[i])
		ctx.WriteByte(')')
		comma = ", "
	}
}

// Format implements the NodeFormatter interface.
func (node *LiteralValuesClause) Format(ctx *FmtCtx) {
	ctx.WriteString("VALUES ")
	comma := ""
	for i := 0; i < node.Rows.NumRows(); i++ {
		ctx.WriteString(comma)
		ctx.WriteByte('(')
		comma2 := ""
		for j := 0; j < node.Rows.NumCols(); j++ {
			ctx.WriteString(comma2)
			ctx.FormatNode(node.Rows.Get(i, j))
			comma2 = ", "
		}
		ctx.WriteByte(')')
		comma = ", "
	}
}
