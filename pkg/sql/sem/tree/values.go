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

import "github.com/cockroachdb/cockroach/pkg/col/coldata"

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
	// Get returns the Expr at row i column j.
	Get(i, j int) Expr
}

// RawRows exposes a [][]TypedExpr as an ExprContainer.
type RawRows [][]TypedExpr

var _ ExprContainer = RawRows{}

// NumRows implements the ExprContainer interface.
func (r RawRows) NumRows() int {
	return len(r)
}

// NumCols implements the ExprContainer interface.
func (r RawRows) NumCols() int {
	return len(r[0])
}

// Get implements the ExprContainer interface.
func (r RawRows) Get(i, j int) Expr {
	return r[i][j]
}

// LiteralValuesClause is like ValuesClause but values have been typed checked
// and evaluated and are assumed to be ready to use Datums.
type LiteralValuesClause struct {
	Rows ExprContainer
}

// VectorRows lets us store a Batch in a tree.LiteralValuesClause.
type VectorRows struct {
	Batch coldata.Batch
}

// NumRows implements the ExprContainer interface.
func (r VectorRows) NumRows() int {
	return r.Batch.Length()
}

// NumCols implements the ExprContainer interface.
func (r VectorRows) NumCols() int {
	return r.Batch.Width()
}

// Get implements the ExprContainer interface.
func (r VectorRows) Get(i, j int) Expr {
	return DNull
}

var _ ExprContainer = VectorRows{}

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
