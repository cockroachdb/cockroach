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

// Split represents an `ALTER TABLE/INDEX .. SPLIT AT ..` statement.
type Split struct {
	TableOrIndex TableIndexName
	// Each row contains values for the columns in the PK or index (or a prefix
	// of the columns).
	Rows *Select
	// Splits can last a specified amount of time before becoming eligible for
	// automatic merging.
	ExpireExpr Expr
}

// Format implements the NodeFormatter interface.
func (node *Split) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	ctx.WriteString(" SPLIT AT ")
	ctx.FormatNode(node.Rows)
	if node.ExpireExpr != nil {
		ctx.WriteString(" WITH EXPIRATION ")
		ctx.FormatNode(node.ExpireExpr)
	}
}

// Unsplit represents an `ALTER TABLE/INDEX .. UNSPLIT AT ..` statement.
type Unsplit struct {
	TableOrIndex TableIndexName
	// Each row contains values for the columns in the PK or index (or a prefix
	// of the columns).
	Rows *Select
	All  bool
}

// Format implements the NodeFormatter interface.
func (node *Unsplit) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	if node.All {
		ctx.WriteString(" UNSPLIT ALL")
	} else {
		ctx.WriteString(" UNSPLIT AT ")
		ctx.FormatNode(node.Rows)
	}
}

// Relocate represents an `ALTER TABLE/INDEX .. EXPERIMENTAL_RELOCATE ..`
// statement.
type Relocate struct {
	// TODO(a-robinson): It's not great that this can only work on ranges that
	// are part of a currently valid table or index.
	TableOrIndex TableIndexName
	// Each row contains an array with store ids and values for the columns in the
	// PK or index (or a prefix of the columns).
	// See docs/RFCS/sql_split_syntax.md.
	Rows          *Select
	RelocateLease bool
}

// Format implements the NodeFormatter interface.
func (node *Relocate) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	ctx.WriteString(" EXPERIMENTAL_RELOCATE ")
	if node.RelocateLease {
		ctx.WriteString("LEASE ")
	}
	ctx.FormatNode(node.Rows)
}

// Scatter represents an `ALTER TABLE/INDEX .. SCATTER ..`
// statement.
type Scatter struct {
	TableOrIndex TableIndexName
	// Optional from and to values for the columns in the PK or index (or a prefix
	// of the columns).
	// See docs/RFCS/sql_split_syntax.md.
	From, To Exprs
}

// Format implements the NodeFormatter interface.
func (node *Scatter) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	ctx.WriteString(" SCATTER")
	if node.From != nil {
		ctx.WriteString(" FROM (")
		ctx.FormatNode(&node.From)
		ctx.WriteString(") TO (")
		ctx.FormatNode(&node.To)
		ctx.WriteString(")")
	}
}
