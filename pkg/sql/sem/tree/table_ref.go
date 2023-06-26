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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID = catid.ColumnID

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID = catid.ColumnID

// TableRef represents a numeric table reference.
// (Syntax !NNN in SQL.)
type TableRef struct {
	// TableID is the descriptor ID of the requested table.
	TableID int64

	// ColumnIDs is the list of column IDs requested in the table.
	// Note that a nil array here means "unspecified" (all columns)
	// whereas an array of length 0 means "zero columns".
	// Lists of zero columns are not supported and will throw an error.
	Columns []ColumnID

	// As determines the names that can be used in the surrounding query
	// to refer to this source.
	As AliasClause
}

// Format implements the NodeFormatter interface.
func (n *TableRef) Format(ctx *FmtCtx) {
	ctx.Printf("[%d", n.TableID)
	if n.Columns != nil {
		ctx.WriteByte('(')
		for i, c := range n.Columns {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.Printf("%d", c)
		}
		ctx.WriteByte(')')
	}
	if n.As.Alias != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&n.As)
	}
	ctx.WriteByte(']')
}
func (n *TableRef) String() string { return AsString(n) }

// TableIDRef represents a table by its descriptor ID.
type TableIDRef struct {
	ID int64
}

func (expr *TableIDRef) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("{TABLE:%v}", expr.ID))
}

func (expr *TableIDRef) String() string {
	return AsString(expr)
}

func (expr *TableIDRef) WalkTableExpr(visitor Visitor) TableExpr {
	newExpr, changed := WalkExpr(visitor, &TableIDRefExpr{TableIDRef: *expr})
	if changed {
		switch t := newExpr.(type) {
		case *TableIDRefExpr:
			return &t.TableIDRef
		case *TableNameExpr:
			return &t.TableName
		default:
			panic("TableIDRef cannot be changed to other types other than TableIDRef and TableName")
		}
	}
	return expr
}

// TableNameExpr is a wrapper of TableName implementing the Expr interface. It's
// being used as a helper for UDF/View query rewriting where the expression need
// to be walked by WalkExpr() function. Wrapping TableName because we can't make
// TableName implement Expr since all Exprs are parenthesized in
// FmtAlwaysGroupExprs with formatting flag and parenthesized TableName cannot
// be parsed as valid syntax.
type TableNameExpr struct {
	TableName
}

// TableIDRefExpr is a wrapper of TableIDRef implementing the Expr interface.
// It's being used as a helper for UDF/View query rewriting where the expression
// need to be walked by WalkExpr() function. Wrapping TableIDRef because we
// can't make TableIDRef implement Expr since all Exprs are parenthesized in
// FmtAlwaysGroupExprs with formatting flag and parenthesized TableIDRef cannot
// be parsed as valid syntax.
type TableIDRefExpr struct {
	TableIDRef
}

// Format implements the NodeFormatter interface.
func (expr *TableIDRefExpr) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("{TABLE:%v}", expr.ID))
}

func (expr *TableIDRefExpr) String() string {
	return AsString(expr)
}

// ColumnNameRef represent a column prefixed with a table id reference.
type ColumnNameRef struct {
	Table      *TableIDRef
	ColumnName Name
}

// Format implements the NodeFormatter interface.
func (expr *ColumnNameRef) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf("{TABLE:%d}.", expr.Table.ID))
	expr.ColumnName.Format(ctx)
}

func (expr *ColumnNameRef) String() string {
	return AsString(expr)
}
