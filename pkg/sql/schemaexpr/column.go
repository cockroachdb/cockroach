// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// DequalifyColumnRefs returns an expression with database nad table names
// stripped from qualified column names.
//
// For example:
//
//   tab.a > 0 AND db.tab.b = 'foo'
//   =>
//   a > 0 AND b = 'foo'
//
// This dequalification is necessary when CHECK constraints, computed columns,
// or partial index predicates are created. If the table name was not stripped,
// these expressions would become invalid if the table is renamed.
func DequalifyColumnRefs(
	ctx context.Context, source *sqlbase.DataSourceInfo, expr tree.Expr,
) (tree.Expr, error) {
	resolver := sqlbase.ColumnResolver{Source: source}
	return tree.SimpleVisit(
		expr,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			if vBase, ok := expr.(tree.VarName); ok {
				v, err := vBase.NormalizeVarName()
				if err != nil {
					return false, nil, err
				}
				if c, ok := v.(*tree.ColumnItem); ok {
					_, err := c.Resolve(ctx, &resolver)
					if err != nil {
						return false, nil, err
					}
					colIdx := resolver.ResolverState.ColIdx
					col := source.SourceColumns[colIdx]
					return false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}, nil
				}
			}
			return true, expr, err
		},
	)
}

// iterColDescriptors iterates over the expression's variable columns and
// calls f on each.
//
// If the expression references a column that does not exist in the table
// descriptor, iterColDescriptors errs with pgcode.UndefinedColumn.
func iterColDescriptors(
	desc *sqlbase.MutableTableDescriptor, rootExpr tree.Expr, f func(*sqlbase.ColumnDescriptor) error,
) error {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, pgerror.Newf(pgcode.UndefinedColumn,
				"column %q does not exist, referenced in %q", c.ColumnName, rootExpr.String())
		}

		if err := f(col); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	return err
}

// DeserializeTableDescExpr takes in a serialized expression and a table, and
// returns an expression that has all user defined types resolved for
// formatting. It is intended to be used when displaying a serialized
// expression within a TableDescriptor. For example, a DEFAULT expression
// of a table containing a user defined type t with id 50 would have all
// references to t replaced with the id 50. In order to display this expression
// back to an end user, the serialized id 50 needs to be replaced back with t.
// DeserializeTableDescExpr performs this logic, but only returns a
// tree.Expr to be clear that these returned expressions are not safe to Eval.
func DeserializeTableDescExpr(
	ctx context.Context, semaCtx *tree.SemaContext, desc *sqlbase.TableDescriptor, exprStr string,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}
	expr, _, err = desc.ReplaceColumnVarsInExprWithDummies(expr)
	if err != nil {
		return nil, err
	}
	typed, err := expr.TypeCheck(ctx, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}
	return typed, nil
}

// FormatColumnForDisplay formats a column descriptor as a SQL string, and displays
// user defined types in serialized expressions with human friendly formatting.
func FormatColumnForDisplay(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	tbl *sqlbase.TableDescriptor,
	desc *sqlbase.ColumnDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.FormatNameP(&desc.Name)
	f.WriteByte(' ')
	f.WriteString(desc.Type.SQLString())
	if desc.Nullable {
		f.WriteString(" NULL")
	} else {
		f.WriteString(" NOT NULL")
	}
	if desc.DefaultExpr != nil {
		f.WriteString(" DEFAULT ")
		typed, err := DeserializeTableDescExpr(ctx, semaCtx, tbl, *desc.DefaultExpr)
		if err != nil {
			return "", err
		}
		f.WriteString(tree.SerializeForDisplay(typed))
	}
	if desc.IsComputed() {
		f.WriteString(" AS (")
		typed, err := DeserializeTableDescExpr(ctx, semaCtx, tbl, *desc.ComputeExpr)
		if err != nil {
			return "", err
		}
		f.WriteString(tree.SerializeForDisplay(typed))
		f.WriteString(") STORED")
	}
	return f.CloseAndGetString(), nil
}
