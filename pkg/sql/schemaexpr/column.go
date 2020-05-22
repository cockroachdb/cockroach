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
	expr, _, err = replaceVars(desc, expr)
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

// dummyColumn represents a variable column that can type-checked. It is used
// in validating check constraint and partial index predicate expressions. This
// validation requires that the expression can be both both typed-checked and
// examined for variable expressions.
type dummyColumn struct {
	typ  *types.T
	name tree.Name
}

// String implements the Stringer interface.
func (d *dummyColumn) String() string {
	return tree.AsString(d)
}

// Format implements the NodeFormatter interface.
func (d *dummyColumn) Format(ctx *tree.FmtCtx) {
	d.name.Format(ctx)
}

// Walk implements the Expr interface.
func (d *dummyColumn) Walk(_ tree.Visitor) tree.Expr {
	return d
}

// TypeCheck implements the Expr interface.
func (d *dummyColumn) TypeCheck(
	_ context.Context, _ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return d, nil
}

// Eval implements the TypedExpr interface.
func (*dummyColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("dummyColumnItem.Eval() is undefined")
}

// ResolvedType implements the TypedExpr interface.
func (d *dummyColumn) ResolvedType() *types.T {
	return d.typ
}

// replaceVars replaces the occurrences of column names in an expression with
// dummyColumns containing their type, so that they may be type-checked. It
// returns this new expression tree alongside a set containing the ColumnID of
// each column seen in the expression.
//
// If the expression references a column that does not exist in the table
// descriptor, replaceVars errs with pgcode.UndefinedColumn.
func replaceVars(
	desc *sqlbase.TableDescriptor, rootExpr tree.Expr,
) (tree.Expr, sqlbase.TableColSet, error) {
	var colIDs sqlbase.TableColSet

	newExpr, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
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

		// Check if a column is being referenced from another table. It may be the
		// case where the column being referenced has the same name as a column
		// in our current table.
		if c.TableName != nil {
			// If NumParts > 1, a database or schema name was provided.
			// To disallow cross database / schema column references, we don't
			// allow database and schema identifiers with the column name.
			if c.TableName.NumParts > 1 {
				return false, nil,
					pgerror.Newf(pgcode.InvalidColumnReference,
						"cannot reference columns with a specified database or schema")
			}
			name := c.TableName.ToTableName()
			if name.Table() != desc.Name {
				return false, nil,
					pgerror.Newf(pgcode.InvalidColumnReference,
						"cannot reference columns from other tables, "+
							"tried to reference column from table %s", name.String())
			}
		}

		col, dropped, err := desc.FindColumnByName(c.ColumnName)
		if err != nil || dropped {
			return false, nil, pgerror.Newf(pgcode.UndefinedColumn,
				"column %q does not exist, referenced in %q", c.ColumnName, rootExpr.String())
		}
		colIDs.Add(col.ID)

		// Convert to a dummyColumn of the correct type.
		return false, &dummyColumn{typ: col.Type, name: c.ColumnName}, nil
	})

	return newExpr, colIDs, err
}

// ReplaceColumnVarsAndSanitizeExpr takes an expr and replaces all column
// variables with a dummyColumn of the column's type and then sanitizes
// the expression.
func ReplaceColumnVarsAndSanitizeExpr(
	ctx context.Context,
	desc *sqlbase.TableDescriptor,
	expr tree.Expr,
	types *types.T,
	op string,
	semaCtx *tree.SemaContext,
	allowImpure bool,
) (tree.TypedExpr, sqlbase.TableColSet, error) {
	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, colIDs, err := replaceVars(desc, expr)
	if err != nil {
		return nil, colIDs, err
	}

	typedExpr, err := sqlbase.SanitizeVarFreeExpr(
		ctx,
		replacedExpr,
		types,
		op,
		semaCtx,
		allowImpure,
	)

	if err != nil {
		return nil, colIDs, err
	}

	return typedExpr, colIDs, nil
}
