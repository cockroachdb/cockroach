// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// FixCastForStyleVisitor is used to rewrite cast expressions that contain cast
// that cause formatting issues when DateStyle/IntervalStyle is enabled. The
// issue is caused by the volatility of the cast being stable.
// FixCastForStyleVisitor detects these cast and wrap them in a builtin that
// contains an immutable version of the cast. The visitor only checks for string
// to interval and various date/interval types to string cast. This is because
// cast that we thought would cause issues with DateStyle/IntervalStyle are
// already blocked in computed columns and partial indexes. These casts are as
// follows: string::date, string::timestamp, string::timestamptz, string::time,
// timestamptz::string, string::timetz
type FixCastForStyleVisitor struct {
	ctx     context.Context
	semaCtx *tree.SemaContext
	desc    *descpb.TableDescriptor
	typ     *types.T
	err     error
}

var _ tree.Visitor = &FixCastForStyleVisitor{}

// VisitPre implements the Visitor interface.
func (v *FixCastForStyleVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *FixCastForStyleVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}

	if expr, ok := expr.(*tree.CastExpr); ok {
		desc := tabledesc.NewBuilder(v.desc)
		tDesc := desc.BuildImmutableTable()

		_, _, _, err := schemaexpr.DequalifyAndValidateExpr(
			v.ctx,
			tDesc,
			expr,
			v.typ,
			"DateStyle visitor",
			v.semaCtx,
			tree.VolatilityImmutable,
			tree.NewUnqualifiedTableName(tree.Name(v.desc.GetName())),
		)
		if err != nil {
			replacedExpr, err := schemaexpr.MakeDummyColForTypeCheck(v.ctx, tDesc, expr.Expr, tree.NewUnqualifiedTableName(tree.Name(v.desc.GetName())))
			if err != nil {
				return expr
			}
			typedExpr, err := tree.TypeCheck(v.ctx, replacedExpr, v.semaCtx, v.typ)
			if err != nil {
				return expr
			}
			innerTyp := typedExpr.ResolvedType()

			var newExpr *tree.FuncExpr
			switch innerTyp.Family() {
			case types.StringFamily:
				if v.typ.Family() == types.IntervalFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_interval"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
			case types.IntervalFamily, types.DateFamily, types.TimestampFamily:
				if v.typ.Family() == types.StringFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("to_char"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
			}
		}
	}
	return expr
}

// MakeFixCastForStyleVisitor creates a FixCastForStyleVisitor instance.
func MakeFixCastForStyleVisitor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	desc *descpb.TableDescriptor,
	typ *types.T,
	err error,
) FixCastForStyleVisitor {
	return FixCastForStyleVisitor{ctx: ctx, semaCtx: semaCtx, desc: desc, typ: typ, err: err}
}
