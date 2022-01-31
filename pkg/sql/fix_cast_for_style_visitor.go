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

// TODO: Reread & make sure that you explain that context-dependent operators are being tested even thought their blocked.

//FixCastForStyleVisitor is used to rewrite cast expressions that contain casts
// that cause formatting issues when DateStyle/IntervalStyle is enabled. The
// issue is caused by the volatility of the cast being stable.
// FixCastForStyleVisitor detects these cast and replaces them with a builtin that
// is an immutable version of the cast. There are casts that are disallowed due to them
// being context dependent, the visitor will still check for them and replace them. This
// is to account for the possibility these cast exist in an older version cluster.
//These casts are as follows: string::date, string::timestamp, string::timestamptz, string::time,
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
			"FixCastForStyleVisitor",
			v.semaCtx,
			tree.VolatilityImmutable,
			tree.NewUnqualifiedTableName(tree.Name(v.desc.GetName())),
		)
		if err != nil {
			_, innerTyp, _, err := schemaexpr.DequalifyAndValidateExpr(
				v.ctx,
				tDesc,
				expr.Expr,
				types.Any,
				"FixCastForStyleVisitor",
				v.semaCtx, tree.VolatilityStable,
				tree.NewUnqualifiedTableName(tree.Name(v.desc.GetName())),
			)
			if err != nil {
				v.err = err
				return expr
			}
			x := expr.String()
			print(x)
			var newExpr tree.Expr
			switch innerTyp.Family() {
			case types.StringFamily:
				if v.typ.Family() == types.IntervalFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_interval"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
				if v.typ.Family() == types.DateFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_date"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
				if v.typ.Family() == types.TimeFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_time"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
				if v.typ.Family() == types.TimeTZFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_timetz"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
				if v.typ.Family() == types.TimestampFamily {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_timestamp"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
				return expr
			case types.IntervalFamily, types.DateFamily, types.TimestampFamily, types.TimeFamily, types.TimeTZFamily, types.TimestampTZFamily:
				if v.typ.Family() == types.StringFamily {
					newExpr = &tree.CastExpr{
						Expr:       &tree.FuncExpr{Func: tree.WrapFunction("to_char"), Exprs: tree.Exprs{expr.Expr}},
						Type:       expr.Type,
						SyntaxMode: tree.CastShort,
					}
					return newExpr
				}
			}
		}
		v.err = err
	}

	return expr
}

// MakeFixCastForStyleVisitor creates a FixCastForStyleVisitor instance.
func MakeFixCastForStyleVisitor(
	ctx context.Context, semaCtx *tree.SemaContext, desc *descpb.TableDescriptor, typ *types.T,
) FixCastForStyleVisitor {
	return FixCastForStyleVisitor{ctx: ctx, semaCtx: semaCtx, desc: desc, typ: typ}
}
