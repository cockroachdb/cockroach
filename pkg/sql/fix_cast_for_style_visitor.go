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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// fixCastForStyleVisitor is used to rewrite cast expressions that contain casts
// that cause formatting issues when DateStyle/IntervalStyle is enabled. The
// issue is caused by the volatility of the cast being stable. The expression
// passed into fixCastForStyleVisitor must be a TypedExpr.
// fixCastForStyleVisitor replaces these casts with a builtin that is an
// immutable version of the cast. There are casts that are disallowed due to
// them being context dependent, the visitor will still check for them and
// replace them. This is to account for the possibility these casts exist in an
// older cluster. These casts are as follows: string::date, string::timestamp,
// string::timestamptz, string::time, timestamptz::string, string::timetz
type fixCastForStyleVisitor struct {
	ctx     context.Context
	semaCtx *tree.SemaContext
	tDesc   catalog.TableDescriptor
	err     error
}

var _ tree.Visitor = &fixCastForStyleVisitor{}

// VisitPre implements the Visitor interface.
func (v *fixCastForStyleVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	_, _, _, err := schemaexpr.DequalifyAndValidateExpr(
		v.ctx,
		v.tDesc,
		expr,
		types.Any,
		"fixCastForStyleVisitor",
		v.semaCtx,
		tree.VolatilityImmutable,
		tree.NewUnqualifiedTableName(tree.Name(v.tDesc.GetName())),
	)
	if err == nil {
		return false, expr
	}

	return true, expr
}

// VisitPost implements the Visitor interface.
func (v *fixCastForStyleVisitor) VisitPost(expr tree.Expr) tree.Expr {
	if v.err != nil {
		return expr
	}

	if expr, ok := expr.(*tree.CastExpr); ok {
		sd := sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{
				IntervalStyleEnabled: v.semaCtx.IntervalStyleEnabled,
				DateStyleEnabled:     v.semaCtx.DateStyleEnabled,
			},
		}
		innerExpr := expr.Expr.(tree.TypedExpr)
		outerTyp := expr.ResolvedType()
		innerTyp := innerExpr.ResolvedType()
		volatility, ok := tree.LookupCastVolatility(innerTyp, outerTyp, &sd)
		if !ok {
			v.err = errors.AssertionFailedf("Not a valid cast %s -> %s", innerTyp.SQLString(), outerTyp.SQLString())
		}
		if volatility <= tree.VolatilityImmutable {
			return expr
		}

		var newExpr tree.Expr
		switch innerTyp.Family() {
		case types.StringFamily:
			switch outerTyp.Family() {
			case types.IntervalFamily:
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("parse_interval"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			case types.DateFamily:
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("parse_date"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			case types.TimeFamily:
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("parse_time"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			case types.TimeTZFamily:
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("parse_timetz"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			case types.TimestampFamily:
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("parse_timestamp"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			}
		case types.IntervalFamily, types.DateFamily, types.TimestampFamily, types.TimeFamily, types.TimeTZFamily, types.TimestampTZFamily:
			if outerTyp.Family() == types.StringFamily {
				newExpr = &tree.CastExpr{
					Expr:       &tree.FuncExpr{Func: tree.WrapFunction("to_char"), Exprs: tree.Exprs{expr.Expr}},
					Type:       expr.Type,
					SyntaxMode: tree.CastShort,
				}
				return newExpr
			}
		}
	}
	return expr
}

// makeFixCastForStyleVisitor creates a fixCastForStyleVisitor instance.
func makeFixCastForStyleVisitor(
	ctx context.Context, semaCtx *tree.SemaContext,
) fixCastForStyleVisitor {
	return fixCastForStyleVisitor{ctx: ctx, semaCtx: semaCtx}
}

// ResolveCastForStyleUsingVisitor checks expression for stable cast that affect
// DateStyle/IntervalStyle and rewrites them.
func ResolveCastForStyleUsingVisitor(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	desc *descpb.TableDescriptor,
	expr tree.Expr,
	tn *tree.TableName,
) (tree.Expr, bool, error) {
	v := makeFixCastForStyleVisitor(ctx, semaCtx)

	descBuilder := tabledesc.NewBuilder(desc)
	tDesc := descBuilder.BuildImmutableTable()
	v.tDesc = tDesc

	typedExpr, err := schemaexpr.DequalifyAndTypeCheckExpr(ctx, tDesc, expr, semaCtx, tn)
	if err != nil {
		v.err = err
		return nil, false, err
	}
	expr, changed := tree.WalkExpr(&v, typedExpr)
	return expr, changed, v.err
}
