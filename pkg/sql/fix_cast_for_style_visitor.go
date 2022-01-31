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
	err     error
	ctx     context.Context
	semaCtx *tree.SemaContext
	desc    *descpb.TableDescriptor
	typ     *types.T
}

var _ tree.Visitor = &FixCastForStyleVisitor{}

func (v *FixCastForStyleVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}

	return true, expr
}

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
			typedExpr, err := tree.TypeCheck(v.ctx, replacedExpr, v.semaCtx, v.typ)
			if err != nil {
				return expr
			}
			innerTyp := typedExpr.ResolvedType()

			var newExpr *tree.FuncExpr
			switch innerTyp.Family() {
			case types.StringFamily:
				if v.typ == types.Interval {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_interval"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
			case types.IntervalFamily, types.DateFamily, types.TimestampFamily, types.TimeFamily, types.TimeTZFamily:
				if v.typ == types.String {
					newExpr = &tree.FuncExpr{Func: tree.WrapFunction("to_char"), Exprs: tree.Exprs{expr.Expr}}
					return newExpr
				}
			}
		}
	}
	return expr
}

func MakeFixCastForStyleVisitor(
	err error,
	ctx context.Context,
	semaCtx *tree.SemaContext,
	desc *descpb.TableDescriptor,
	typ *types.T,
) FixCastForStyleVisitor {
	return FixCastForStyleVisitor{err: err, ctx: ctx, semaCtx: semaCtx, desc: desc, typ: typ}
}
