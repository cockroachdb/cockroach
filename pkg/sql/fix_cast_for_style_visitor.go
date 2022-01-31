package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

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
		_, typ, _, err := schemaexpr.DequalifyAndValidateExpr(
			v.ctx,
			tDesc,
			expr,
			v.typ,
			"DateStyle visitor",
			v.semaCtx,
			tree.VolatilityStable,
			tree.NewUnqualifiedTableName(tree.Name(v.desc.GetName())),
		)
		if err != nil {
			return expr
		}

		var newExpr *tree.FuncExpr
		switch v := typ; v {
		case types.String:
			newExpr = &tree.FuncExpr{Func: tree.WrapFunction("to_char"), Exprs: tree.Exprs{expr.Expr}}
			return newExpr
		case types.Interval:
			newExpr = &tree.FuncExpr{Func: tree.WrapFunction("parse_interval"), Exprs: tree.Exprs{expr.Expr}}
			return newExpr
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
