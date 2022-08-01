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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// SpanConstraintRequirement indicates how strict span constraint logic should be.
type SpanConstraintRequirement int

const (
	// MustFullyConstrain indicates spans must be fully constrained.
	MustFullyConstrain SpanConstraintRequirement = iota
	// BestEffortConstrain allows constraint to partially satisfy predicate.
	BestEffortConstrain
)

// SpanConstrainer is an interface for constraining spans.
type SpanConstrainer interface {
	// ConstrainPrimaryIndexSpanByExpr constrains primary index span of the
	// table using specified filter expression.
	// Table name parameter is the name of the table used in the expression.
	// Returns constrained spans that satisfy the expression.
	// If the caller requires constraint to be MustFullyConstrain, but the
	// expression cannot be fully satisfied, returns an error.
	// The expression must be boolean expression.
	// If the expression is a contradiction, returns an error.
	ConstrainPrimaryIndexSpanByExpr(
		ctx context.Context,
		req SpanConstraintRequirement,
		tn *tree.TableName,
		desc catalog.TableDescriptor,
		evalCtx *eval.Context,
		semaCtx *tree.SemaContext,
		filter tree.Expr,
	) (_ []roachpb.Span, remainingFilter tree.Expr, _ error)
}

// ConstrainPrimaryIndexSpanByExpr implements SpanConstrainer
func (p *planner) ConstrainPrimaryIndexSpanByExpr(
	ctx context.Context,
	req SpanConstraintRequirement,
	tn *tree.TableName,
	desc catalog.TableDescriptor,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	filter tree.Expr,
) (_ []roachpb.Span, remainingFilter tree.Expr, _ error) {
	var oc optCatalog
	oc.init(p)
	oc.reset()

	tbl, err := newOptTable(desc, oc.codec(), nil /* stats */, emptyZoneConfig)
	if err != nil {
		return nil, nil, err
	}

	var nf norm.Factory
	nf.Init(evalCtx, &oc)
	nf.Metadata().AddTable(tbl, tn)

	b := optbuilder.NewScalar(ctx, semaCtx, evalCtx, &nf)
	if err := b.Build(filter); err != nil {
		return nil, nil, err
	}

	root := nf.Memo().RootExpr().(opt.ScalarExpr)
	if root.DataType() != types.Bool {
		return nil, nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"expected boolean expression, found expression of type %s", root.DataType())
	}

	fe := memo.FiltersExpr{nf.ConstructFiltersItem(root)}
	fe = nf.CustomFuncs().SimplifyFilters(fe)
	fe = nf.CustomFuncs().ConsolidateFilters(fe)

	if fe.IsTrue() {
		return []roachpb.Span{desc.PrimaryIndexSpan(oc.codec())}, tree.DBoolTrue, nil
	}
	if fe.IsFalse() {
		return nil, nil, errors.Newf("filter %q is a contradiction", filter)
	}

	primary := desc.GetPrimaryIndex()
	indexCols := make([]opt.OrderingColumn, len(primary.IndexDesc().KeyColumnIDs))
	var notNullIndexCols opt.ColSet
	for i, colID := range primary.IndexDesc().KeyColumnIDs {
		if primary.GetKeyColumnDirection(i) == catpb.IndexColumn_ASC {
			indexCols[i] = opt.OrderingColumn(colID)
		} else {
			indexCols[i] = opt.OrderingColumn(-colID)
		}
		notNullIndexCols.Add(opt.ColumnID(colID))
	}

	const consolidate = true
	var ic idxconstraint.Instance

	ic.Init(
		fe, nil, indexCols, notNullIndexCols, nil,
		consolidate, evalCtx, &nf, nil,
	)

	remaining := ic.RemainingFilters()
	if req == MustFullyConstrain {
		if !remaining.IsTrue() {
			err = errors.Newf(
				"primary key span %s cannot be fully constrained by expression %q",
				desc.PrimaryIndexSpan(oc.codec()), filter)
			if len(indexCols) > 1 {
				// Constraints over composite keys are hard.  Give a bit of a hint.
				err = errors.WithHint(err,
					"try constraining prefix columns of the composite key with equality or an IN clause")
			}
			return nil, nil, err
		}

		if ic.Constraint().IsUnconstrained() {
			return nil, nil, errors.Newf("filter %q is a tautology; use 'true' or omit constraint", filter)
		}
	}

	if ic.Constraint().IsContradiction() {
		return nil, nil, errors.Newf("filter %q is a contradiction", filter)
	}

	if remaining.IsTrue() {
		remainingFilter = tree.DBoolTrue
	} else {
		eb := execbuilder.New(newExecFactory(p), &p.optPlanningCtx.optimizer,
			nf.Memo(), &oc, &remaining, evalCtx, false)
		eb.SetBuiltinFuncWrapper(semaCtx.FunctionResolver)
		expr, err := eb.BuildScalar()
		if err != nil {
			return nil, nil, err
		}
		remainingFilter = replaceIndexedVarsWithColumnNames(tbl, tn, expr)
	}

	var sb span.Builder
	sb.Init(evalCtx, oc.codec(), desc, desc.GetPrimaryIndex())
	spans, err := sb.SpansFromConstraint(ic.Constraint(), span.NoopSplitter())
	if err != nil {
		return nil, nil, err
	}
	return spans, remainingFilter, nil
}

type replaceIndexedVars struct {
	tbl *optTable
	tn  *tree.UnresolvedObjectName
}

var _ tree.Visitor = (*replaceIndexedVars)(nil)

func replaceIndexedVarsWithColumnNames(
	tbl *optTable, alias *tree.TableName, expr tree.Expr,
) tree.Expr {
	var tn *tree.UnresolvedObjectName
	if alias.Table() != "" {
		tn = alias.ToUnresolvedObjectName()
	}
	v := replaceIndexedVars{tbl: tbl, tn: tn}
	expr, _ = tree.WalkExpr(&v, expr)
	return expr
}

func (v *replaceIndexedVars) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	return true, expr
}

func (v *replaceIndexedVars) VisitPost(expr tree.Expr) tree.Expr {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return &tree.ColumnItem{ColumnName: v.tbl.Column(t.Idx).ColName(), TableName: v.tn}
	default:
		return expr
	}
}
