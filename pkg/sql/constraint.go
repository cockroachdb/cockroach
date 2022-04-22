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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
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

// SpanConstrainer is an interface for constraining spans.
type SpanConstrainer interface {
	// ConstrainPrimaryIndexSpanByExpr constrains primary index span using
	// specified filter expression.
	// Returns constrained spans that fully satisfy the expression.
	// If the expression cannot be fully satisfied, returns an error.
	// Expression is required to specify constraints over unqualified
	// primary key columns only. The expression must be boolean expression.
	// If the expression is a contradiction, returns an error.
	ConstrainPrimaryIndexSpanByExpr(
		ctx context.Context,
		desc catalog.TableDescriptor,
		evalCtx *eval.Context,
		semaCtx *tree.SemaContext,
		filter tree.Expr,
	) ([]roachpb.Span, error)
}

// ConstrainPrimaryIndexSpanByExpr implements SpanConstrainer
func (p *planner) ConstrainPrimaryIndexSpanByExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	filter tree.Expr,
) ([]roachpb.Span, error) {
	var oc optCatalog
	oc.init(p)
	oc.reset()

	tbl, err := newOptTable(desc, oc.codec(), nil /* stats */, emptyZoneConfig)
	if err != nil {
		return nil, err
	}

	var nf norm.Factory
	nf.Init(evalCtx, &oc)
	nf.Metadata().AddTable(tbl, &tree.TableName{})

	b := optbuilder.NewScalar(ctx, semaCtx, evalCtx, &nf)
	if err := b.Build(filter); err != nil {
		return nil, err
	}

	root := nf.Memo().RootExpr().(opt.ScalarExpr)
	if root.DataType() != types.Bool {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"expected boolean expression, found expression of type %s", root.DataType())
	}

	fe := memo.FiltersExpr{nf.ConstructFiltersItem(root)}
	fe = nf.CustomFuncs().SimplifyFilters(fe)
	fe = nf.CustomFuncs().ConsolidateFilters(fe)

	if fe.IsTrue() {
		return []roachpb.Span{desc.PrimaryIndexSpan(oc.codec())}, nil
	}
	if fe.IsFalse() {
		return nil, errors.Newf("filter %q is a contradiction", filter)
	}

	primary := desc.GetPrimaryIndex()
	indexCols := make([]opt.OrderingColumn, len(primary.IndexDesc().KeyColumnIDs))
	var notNullIndexCols opt.ColSet
	for i, colID := range primary.IndexDesc().KeyColumnIDs {
		if primary.GetKeyColumnDirection(i) == descpb.IndexDescriptor_ASC {
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

	if !ic.RemainingFilters().IsTrue() {
		err = errors.Newf(
			"primary key span %s cannot be fully constrained by expression %q",
			desc.PrimaryIndexSpan(oc.codec()), filter)
		if len(indexCols) > 1 {
			// Constraints over composite keys are hard.  Give a bit of a hint.
			err = errors.WithHint(err,
				"try constraining prefix columns of the composite key with equality or an IN clause")
		}
		return nil, err
	}

	if ic.Constraint().IsContradiction() {
		return nil, errors.Newf("filter %q is a contradiction", filter)
	}
	if ic.Constraint().IsUnconstrained() {
		return nil, errors.Newf("filter %q is a tautology; use 'true' or omit constraint", filter)
	}

	var sb span.Builder
	sb.Init(evalCtx, oc.codec(), desc, desc.GetPrimaryIndex())
	return sb.SpansFromConstraint(ic.Constraint(), span.NoopSplitter())
}
