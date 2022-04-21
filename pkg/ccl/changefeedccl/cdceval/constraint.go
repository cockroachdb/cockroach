// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ConstrainPrimaryIndexSpanByFilter attempts to constrain table primary index span via specified
// filter.  Returns constrained span, and a possibly empty remaining filter that needs to be evaluated.
func ConstrainPrimaryIndexSpanByFilter(
	ctx context.Context, execCtx sql.JobExecContext, filterStr string, descr catalog.TableDescriptor,
) ([]roachpb.Span, string, error) {
	if filterStr == "" {
		return nil, "", errors.AssertionFailedf("unexpected empty filter")
	}

	filterExpr, err := parser.ParseExpr(filterStr)
	if err != nil {
		return nil, "", pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"filter expression %q must be a valid SQL expression", filterStr)
	}

	semaCtx := newSemaCtx()
	tableName := tree.NewUnqualifiedTableName(tree.Name(descr.GetName()))
	spans, remainingFilter, err := execCtx.ConstrainPrimaryIndexSpanByExpr(
		ctx, sql.BestEffortConstrain, tableName, descr, &execCtx.ExtendedEvalContext().Context, semaCtx, filterExpr)
	if err != nil {
		return nil, "", err
	}
	if remainingFilter == tree.DBoolTrue {
		return spans, "", nil
	}
	return spans, tree.AsStringWithFQNames(remainingFilter, &semaCtx.Annotations), nil
}
