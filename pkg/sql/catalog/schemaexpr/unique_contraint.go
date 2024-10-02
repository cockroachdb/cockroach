// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ValidateUniqueWithoutIndexPredicate verifies that an expression is a valid
// unique without index predicate. If the expression is valid, it returns the
// serialized expression with the columns dequalified.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include non-immutable, aggregate, window, or set returning
//     functions.
func ValidateUniqueWithoutIndexPredicate(
	ctx context.Context,
	tn tree.TableName,
	desc catalog.TableDescriptor,
	pred tree.Expr,
	semaCtx *tree.SemaContext,
	version clusterversion.ClusterVersion,
) (string, error) {
	expr, _, _, err := DequalifyAndValidateExpr(
		ctx,
		desc,
		pred,
		types.Bool,
		tree.UniqueWithoutIndexPredicateExpr,
		semaCtx,
		volatility.Immutable,
		&tn,
		version,
	)
	if err != nil {
		return "", err
	}
	return expr, nil
}
