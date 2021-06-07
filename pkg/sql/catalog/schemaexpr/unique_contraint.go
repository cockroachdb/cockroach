// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
//
func ValidateUniqueWithoutIndexPredicate(
	ctx context.Context,
	tn tree.TableName,
	desc catalog.TableDescriptor,
	pred tree.Expr,
	semaCtx *tree.SemaContext,
) (string, error) {
	expr, _, _, err := DequalifyAndValidateExpr(
		ctx,
		desc,
		pred,
		types.Bool,
		"unique without index predicate",
		semaCtx,
		tree.VolatilityImmutable,
		&tn,
	)
	if err != nil {
		return "", err
	}
	return expr, nil
}
