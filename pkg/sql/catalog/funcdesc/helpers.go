// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

var schemaExprContextAllowingUDF = map[tree.SchemaExprContext]clusterversion.Key{
	tree.CheckConstraintExpr:           clusterversion.MinSupported,
	tree.ColumnDefaultExprInNewTable:   clusterversion.MinSupported,
	tree.ColumnDefaultExprInSetDefault: clusterversion.MinSupported,

	tree.PolicyUsingExpr:     clusterversion.V25_2,
	tree.PolicyWithCheckExpr: clusterversion.V25_2,

	tree.ColumnDefaultExprInAddColumn: clusterversion.V25_3,
	tree.ColumnDefaultExprInNewView:   clusterversion.V25_3,
	tree.ColumnOnUpdateExpr:           clusterversion.V25_3,
	tree.ExpressionIndexElementExpr:   clusterversion.V25_3,
	tree.IndexPredicateExpr:           clusterversion.V25_3,
	tree.StoredComputedColumnExpr:     clusterversion.V25_3,
	tree.VirtualComputedColumnExpr:    clusterversion.V25_3,
}

// MaybeFailOnUDFUsage returns an error if the given expression or any
// sub-expression used a UDF unless it's explicitly listed as an allowed use
// case.
func MaybeFailOnUDFUsage(
	expr tree.TypedExpr, exprContext tree.SchemaExprContext, version clusterversion.ClusterVersion,
) error {
	if supportedVersion, ok := schemaExprContextAllowingUDF[exprContext]; ok && version.IsActive(supportedVersion) {
		return nil
	}
	visitor := &tree.UDFDisallowanceVisitor{}
	tree.WalkExpr(visitor, expr)
	if visitor.FoundUDF {
		return unimplemented.NewWithIssue(83234, "usage of user-defined function from relations not supported")
	}
	return nil
}
