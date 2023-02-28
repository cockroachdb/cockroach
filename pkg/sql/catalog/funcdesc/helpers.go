// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

var schemaExprContextAllowingUDF = map[tree.SchemaExprContext]clusterversion.Key{
	tree.CheckConstraintExpr:           clusterversion.V23_1,
	tree.ColumnDefaultExprInNewTable:   clusterversion.V23_1,
	tree.ColumnDefaultExprInSetDefault: clusterversion.V23_1,
}

// MaybeFailOnUDFUsage returns an error if the given expression or any
// sub-expression used a UDF unless it's explicitly listed as an allowed use
// case.
// TODO(chengxiong): remove this function when we start allowing UDF references.
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
