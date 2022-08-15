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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// MakeHashShardComputeExpr creates the serialized computed expression for a hash shard
// column based on the column names and the number of buckets. The expression will be
// of the form:
//
//	mod(fnv32(crdb_internal.datums_to_bytes(...)),buckets)
func MakeHashShardComputeExpr(colNames []string, buckets int) *string {
	unresolvedFunc := func(funcName string) tree.ResolvableFunctionReference {
		return tree.ResolvableFunctionReference{
			FunctionReference: &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{funcName},
			},
		}
	}
	columnItems := func() tree.Exprs {
		exprs := make(tree.Exprs, len(colNames))
		for i := range exprs {
			exprs[i] = &tree.ColumnItem{ColumnName: tree.Name(colNames[i])}
		}
		return exprs
	}
	hashedColumnsExpr := func() tree.Expr {
		return &tree.FuncExpr{
			Func: unresolvedFunc("fnv32"),
			Exprs: tree.Exprs{
				&tree.FuncExpr{
					Func:  unresolvedFunc("crdb_internal.datums_to_bytes"),
					Exprs: columnItems(),
				},
			},
		}
	}
	modBuckets := func(expr tree.Expr) tree.Expr {
		return &tree.FuncExpr{
			Func: unresolvedFunc("mod"),
			Exprs: tree.Exprs{
				expr,
				tree.NewDInt(tree.DInt(buckets)),
			},
		}
	}
	res := tree.Serialize(modBuckets(hashedColumnsExpr()))
	return &res
}
