// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

// PartitionByForRegionalByRow constructs the tree.PartitionBy clause for
// REGIONAL BY ROW tables.
func PartitionByForRegionalByRow(regionConfig RegionConfig, col tree.Name) *tree.PartitionBy {
	listPartition := make([]tree.ListPartition, len(regionConfig.Regions()))
	for i, region := range regionConfig.Regions() {
		listPartition[i] = tree.ListPartition{
			Name:  tree.Name(region),
			Exprs: tree.Exprs{tree.NewStrVal(string(region))},
		}
	}

	return &tree.PartitionBy{
		Fields: tree.NameList{col},
		List:   listPartition,
	}
}

// RegionalByRowDefaultColDef builds the default column definition of the
// `crdb_region` column for REGIONAL BY ROW tables.
func RegionalByRowDefaultColDef(
	oid oid.Oid, defaultExpr tree.Expr, onUpdateExpr tree.Expr,
) *tree.ColumnTableDef {
	c := &tree.ColumnTableDef{
		Name:   tree.RegionalByRowRegionDefaultColName,
		Type:   &tree.OIDTypeReference{OID: oid},
		Hidden: true,
	}
	c.Nullable.Nullability = tree.NotNull
	c.DefaultExpr.Expr = defaultExpr
	c.OnUpdateExpr.Expr = onUpdateExpr

	return c
}

// RegionalByRowGatewayRegionDefaultExpr builds an expression which returns the
// default gateway region of a REGIONAL BY ROW table.
func RegionalByRowGatewayRegionDefaultExpr(oid oid.Oid) tree.Expr {
	return &tree.CastExpr{
		Expr: &tree.FuncExpr{
			Func: tree.WrapFunction(builtinconstants.DefaultToDatabasePrimaryRegionBuiltinName),
			Exprs: []tree.Expr{
				&tree.FuncExpr{
					Func: tree.WrapFunction(builtinconstants.GatewayRegionBuiltinName),
				},
			},
		},
		Type:       &tree.OIDTypeReference{OID: oid},
		SyntaxMode: tree.CastShort,
	}
}

// MaybeRegionalByRowOnUpdateExpr returns a gateway region default statement if
// the auto rehoming session setting is enabled, nil otherwise.
func MaybeRegionalByRowOnUpdateExpr(evalCtx *eval.Context, enumOid oid.Oid) tree.Expr {
	if evalCtx.SessionData().AutoRehomingEnabled {
		return &tree.CastExpr{
			Expr: &tree.FuncExpr{
				Func: tree.WrapFunction(builtinconstants.RehomeRowBuiltinName),
			},
			Type:       &tree.OIDTypeReference{OID: enumOid},
			SyntaxMode: tree.CastShort,
		}
	}
	return nil
}
