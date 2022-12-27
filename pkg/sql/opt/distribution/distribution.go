// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distribution

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// CanProvide returns true if the given operator returns rows that can
// satisfy the given required distribution.
func CanProvide(
	ctx context.Context, evalCtx *eval.Context, expr memo.RelExpr, required *physical.Distribution,
) bool {
	if required.Any() {
		return true
	}
	if buildutil.CrdbTestBuild {
		checkRequired(required)
	}

	var provided physical.Distribution
	switch t := expr.(type) {
	case *memo.DistributeExpr:
		return true

	case *memo.LocalityOptimizedSearchExpr:
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		tabMeta := t.Memo().Metadata().TableMeta(t.Table)
		if t.Distribution.Regions != nil {
			provided = t.Distribution
		} else {
			provided.FromIndexScan(ctx, evalCtx, tabMeta, t.Index, t.Constraint)
		}

	case *memo.LookupJoinExpr:
		if t.LocalityOptimized {
			provided.FromLocality(evalCtx.Locality)
		}

	default:
		// Other operators can pass through the distribution to their children.
	}
	return provided.Any() || provided.Equals(*required)
}

// BuildChildRequired returns the distribution that must be required of its
// given child in order to satisfy a required distribution. Can only be called if
// CanProvide is true for the required distribution.
func BuildChildRequired(
	parent memo.RelExpr, required *physical.Distribution, childIdx int,
) physical.Distribution {
	if required.Any() {
		return physical.Distribution{}
	}

	switch parent.(type) {
	case *memo.DistributeExpr:
		return physical.Distribution{}

	case *memo.LocalityOptimizedSearchExpr:
		return physical.Distribution{}

	case *memo.ScanExpr:
		return physical.Distribution{}
	}

	if buildutil.CrdbTestBuild {
		checkRequired(required)
	}
	return *required
}

// BuildProvided returns a specific distribution that the operator provides. The
// returned distribution must match the required distribution.
//
// This function assumes that the provided distributions have already been set in
// the children of the expression.
func BuildProvided(
	ctx context.Context, evalCtx *eval.Context, expr memo.RelExpr, required *physical.Distribution,
) physical.Distribution {
	var provided physical.Distribution
	switch t := expr.(type) {
	case *memo.DistributeExpr:
		return *required

	case *memo.LocalityOptimizedSearchExpr:
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		tabMeta := t.Memo().Metadata().TableMeta(t.Table)
		if t.Distribution.Regions != nil {
			provided = t.Distribution
		} else {
			provided.FromIndexScan(ctx, evalCtx, tabMeta, t.Index, t.Constraint)
		}

	default:
		// TODO(msirek): Clarify the distinction between a distribution which can
		//               provide any required distribution and one which can provide
		//               none. See issue #86641.
		if lookupJoinExpr, ok := expr.(*memo.LookupJoinExpr); ok {
			if lookupJoinExpr.LocalityOptimized {
				// Locality-optimized join is considered to be local.
				provided.FromLocality(evalCtx.Locality)
				break
			}
		}
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if relExpr, ok := expr.Child(i).(memo.RelExpr); ok {
				childDistribution := relExpr.ProvidedPhysical().Distribution
				provided = provided.Union(childDistribution)
			}
		}
	}

	if buildutil.CrdbTestBuild {
		checkProvided(&provided, required)
	}

	return provided
}

// GetDEnumAsStringFromConstantExpr returns the string representation of a DEnum
// ConstantExpr, if expr is such a ConstantExpr.
func GetDEnumAsStringFromConstantExpr(expr opt.Expr) (enumAsString string, ok bool) {
	if constExpr, ok := expr.(*memo.ConstExpr); ok {
		if enumValue, ok := constExpr.Value.(*tree.DEnum); ok {
			return enumValue.LogicalRep, true
		}
	}
	return "", false
}

// BuildLookupJoinLookupTableDistribution builds the Distribution that results
// from performing lookups of a LookupJoin, if that distribution can be
// statically determined. If crdbRegionColID is non-zero, it is the column ID
// of the input REGIONAL BY ROW table holding the crdb_region column, and
// inputDistribution is the distribution of the operation on that table
// (Scan or LocalityOptimizedSearch).
func BuildLookupJoinLookupTableDistribution(
	ctx context.Context,
	evalCtx *eval.Context,
	lookupJoin *memo.LookupJoinExpr,
	crdbRegionColID opt.ColumnID,
	inputDistribution physical.Distribution,
) (provided physical.Distribution) {
	lookupTableMeta := lookupJoin.Memo().Metadata().TableMeta(lookupJoin.Table)
	lookupTable := lookupTableMeta.Table

	if lookupJoin.LocalityOptimized || lookupJoin.ChildOfLocalityOptimizedSearch {
		provided.FromLocality(evalCtx.Locality)
		return provided
	} else if lookupTable.IsGlobalTable() {
		provided.FromLocality(evalCtx.Locality)
		return provided
	} else if homeRegion, ok := lookupTable.HomeRegion(); ok {
		provided.Regions = []string{homeRegion}
		return provided
	} else if lookupTable.IsRegionalByRow() {
		if len(lookupJoin.KeyCols) > 0 {
			inputExpr := lookupJoin.Input
			firstKeyColID := lookupJoin.LookupJoinPrivate.KeyCols[0]
			if invertedJoinExpr, ok := inputExpr.(*memo.InvertedJoinExpr); ok {
				if filterExpr, ok := invertedJoinExpr.GetConstExprFromFilter(firstKeyColID); ok {
					if homeRegion, ok = GetDEnumAsStringFromConstantExpr(filterExpr); ok {
						provided.Regions = []string{homeRegion}
						return provided
					}
				}
			} else if projectExpr, ok := inputExpr.(*memo.ProjectExpr); ok {
				regionName := projectExpr.GetProjectedEnumConstant(firstKeyColID)
				if regionName != "" {
					provided.Regions = []string{regionName}
					return provided
				}
			}
			if crdbRegionColID == firstKeyColID {
				provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
				if !inputDistribution.Any() &&
					(provided.Any() || len(provided.Regions) > len(inputDistribution.Regions)) {
					return inputDistribution
				}
				return provided
			}
		} else if len(lookupJoin.LookupJoinPrivate.LookupExpr) > 0 {
			if filterIdx, ok := lookupJoin.GetConstPrefixFilter(lookupJoin.Memo().Metadata()); ok {
				firstIndexColEqExpr := lookupJoin.LookupJoinPrivate.LookupExpr[filterIdx].Condition
				if firstIndexColEqExpr.Op() == opt.EqOp {
					if regionName, ok := GetDEnumAsStringFromConstantExpr(firstIndexColEqExpr.Child(1)); ok {
						provided.Regions = []string{regionName}
						return provided
					}
				}
			} else if lookupJoin.ColIsEquivalentWithLookupIndexPrefix(lookupJoin.Memo().Metadata(), crdbRegionColID) {
				// We have a `crdb_region = crdb_region` term in `LookupJoinPrivate.LookupExpr`.
				provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
				if !inputDistribution.Any() &&
					(provided.Any() || len(provided.Regions) > len(inputDistribution.Regions)) {
					return inputDistribution
				}
				return provided
			}
		}
	}
	provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
	return provided
}

// BuildInvertedJoinLookupTableDistribution builds the Distribution that results
// from performing lookups of an inverted join, if that distribution can be
// statically determined.
func BuildInvertedJoinLookupTableDistribution(
	ctx context.Context, evalCtx *eval.Context, invertedJoin *memo.InvertedJoinExpr,
) (provided physical.Distribution) {
	lookupTableMeta := invertedJoin.Memo().Metadata().TableMeta(invertedJoin.Table)
	lookupTable := lookupTableMeta.Table

	if lookupTable.IsGlobalTable() {
		provided.FromLocality(evalCtx.Locality)
		return provided
	} else if homeRegion, ok := lookupTable.HomeRegion(); ok {
		provided.Regions = []string{homeRegion}
		return provided
	} else if lookupTable.IsRegionalByRow() {
		if len(invertedJoin.PrefixKeyCols) > 0 {
			if projectExpr, ok := invertedJoin.Input.(*memo.ProjectExpr); ok {
				colID := invertedJoin.PrefixKeyCols[0]
				homeRegion = projectExpr.GetProjectedEnumConstant(colID)
				provided.Regions = []string{homeRegion}
				return provided
			}
		}
	}
	provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, invertedJoin.Index, nil)
	return provided
}

func checkRequired(required *physical.Distribution) {
	// There should be exactly one region in the required distribution (for now,
	// assuming this is coming from the gateway).
	if len(required.Regions) != 1 {
		panic(errors.AssertionFailedf(
			"There should be at most one region in the required distribution: %s", required.String(),
		))
	}
	check(required)
}

func checkProvided(provided, required *physical.Distribution) {
	if !provided.Any() && !required.Any() && !provided.Equals(*required) {
		panic(errors.AssertionFailedf("expression can't provide required distribution"))
	}
	check(provided)
}

func check(distribution *physical.Distribution) {
	for i := range distribution.Regions {
		if i > 0 {
			if distribution.Regions[i] <= distribution.Regions[i-1] {
				panic(errors.AssertionFailedf(
					"Distribution regions are not sorted and deduplicated: %s", distribution.String(),
				))
			}
		}
	}
}
