// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	ctx context.Context,
	evalCtx *eval.Context,
	mem *memo.Memo,
	expr memo.RelExpr,
	required *physical.Distribution,
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
		tabMeta := mem.Metadata().TableMeta(t.Table)
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
	ctx context.Context,
	evalCtx *eval.Context,
	mem *memo.Memo,
	expr memo.RelExpr,
	required *physical.Distribution,
) physical.Distribution {
	var provided physical.Distribution
	switch t := expr.(type) {
	case *memo.DistributeExpr:
		return *required

	case *memo.LocalityOptimizedSearchExpr:
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		tabMeta := mem.Metadata().TableMeta(t.Table)
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

// getCRBDRegionColSetFromInput finds the set of column ids in the input to a
// lookup join which are known to all be `crdb_region` columns, either in a Scan
// or by being a `crdb_region` lookup table key column in a lookup join or chain
// of lookup joins, all connected by `crdb_region` key column lookups. This
// column set plus the distribution of the lookup join's input relation is
// returned.
func getCRBDRegionColSetFromInput(
	ctx context.Context,
	evalCtx *eval.Context,
	mem *memo.Memo,
	join *memo.LookupJoinExpr,
	required *physical.Required,
	maybeGetBestCostRelation func(grp memo.RelExpr, required *physical.Required) (best memo.RelExpr, ok bool),
) (crdbRegionColSet opt.ColSet, inputDistribution physical.Distribution) {
	var needRemap bool
	var setOpCols opt.ColSet

	if bestCostInputRel, ok := maybeGetBestCostRelation(join.Input, required); ok {
		maybeScan := bestCostInputRel
		var projectExpr *memo.ProjectExpr
		if projectExpr, ok = maybeScan.(*memo.ProjectExpr); ok {
			maybeScan, ok = maybeGetBestCostRelation(projectExpr.Input, required)
			if !ok {
				return crdbRegionColSet, physical.Distribution{}
			}
		}
		if selectExpr, ok := maybeScan.(*memo.SelectExpr); ok {
			maybeScan, ok = maybeGetBestCostRelation(selectExpr.Input, required)
			if !ok {
				return crdbRegionColSet, physical.Distribution{}
			}
		}
		if indexJoinExpr, ok := maybeScan.(*memo.IndexJoinExpr); ok {
			maybeScan, ok = maybeGetBestCostRelation(indexJoinExpr.Input, required)
			if !ok {
				return crdbRegionColSet, physical.Distribution{}
			}
		}
		if lookupJoinExpr, ok := maybeScan.(*memo.LookupJoinExpr); ok {
			crdbRegionColSet, inputDistribution =
				BuildLookupJoinLookupTableDistribution(
					ctx, evalCtx, mem, lookupJoinExpr, required, maybeGetBestCostRelation)
			return crdbRegionColSet, inputDistribution
		}
		if localityOptimizedScan, ok := maybeScan.(*memo.LocalityOptimizedSearchExpr); ok {
			maybeScan = localityOptimizedScan.Local
			needRemap = true
			setOpCols = localityOptimizedScan.Relational().OutputCols
		}
		scanExpr, ok := maybeScan.(*memo.ScanExpr)
		if !ok {
			return crdbRegionColSet, physical.Distribution{}
		}
		tab := mem.Metadata().Table(scanExpr.Table)
		if !tab.IsRegionalByRow() {
			return crdbRegionColSet, physical.Distribution{}
		}
		inputDistribution =
			BuildProvided(ctx, evalCtx, mem, scanExpr, &required.Distribution)
		index := tab.Index(scanExpr.Index)
		crdbRegionColID := scanExpr.Table.IndexColumnID(index, 0)
		if needRemap {
			scanCols := scanExpr.Relational().OutputCols
			if scanCols.Len() == setOpCols.Len() {
				destCol, _ := setOpCols.Next(0)
				for srcCol, ok := scanCols.Next(0); ok; srcCol, ok = scanCols.Next(srcCol + 1) {
					if srcCol == crdbRegionColID {
						crdbRegionColID = destCol
						break
					}
					destCol, _ = setOpCols.Next(destCol + 1)
				}
			}
		}
		if projectExpr != nil {
			if !projectExpr.Passthrough.Contains(crdbRegionColID) {
				return crdbRegionColSet, physical.Distribution{}
			}
		}
		crdbRegionColSet.Add(crdbRegionColID)
	}
	return crdbRegionColSet, inputDistribution
}

// BuildLookupJoinLookupTableDistribution builds and returns the Distribution of
// a lookup table in a lookup join if that distribution can be statically
// determined. It also finds the set of column ids in the input to the lookup
// join which are known to all be `crdb_region` columns, either in a Scan or by
// being a `crdb_region` lookup table key column in a lookup join or chain of
// lookup joins, all connected by `crdb_region` key column lookups. This column
// set plus the distribution of the lookup join's lookup table is returned.
// Parameter `maybeGetBestCostRelation` is expected to be a function that looks
// up the best cost relation in the `grp` relation's memo group. Typically this
// is `xform.Coster.MaybeGetBestCostRelation`. It's passed as a function since
// `BuildLookupJoinLookupTableDistribution` may be called from packages not
// allowed to import the `xform` package due to import cycles.
func BuildLookupJoinLookupTableDistribution(
	ctx context.Context,
	evalCtx *eval.Context,
	mem *memo.Memo,
	lookupJoin *memo.LookupJoinExpr,
	required *physical.Required,
	maybeGetBestCostRelation func(grp memo.RelExpr, required *physical.Required) (best memo.RelExpr, ok bool),
) (crdbRegionColSet opt.ColSet, provided physical.Distribution) {
	localCrdbRegionColSet, inputDistribution :=
		getCRBDRegionColSetFromInput(ctx, evalCtx, mem, lookupJoin, required, maybeGetBestCostRelation)

	lookupTableMeta := mem.Metadata().TableMeta(lookupJoin.Table)
	lookupTable := lookupTableMeta.Table

	idx := lookupTable.Index(lookupJoin.Index)
	col := idx.Column(0)
	ord := col.Ordinal()
	colIDOfFirstLookupIndexColumn := lookupTableMeta.MetaID.ColumnID(ord)

	if lookupJoin.LocalityOptimized || lookupJoin.ChildOfLocalityOptimizedSearch {
		provided.FromLocality(evalCtx.Locality)
		return crdbRegionColSet, provided
	} else if lookupTable.IsGlobalTable() {
		provided.FromLocality(evalCtx.Locality)
		return crdbRegionColSet, provided
	} else if homeRegion, ok := lookupTable.HomeRegion(); ok {
		provided.Regions = []string{homeRegion}
		return crdbRegionColSet, provided
	} else if lookupTable.IsRegionalByRow() {
		if len(lookupJoin.KeyCols) > 0 {
			inputExpr := lookupJoin.Input
			firstKeyColID := lookupJoin.LookupJoinPrivate.KeyCols[0]
			if invertedJoinExpr, ok := inputExpr.(*memo.InvertedJoinExpr); ok {
				if filterExpr, ok := invertedJoinExpr.GetConstExprFromFilter(firstKeyColID); ok {
					if homeRegion, ok = GetDEnumAsStringFromConstantExpr(filterExpr); ok {
						provided.Regions = []string{homeRegion}
						crdbRegionColSet.UnionWith(localCrdbRegionColSet)
						crdbRegionColSet.Add(colIDOfFirstLookupIndexColumn)
						return crdbRegionColSet, provided
					}
				}
			} else if projectExpr, ok := inputExpr.(*memo.ProjectExpr); ok {
				regionName := projectExpr.GetProjectedEnumConstant(firstKeyColID)
				if regionName != "" {
					provided.Regions = []string{regionName}
					crdbRegionColSet.UnionWith(localCrdbRegionColSet)
					crdbRegionColSet.Add(colIDOfFirstLookupIndexColumn)
					return crdbRegionColSet, provided
				}
			}
			if localCrdbRegionColSet.Contains(firstKeyColID) {
				provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
				if !inputDistribution.Any() &&
					(provided.Any() || len(provided.Regions) > len(inputDistribution.Regions)) {
					crdbRegionColSet.UnionWith(localCrdbRegionColSet)
					crdbRegionColSet.Add(colIDOfFirstLookupIndexColumn)
					return crdbRegionColSet, inputDistribution
				}
				return crdbRegionColSet, provided
			}
		} else if len(lookupJoin.LookupJoinPrivate.LookupExpr) > 0 {
			if filterIdx, ok := lookupJoin.GetConstPrefixFilter(mem.Metadata()); ok {
				firstIndexColEqExpr := lookupJoin.LookupJoinPrivate.LookupExpr[filterIdx].Condition
				if firstIndexColEqExpr.Op() == opt.EqOp {
					if regionName, ok := GetDEnumAsStringFromConstantExpr(firstIndexColEqExpr.Child(1)); ok {
						provided.Regions = []string{regionName}
						crdbRegionColSet.UnionWith(localCrdbRegionColSet)
						crdbRegionColSet.Add(colIDOfFirstLookupIndexColumn)
						return crdbRegionColSet, provided
					}
				}
			} else if lookupJoin.LookupIndexPrefixIsEquatedWithColInColSet(mem.Metadata(), localCrdbRegionColSet) {
				// We have a `crdb_region = crdb_region` term in `LookupJoinPrivate.LookupExpr`.
				provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
				if !inputDistribution.Any() &&
					(provided.Any() || len(provided.Regions) > len(inputDistribution.Regions)) {
					crdbRegionColSet.UnionWith(localCrdbRegionColSet)
					crdbRegionColSet.Add(colIDOfFirstLookupIndexColumn)
					return crdbRegionColSet, inputDistribution
				}
				return crdbRegionColSet, provided
			}
		}
	}
	provided.FromIndexScan(ctx, evalCtx, lookupTableMeta, lookupJoin.Index, nil)
	return crdbRegionColSet, provided
}

// BuildInvertedJoinLookupTableDistribution builds the Distribution that results
// from performing lookups of an inverted join, if that distribution can be
// statically determined.
func BuildInvertedJoinLookupTableDistribution(
	ctx context.Context, evalCtx *eval.Context, mem *memo.Memo, invertedJoin *memo.InvertedJoinExpr,
) (provided physical.Distribution) {
	lookupTableMeta := mem.Metadata().TableMeta(invertedJoin.Table)
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
