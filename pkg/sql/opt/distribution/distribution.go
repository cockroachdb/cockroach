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
func CanProvide(evalCtx *eval.Context, expr memo.RelExpr, required *physical.Distribution) bool {
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
		provided.FromIndexScan(evalCtx, tabMeta, t.Index, t.Constraint, t.Memo().RootIsExplain())

	case *memo.LookupJoinExpr:
		// At this point, the distribution of the input relation to the lookup join
		// has not yet been determined. We can determine the distribution of the
		// lookup table though, which is enough to cause any required distribute
		// operation to be applied on the input to the lookup join instead of on
		// the lookup join itself.
		provided = BuildLookupJoinLookupTableDistribution(t.Memo().EvalContext(), t, t.Memo().RootIsExplain())

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
	evalCtx *eval.Context, expr memo.RelExpr, required *physical.Distribution,
) physical.Distribution {
	var provided physical.Distribution
	switch t := expr.(type) {
	case *memo.DistributeExpr:
		return *required

	case *memo.LocalityOptimizedSearchExpr:
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		tabMeta := t.Memo().Metadata().TableMeta(t.Table)
		provided.FromIndexScan(evalCtx, tabMeta, t.Index, t.Constraint, t.Memo().RootIsExplain())

	case *memo.LookupJoinExpr:
		inputDistribution := t.Input.ProvidedPhysical().Distribution
		if inputDistribution.Any() {
			return inputDistribution
		}
		provided = BuildLookupJoinLookupTableDistribution(evalCtx, t, t.Memo().RootIsExplain())
		if provided.Any() {
			return provided
		}
		provided = provided.Union(inputDistribution)

	default:
		hasRelExpr := false
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if relExpr, ok := expr.Child(i).(memo.RelExpr); ok {
				hasRelExpr = true
				childDistribution := relExpr.ProvidedPhysical().Distribution
				if len(childDistribution.Regions) == 0 {
					// An empty distribution currently means the same as a distribution
					// to all regions in the database, and the Union function does not
					// encapsulate this meaning in its implementation, so we manually
					// handle unioning of this case here.
					provided.Regions = provided.Regions[:0]
					break
				}
				provided = provided.Union(childDistribution)
			}
		}
		// An expression that does not rely on any child relations, like a VALUES
		// clause, can be considered to be fully local.
		if !hasRelExpr {
			provided.FromLocality(evalCtx.Locality)
		}
	}

	if buildutil.CrdbTestBuild {
		checkProvided(&provided, required)
	}

	return provided
}

// BuildLookupJoinLookupTableDistribution builds the Distribution that results
// from performing lookups of a LookupJoin, if that distribution can be
// statically determined.
func BuildLookupJoinLookupTableDistribution(
	evalCtx *eval.Context, lookupJoin *memo.LookupJoinExpr, forExplain bool,
) (provided physical.Distribution) {
	lookupTableMeta := lookupJoin.Memo().Metadata().TableMeta(lookupJoin.Table)
	lookupTable := lookupTableMeta.Table
	if lookupTable.IsGlobalTable() {
		provided.FromLocality(evalCtx.Locality)
	} else if homeRegion, ok := lookupTable.HomeRegion(); ok {
		provided.Regions = []string{homeRegion}
	} else if lookupTable.IsRegionalByRow() {
		if len(lookupJoin.LookupJoinPrivate.KeyCols) > 0 {
			if projectExpr, ok := lookupJoin.Input.(*memo.ProjectExpr); ok {
				colID := lookupJoin.LookupJoinPrivate.KeyCols[0]
				regionName := projectExpr.GetProjectedEnumConstant(colID)
				if regionName != "" {
					provided.Regions = []string{regionName}
				}
			}
		} else if lookupJoin.LookupJoinPrivate.LookupColsAreTableKey &&
			len(lookupJoin.LookupJoinPrivate.LookupExpr) > 0 {
			firstIndexColEqExpr := lookupJoin.LookupJoinPrivate.LookupExpr[0].Condition
			if firstIndexColEqExpr.Op() == opt.EqOp {
				if firstIndexColEqExpr.Child(1).Op() == opt.ConstOp {
					constExpr := firstIndexColEqExpr.Child(1).(*memo.ConstExpr)
					if enumValue, ok := constExpr.Value.(*tree.DEnum); ok {
						regionName := enumValue.LogicalRep
						provided.Regions = []string{regionName}
					}
				}
			}
		}
	} else {
		provided.FromIndexScan(evalCtx, lookupTableMeta, lookupJoin.Index, nil, forExplain)
	}
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
