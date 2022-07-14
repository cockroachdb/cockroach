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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
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
		// Locality optimized search is legal with the EnforceHomeRegion flag,
		// but only with the SURVIVE ZONE FAILURE option.
		if evalCtx.SessionData().EnforceHomeRegion && t.Local.Op() == opt.ScanOp {
			scanExpr := t.Local.(*memo.ScanExpr)
			tabMeta := t.Memo().Metadata().TableMeta(scanExpr.Table)
			errorOnInvalidMultiregionDB(evalCtx, tabMeta)
		}
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		tabMeta := t.Memo().Metadata().TableMeta(t.Table)
		// Tables in database that don't use the SURVIVE ZONE FAILURE option are
		// disallowed when EnforceHomeRegion is true.
		if evalCtx.SessionData().EnforceHomeRegion {
			errorOnInvalidMultiregionDB(evalCtx, tabMeta)
		}
		provided.FromIndexScan(evalCtx, tabMeta, t.Index, t.Constraint)

	default:
		// Other operators can pass through the distribution to their children.
	}
	return provided.Any() || provided.Equals(*required)
}

// errorOnInvalidMultiregionDB panics if the table described by tabMeta is owned
// by a non-multiregion database or a multiregion database with SURVIVE REGION
// FAILURE goal.
func errorOnInvalidMultiregionDB(evalCtx *eval.Context, tabMeta *opt.TableMeta) {
	survivalGoal, ok := tabMeta.GetDatabaseSurvivalGoal(evalCtx.Ctx(), evalCtx.Planner)
	// non-multiregional database or SURVIVE REGION FAILURE option
	if !ok {
		err := pgerror.New(pgcode.QueryHasNoHomeRegion,
			"Query has no home region. Try accessing only tables in multi-region databases with ZONE survivability.")
		panic(err)
	} else if survivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		err := pgerror.New(pgcode.QueryHasNoHomeRegion,
			"Query has no home region. Try accessing only tables in multi-region databases with ZONE survivability.")
		panic(err)
	}
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
		// Locality optimized search is legal with the EnforceHomeRegion flag,
		// but only with the SURVIVE ZONE FAILURE option.
		if evalCtx.SessionData().EnforceHomeRegion && t.Local.Op() == opt.ScanOp {
			scanExpr := t.Local.(*memo.ScanExpr)
			tabMeta := t.Memo().Metadata().TableMeta(scanExpr.Table)
			errorOnInvalidMultiregionDB(evalCtx, tabMeta)
		}
		provided.FromLocality(evalCtx.Locality)

	case *memo.ScanExpr:
		// Tables in database that don't use the SURVIVE ZONE FAILURE option are
		// disallowed when EnforceHomeRegion is true.
		tabMeta := t.Memo().Metadata().TableMeta(t.Table)
		if evalCtx.SessionData().EnforceHomeRegion {
			errorOnInvalidMultiregionDB(evalCtx, tabMeta)
		}
		provided.FromIndexScan(evalCtx, tabMeta, t.Index, t.Constraint)

	default:
		for i, n := 0, expr.ChildCount(); i < n; i++ {
			if relExpr, ok := expr.Child(i).(memo.RelExpr); ok {
				provided = provided.Union(relExpr.ProvidedPhysical().Distribution)
			}
		}
	}

	if buildutil.CrdbTestBuild {
		checkProvided(&provided, required)
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
