// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/errors"
)

func registerUnoptimizedQueryOracle(r registry.Registry) {
	disableRuleSpecs := []struct {
		disableRules           string
		disableRuleProbability float64
	}{
		{
			disableRules:           "all",
			disableRuleProbability: 1.0,
		},
		{
			disableRules:           "half",
			disableRuleProbability: 0.5,
		},
	}
	for i := range disableRuleSpecs {
		disableRuleSpec := &disableRuleSpecs[i]
		for _, setupName := range []string{sqlsmith.RandTableSetupName, sqlsmith.SeedMultiRegionSetupName} {
			setupName := setupName
			var clusterSpec spec.ClusterSpec
			switch setupName {
			case sqlsmith.SeedMultiRegionSetupName:
				clusterSpec = r.MakeClusterSpec(9, spec.Geo(), spec.GatherCores())
			default:
				clusterSpec = r.MakeClusterSpec(1)
			}
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"unoptimized-query-oracle/disable-rules=%s/%s", disableRuleSpec.disableRules, setupName,
				),
				Owner:            registry.OwnerSQLQueries,
				NativeLibs:       registry.LibGEOS,
				Timeout:          time.Hour * 1,
				RequiresLicense:  true,
				Cluster:          clusterSpec,
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.Suites(registry.Weekly),
				Randomized:       true,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runQueryComparison(ctx, t, c, &queryComparisonTest{
						name:      "unoptimized-query-oracle",
						setupName: setupName,
						run: func(s queryGenerator, r *rand.Rand, h queryComparisonHelper) error {
							return runUnoptimizedQueryOracleImpl(s, r, h, disableRuleSpec.disableRuleProbability)
						},
					})
				},
				ExtraLabels: []string{"O-rsg"},
			})
		}
	}
}

// runUnoptimizedQueryOracleImpl executes the same query two times, once with
// some (or all) optimizer rules and the vectorized execution engine disabled,
// and once with normal optimization and/or execution. If the results of the two
// executions are not equal an error is returned.
func runUnoptimizedQueryOracleImpl(
	qgen queryGenerator, rnd *rand.Rand, h queryComparisonHelper, disableRuleProbability float64,
) error {
	var stmt string
	// Ignore panics from Generate.
	func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		stmt = qgen.Generate()
	}()

	var verboseLogging bool
	defer func() {
		// We're logging all statements, even if no error is encountered, to
		// make it easier to reproduce the problems since it appears that the
		// state of the previous iteration might influence the execution of the
		// current iteration.
		h.logStatements()
		if verboseLogging {
			h.logVerboseOutput()
		}
	}()

	// First, run the statement with some optimizer rules and vectorized execution
	// disabled.
	seedStmt := fmt.Sprintf("SET testing_optimizer_random_seed = %d", rnd.Int63())
	if err := h.execStmt(seedStmt); err != nil {
		return h.makeError(err, "failed to set random seed")
	}
	disableOptimizerStmt := fmt.Sprintf(
		"SET testing_optimizer_disable_rule_probability = %f", disableRuleProbability,
	)
	if err := h.execStmt(disableOptimizerStmt); err != nil {
		return h.makeError(err, "failed to disable optimizer rules")
	}
	disableVectorizeStmt := "SET vectorize = off"
	if err := h.execStmt(disableVectorizeStmt); err != nil {
		return h.makeError(err, "failed to disable the vectorized engine")
	}
	disableDistSQLStmt := "SET distsql = off"
	if err := h.execStmt(disableDistSQLStmt); err != nil {
		return h.makeError(err, "failed to disable DistSQL")
	}

	unoptimizedRows, err := h.runQuery(stmt)
	if err != nil {
		// Skip unoptimized statements that fail with an error (unless it's an
		// internal error).
		if es := err.Error(); strings.Contains(es, "internal error") {
			verboseLogging = true
			// The stmt wasn't already included since it resulted in an error,
			// but to make the reproduction easier, we do want to include it.
			h.addStmtForLogging(stmt, nil /* rows */)
			return h.makeError(err, "internal error while running unoptimized statement")
		}
		//nolint:returnerrcheck
		return nil
	}

	// It then changes the settings to re-enable optimizer and/or re-enable
	// vectorized execution and/or disable not visible index feature.
	resetSeedStmt := "RESET testing_optimizer_random_seed"
	resetOptimizerStmt := "RESET testing_optimizer_disable_rule_probability"
	resetVectorizeStmt := "RESET vectorize"
	enable := rnd.Intn(3)
	if enable > 0 {
		if err := h.execStmt(resetSeedStmt); err != nil {
			return h.makeError(err, "failed to reset random seed")
		}
		if err := h.execStmt(resetOptimizerStmt); err != nil {
			return h.makeError(err, "failed to reset the optimizer")
		}
	}
	if enable < 2 {
		if err := h.execStmt(resetVectorizeStmt); err != nil {
			return h.makeError(err, "failed to reset the vectorized engine")
		}
		// Disable not visible index feature to run the statement with more optimization.
		if err := h.execStmt("SET optimizer_use_not_visible_indexes = true"); err != nil {
			return h.makeError(err, "failed to disable not visible index feature")
		}
	}
	if roll := rnd.Intn(4); roll > 0 {
		distSQLMode := "auto"
		if roll == 3 {
			distSQLMode = "on"
		}
		if err := h.execStmt(fmt.Sprintf("SET distsql = %s", distSQLMode)); err != nil {
			return h.makeError(err, "failed to re-enable DistSQL")
		}
	}

	// Then, rerun the statement with optimization and/or vectorization enabled
	// and/or not visible index feature disabled.
	optimizedRows, err := h.runQuery(stmt)
	if err != nil {
		// If the optimized plan fails with an internal error while the unoptimized plan
		// succeeds, we'd like to know, so consider this a test failure.
		es := err.Error()
		if strings.Contains(es, "internal error") {
			verboseLogging = true
			// The stmt wasn't already included since it resulted in an error,
			// but to make the reproduction easier, we do want to include it.
			h.addStmtForLogging(stmt, nil /* rows */)
			return h.makeError(err, "internal error while running optimized statement")
		}
		// Otherwise, skip optimized statements that fail with a non-internal
		// error. This could happen if the statement contains bad arguments to a
		// function call, for example, and the unoptimized plan was able to skip
		// evaluation of the function due to short-circuiting (see #81032 for an
		// example).
		//nolint:returnerrcheck
		return nil
	}
	diff, err := unsortedMatricesDiffWithFloatComp(unoptimizedRows, optimizedRows, h.colTypes)
	if err != nil {
		return err
	}
	if diff != "" {
		// We have a mismatch in the unoptimized vs optimized query outputs.
		verboseLogging = true
		return h.makeError(errors.Newf(
			"expected unoptimized and optimized results to be equal\n%s\nsql: %s",
			diff, stmt,
		), "")
	}

	// Reset all settings in case they weren't reset above.
	if err := h.execStmt(resetSeedStmt); err != nil {
		return h.makeError(err, "failed to reset random seed")
	}
	if err := h.execStmt(resetOptimizerStmt); err != nil {
		return h.makeError(err, "failed to reset the optimizer")
	}
	if err := h.execStmt(resetVectorizeStmt); err != nil {
		return h.makeError(err, "failed to reset the vectorized engine")
	}
	if err := h.execStmt("RESET optimizer_use_not_visible_indexes"); err != nil {
		return h.makeError(err, "failed to reset not visible index feature")
	}

	return nil
}
