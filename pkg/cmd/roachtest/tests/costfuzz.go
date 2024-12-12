// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/errors"
)

const WorkloadReplaySetupName = "workload-replay"

func registerCostFuzz(r registry.Registry) {
	for _, setupName := range []string{WorkloadReplaySetupName, sqlsmith.RandTableSetupName,
		sqlsmith.SeedMultiRegionSetupName, sqlsmith.RandMultiRegionSetupName} {
		setupName := setupName
		redactResults := false
		timeOut := time.Hour * 1
		var clusterSpec spec.ClusterSpec
		switch setupName {
		case sqlsmith.SeedMultiRegionSetupName, sqlsmith.RandMultiRegionSetupName:
			clusterSpec = r.MakeClusterSpec(9, spec.Geo(), spec.GatherCores())
		case WorkloadReplaySetupName:
			clusterSpec = r.MakeClusterSpec(1)
			timeOut = time.Hour * 2
			redactResults = true
		default:
			clusterSpec = r.MakeClusterSpec(1)
		}
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("costfuzz/%s", setupName),
			Owner:            registry.OwnerSQLQueries,
			Timeout:          timeOut,
			RedactResults:    redactResults,
			Cluster:          clusterSpec,
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			NativeLibs:       registry.LibGEOS,
			Randomized:       true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				// When running in CI, only allow running workload-replay in the private roachtest,
				// which has the required credentials.
				if setupName == WorkloadReplaySetupName && os.Getenv("TC_BUILD_ID") != "" && os.Getenv("ROACHTEST_PRIVATE") != "1" {
					t.Skipf("runs in private roachtest only")
					return
				}
				runQueryComparison(ctx, t, c, &queryComparisonTest{
					name:          "costfuzz",
					setupName:     setupName,
					isMultiRegion: clusterSpec.Geo,
					nodeCount:     clusterSpec.NodeCount,
					run:           runCostFuzzQuery,
				})
			},
			ExtraLabels: []string{"O-rsg"},
		})
	}
}

// runCostFuzzQuery executes the same query two times, once with normal costs
// and once with randomly perturbed costs. If the results of the two executions
// are not equal an error is returned.
func runCostFuzzQuery(qgen queryGenerator, rnd *rand.Rand, h queryComparisonHelper) error {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	stmt := qgen.Generate()
	conn, connInfo := h.chooseConn()

	// First, run the statement without cost perturbation.
	controlRows, err := h.runQuery(stmt, conn, connInfo)
	if err != nil {
		// Skip statements that fail with an error.
		//nolint:returnerrcheck
		return nil
	}

	// Maybe use a different connection for the second query.
	conn, connInfo = h.chooseConn()

	seedStmt := fmt.Sprintf("SET testing_optimizer_random_seed = %d", rnd.Int63())
	if err := h.execStmt(seedStmt, conn, connInfo); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to set random seed")
	}
	// Perturb costs such that an expression with cost c will be randomly assigned
	// a new cost in the range [0, 2*c).
	perturbCostsStmt := "SET testing_optimizer_cost_perturbation = 1.0"
	if err := h.execStmt(perturbCostsStmt, conn, connInfo); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to perturb costs")
	}

	// Then, rerun the statement with cost perturbation.
	perturbRows, err2 := h.runQuery(stmt, conn, connInfo)
	if err2 != nil {
		// If the perturbed plan fails with an internal error while the normal plan
		// succeeds, we'd like to know, so consider this a test failure.
		es := err2.Error()
		if strings.Contains(es, "internal error") {
			h.logStatements()
			h.logVerboseOutput()
			return h.makeError(err, "internal error while running perturbed statement")
		}
		// Otherwise, skip perturbed statements that fail with a non-internal
		// error. This could happen if the statement contains bad arguments to a
		// function call, for example, and the normal plan was able to skip
		// evaluation of the function due to short-circuiting (see #81032 for an
		// example).
		//nolint:returnerrcheck
		return nil
	}

	diff, err := unsortedMatricesDiffWithFloatComp(controlRows, perturbRows, h.colTypes)
	if err != nil {
		return err
	}
	if diff != "" {
		// We have a mismatch in the perturbed vs control query outputs.
		h.logStatements()
		h.logVerboseOutput()
		return h.makeError(errors.Newf(
			"expected unperturbed and perturbed results to be equal\n%s\nsql: %s\n",
			diff, stmt,
		), "")
	}

	// TODO(michae2): If we run into the "-0 flake" described in PR #79551 then
	// we'll need some other strategy for comparison besides diffing the printed
	// results. One idea is to CREATE TABLE AS SELECT with both queries, and then
	// EXCEPT ALL the table contents. But this might be very slow.

	// Finally, disable cost perturbation for the next statement.
	resetSeedStmt := "RESET testing_optimizer_random_seed"
	if err := h.execStmt(resetSeedStmt, conn, connInfo); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to reset random seed")
	}
	resetPerturbCostsStmt := "RESET testing_optimizer_cost_perturbation"
	if err := h.execStmt(resetPerturbCostsStmt, conn, connInfo); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to disable cost perturbation")
	}
	return nil
}
