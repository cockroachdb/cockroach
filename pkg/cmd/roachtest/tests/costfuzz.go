// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func registerCostFuzz(r registry.Registry) {
	for _, setupName := range []string{sqlsmith.RandTableSetupName, sqlsmith.SeedMultiRegionSetupName} {
		setupName := setupName
		var clusterSpec spec.ClusterSpec
		switch setupName {
		case sqlsmith.SeedMultiRegionSetupName:
			clusterSpec = r.MakeClusterSpec(4, spec.Geo())
		default:
			clusterSpec = r.MakeClusterSpec(1)
		}
		r.Add(registry.TestSpec{
			Name:            fmt.Sprintf("costfuzz/%s", setupName),
			Owner:           registry.OwnerSQLQueries,
			Timeout:         time.Hour * 1,
			RequiresLicense: true,
			Tags:            nil,
			Cluster:         clusterSpec,
			NativeLibs:      registry.LibGEOS,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runQueryComparison(ctx, t, c, &queryComparisonTest{
					name: "costfuzz", setupName: setupName, run: runCostFuzzQuery,
				})
			},
		})
	}
}

// runCostFuzzQuery executes the same query two times, once with normal costs
// and once with randomly perturbed costs. If the results of the two executions
// are not equal an error is returned.
func runCostFuzzQuery(smither *sqlsmith.Smither, rnd *rand.Rand, h queryComparisonHelper) error {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	// Drop the control and perturb if they exist.
	err := h.execStmt("DROP TABLE IF EXISTS control")
	if err != nil {
		fmt.Println("FAILED ON FIRST DROP")
		h.logStatements()
		return err
	}
	err = h.execStmt("DROP TABLE IF EXISTS perturb")
	if err != nil {
		fmt.Println("FAILED ON SECOND DROP")
		h.logStatements()
		return err
	}

	stmt := smither.Generate()

	// First, run the statement without cost perturbation and save the results
	// to the control table.
	err = h.execStmt(fmt.Sprintf("CREATE TABLE control AS (%s)", stmt))
	if err != nil {
		fmt.Println("FAILED ON FIRST CREATE")
		fmt.Println(err)
		h.logStatements()
		// Skip statements that fail with an error.
		//nolint:returnerrcheck
		return nil
	}

	seedStmt := fmt.Sprintf("SET testing_optimizer_random_seed = %d", rnd.Int63())
	if err := h.execStmt(seedStmt); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to set random seed")
	}
	// Perturb costs such that an expression with cost c will be randomly assigned
	// a new cost in the range [0, 2*c).
	perturbCostsStmt := "SET testing_optimizer_cost_perturbation = 1.0"
	if err := h.execStmt(perturbCostsStmt); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to perturb costs")
	}

	// Then, rerun the statement with cost perturbation and save the results to
	// the perturb table.
	err2 := h.execStmt(fmt.Sprintf("CREATE TABLE perturb AS (%s)", stmt))
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

	// The contents of control and perturb should identical.
	diff1, err := h.runQuery("SELECT * FROM control EXCEPT ALL SELECT * FROM perturb")
	if err != nil {
		return err
	}
	diff2, err := h.runQuery("SELECT * FROM perturb EXCEPT ALL SELECT * FROM control")
	if err != nil {
		return err
	}
	if len(diff1) > 0 || len(diff2) > 0 {
		// We have a mismatch in the perturbed vs control query outputs.
		h.logStatements()
		h.logVerboseOutput()
		var diff1Str strings.Builder
		for i := range diff1 {
			diff1Str.WriteString(strings.Join(diff1[i], ","))
			diff1Str.WriteByte('\n')
		}
		var diff2Str strings.Builder
		for i := range diff2 {
			diff2Str.WriteString(strings.Join(diff2[i], ","))
			diff2Str.WriteByte('\n')
		}
		return h.makeError(errors.Newf(
			"expected unperturbed and perturbed results to be equal\ndiff1:\n%s\ndiff2:\n%s\nsql: %s\n",
			diff1Str.String(), diff2Str.String(), stmt,
		), "")
	}

	// Finally, disable cost perturbation for the next statement.
	resetSeedStmt := "RESET testing_optimizer_random_seed"
	if err := h.execStmt(resetSeedStmt); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to reset random seed")
	}
	resetPerturbCostsStmt := "RESET testing_optimizer_cost_perturbation"
	if err := h.execStmt(resetPerturbCostsStmt); err != nil {
		h.logStatements()
		return h.makeError(err, "failed to disable cost perturbation")
	}
	return nil
}
