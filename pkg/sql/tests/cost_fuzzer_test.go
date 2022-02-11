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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq"
)

func TestCostFuzzer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	outDir := scope.GetDirectory()
	defer scope.Close(t)
	ctx := context.Background()
	cluster := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{})
	cluster.Start(t)
	defer cluster.Stopper().Stop(ctx)

	conn := cluster.Conns[0]
	rng, _ := randutil.NewTestRand()

	setup := sqlsmith.Setups[sqlsmith.RandTableSetupName](rng)
	if _, err := conn.Exec(setup); err != nil {
		t.Fatal(err)
	}
	setupLog := []string{setup}

	// Initialize a smither that generates only INSERT, UPDATE, and DELETE
	// statements with the MutationsOnly option.
	mutatingSmither, err := sqlsmith.NewSmither(conn, rng, sqlsmith.MutationsOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer mutatingSmither.Close()
	smither, err := sqlsmith.NewSmither(conn, rng,
		sqlsmith.DisableMutations(), sqlsmith.DisableImpureFuncs(), sqlsmith.DisableLimits(),
		sqlsmith.SetComplexity(.3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	logfile := filepath.Join(outDir, "setup.log")
	defer func() {
		// If the test failed, print out the log of create table statements and
		// mutations so reproductions can be created.
		if t.Failed() {
			if err := os.WriteFile(logfile, []byte(strings.Join(setupLog, ";\n\n")+";"), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}()

	runner := sqlutils.MakeSQLRunner(conn)
	for i := 0; i < 100000; i++ {
		_, err = conn.Exec("SET testing_optimizer_random_cost_seed = 0")
		if err != nil {
			t.Fatalf("failed to perturb costs %s", err)
		}
		// Run 1000 mutations first so that the tables have rows. Run a mutation
		// for a tenth of the iterations after that to continually change the
		// state of the database.
		if i < 1000 || i%10 == 0 {
			stmt := mutatingSmither.Generate()
			setupLog = append(setupLog, stmt)
			if _, err := conn.Exec(stmt); err != nil {
				// Errors during execution of randomized mutations are often about
				// things like inserting into tables with foreign keys that aren't
				// properly matching. We can put these errors out to pasture.
				continue
			}
		}

		stmt := smither.Generate()

		// First, run the statement without cost perturbation.
		rows, err := conn.Query(stmt)
		if err != nil {
			// Ignore errors.
			continue
		}
		defer rows.Close()
		unperturbedRows, err := sqlutils.RowsToStrMatrix(rows)
		if err != nil {
			// Ignore errors.
			continue
		}
		seed := rng.Int63()
		_, err = conn.Exec(fmt.Sprintf("SET testing_optimizer_random_cost_seed = %d", seed))
		if err != nil {
			t.Fatalf("failed to perturb costs %s", err)
		}

		// Then, run the query with cost perturbation.
		rows2, err := conn.Query(stmt)
		if err != nil {
			pErr := err.(*pq.Error)
			if !strings.HasPrefix(string(pErr.Code), "XX") {
				continue
			}
			t.Fatalf(`internal error while running perturbed query: %v

Repro sql (paste into cockroach demo):

\i %s
SET testing_optimizer_random_cost_seed = %d;
%s
;
`, err, logfile, seed, stmt)
		}
		perturbedRows, err := sqlutils.RowsToStrMatrix(rows2)
		_ = rows2.Close()
		if err != nil {
			// Ignore errors.
			continue
		}
		if diff := UnsortedMatricesDiff(unperturbedRows, perturbedRows); diff != "" {
			// We have a mismatch in the perturbed vs non-perturbed query outputs.
			// Output the real plan and the perturbed plan, along with the seed, so
			// that the perturbed query is reproducible.
			perturbedExplain := sqlutils.MatrixToStr(runner.QueryStr(t, "EXPLAIN "+stmt))
			runner.Exec(t, "SET testing_optimizer_random_cost_seed = 0")
			unperturbedExplain := sqlutils.MatrixToStr(runner.QueryStr(t, "EXPLAIN "+stmt))

			t.Fatalf(
				`expected unperturbed and perturbed results to be equal:
%s

sql:
%s

testing_optimizer_random_cost_seed = %d
non-perturbed explain:
%s

perturbed explain:
%s

schema:
%s

%s
%s

setup logs written to %s
`,
				diff, stmt, seed, unperturbedExplain, perturbedExplain, setup, unperturbedRows, perturbedRows, logfile)
		}
	}
}
