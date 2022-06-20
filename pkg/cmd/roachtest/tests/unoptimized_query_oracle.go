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
	gosql "database/sql"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

func registerUnoptimizedQueryOracle(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "unoptimized-query-oracle",
		Owner:           registry.OwnerSQLQueries,
		Timeout:         time.Hour * 1,
		RequiresLicense: true,
		Tags:            nil,
		Cluster:         r.MakeClusterSpec(1),
		Run:             runUnoptimizedQueryOracle,
	})
}

func runUnoptimizedQueryOracle(ctx context.Context, t test.Test, c cluster.Cluster) {
	runQueryComparison(ctx, t, c,
		&queryComparisonTest{name: "unoptimized-query-oracle", run: runUnoptimizedQueryOracleImpl})
}

// runUnoptimizedQueryOracleImpl executes the same query two times, once with
// the optimizer and vectorized execution engine disabled, and once with normal
// optimization and/or execution. If the results of the two executions are not
// equal an error is returned.
func runUnoptimizedQueryOracleImpl(
	conn *gosql.DB, smither *sqlsmith.Smither, rnd *rand.Rand, logStmt func(string),
) error {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	stmt := smither.Generate()

	// Keep track of each statement that is executed so we can log them all later.
	var statements []string
	execStmt := func(stmt string) error {
		statements = append(statements, stmt)
		_, err := conn.Exec(stmt)
		return err
	}
	runQuery := func(stmt string) (*gosql.Rows, error) {
		statements = append(statements, stmt)
		return conn.Query(stmt)
	}
	logStatements := func() {
		for _, stmt := range statements {
			logStmt(stmt)
		}
	}

	// First, run the statement with the optimizer and execution engine disabled.
	disableOptimizerStmt := "SET testing_optimizer_disable_rule_probability = 1"
	if err := execStmt(disableOptimizerStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to disable the optimizer")
	}
	disableVectorizeStmt := "SET vectorize = off"
	if err := execStmt(disableVectorizeStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to disable the vectorized engine")
	}

	rows, err := runQuery(stmt)
	if err != nil {
		// Skip unoptimized statements that fail with an error.
		//nolint:returnerrcheck
		return nil
	}
	defer rows.Close()
	unoptimizedRows, err := sqlutils.RowsToStrMatrix(rows)
	if err != nil {
		// Skip statements whose results cannot be printed.
		//nolint:returnerrcheck
		return nil
	}

	// Re-enable either the optimizer, vectorized execution, or both for the next
	// statement.
	enable := rand.Intn(3)
	if enable > 0 {
		resetOptimizerStmt := "RESET testing_optimizer_disable_rule_probability"
		if err := execStmt(resetOptimizerStmt); err != nil {
			logStatements()
			return errors.Wrap(err, "failed to reset the optimizer")
		}
	}
	if enable < 2 {
		resetVectorizeStmt := "RESET vectorize"
		if err := execStmt(resetVectorizeStmt); err != nil {
			logStatements()
			return errors.Wrap(err, "failed to reset the vectorized engine")
		}
	}

	// Then, rerun the statement with optimization and vectorization enabled.
	rows2, err := runQuery(stmt)
	if err != nil {
		// If the optimized plan fails with an internal error while the unoptimized plan
		// succeeds, we'd like to know, so consider this a test failure.
		es := err.Error()
		if strings.Contains(es, "internal error") {
			logStatements()
			return errors.Wrap(err, "internal error while running optimized statement")
		}
		// Otherwise, skip optimized statements that fail with a non-internal
		// error. This could happen if the statement contains bad arguments to a
		// function call, for example, and the unoptimized plan was able to skip
		// evaluation of the function due to short-circuiting (see #81032 for an
		// example).
		//nolint:returnerrcheck
		return nil
	}
	defer rows2.Close()
	optimizedRows, err := sqlutils.RowsToStrMatrix(rows2)
	// If we've gotten this far, we should be able to print the results of the
	// optimized statement, so consider it a test failure if we cannot.
	if err != nil {
		logStatements()
		return errors.Wrap(err, "error while printing optimized statement results")
	}
	if diff := unsortedMatricesDiff(unoptimizedRows, optimizedRows); diff != "" {
		// We have a mismatch in the unoptimized vs optimized query outputs.
		logStatements()
		return errors.Newf(
			"expected unoptimized and optimized results to be equal\n%s\nsql: %s",
			diff, stmt,
		)
	}

	return nil
}
