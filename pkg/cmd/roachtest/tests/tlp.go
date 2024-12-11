// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const statementTimeout = time.Minute

func registerTLP(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "tlp",
		Owner:            registry.OwnerSQLQueries,
		Timeout:          time.Hour * 1,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		NativeLibs:       registry.LibGEOS,
		Randomized:       true,
		Run:              runTLP,
		ExtraLabels:      []string{"O-rsg"},
	})
}

func runTLP(ctx context.Context, t test.Test, c cluster.Cluster) {
	timeout := 10 * time.Minute
	// Run 10 minute iterations of TLP in a loop for about the entire test,
	// giving 5 minutes at the end to allow the test to shut down cleanly.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, t.Spec().(*registry.TestSpec).Timeout-5*time.Minute)
	defer cancel()
	done := ctx.Done()
	shouldExit := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	for i := 0; ; i++ {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		if shouldExit() {
			return
		}

		runOneTLP(ctx, i, timeout, t, c)
		// If this iteration of the TLP was interrupted because the timeout of
		// ctx has been reached, we want to cleanly exit from the test, without
		// wiping out the cluster (if we tried that, we'd get an error because
		// ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
		c.Wipe(ctx)
	}
}

func runOneTLP(
	ctx context.Context, iter int, timeout time.Duration, t test.Test, c cluster.Cluster,
) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a TLP error.
	tlpLog, err := os.Create(filepath.Join(t.ArtifactsDir(), fmt.Sprintf("tlp%03d.log", iter)))
	if err != nil {
		t.Fatalf("could not create tlp.log: %v", err)
	}
	defer tlpLog.Close()
	logStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(tlpLog, stmt)
		if !strings.HasSuffix(stmt, ";") {
			fmt.Fprint(tlpLog, ";")
		}
		fmt.Fprint(tlpLog, "\n\n")
	}

	conn := c.Conn(ctx, t.L(), 1)

	rnd, seed := randutil.NewTestRand()
	t.L().Printf("seed: %d", seed)

	setup := sqlsmith.Setups[sqlsmith.RandTableSetupName](rnd)

	t.Status("executing setup")
	t.L().Printf("setup:\n%s", strings.Join(setup, "\n"))
	for _, stmt := range setup {
		if _, err := conn.Exec(stmt); err != nil {
			t.Fatal(err)
		} else {
			logStmt(stmt)
		}
	}

	setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", statementTimeout.String())
	t.Status("setting statement_timeout")
	t.L().Printf("statement timeout:\n%s", setStmtTimeout)
	if _, err := conn.Exec(setStmtTimeout); err != nil {
		t.Fatal(err)
	}
	logStmt(setStmtTimeout)

	// Initialize a smither that generates only INSERT, UPDATE, and DELETE
	// statements with the MutationsOnly option. Smither.GenerateTLP always
	// returns SELECT queries, so the MutationsOnly option is used only for
	// randomly mutating the database.
	mutSmither, err := sqlsmith.NewSmither(conn, rnd, sqlsmith.MutationsOnly(), sqlsmith.SimpleNames())
	if err != nil {
		t.Fatal(err)
	}
	defer mutSmither.Close()

	// Initialize a smither that will never generate mutations.
	tlpSmither, err := sqlsmith.NewSmither(conn, rnd,
		sqlsmith.DisableMutations(), sqlsmith.DisableNondeterministicFns(), sqlsmith.SimpleNames(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tlpSmither.Close()

	t.Status("running TLP")
	until := time.After(timeout)
	done := ctx.Done()
	for i := 1; ; i++ {
		select {
		case <-until:
			return
		case <-done:
			return
		default:
		}

		if i%1000 == 0 {
			t.Status("running TLP: ", i, " statements completed")
		}

		// Run 1000 mutations first so that the tables have rows. Run a mutation
		// for a fraction of iterations after that to continually change the
		// state of the database.
		if i < 1000 || i%10 == 0 {
			runMutationStatement(t, conn, "", mutSmither, logStmt)
			continue
		}

		if err := runTLPQuery(t, conn, tlpSmither, logStmt); err != nil {
			t.Fatal(err)
		}
	}
}

// runMutationsStatement runs a random INSERT, UPDATE, or DELETE statement that
// potentially modifies the state of the database.
func runMutationStatement(
	t task.Tasker, conn *gosql.DB, connInfo string, smither *sqlsmith.Smither, logStmt func(string),
) {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	stmt := smither.Generate()

	// Ignore timeouts.
	var err error
	_ = runWithTimeout(t, func() error {
		// Ignore errors. Log successful statements.
		if _, err = conn.Exec(stmt); err == nil {
			logStmt(connInfo + stmt)
		}
		return nil
	})
}

// runTLPQuery runs two queries to perform TLP. One is partitioned and one is
// unpartitioned. If the results of the queries are not equal, an error is
// returned. GenerateTLP also returns any placeholder arguments needed for the
// partitioned query. See GenerateTLP for more information on TLP and the
// generated queries.
func runTLPQuery(
	t task.Tasker, conn *gosql.DB, smither *sqlsmith.Smither, logStmt func(string),
) error {
	// Ignore panics from GenerateTLP.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	unpartitioned, partitioned, args := smither.GenerateTLP()
	combined := sqlsmith.CombinedTLP(unpartitioned, partitioned)

	return runWithTimeout(t, func() error {
		counts := conn.QueryRow(combined, args...)
		var undiffCount, diffCount int
		if err := counts.Scan(&undiffCount, &diffCount); err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}
		if undiffCount == 0 && diffCount == 0 {
			return nil
		}

		// We found a TLP mismatch! Run individual queries again to print a diff.

		rows1, err := conn.Query(unpartitioned)
		if err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}
		defer rows1.Close()
		unpartitionedRows, err := sqlutils.RowsToStrMatrix(rows1)
		if err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}
		rows2, err := conn.Query(partitioned, args...)
		if err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}
		defer rows2.Close()
		partitionedRows, err := sqlutils.RowsToStrMatrix(rows2)
		if err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}

		diff := unsortedMatricesDiff(unpartitionedRows, partitionedRows)
		logStmt(unpartitioned)
		logStmt(partitioned)
		return errors.Newf(
			"expected unpartitioned and partitioned results to be equal\n%s\nsql: %s\n%s\nwith args: %s",
			diff, unpartitioned, partitioned, args)
	})
}

func runWithTimeout(t task.Tasker, f func() error) error {
	done := make(chan error, 1)
	t.Go(func(context.Context, *logger.Logger) error {
		err := f()
		done <- err
		return nil
	})
	select {
	case <-time.After(statementTimeout + time.Second*5):
		// Ignore timeouts.
		return nil
	case err := <-done:
		return err
	}
}
