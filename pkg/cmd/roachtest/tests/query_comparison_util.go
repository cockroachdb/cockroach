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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type queryComparisonTest struct {
	name string
	run  func(*gosql.DB, *sqlsmith.Smither, *rand.Rand, func(string)) error
}

func runQueryComparison(
	ctx context.Context, t test.Test, c cluster.Cluster, qct *queryComparisonTest,
) {
	roundTimeout := 10 * time.Minute
	// Run 10 minute iterations of query comparison in a loop for about the entire
	// test, giving 5 minutes at the end to allow the test to shut down cleanly.
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

	c.Put(ctx, t.Cockroach(), "./cockroach")
	if err := c.PutLibraries(ctx, "./lib"); err != nil {
		t.Fatalf("could not initialize libraries: %v", err)
	}

	for i := 0; ; i++ {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		if shouldExit() {
			return
		}

		runOneRoundQueryComparison(ctx, i, roundTimeout, t, c, qct)
		// If this iteration was interrupted because the timeout of ctx has been
		// reached, we want to cleanly exit from the test, without wiping out the
		// cluster (if we tried that, we'd get an error because ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
		c.Wipe(ctx)
	}
}

// runOneRoundQueryComparison creates a random schema, inserts random data, and
// then executes queries until the roundTimeout is reached.
func runOneRoundQueryComparison(
	ctx context.Context,
	iter int,
	roundTimeout time.Duration,
	t test.Test,
	c cluster.Cluster,
	qct *queryComparisonTest,
) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a final error or panic.
	logPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("%s%03d.log", qct.name, iter))
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("could not create %s.log: %v", qct.name, err)
	}
	defer log.Close()
	logStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(log, stmt)
		if !strings.HasSuffix(stmt, ";") {
			fmt.Fprint(log, ";")
		}
		fmt.Fprint(log, "\n\n")
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
	// statements with the MutationsOnly option.
	mutatingSmither, err := sqlsmith.NewSmither(conn, rnd, sqlsmith.MutationsOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer mutatingSmither.Close()

	// Initialize a smither that generates only deterministic SELECT statements.
	smither, err := sqlsmith.NewSmither(conn, rnd,
		sqlsmith.DisableMutations(), sqlsmith.DisableImpureFns(), sqlsmith.DisableLimits(),
		sqlsmith.SetComplexity(.3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	t.Status("running ", qct.name)
	until := time.After(roundTimeout)
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
			t.Status("running ", qct.name, ": ", i, " statements completed")
		}

		// Run 1000 mutations first so that the tables have rows. Run a mutation for
		// a tenth of the iterations after that to continually change the state of
		// the database.
		if i < 1000 || i%10 == 0 {
			runMutationStatement(conn, mutatingSmither, logStmt)
			continue
		}

		if err := qct.run(conn, smither, rnd, logStmt); err != nil {
			t.Fatal(err)
		}
	}
}
