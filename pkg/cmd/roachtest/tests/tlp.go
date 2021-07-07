// Copyright 2021 The Cockroach Authors.
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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const statementTimeout = time.Minute

func registerTLP(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "tlp",
		Owner:   registry.OwnerSQLQueries,
		Timeout: time.Minute * 5,
		Tags:    nil,
		Cluster: r.MakeClusterSpec(1),
		Run:     runTLP,
	})
}

func runTLP(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a TLP error.
	tlpLog, err := os.Create(filepath.Join(t.ArtifactsDir(), "tlp.log"))
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

	conn := c.Conn(ctx, 1)

	rnd, seed := randutil.NewPseudoRand()
	t.L().Printf("seed: %d", seed)

	c.Put(ctx, t.Cockroach(), "./cockroach")
	if err := c.PutLibraries(ctx, "./lib"); err != nil {
		t.Fatalf("could not initialize libraries: %v", err)
	}
	c.Start(ctx)

	setup := sqlsmith.Setups["rand-tables"](rnd)

	t.Status("executing setup")
	t.L().Printf("setup:\n%s", setup)
	if _, err := conn.Exec(setup); err != nil {
		t.Fatal(err)
	} else {
		logStmt(setup)
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
	smither, err := sqlsmith.NewSmither(conn, rnd, sqlsmith.MutationsOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	t.Status("running TLP")
	until := time.After(t.Spec().(*registry.TestSpec).Timeout / 2)
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
		// for a tenth of the iterations after that to continually change the
		// state of the database.
		if i < 1000 || i%10 == 0 {
			runMutationStatement(conn, smither, logStmt)
			continue
		}

		if err := runTLPQuery(conn, smither, logStmt); err != nil {
			t.Fatal(err)
		}
	}
}

// runMutationsStatement runs a random INSERT, UPDATE, or DELETE statement that
// potentially modifies the state of the database.
func runMutationStatement(conn *gosql.DB, smither *sqlsmith.Smither, logStmt func(string)) {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	stmt := smither.Generate()

	// Ignore timeouts.
	_ = runWithTimeout(func() error {
		// Ignore errors. Log successful statements.
		if _, err := conn.Exec(stmt); err == nil {
			logStmt(stmt)
		}
		return nil
	})
}

// runTLPQuery runs two queries to perform TLP. If the results of the query are
// not equal, an error is returned. Currently GenerateTLP always returns
// unpartitioned and partitioned queries of the form "SELECT count(*) ...". The
// resulting counts of the queries are compared in order to verify logical
// correctness. See GenerateTLP for more information on TLP and the generated
// queries.
func runTLPQuery(conn *gosql.DB, smither *sqlsmith.Smither, logStmt func(string)) error {
	// Ignore panics from GenerateTLP.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	unpartitioned, partitioned := smither.GenerateTLP()

	return runWithTimeout(func() error {
		var unpartitionedCount int
		row := conn.QueryRow(unpartitioned)
		if err := row.Scan(&unpartitionedCount); err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}

		var partitionedCount int
		row = conn.QueryRow(partitioned)
		if err := row.Scan(&partitionedCount); err != nil {
			// Ignore errors.
			//nolint:returnerrcheck
			return nil
		}

		if unpartitionedCount != partitionedCount {
			logStmt(unpartitioned)
			logStmt(partitioned)
			return errors.Newf(
				"expected unpartitioned count %d to equal partitioned count %d\nsql: %s\n%s",
				unpartitionedCount, partitionedCount, unpartitioned, partitioned)
		}

		return nil
	})
}

func runWithTimeout(f func() error) error {
	done := make(chan error, 1)
	go func() {
		err := f()
		done <- err
	}()
	select {
	case <-time.After(statementTimeout + time.Second*5):
		// Ignore timeouts.
		return nil
	case err := <-done:
		return err
	}
}
