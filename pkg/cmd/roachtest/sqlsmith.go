// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func registerSQLSmith(r *testRegistry) {
	setups := map[string]sqlsmith.Setup{
		"empty":       sqlsmith.Setups["empty"],
		"seed":        sqlsmith.Setups["seed"],
		"rand-tables": sqlsmith.Setups["rand-tables"],
		"tpch-sf1": func(r *rand.Rand) string {
			return `RESTORE TABLE tpch.* FROM 'gs://cockroach-fixtures/workload/tpch/scalefactor=1/backup' WITH into_db = 'defaultdb';`
		},
		"tpcc": func(r *rand.Rand) string {
			const version = "version=2.1.0,fks=true,interleaved=false,seed=1,warehouses=1"
			var sb strings.Builder
			for _, t := range []string{
				"customer",
				"district",
				"history",
				"item",
				"new_order",
				"order",
				"order_line",
				"stock",
				"warehouse",
			} {
				fmt.Fprintf(&sb, "RESTORE TABLE tpcc.%s FROM 'gs://cockroach-fixtures/workload/tpcc/%[2]s/%[1]s' WITH into_db = 'defaultdb';\n", t, version)
			}
			return sb.String()
		},
	}
	settings := map[string]sqlsmith.SettingFunc{
		"default":      sqlsmith.Settings["default"],
		"no-mutations": sqlsmith.Settings["no-mutations"],
		"no-ddl":       sqlsmith.Settings["no-ddl"],
	}

	runSQLSmith := func(ctx context.Context, t *test, c *cluster, setupName, settingName string) {
		// Set up a statement logger for easy reproduction. We only
		// want to log successful statements and statements that
		// produced a final error or panic.
		smithLog, err := os.Create(filepath.Join(t.artifactsDir, "sqlsmith.log"))
		if err != nil {
			t.Fatalf("could not create sqlsmith.log: %v", err)
		}
		defer smithLog.Close()
		logStmt := func(stmt string) {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				return
			}
			fmt.Fprint(smithLog, stmt)
			if !strings.HasSuffix(stmt, ";") {
				fmt.Fprint(smithLog, ";")
			}
			fmt.Fprint(smithLog, "\n\n")
		}

		rng, seed := randutil.NewPseudoRand()
		c.l.Printf("seed: %d", seed)

		c.Put(ctx, cockroach, "./cockroach")
		c.Start(ctx, t)

		setupFunc, ok := setups[setupName]
		if !ok {
			t.Fatalf("unknown setup %s", setupName)
		}
		settingFunc, ok := settings[settingName]
		if !ok {
			t.Fatalf("unknown setting %s", settingName)
		}

		setup := setupFunc(rng)
		setting := settingFunc(rng)

		conn := c.Conn(ctx, 1)
		t.Status("executing setup")
		c.l.Printf("setup:\n%s", setup)
		if _, err := conn.Exec(setup); err != nil {
			t.Fatal(err)
		} else {
			logStmt(setup)
		}

		const timeout = time.Minute
		setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", timeout.String())
		t.Status("setting statement_timeout")
		c.l.Printf("statement timeout:\n%s", setStmtTimeout)
		if _, err := conn.Exec(setStmtTimeout); err != nil {
			t.Fatal(err)
		}
		logStmt(setStmtTimeout)

		smither, err := sqlsmith.NewSmither(conn, rng, setting.Options...)
		if err != nil {
			t.Fatal(err)
		}

		t.Status("smithing")
		until := time.After(t.spec.Timeout / 2)
		done := ctx.Done()
		for i := 1; ; i++ {
			if i%10000 == 0 {
				t.Status("smithing: ", i, " statements completed")
			}
			select {
			case <-done:
				return
			case <-until:
				return
			default:
			}
			stmt := smither.Generate()
			err := func() error {
				done := make(chan error, 1)
				go func(context.Context) {
					// At the moment, CockroachDB doesn't support pgwire query
					// cancellation which is needed for correct handling of context
					// cancellation, so instead of using a context with timeout, we opt
					// in for using CRDB's 'statement_timeout'.
					// TODO(yuzefovich): once #41335 is implemented, go back to using a
					// context with timeout.
					_, err := conn.Exec(stmt)
					if err == nil {
						logStmt(stmt)
					}
					done <- err
				}(ctx)
				select {
				case <-time.After(timeout * 2):
					// SQLSmith generates queries that either perform full table scans of
					// large tables or backup/restore operations that timeout. These
					// should not cause an issue to be raised, as they most likely are
					// just timing out.
					// TODO (rohany): once #45463 and #45461 have been resolved, return
					//  to calling t.Fatalf here.
					c.l.Printf("query timed out, but did not cancel execution:\n%s;", stmt)
					return nil
				case err := <-done:
					return err
				}
			}()
			if err != nil {
				es := err.Error()
				if strings.Contains(es, "internal error") {
					logStmt(stmt)
					t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
				} else if strings.Contains(es, "communication error") {
					// A communication error can be because
					// a non-gateway node has crashed.
					logStmt(stmt)
					t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
				}
				// Ignore other errors because they happen so
				// frequently (due to sqlsmith not crafting
				// executable queries 100% of the time) and are
				// never interesting.
			}

			// Ping the gateway to make sure it didn't crash.
			if err := conn.PingContext(ctx); err != nil {
				logStmt(stmt)
				t.Fatalf("ping: %v\nprevious sql:\n%s;", err, stmt)
			}
		}
	}

	register := func(setup, setting string) {
		r.Add(testSpec{
			Name: fmt.Sprintf("sqlsmith/setup=%s/setting=%s", setup, setting),
			// NB: sqlsmith failures should never block a release.
			Owner:      OwnerSQLExec,
			Cluster:    makeClusterSpec(4),
			MinVersion: "v20.2.0",
			Timeout:    time.Minute * 20,
			Run: func(ctx context.Context, t *test, c *cluster) {
				runSQLSmith(ctx, t, c, setup, setting)
			},
		})
	}

	for setup := range setups {
		for setting := range settings {
			register(setup, setting)
		}
	}
	setups["seed-vec"] = sqlsmith.Setups["seed-vec"]
	settings["ddl-nodrop"] = sqlsmith.Settings["ddl-nodrop"]
	settings["vec"] = sqlsmith.SettingVectorize
	register("seed-vec", "vec")
	register("tpcc", "ddl-nodrop")
}
