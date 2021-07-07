// Copyright 2019 The Cockroach Authors.
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

func registerSQLSmith(r registry.Registry) {
	setups := map[string]sqlsmith.Setup{
		"empty":       sqlsmith.Setups["empty"],
		"seed":        sqlsmith.Setups["seed"],
		"rand-tables": sqlsmith.Setups["rand-tables"],
		"tpch-sf1": func(r *rand.Rand) string {
			return `RESTORE TABLE tpch.* FROM 'gs://cockroach-fixtures/workload/tpch/scalefactor=1/backup?AUTH=implicit' WITH into_db = 'defaultdb';`
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
				fmt.Fprintf(&sb, "RESTORE TABLE tpcc.%s FROM 'gs://cockroach-fixtures/workload/tpcc/%[2]s/%[1]s?AUTH=implicit' WITH into_db = 'defaultdb';\n", t, version)
			}
			return sb.String()
		},
	}
	settings := map[string]sqlsmith.SettingFunc{
		"default":      sqlsmith.Settings["default"],
		"no-mutations": sqlsmith.Settings["no-mutations"],
		"no-ddl":       sqlsmith.Settings["no-ddl"],
	}

	runSQLSmith := func(ctx context.Context, t test.Test, c cluster.Cluster, setupName, settingName string) {
		// Set up a statement logger for easy reproduction. We only
		// want to log successful statements and statements that
		// produced a final error or panic.
		smithLog, err := os.Create(filepath.Join(t.ArtifactsDir(), "sqlsmith.log"))
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
		t.L().Printf("seed: %d", seed)

		c.Put(ctx, t.Cockroach(), "./cockroach")
		if err := c.PutLibraries(ctx, "./lib"); err != nil {
			t.Fatalf("could not initialize libraries: %v", err)
		}
		c.Start(ctx)

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
		t.L().Printf("setup:\n%s", setup)
		if _, err := conn.Exec(setup); err != nil {
			t.Fatal(err)
		} else {
			logStmt(setup)
		}

		const timeout = time.Minute
		setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", timeout.String())
		t.Status("setting statement_timeout")
		t.L().Printf("statement timeout:\n%s", setStmtTimeout)
		if _, err := conn.Exec(setStmtTimeout); err != nil {
			t.Fatal(err)
		}
		logStmt(setStmtTimeout)

		smither, err := sqlsmith.NewSmither(conn, rng, setting.Options...)
		if err != nil {
			t.Fatal(err)
		}

		// We will enable panic injection on this connection in the vectorized
		// engine (and will ignore the injected errors) in order to test that
		// the panic-catching mechanism of error propagation works as expected.
		// Note: it is important to enable this testing knob only after all
		// other setup queries have already completed, including the smither
		// instantiation (otherwise, the setup might fail because of the
		// injected panics).
		injectPanicsStmt := "SET testing_vectorize_inject_panics=true;"
		if _, err := conn.Exec(injectPanicsStmt); err != nil {
			t.Fatal(err)
		}
		logStmt(injectPanicsStmt)

		t.Status("smithing")
		until := time.After(t.Spec().(*registry.TestSpec).Timeout / 2)
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

			stmt := ""
			err := func() error {
				done := make(chan error, 1)
				go func(context.Context) {
					// Generate can potentially panic in bad cases, so
					// to avoid Go routines from dying we are going
					// catch that here, and only pass the error into
					// the channel.
					defer func() {
						if r := recover(); r != nil {
							done <- errors.Newf("Caught error %s", r)
							return
						}
					}()

					stmt = smither.Generate()
					if stmt == "" {
						// If an empty statement is generated, then ignore it.
						done <- errors.Newf("Empty statement returned by generate")
						return
					}

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
					t.L().Printf("query timed out, but did not cancel execution:\n%s;", stmt)
					return nil
				case err := <-done:
					return err
				}
			}()
			if err != nil {
				es := err.Error()
				if strings.Contains(es, "internal error") {
					// TODO(yuzefovich): we temporarily ignore internal errors
					// that are because of #40929.
					var expectedError bool
					for _, exp := range []string{
						"could not parse \"0E-2019\" as type decimal",
					} {
						expectedError = expectedError || strings.Contains(es, exp)
					}
					if !expectedError {
						logStmt(stmt)
						t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
					}
				} else if strings.Contains(es, "communication error") {
					// A communication error can be because
					// a non-gateway node has crashed.
					logStmt(stmt)
					t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
				} else if strings.Contains(es, "Empty statement returned by generate") ||
					stmt == "" {
					// Either were unable to generate a statement or
					// we panicked making one.
					t.Fatalf("Failed generating a query %s", err)
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
		r.Add(registry.TestSpec{
			Name: fmt.Sprintf("sqlsmith/setup=%s/setting=%s", setup, setting),
			// NB: sqlsmith failures should never block a release.
			Owner:   registry.OwnerSQLQueries,
			Cluster: r.MakeClusterSpec(4),
			Timeout: time.Minute * 20,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
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
