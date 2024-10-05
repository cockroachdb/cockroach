// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

func registerSQLSmith(r registry.Registry) {
	const numNodes = 4
	setups := map[string]sqlsmith.Setup{
		"empty":                     sqlsmith.Setups["empty"],
		"seed":                      sqlsmith.Setups["seed"],
		sqlsmith.RandTableSetupName: sqlsmith.Setups[sqlsmith.RandTableSetupName],
		"tpch-sf1": func(r *rand.Rand) []string {
			return []string{`
RESTORE TABLE tpch.* FROM 'gs://cockroach-fixtures-us-east1/workload/tpch/scalefactor=1/backup?AUTH=implicit'
WITH into_db = 'defaultdb', unsafe_restore_incompatible_version;
`}
		},
		"tpcc": func(r *rand.Rand) []string {
			const version = "version=2.1.0,fks=true,interleaved=false,seed=1,warehouses=1"
			var stmts []string
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
				stmts = append(
					stmts,
					fmt.Sprintf(`
RESTORE TABLE tpcc.%s FROM 'gs://cockroach-fixtures-us-east1/workload/tpcc/%[2]s/%[1]s?AUTH=implicit'
WITH into_db = 'defaultdb', unsafe_restore_incompatible_version;
`,
						t, version,
					),
				)
			}
			return stmts
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

		rng, seed := randutil.NewTestRand()
		t.L().Printf("seed: %d", seed)

		c.SetRandomSeed(rng.Int63())
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

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

		allConns := make([]*gosql.DB, 0, numNodes)
		for node := 1; node <= numNodes; node++ {
			allConns = append(allConns, c.Conn(ctx, t.L(), node))
		}
		conn := allConns[0]
		t.Status("executing setup")
		t.L().Printf("setup:\n%s", strings.Join(setup, "\n"))
		for _, stmt := range setup {
			logStmt(stmt)
			if _, err := conn.Exec(stmt); err != nil {
				if strings.Contains(err.Error(), "does not exist") {
					// This is likely to be an elusive 'pq: column
					// "crdb_internal_idx_expr" does not exist' error that we
					// cannot reproduce. The current hypothesis is that the
					// CREATE TABLE statement contains some non-visible
					// characters that get lost when printing as a string, so we
					// will log this statement as a sequence of integers so that
					// later we can reconstruct the stmt precisely.
					for _, char := range stmt {
						fmt.Fprintf(smithLog, "%d ", char)
					}
					fmt.Fprint(smithLog, "\n\n")
				}
				t.Fatalf("error: %s\nstatement: %s", err.Error(), stmt)
			}
		}

		if settingName == "multi-region" {
			setupMultiRegionDatabase(t, conn, logStmt)
		}

		const timeout = time.Minute
		setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", timeout.String())
		t.Status("setting statement_timeout")
		t.L().Printf("statement timeout:\n%s", setStmtTimeout)
		if _, err := conn.Exec(setStmtTimeout); err != nil {
			t.Fatal(err)
		}
		logStmt(setStmtTimeout)

		setting.Options = append(setting.Options, sqlsmith.SimpleNames())
		smither, err := sqlsmith.NewSmither(conn, rng, setting.Options...)
		if err != nil {
			t.Fatal(err)
		}

		// We will enable panic injection on this connection in the vectorized
		// engine and optimizer (and will ignore the injected errors) in order
		// to test that the panic-catching mechanism of error propagation works
		// as expected.
		// Note: it is important to enable this testing knob only after all
		// other setup queries have already completed, including the smither
		// instantiation (otherwise, the setup might fail because of the
		// injected panics).
		injectVectorizePanicsStmt := "SET testing_vectorize_inject_panics=true;"
		if _, err := conn.Exec(injectVectorizePanicsStmt); err != nil {
			t.Fatal(err)
		}
		logStmt(injectVectorizePanicsStmt)
		injectOptimizerPanicsStmt := "SET testing_optimizer_inject_panics=true;"
		// Because we've already enabled the vectorized panic injection,
		// enabling the optimizer panic injection might fail due to the injected
		// vectorized panic. To go around this, we will retry this statement at
		// most 100 times.
		const maxRetries = 100
		for attempt := 1; ; attempt++ {
			if _, err = conn.Exec(injectOptimizerPanicsStmt); err == nil {
				break
			}
			if attempt == maxRetries {
				t.Fatalf("failed to enable optimizer panic injection with %d retries: %v", maxRetries, err)
			}
		}
		logStmt(injectOptimizerPanicsStmt)

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
				m := c.NewMonitor(ctx, c.Node(1))
				m.Go(func(context.Context) error {
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
						return nil
					}

					// TODO(yuzefovich): investigate why using the context with
					// a timeout results in poisoning the connection (#101208).
					_, err := conn.Exec(stmt)
					if err == nil {
						logStmt(stmt)
						stmt = "EXPLAIN " + stmt
						_, err = conn.Exec(stmt)
						if err == nil {
							logStmt(stmt)
						}
					}
					done <- err
					return nil
				})
				defer m.Wait()
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
					var expectedError bool
					for _, exp := range []string{
						// Optimizer panic-injection surfaces as an internal error.
						"injected panic in optimizer",
					} {
						expectedError = expectedError || strings.Contains(es, exp)
					}
					if !expectedError {
						logStmt(stmt)
						t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
					}
				} else {
					if strings.Contains(es, "Empty statement returned by generate") {
						// We were unable to generate a statement - this is
						// never expected.
						t.Fatalf("Failed generating a query %s", err)
					}
					if stmt == "" {
						// We panicked when generating a statement.
						//
						// The panic might be expected if it was a vectorized
						// panic that was injected when sqlsmith itself issued a
						// query to generate another query (for example, in
						// getDatabaseRegions).
						expectedError := strings.Contains(es, "injected panic in ")
						if !expectedError {
							t.Fatalf("Panicked when generating a query %s", err)
						}
					}
				}
				// Ignore other errors because they happen so
				// frequently (due to sqlsmith not crafting
				// executable queries 100% of the time) and are
				// never interesting.
				// TODO(yuzefovich): reevaluate this assumption.
			}

			// Ping all nodes to make sure they didn't crash.
			for idx, c := range allConns {
				if err := c.PingContext(ctx); err != nil {
					logStmt(stmt)
					nodeID := idx + 1
					errStr := fmt.Sprintf("ping node %d: %v\n", nodeID, err)
					hintStr := fmt.Sprintf(
						"HINT: node likely crashed, check logs in artifacts > logs/%d.unredacted\n",
						nodeID,
					)

					var sb strings.Builder
					// Print the error message and a hint.
					sb.WriteString(errStr)
					sb.WriteString(hintStr)
					// Print the previous SQL.
					sb.WriteString(fmt.Sprintf("previous sql:\n%s;", stmt))
					// Print the error message and hint again because
					// github-post prunes the top of the error message away when
					// the SQL is too long.
					sb.WriteString(errStr)
					sb.WriteString(hintStr)

					t.Fatalf(sb.String())
				}
			}
		}
	}

	register := func(setup, setting string) {
		var clusterSpec spec.ClusterSpec
		if strings.Contains(setting, "multi-region") {
			clusterSpec = r.MakeClusterSpec(numNodes, spec.Geo())
		} else {
			clusterSpec = r.MakeClusterSpec(numNodes)
		}
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("sqlsmith/setup=%s/setting=%s", setup, setting),
			Owner:   registry.OwnerSQLQueries,
			Cluster: clusterSpec,
			// Uses gs://cockroach-fixtures-us-east1. See:
			// https://github.com/cockroachdb/cockroach/issues/105968
			CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
			Suites:           registry.Suites(registry.Nightly),
			Randomized:       true,
			Leases:           registry.MetamorphicLeases,
			NativeLibs:       registry.LibGEOS,
			Timeout:          time.Minute * 20,
			RequiresLicense:  true,
			// NB: sqlsmith failures should never block a release.
			NonReleaseBlocker: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSQLSmith(ctx, t, c, setup, setting)
			},
			ExtraLabels: []string{"O-rsg"},
		})
	}

	for setup := range setups {
		for setting := range settings {
			register(setup, setting)
		}
	}
	setups["seed-multi-region"] = sqlsmith.Setups["seed-multi-region"]
	settings["ddl-nodrop"] = sqlsmith.Settings["ddl-nodrop"]
	settings["multi-region"] = sqlsmith.Settings["multi-region"]
	register("tpcc", "ddl-nodrop")
	register("seed-multi-region", "multi-region")
}

// setupMultiRegionDatabase is used to set up a multi-region database.
func setupMultiRegionDatabase(t test.Test, conn *gosql.DB, logStmt func(string)) {
	t.Helper()
	regionsSet := make(map[string]struct{})
	var region, zone string
	rows, err := conn.Query("SHOW REGIONS FROM CLUSTER")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		if err := rows.Scan(&region, &zone); err != nil {
			t.Fatal(err)
		}
		regionsSet[region] = struct{}{}
	}

	var regionList []string
	for region := range regionsSet {
		regionList = append(regionList, region)
	}

	if len(regionList) == 0 {
		t.Fatal(errors.New("no regions, cannot run multi-region config"))
	}

	stmt := fmt.Sprintf(`ALTER DATABASE defaultdb SET PRIMARY REGION "%s";
ALTER TABLE seed_mr_table SET LOCALITY REGIONAL BY ROW;
INSERT INTO seed_mr_table DEFAULT VALUES;`, regionList[0])
	if _, err := conn.Exec(stmt); err != nil {
		t.Fatal(err)
	} else {
		logStmt(stmt)
	}
}
