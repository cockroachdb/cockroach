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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func registerSQLSmith(r *testRegistry) {
	setups := map[string]sqlsmith.Setup{
		"empty": sqlsmith.Setups["empty"],
		"seed":  sqlsmith.Setups["seed"],
		"tpch-sf1": func(r *rand.Rand) string {
			return `RESTORE DATABASE tpch FROM 'gs://cockroach-fixtures/workload/tpch/scalefactor=1/backup';`
		},
	}

	runSQLSmith := func(ctx context.Context, t *test, c *cluster, setupName, settingName string) {
		rng, seed := randutil.NewPseudoRand()
		c.l.Printf("seed: %d", seed)

		c.Put(ctx, cockroach, "./cockroach")
		c.Start(ctx, t)

		setupFunc, ok := setups[setupName]
		if !ok {
			t.Fatalf("unknown setup %s", setupName)
		}
		settingFunc, ok := sqlsmith.Settings[settingName]
		if !ok {
			t.Fatalf("unknown setting %s", settingName)
		}

		setup := setupFunc(rng)
		setting := settingFunc(rng)

		conn := c.Conn(ctx, 1)
		t.Status("executing setup")
		if _, err := conn.Exec(setup); err != nil {
			t.Fatal(err)
		}

		smither, err := sqlsmith.NewSmither(conn, rng, setting.Options...)
		if err != nil {
			t.Fatal(err)
		}

		t.Status("smithing")
		until := time.After(time.Hour)
		done := ctx.Done()
		for i := 1; ; i++ {
			if i%500 == 0 {
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
				const timeout = time.Minute
				go func() {
					ctx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()
					_, err := conn.ExecContext(ctx, stmt)
					done <- err
				}()
				select {
				case <-time.After(timeout + 5*time.Second):
					t.Fatalf("query timed out, but did not cancel execution:\n%s;", stmt)
				case err := <-done:
					return err
				}
				panic("unreachable")
			}()
			if err != nil {
				es := err.Error()
				if strings.Contains(es, "internal error") {
					t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
				} else if strings.Contains(es, "communication error") {
					// A communication error can be because
					// a non-gateway node has crashed.
					t.Fatalf("error: %s\nstmt:\n%s;", err, stmt)
				}
			}

			// Ping the gateway to make sure it didn't crash.
			if err := conn.PingContext(ctx); err != nil {
				t.Fatalf("ping: %v\nprevious sql:\n%s;", err, stmt)
			}
		}
	}

	for setupName := range setups {
		for _, settingName := range []string{"default", "no-mutations", "no-ddl"} {
			setupName := setupName
			settingName := settingName
			r.Add(testSpec{
				Name:    fmt.Sprintf("sqlsmith/setup=%s/setting=%s", setupName, settingName),
				Cluster: makeClusterSpec(4),
				Timeout: 2 * time.Hour,
				Run: func(ctx context.Context, t *test, c *cluster) {
					runSQLSmith(ctx, t, c, setupName, settingName)
				},
			})
		}
	}
}
