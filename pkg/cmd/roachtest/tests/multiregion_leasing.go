// Copyright 2024 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Benchmarks and confirms the performance of the leasing infrastructure.
func runAcceptanceMultiRegionBenchmarkLeasing(ctx context.Context, t test.Test, c cluster.Cluster) {
	var durations [2]time.Duration
	defaultOpts := install.MakeClusterSettings(install.SecureOption(true))
	for modeIdx, sessionBasedLeasingEnabled := range []bool{true, false} {
		func() {
			t.Status("Setting up Session Based Leasing: ", sessionBasedLeasingEnabled)
			// When session based leasing is disabled, force expiry based leasing.
			if !sessionBasedLeasingEnabled {
				c.Start(ctx, t.L(), option.DefaultStartOpts(), defaultOpts,
					c.All())
				options := map[string]string{
					"sql.catalog.experimental_use_session_based_leasing": "off",
					"sql.catalog.descriptor_lease_duration":              "30s",
					"sql.catalog.descriptor_lease_renewal_fraction":      "6s",
				}
				conn := c.Conn(ctx, t.L(), c.Node(1)[0])
				for s, v := range options {
					if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING %s='%s'", s, v)); err != nil {
						t.Fatal(err)
					}
				}
				conn.Close()
				c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.All())
			}

			c.Start(ctx, t.L(), option.DefaultStartOpts(), defaultOpts, c.All())
			// Create 600 tables inside the database, which is above our default lease
			// refresh limit of 500. This will only be done when session based leasing
			// is enabled.
			numTables := 600 / len(c.All())
			grp := ctxgroup.WithContext(ctx)
			if modeIdx == 0 {
				for _, n := range c.All() {
					var node = n
					grp.GoCtx(func(ctx context.Context) error {
						conn, err := c.ConnE(ctx, t.L(), node)
						if err != nil {
							return err
						}
						defer conn.Close()
						tx, err := conn.Begin()
						if err != nil {
							return err
						}
						for i := 0; i < numTables; i++ {
							createTbl := fmt.Sprintf("CREATE TABLE table_%d_%d (i int);", node, i)
							_, err := tx.Exec(createTbl)
							if err != nil {
								return err
							}
						}
						return tx.Commit()
					})
				}
				if err := grp.Wait(); err != nil {
					t.Fatal(err)
				}
			}
			// Next spawn a thread on each node to select from all the tables created
			// above. We are going to do two passes, with multiple threads. After 30
			// seconds the lease will expire in the expiry based model, and checking
			// all of these tables will take more than the expiry time. Additionally,
			// a 100 tables will *never* be auto-refreshed.
			grp = ctxgroup.WithContext(ctx)
			for _, n := range c.All() {
				var node = n
				const numWorkers = 8
				tablesPerWorker := numTables / numWorkers
				for workerId := 0; workerId < numWorkers; workerId++ {
					workerId := workerId
					grp.GoCtx(func(ctx context.Context) error {
						conn, err := c.ConnE(ctx, t.L(), node)
						if err != nil {
							return err
						}
						defer conn.Close()
						for iter := 0; iter < 8; iter++ {
							for i := workerId * tablesPerWorker; i < (workerId+1)*tablesPerWorker; i++ {
								for j := 1; j <= len(c.All()); j++ {
									createTbl := fmt.Sprintf("SELECT * FROM table_%d_%d;", j, i)
									_, err := conn.Exec(createTbl)
									if err != nil {
										return err
									}
								}
							}
						}
						return nil
					})
				}
			}
			t.Status("Benchmarking Session Based Leasing: ", sessionBasedLeasingEnabled)
			startTime := timeutil.Now()
			if err := grp.Wait(); err != nil {
				t.Fatal(err)
			}
			c.Stop(ctx, t.L(), option.DefaultStopOpts())
			durations[modeIdx] = timeutil.Since(startTime)
		}()
	}

	// Confirm that session based leasing took less time for the selects.
	if durations[0] > durations[1] {
		t.Fatal("Expected session based leasing to be faster with many descriptors.")
	}

	// Track the percentage improvement between the two.
	improvementPct := ((durations[1] * 100) / durations[0]) - 100
	t.Status(fmt.Sprintf("session based leasing produced an improvement of %d%%", improvementPct))
	// We see an up to 50% improvement in this scenario, so fail fatally if we miss
	// a percentage of that mark.
	if improvementPct < 20 {
		t.Fatal("lower than expected improvement in execution time")
	}
}
