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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

// Benchmarks and confirms the performance of the leasing infrastructure.
func runSchemaChangeMultiRegionBenchmarkLeasing(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	var durations [2][]time.Duration
	defaultOpts := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), defaultOpts)
	// Create 600 tables inside the database, which is above our default lease
	// refresh limit of 500. This will only be done when session based leasing
	// is enabled.
	numTables := 600 / len(c.All())
	{
		t.Status("Creating tables for benchmark")
		grp := ctxgroup.WithContext(ctx)
		for _, n := range c.All() {
			var node = n
			grp.GoCtx(func(ctx context.Context) error {
				createTables := func() error {
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
				}
				var err error
				for retry := 0; retry < 10; retry++ {
					err = createTables()
					// No error detected, so done.
					if err == nil {
						return nil
					}
					// Check for serializable errors, which we will retry.
					if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) &&
						pgcode.MakeCode(string(pqErr.Code)) == pgcode.SerializationFailure) {
						continue
					}
					// Otherwise an unknown error so bail out.
					return err
				}
				// Return the last error, if we don't sort this in the retry
				// count.
				return err
			})
		}
		if err := grp.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	for numTestIters := 0; numTestIters < 3; numTestIters++ {
		for modeIdx, sessionBasedLeasingEnabled := range []bool{true, false} {
			func() {
				// When session based leasing is disabled, force expiry based leasing.
				var options map[string]string
				if !sessionBasedLeasingEnabled {
					// Note: After flipping the cluster settings we need to restart all nodes,
					// since the leasing code is not designed switch to session based leasing
					// to expiry based (in an online manner). Only the reverse direction is
					// supported online with a migration.
					options = map[string]string{
						"sql.catalog.experimental_use_session_based_leasing": "off",
						"sql.catalog.descriptor_lease_duration":              "30s",
						"sql.catalog.descriptor_lease_renewal_fraction":      "6s",
					}

				} else {
					options = map[string]string{
						"sql.catalog.experimental_use_session_based_leasing": "auto",
					}
				}
				conn := c.Conn(ctx, t.L(), c.Node(1)[0])
				defer conn.Close()
				for s, v := range options {
					if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING %s='%s'", s, v)); err != nil {
						t.Fatal(err)
					}
				}
				c.Stop(ctx, t.L(), option.DefaultStopOpts())
				c.Start(ctx, t.L(), option.DefaultStartOpts(), defaultOpts)

				// Next spawn a thread on each node to select from all the tables created
				// above. We are going to do two passes, with multiple threads. After 30
				// seconds the lease will expire in the expiry based model, and checking
				// all of these tables will take more than the expiry time. Additionally,
				// a 100 tables will *never* be auto-refreshed.
				const numWorkersPerNode = 8
				connPoolPerNode := make([]*pgxpool.Pool, 0, len(c.All()))
				defer func() {
					for _, pool := range connPoolPerNode {
						pool.Close()
					}
				}()
				for _, node := range c.All() {
					url, err := c.ExternalPGUrl(ctx, t.L(), c.Node(node), roachprod.PGURLOptions{Secure: true})
					if err != nil {
						t.Fatal(err)
					}
					config, err := pgxpool.ParseConfig(url[0])
					if err != nil {
						t.Fatal(err)
					}
					config.MinConns = numWorkersPerNode
					connPool, err := pgxpool.NewWithConfig(ctx, config)
					if err != nil {
						t.Fatal(err)
					}
					connPoolPerNode = append(connPoolPerNode, connPool)
				}
				grp := ctxgroup.WithContext(ctx)
				startTime := timeutil.Now()
				for nodeIdx := range c.All() {
					nodeIdx := nodeIdx
					tablesPerWorker := numTables / numWorkersPerNode
					for workerId := 0; workerId < numWorkersPerNode; workerId++ {
						workerId := workerId
						grp.GoCtx(func(ctx context.Context) error {
							conn, err := connPoolPerNode[nodeIdx].Acquire(ctx)
							if err != nil {
								return err
							}
							defer conn.Release()
							const numIterations = 8
							for iter := 0; iter < numIterations; iter++ {
								for i := workerId * tablesPerWorker; i < (workerId+1)*tablesPerWorker; i++ {
									for j := 1; j <= len(c.All()); j++ {
										createTbl := fmt.Sprintf("SELECT * FROM table_%d_%d;", j, i)
										_, err := conn.Exec(ctx, createTbl)
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
				t.Status(fmt.Sprintf("starting benchmark session_based_leasing=%t", sessionBasedLeasingEnabled))
				if err := grp.Wait(); err != nil {
					t.Fatal(err)
				}
				durations[modeIdx] = append(durations[modeIdx], timeutil.Since(startTime))
			}()
		}
	}
	c.Stop(ctx, t.L(), option.DefaultStopOpts())

	percentages := make([]int, len(durations))
	for i := range percentages {
		percentages[i] = int((durations[1][i]*100)/durations[0][i]) - 100
	}
	sort.SliceStable(percentages, func(i, j int) bool {
		return percentages[i] < percentages[j]
	})
	// Confirm that session based leasing took less time for the selects.
	medianPercentage := percentages[len(percentages)/2]
	if medianPercentage < 0 {
		t.Fatal(fmt.Sprintf("Expected session based leasing to be faster with many descriptors (%d%%), observed"+
			"the following percentages %v.",
			medianPercentage,
			percentages))
	}

	// Track the percentage improvement between the two.
	t.Status(fmt.Sprintf("session based leasing produced an improvement of %d%% "+
		"(session based exucution time=%s, expiry based execution time=%s)", medianPercentage,
		durations[0], durations[1]))
	// We see an up to 50% improvement in this scenario, so fail fatally if we miss
	// a percentage of that mark.
	if medianPercentage < 15 {
		t.Fatal("lower than expected improvement in execution time")
	}
}

func registerSchemaChangeMultiRegionBenchmarkLeasing(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "schemachange/leasing-benchmark",
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			3,
			spec.Geo(),
			spec.GCEZones("us-west1-b,us-east1-b,australia-southeast1-a"),
			spec.AWSZones("us-east-1a,us-west-2b,ap-southeast-2b"),
		),
		CompatibleClouds: registry.AllExceptLocal,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Timeout:          time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeMultiRegionBenchmarkLeasing(ctx, t, c)
		},
	})
}
