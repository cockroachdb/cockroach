// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// Benchmarks and confirms the performance of the leasing infrastructure.
func runSchemaChangeMultiRegionBenchmarkLeasing(
	ctx context.Context, t test.Test, c cluster.Cluster,
) {
	var durations [2]time.Duration
	defaultOpts := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), defaultOpts)
	// Create 600 tables inside the database, which is above our default lease
	// refresh limit of 500. This will only be done when session based leasing
	// is enabled.
	numTables := 600 / len(c.All())
	const selectQueryFileName = "out.sql"
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
		// Generate a file to select from all tables that will only get a lease on
		// a table.
		selectQueryFile := strings.Builder{}
		selectFilePath := t.ArtifactsDir() + string(os.PathSeparator) + selectQueryFileName
		for i := 0; i < numTables; i++ {
			for j := 1; j <= len(c.All()); j++ {
				// Select the OID for the table, which should involve no communication
				// with other nodes.
				createTbl := fmt.Sprintf("SELECT 'table_%d_%d'::REGCLASS::OID;", j, i)
				selectQueryFile.WriteString(createTbl)
				selectQueryFile.WriteString(";\n")
			}
		}
		err := os.WriteFile(selectFilePath, []byte(selectQueryFile.String()), 0644)
		if err != nil {
			t.Fatal(err)
		}
		c.Put(ctx, selectFilePath, selectQueryFileName)
	}

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
			c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), defaultOpts)

			grp := ctxgroup.WithContext(ctx)
			var totalTimeSeconds syncutil.AtomicFloat64
			// Start one thread per-node that will execute the cockroach sql command
			// with a script that selects from each table. The number of tables that
			// we have is high so the lease manager can't keep everything refreshed
			// intentionally. The session based leasing will not have to deal with
			// renewals and take a shorter execution time.
			t.Status(fmt.Sprintf("starting benchmark for session_based_leasing=%t", sessionBasedLeasingEnabled))
			for _, node := range c.All() {
				nodeIdx := node
				grp.GoCtx(func(ctx context.Context) error {
					numIterations := 4
					for iter := 0; iter < numIterations; iter++ {
						output, err := c.RunWithDetailsSingleNode(ctx,
							t.L(),
							install.WithNodes(install.Nodes{install.Node(nodeIdx)}),
							fmt.Sprintf(
								"time -p ./cockroach sql -f %s --url={pgurl:%d} > /dev/null",
								selectQueryFileName,
								nodeIdx),
						)
						if err != nil {
							return err
						}
						// Output will contain lines from the `time -p` command of the format:
						// 	real 173.58
						// 	user 0.47
						// 	sys 0.16
						// We extract the first line to get the elapsed time for the set of queries.
						outputSlice := strings.Split(output.Output(false), "\n")
						realExecTimeSeconds := 0.0
						realPrefix := ""
						_, err = fmt.Sscanf(outputSlice[len(outputSlice)-4], "%s%f", &realPrefix, &realExecTimeSeconds)
						if err != nil {
							return err
						}
						// Update our tally of execution time for this set of queries.
						t.Status(fmt.Sprintf(
							"session_based_leasing=%t node=%d iteration=%d completed in %.2f",
							sessionBasedLeasingEnabled, nodeIdx, iter, realExecTimeSeconds,
						))
						totalTimeSeconds.Add(realExecTimeSeconds)
					}
					return nil
				})
			}
			if err := grp.Wait(); err != nil {
				t.Fatal(err)
			}
			durations[modeIdx] = time.Duration(totalTimeSeconds.Load() * float64(time.Second))
			t.Status(fmt.Sprintf("benchmark completed for session_based_leasing=%t in %d", sessionBasedLeasingEnabled, durations[modeIdx]))
		}()
	}
	c.Stop(ctx, t.L(), option.DefaultStopOpts())

	percentage := (1 - float64(durations[0])/float64(durations[1])) * 100
	t.Status(fmt.Sprintf(
		"observed a %.2f%% improvement; session based leasing time is %s, expiry based leasing time is %s",
		percentage, durations[0], durations[1],
	))
	if percentage < 0 {
		t.Fatalf(
			"expected session based leasing to be faster with many descriptors; got %.2f%% improvement",
			percentage,
		)
	}
	// We see an at least a 13% improvement in this scenario, so fail if we miss
	// a percentage of that mark.
	if percentage < 13 {
		t.Fatalf(
			"lower than expected improvement in execution time; got %.2f%%, expected at least 13%%",
			percentage,
		)
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
		Leases:           registry.DefaultLeases,
		Timeout:          2 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeMultiRegionBenchmarkLeasing(ctx, t, c)
		},
	})
}
