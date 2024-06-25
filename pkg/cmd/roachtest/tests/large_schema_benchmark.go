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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

func registerSchemaChangeBenchmarkLargeSchema(r registry.Registry, numTables int) {
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("tpcc/large-schema-benchmark/tables=%d", numTables),
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			// 9 CRDB nodes and one will be used for the TPCC workload
			// runner.
			10,
			spec.CPU(8),
			spec.WorkloadNode(),
			spec.WorkloadNodeCPU(8),
			spec.VolumeSize(800),
			spec.GCEVolumeType("pd-ssd"),
			spec.GCEMachineType("n2-standard-8"),
		),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          18 * time.Hour,
		RequiresLicense:  true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			numWorkers := (len(c.All()) - 1) * 10
			// Number of tables per-database from the TPCC template.
			const numTablesForTPCC = 9
			// Active databases will continually execute a mix of TPCC + ORM
			// queries without any delays.
			var activeDBList []string
			// Inactive databases will have the default TPCC think time delays between
			// ORM queries.
			var inactiveDBList []string
			// 25% of the tables created will be inactive.
			inactiveStart := numTables / 4
			// Determine how many tables are left.
			numTablesRemaining := numTables
			databaseIdx := 0
			// Each database will have more schemas than the last until
			// the max per schema is hit.
			numSchemasForDatabase := 1
			const MaxSchemasForDatabase = 72
			for numTablesRemaining > 0 {
				schemaIdx := 0
				databaseName := fmt.Sprintf("warehouse_%d", databaseIdx)
				var newDatabaseAndSchemas []string
				numToSubtract := 0
				for schemaIdx = 0; schemaIdx < numSchemasForDatabase; schemaIdx++ {
					schemaName := fmt.Sprintf("schema_%d", schemaIdx)
					// First schema is always called public.
					if schemaIdx == 0 {
						schemaName = "public"
					}
					newDatabaseAndSchemas = append(newDatabaseAndSchemas, fmt.Sprintf("%s.%s", databaseName, schemaName))
					numToSubtract += numTablesForTPCC
				}
				if numTablesRemaining >= numTables-inactiveStart {
					inactiveDBList = append(inactiveDBList, newDatabaseAndSchemas...)
				} else {
					activeDBList = append(activeDBList, newDatabaseAndSchemas...)
				}
				numTablesRemaining -= numToSubtract
				numSchemasForDatabase += 1
				numSchemasForDatabase = min(numSchemasForDatabase, MaxSchemasForDatabase)
				databaseIdx += 1
			}

			// Create all the databases based on our lists of active vs inactive
			// ones.
			const inactiveDbListType = 1
			for dbListType, dbList := range [][]string{activeDBList, inactiveDBList} {
				populateFileName := fmt.Sprintf("populate_%d", dbListType)
				options := tpccOptions{
					WorkloadCmd:    "tpccmultidb",
					DB:             strings.Split(dbList[0], ".")[0],
					SetupType:      usingInit,
					Warehouses:     len(c.All()) - 1,
					ExtraSetupArgs: fmt.Sprintf("--db-list-file=%s", populateFileName),
				}
				if dbListType == inactiveDbListType {
					options.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
					}
				}
				err := c.PutString(ctx, strings.Join(dbList, "\n"), populateFileName, 0755, c.WorkloadNode())
				require.NoError(t, err)
			}
			// Upload a file containing the ORM queries.
			require.NoError(t, c.PutString(ctx, LargeSchemaOrmQueries, "ormQueries.sql", 0755, c.WorkloadNode()))
			mon := c.NewMonitor(ctx, c.All())
			// Next startup the workload for our list of databases from earlier.
			for dbListType, dbList := range [][]string{activeDBList, inactiveDBList} {
				dbList := dbList
				dbListType := dbListType
				populateFileName := fmt.Sprintf("populate_%d", dbListType)
				mon.Go(func(ctx context.Context) error {
					waitEnabled := "--wait 0.0"
					// Export histograms out for the roach perf dashboard
					histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
					var wlInstance []workloadInstance
					// Inactive databases will intentionally have wait time on
					// them and not include them in our histograms.
					if dbListType == inactiveDbListType {
						waitEnabled = "--wait 1.0"
						histograms = ""
						// Use a different prometheus port for the inactive databases,
						// this will not be measured.
						wlInstance = append(
							wlInstance,
							workloadInstance{
								nodes:          c.CRDBNodes(),
								prometheusPort: 5050,
							},
						)
					}
					options := tpccOptions{
						WorkloadCmd:       "tpccmultidb",
						DB:                strings.Split(dbList[0], ".")[0],
						Warehouses:        len(c.All()) - 1,
						SkipSetup:         true,
						DisablePrometheus: true,
						WorkloadInstances: wlInstance,
						Duration:          time.Minute * 60,
						ExtraRunArgs: fmt.Sprintf("--db-list-file=%s --txn-preamble-file=%s --conns=%d --workers=%d %s %s",
							populateFileName,
							"ormQueries.sql",
							numWorkers,
							numWorkers,
							waitEnabled,
							histograms),
					}
					runTPCC(ctx, t, t.L(), c, options)
					return nil
				})
			}
			mon.Wait()
		},
	})
}

// LargeSchemaOrmQueries is extracted from the round trip analysis tests for
// ORM queries.
const LargeSchemaOrmQueries = `
-- JDBC ORM query for types
    SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid 
      FROM pg_catalog.pg_type 
      LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r 
              from pg_namespace as ns 
     -- go with older way of unnesting array to be compatible with 8.0
              join ( select s.r, (current_schemas(false))[s.r] as nspname 
                       from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r 
             using ( nspname ) 
           ) as sp
        ON sp.nspoid = typnamespace 
     ORDER BY sp.r, pg_type.oid DESC;
`
