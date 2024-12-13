// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// registerLargeSchemaBenchmarks registers all permutations of
// multi-region large schema benchmarking (for different scales
// and multi-region).
func registerLargeSchemaBenchmarks(r registry.Registry) {
	for _, isMultiRegion := range []bool{false, true} {
		for _, scale := range []int{1000, 5000, 10000, 25000, 40000} {
			// We limit scale on the multi-region variant of this test,
			// since the data import itself can take substantial time.
			if isMultiRegion && scale > 10000 {
				continue
			}
			registerLargeSchemaBenchmark(r, scale, isMultiRegion)
		}
	}
}

func registerLargeSchemaBenchmark(r registry.Registry, numTables int, isMultiRegion bool) {
	clusterSpec := []spec.Option{
		spec.CPU(8),
		spec.WorkloadNode(),
		spec.WorkloadNodeCPU(8),
		spec.VolumeSize(800),
		spec.GCEVolumeType("pd-ssd"),
		spec.GCEMachineType("n2-standard-8"),
	}
	testTimeout := 19 * time.Hour
	regions := ""
	if isMultiRegion {
		regions = "us-east1,us-west1,us-central1"
		// When running this test in the multi-region mode importing gigabytes
		// / terabytes of data can take a substantial amount of time. So, give
		// multi-region variants of this test extra time.
		testTimeout = 24 * time.Hour
		clusterSpec = append(clusterSpec, spec.Geo(),
			spec.GCEZones("us-east1-b,us-west1-b,us-central1-b,"+
				"us-east1-b,us-west1-b,us-central1-b,"+
				"us-east1-b,us-west1-b,us-central1-b,"+
				"us-east1-b"))
	}

	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("tpcc/large-schema-benchmark/multiregion=%t/tables=%d", isMultiRegion, numTables),
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		Cluster: r.MakeClusterSpec(
			// 9 CRDB nodes and one will be used for the TPCC workload
			// runner.
			10,
			clusterSpec...,
		),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          testTimeout,
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
				regionsArg := ""
				importConcurrencyLimit := 32
				if isMultiRegion {
					regionsArg = fmt.Sprintf("--regions=%q --partitions=%d", regions, 3)
					// For multi-region use a slower ingest rate, since the
					// cluster can't keep up.
					importConcurrencyLimit = 12
				}
				options := tpccOptions{
					WorkloadCmd: "tpccmultidb",
					DB:          strings.Split(dbList[0], ".")[0],
					SetupType:   usingInit,
					Warehouses:  len(c.All()) - 1,
					ExtraSetupArgs: fmt.Sprintf("--db-list-file=%s %s --import-concurrency-limit=%d",
						populateFileName,
						regionsArg,
						importConcurrencyLimit,
					),
				}
				if dbListType == inactiveDbListType {
					options.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
					}
				} else {
					options.Start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
						settings := install.MakeClusterSettings()
						startOpts := option.DefaultStartOpts()
						startOpts.RoachprodOpts.ScheduleBackups = false
						c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())
						conn := c.Conn(ctx, t.L(), 1)
						defer conn.Close()
						// Since we will be making a large number of databases / tables
						// quickly,on MR the job retention can slow things down. Let's
						// minimize how long jobs are kept, so that the creation / ingest
						// completes in a reasonable amount of time.
						_, err := conn.Exec("SET CLUSTER SETTING jobs.retention_time='1h'")
						require.NoError(t, err)
						// Use a higher number of retries, since we hit retry errors on importing
						// a large number of tables
						_, err = conn.Exec("SET CLUSTER SETTING kv.transaction.internal.max_auto_retries=500")
						require.NoError(t, err)
						// Create a user that will be used for authentication for the REST
						// API calls.
						_, err = conn.Exec("CREATE USER roachadmin password 'roacher'")
						require.NoError(t, err)
						_, err = conn.Exec("GRANT ADMIN to roachadmin")
						require.NoError(t, err)
					}
				}
				err := c.PutString(ctx, strings.Join(dbList, "\n"), populateFileName, 0755, c.WorkloadNode())
				require.NoError(t, err)
				setupTPCC(ctx, t, t.L(), c, options)
			}
			// Upload a file containing the ORM queries.
			require.NoError(t, c.PutString(ctx, LargeSchemaOrmQueries, "ormQueries.sql", 0755, c.WorkloadNode()))
			mon := c.NewMonitor(ctx, c.All())
			// Upload a file containing the web API calls we want to benchmark.
			require.NoError(t, c.PutString(ctx,
				LargeSchemaAPICalls,
				"apiCalls",
				0755,
				c.WorkloadNode()))
			// Get a list of web console URLs.
			webConsoleURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Range(1, c.Spec().NodeCount-1))
			require.NoError(t, err)
			for urlIdx := range webConsoleURLs {
				webConsoleURLs[urlIdx] = "https://" + webConsoleURLs[urlIdx]
			}
			// Next startup the workload for our list of databases from earlier.
			for dbListType, dbList := range [][]string{activeDBList, inactiveDBList} {
				dbList := dbList
				dbListType := dbListType
				populateFileName := fmt.Sprintf("populate_%d", dbListType)
				mon.Go(func(ctx context.Context) error {
					waitEnabled := "--wait 0.0"
					var wlInstance []workloadInstance
					disableHistogram := false
					// Inactive databases will intentionally have wait time on
					// them and not include them in our histograms.
					if dbListType == inactiveDbListType {
						waitEnabled = "--wait 1.0"

						// disable histogram since they shouldn't be included
						disableHistogram = true

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
						DisableHistogram:  disableHistogram, // We setup the flag above.
						WorkloadInstances: wlInstance,
						Duration:          time.Minute * 60,
						ExtraRunArgs: fmt.Sprintf("--db-list-file=%s --txn-preamble-file=%s --admin-urls=%q "+
							"--console-api-file=apiCalls --console-api-username=%q --console-api-password=%q --conns=%d --workers=%d %s",
							populateFileName,
							"ormQueries.sql",
							strings.Join(webConsoleURLs, ","),
							"roachadmin",
							"roacher",
							numWorkers,
							numWorkers,
							waitEnabled),
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

// LargeSchemaAPICalls are calls into the consoles cluster API.
const LargeSchemaAPICalls = `
api/v2/databases/$targetDb/tables/
`
