// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
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
		spec.VolumeType("pd-ssd"),
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
		// Skip INSPECT and descriptor post-validation because this benchmark
		// creates many databases and tables, and running them would take too long.
		SkipPostValidations: registry.PostValidationInspect | registry.PostValidationInvalidDescriptors,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Cap the total number of workers based on the number of
			// nodes and CPUs on them.
			numTotalWorkers := len(c.All()) - 1
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
						// Disable autocommit before DDL since we need to batch statements
						// in a single transaction for them to complete in a reasonable amount
						// of time. In multi-region this latency can be substantial.
						_, err := conn.Exec("SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'")
						require.NoError(t, err)
						// Allow optimizations to use leased descriptors when querying
						// pg_catalog and information_schema.
						_, err = conn.Exec("SET CLUSTER SETTING sql.catalog.allow_leased_descriptors.enabled = 'true'")
						require.NoError(t, err)
						// Enabled locked descriptor leasing for correctness.
						_, err = conn.Exec("SET CLUSTER SETTING sql.catalog.descriptor_lease.use_locked_timestamps.enabled = 'true'")
						require.NoError(t, err)
						// Since we will be making a large number of databases / tables
						// quickly,on MR the job retention can slow things down. Let's
						// minimize how long jobs are kept, so that the creation / ingest
						// completes in a reasonable amount of time.
						_, err = conn.Exec("SET CLUSTER SETTING jobs.retention_time='1h'")
						require.NoError(t, err)
						// Use a higher number of retries, since we hit retry errors on importing
						// a large number of tables
						_, err = conn.Exec("SET CLUSTER SETTING kv.transaction.internal.max_auto_retries=500")
						require.NoError(t, err)
						// Disable the schema object count limit to allow creating 40,000+
						// tables. This is a guardrail that prevents unbounded growth of the
						// descriptor table, but for this benchmark we intentionally want to
						// test with a large number of tables.
						_, err = conn.Exec("SET CLUSTER SETTING sql.schema.approx_max_object_count = 0")
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
			mon := c.NewDeprecatedMonitor(ctx, c.All())
			// Upload a file containing the web API calls we want to benchmark.
			require.NoError(t, c.PutString(ctx,
				LargeSchemaAPICalls,
				"apiCalls",
				0755,
				c.WorkloadNode()))

			// Determine which nodes to use for the workload. In multi-region mode,
			// we only connect to nodes in the same region as the workload node to
			// avoid cross-region latency being included in query latency measurements.
			// The zone assignment pattern is: us-east1, us-west1, us-central1 repeating,
			// so nodes 1, 4, 7 are in us-east1 (same region as workload node 10).
			localNodes := c.CRDBNodes()
			if isMultiRegion {
				localNodes = c.Nodes(1, 4, 7)
			}

			// Get a list of web console URLs for local nodes only.
			webConsoleURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), localNodes)
			require.NoError(t, err)
			for urlIdx := range webConsoleURLs {
				webConsoleURLs[urlIdx] = "https://" + webConsoleURLs[urlIdx]
			}
			// Next, start up the workload for our list of databases from earlier.
			tpccDatabaseLists := [][]string{activeDBList, inactiveDBList}
			// Cap the number of workers to reduce noise in measurements.
			numWorkers := numTotalWorkers / len(tpccDatabaseLists)
			for dbListType, dbList := range tpccDatabaseLists {
				dbList := dbList
				dbListType := dbListType
				populateFileName := fmt.Sprintf("populate_%d", dbListType)
				mon.Go(func(ctx context.Context) error {
					waitEnabled := "--wait 0.0"
					var wlInstance []workloadInstance
					disableHistogram := false
					numWarehouses := len(c.All()) - 1
					// Inactive databases will intentionally have wait time on
					// them and not include them in our histograms.
					if dbListType == inactiveDbListType {
						waitEnabled = "--wait 1.0"
						// TPCC requires workers = warehouses * 10 when --wait > 0.
						// Use 1 warehouse to allow running with fewer workers.
						numWarehouses = 1

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
					} else {
						// For active databases, connect only to local nodes to avoid
						// cross-region latency in measurements.
						wlInstance = append(
							wlInstance,
							workloadInstance{
								nodes:          localNodes,
								prometheusPort: 2112,
							},
						)
					}
					// TPCC requires workers = warehouses * 10 when --wait > 0.
					// For active DBs (--wait 0), we can use reduced workers.
					// For inactive DBs (--wait 1.0), we use 1 warehouse to allow 10 workers.
					tpccWorkers := numWorkers
					if dbListType == inactiveDbListType {
						tpccWorkers = numWarehouses * tpcc.NumWorkersPerWarehouse
					}
					options := tpccOptions{
						WorkloadCmd:       "tpccmultidb",
						DB:                strings.Split(dbList[0], ".")[0],
						Warehouses:        numWarehouses,
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
							tpccWorkers,
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

// registerLargeSchemaIntrospectionBenchmark registers tests that create
// empty tables and benchmark introspection queries without any data import
// or TPCC workload. This measures how introspection performs with large
// numbers of tables.
func registerLargeSchemaIntrospectionBenchmark(r registry.Registry) {
	for _, numTables := range []int{10_000, 1_000_000} {
		numTables := numTables // capture loop variable
		clusterSpec := []spec.Option{
			spec.CPU(16),
			spec.WorkloadNode(),
			spec.WorkloadNodeCPU(8),
			spec.VolumeSize(500),
			spec.VolumeType("pd-ssd"),
			// Use highmem variant for more memory per node (128 GB vs 64 GB for
			// n2-standard-16). Large schema operations require significant memory
			// for the descriptor lease manager and span config subscriber.
			spec.GCEMachineType("n2-highmem-16"),
		}

		// Adjust timeout based on number of tables. 1M tables takes much longer
		// to create than 10K tables.
		timeout := 4 * time.Hour
		if numTables >= 1_000_000 {
			timeout = 48 * time.Hour
		}

		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("large-schema-benchmark/multiregion=false/tables=%d", numTables),
			Owner:            registry.OwnerSQLFoundations,
			Benchmark:        true,
			Cluster:          r.MakeClusterSpec(10, clusterSpec...),
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Weekly),
			Timeout:          timeout,
			// Skip INSPECT and descriptor post-validation because this benchmark
			// creates many databases and tables, and running them would take too long.
			SkipPostValidations: registry.PostValidationInspect | registry.PostValidationInvalidDescriptors,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runLargeSchemaIntrospectionBenchmark(ctx, t, c, numTables)
			},
		})
	}
}

func runLargeSchemaIntrospectionBenchmark(
	ctx context.Context, t test.Test, c cluster.Cluster, numTables int,
) {
	// Number of tables per-database from the TPCC template.
	const numTablesForTPCC = 9
	const maxSchemasForDatabase = 72

	// Build the list of databases and schemas needed to create numTables.
	var dbList []string
	numTablesRemaining := numTables
	databaseIdx := 0
	numSchemasForDatabase := 1
	for numTablesRemaining > 0 {
		databaseName := fmt.Sprintf("warehouse_%d", databaseIdx)
		for schemaIdx := 0; schemaIdx < numSchemasForDatabase && numTablesRemaining > 0; schemaIdx++ {
			schemaName := fmt.Sprintf("schema_%d", schemaIdx)
			if schemaIdx == 0 {
				schemaName = "public"
			}
			dbList = append(dbList, fmt.Sprintf("%s.%s", databaseName, schemaName))
			numTablesRemaining -= numTablesForTPCC
		}
		numSchemasForDatabase++
		numSchemasForDatabase = min(numSchemasForDatabase, maxSchemasForDatabase)
		databaseIdx++
	}

	t.L().Printf("Creating %d tables across %d database.schema entries", numTables, len(dbList))

	// Start the cluster and configure settings for large schema.
	settings := install.MakeClusterSettings()
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ScheduleBackups = false
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	// Configure cluster settings for large schema operations.
	clusterSettings := []string{
		"SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'",
		"SET CLUSTER SETTING sql.catalog.allow_leased_descriptors.enabled = 'true'",
		"SET CLUSTER SETTING sql.catalog.descriptor_lease.use_locked_timestamps.enabled = 'true'",
		"SET CLUSTER SETTING jobs.retention_time='2h'",
		"SET CLUSTER SETTING kv.transaction.internal.max_auto_retries=1000",
		"SET CLUSTER SETTING sql.schema.approx_max_object_count = 0",
		// Auto stats job can starve out other jobs when there are many tables.
		// See https://github.com/cockroachdb/cockroach/issues/149475.
		"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		// Increase the lease refresh limit to handle the large number of
		// descriptors being created. The default (500) is too low for 1M tables.
		"SET CLUSTER SETTING sql.tablecache.lease.refresh_limit = 50000",
		// Increase the intent tracking limit for bulk DDL transactions. The
		// default is too low for creating 1M tables, causing long waits when
		// transactions block on system.descriptor intents.
		"SET CLUSTER SETTING kv.transaction.max_intents_bytes = 16777216",
		// Increase the refresh span tracking limit for serializable transactions.
		// When creating many tables per transaction, the read spans on
		// system.descriptor and system.namespace accumulate beyond the default
		// 4MB limit. Once exceeded, the transaction loses its ability to refresh
		// and must fully restart on any timestamp push, causing
		// RETRY_SERIALIZABLE errors with "can't refresh txn spans; not valid".
		"SET CLUSTER SETTING kv.transaction.max_refresh_spans_bytes = 67108864",
	}
	for _, stmt := range clusterSettings {
		_, err := conn.Exec(stmt)
		require.NoError(t, err)
	}

	// Create a user for REST API authentication.
	_, err := conn.Exec("CREATE USER roachadmin PASSWORD 'roacher'")
	require.NoError(t, err)
	_, err = conn.Exec("GRANT ADMIN TO roachadmin")
	require.NoError(t, err)

	// Upload the database list file to the workload node.
	const populateFileName = "populate_introspection"
	err = c.PutString(ctx, strings.Join(dbList, "\n"), populateFileName, 0755, c.WorkloadNode())
	require.NoError(t, err)

	// Create the schema using tpccmultidb with --data-loader=none to create
	// only the table schema without loading any data. This significantly
	// speeds up the setup phase since we don't need data for introspection
	// benchmarks.
	t.L().Printf("Starting schema creation with tpccmultidb (schema only, no data)")
	options := tpccOptions{
		WorkloadCmd: "tpccmultidb",
		DB:          strings.Split(dbList[0], ".")[0],
		SetupType:   usingInit,
		Warehouses:  1, // Required for schema generation but no data will be loaded
		ExtraSetupArgs: fmt.Sprintf("--db-list-file=%s --data-loader=none --fks=false",
			populateFileName,
		),
		// Use all CRDB nodes for init to distribute table creation load.
		InitNodes: c.CRDBNodes(),
		Start: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Cluster is already started, this is a no-op.
		},
	}
	setupTPCC(ctx, t, t.L(), c, options)

	t.L().Printf("Schema creation complete, starting introspection benchmark")

	// Set up histogram for benchmarking.
	const benchmarkDuration = 20 * time.Minute
	metricNames := []string{"orm_queries", "api_calls"}
	exp := roachtestutil.CreateWorkloadHistogramExporter(t, c)
	reg := histogram.NewRegistryWithExporter(benchmarkDuration*2, histogram.MockWorkloadName, exp)
	for _, name := range metricNames {
		reg.GetHandle().Get(name)
	}
	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)
	exp.Init(&writer)
	defer roachtestutil.CloseExporter(ctx, exp, t, c, bytesBuf, c.Node(1), "")

	// Upload ORM queries file.
	require.NoError(t, c.PutString(ctx, LargeSchemaOrmQueries, "ormQueries.sql", 0755, c.WorkloadNode()))

	// Get admin UI URLs for API calls.
	webConsoleURLs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.CRDBNodes())
	require.NoError(t, err)
	for urlIdx := range webConsoleURLs {
		webConsoleURLs[urlIdx] = "https://" + webConsoleURLs[urlIdx]
	}

	// Run introspection benchmark with multiple workers.
	numWorkers := (len(c.All()) - 1) * 10
	t.L().Printf("Running introspection benchmark for %v with %d workers", benchmarkDuration, numWorkers)

	// Precreate a connection pool (sql.DB) for each CRDB node. Workers will
	// share these pools based on their assigned node, and the sql.DB driver
	// will manage connection pooling internally.
	nodeConns := make(map[int]*gosql.DB)
	for _, nodeIdx := range c.CRDBNodes() {
		nodeConns[nodeIdx] = c.Conn(ctx, t.L(), nodeIdx)
	}
	defer func() {
		for _, conn := range nodeConns {
			conn.Close()
		}
	}()

	var queryCount atomic.Int64
	startTime := timeutil.Now()
	endTime := startTime.Add(benchmarkDuration)

	g := ctxgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		workerIdx := i
		g.GoCtx(func(ctx context.Context) error {
			// Each worker connects to a different node for load distribution.
			nodeIdx := c.CRDBNodes()[workerIdx%len(c.CRDBNodes())]
			workerConn := nodeConns[nodeIdx]
			// Pick a database from the list for this worker.
			dbSchema := dbList[workerIdx%len(dbList)]
			dbName := strings.Split(dbSchema, ".")[0]

			for timeutil.Now().Before(endTime) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// Execute ORM introspection query.
				start := timeutil.Now()
				rows, err := workerConn.QueryContext(ctx, fmt.Sprintf("USE %s; %s", dbName, LargeSchemaOrmQueries))
				if err != nil {
					t.L().Printf("Worker %d query error: %v", workerIdx, err)
					continue
				}
				rows.Close()
				elapsed := timeutil.Since(start)
				reg.GetHandle().Get("orm_queries").Record(elapsed)
				queryCount.Add(1)
			}
			return nil
		})
	}

	// Periodically tick the histogram and log progress.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	g.GoCtx(func(ctx context.Context) error {
		lastCount := int64(0)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				// Exit the ticker goroutine when the benchmark duration is complete.
				// This prevents the goroutine from running indefinitely if workers
				// exit normally (returning nil) since ctxgroup only cancels ctx
				// when a goroutine returns an error.
				if timeutil.Now().After(endTime) {
					return nil
				}
				currentCount := queryCount.Load()
				elapsed := timeutil.Since(startTime)
				qps := float64(currentCount-lastCount) / 10.0
				t.L().Printf("Progress: %v elapsed, %d total queries, %.1f qps",
					elapsed.Round(time.Second), currentCount, qps)
				lastCount = currentCount
				reg.Tick(func(tick histogram.Tick) {
					_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
				})
			}
		}
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Final tick to capture remaining data.
	reg.Tick(func(tick histogram.Tick) {
		_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
	})

	totalQueries := queryCount.Load()
	totalDuration := timeutil.Since(startTime)
	avgQPS := float64(totalQueries) / totalDuration.Seconds()
	t.L().Printf("Benchmark complete: %d queries in %v (avg %.1f qps)",
		totalQueries, totalDuration.Round(time.Second), avgQPS)
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
