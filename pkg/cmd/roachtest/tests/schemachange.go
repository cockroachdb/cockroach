// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerSchemaChangeDuringKV(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    `schemachange/during/kv`,
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(5),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()

			if err := importTPCHDataset(
				ctx, t, c, "" /* virtualClusterName */, db, 10 /* sf */, c.NewDeprecatedMonitor(ctx),
				c.All(), false /* disableMergeQueue */, false, /* smallRanges */
			); err != nil {
				t.Fatal(err)
			}

			c.Run(ctx, option.WithNodes(c.Node(1)), `./cockroach workload init kv --drop --db=test {pgurl:1}`)
			for node := 1; node <= c.Spec().NodeCount; node++ {
				node := node
				t.Go(func(taskCtx context.Context, _ *logger.Logger) error {
					const cmd = `./cockroach workload run kv --tolerate-errors --min-block-bytes=8 --max-block-bytes=127 --db=test {pgurl%s}`
					return c.RunE(taskCtx, option.WithNodes(c.Node(node)), fmt.Sprintf(cmd, c.Nodes(node)))
				}, task.Name(fmt.Sprintf(`kv-%d`, node)))
			}

			m := c.NewDeprecatedMonitor(ctx, c.All())
			m.Go(func(ctx context.Context) error {
				t.Status("running schema change tests")
				return waitForSchemaChanges(ctx, t.L(), db)
			})
			m.Wait()
		},
	})
}

func waitForSchemaChanges(ctx context.Context, l *logger.Logger, db *gosql.DB) error {
	start := timeutil.Now()

	// These schema changes are over a table that is not actively
	// being updated.
	l.Printf("running schema changes over tpch.customer\n")
	schemaChanges := []string{
		"ALTER TABLE tpch.customer ADD COLUMN newcol INT DEFAULT 23456",
		"CREATE INDEX foo ON tpch.customer (c_name)",
	}
	if err := runSchemaChanges(ctx, l, db, schemaChanges); err != nil {
		return err
	}

	// All these return the same result.
	validationQueries := []string{
		"SELECT count(*) FROM tpch.customer AS OF SYSTEM TIME %s",
		"SELECT count(newcol) FROM tpch.customer AS OF SYSTEM TIME %s",
		"SELECT count(c_name) FROM tpch.customer@foo AS OF SYSTEM TIME %s",
	}
	if err := runValidationQueries(ctx, l, db, start, validationQueries, nil); err != nil {
		return err
	}

	// These schema changes are run later because the above schema
	// changes run for a decent amount of time giving kv.kv
	// an opportunity to get populate through the load generator. These
	// schema changes are acting upon a decent sized table that is also
	// being updated.
	l.Printf("running schema changes over test.kv\n")
	schemaChanges = []string{
		"ALTER TABLE test.kv ADD COLUMN created_at TIMESTAMP DEFAULT now()",
		"CREATE INDEX foo ON test.kv (v)",
	}
	if err := runSchemaChanges(ctx, l, db, schemaChanges); err != nil {
		return err
	}

	// All these return the same result.
	validationQueries = []string{
		"SELECT count(*) FROM test.kv AS OF SYSTEM TIME %s",
		"SELECT count(v) FROM test.kv AS OF SYSTEM TIME %s",
		"SELECT count(v) FROM test.kv@foo AS OF SYSTEM TIME %s",
	}
	// Queries to hone in on index validation problems.
	indexValidationQueries := []string{
		"SELECT count(k) FROM test.kv@kv_pkey AS OF SYSTEM TIME %s WHERE created_at > $1 AND created_at <= $2",
		"SELECT count(v) FROM test.kv@foo AS OF SYSTEM TIME %s WHERE created_at > $1 AND created_at <= $2",
	}
	return runValidationQueries(ctx, l, db, start, validationQueries, indexValidationQueries)
}

func runSchemaChanges(
	ctx context.Context, l *logger.Logger, db *gosql.DB, schemaChanges []string,
) error {
	for _, cmd := range schemaChanges {
		start := timeutil.Now()
		l.Printf("starting schema change: %s\n", cmd)
		if _, err := db.Exec(cmd); err != nil {
			l.Errorf("hit schema change error: %s, for %s, in %s\n", err, cmd, timeutil.Since(start))
			return err
		}
		l.Printf("completed schema change: %s, in %s\n", cmd, timeutil.Since(start))
		// TODO(vivek): Monitor progress of schema changes and log progress.
	}

	return nil
}

// The validationQueries all return the same result.
func runValidationQueries(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	start time.Time,
	validationQueries []string,
	indexValidationQueries []string,
) error {
	// Sleep for a bit before validating the schema changes to
	// accommodate for time differences between nodes. Some of the
	// schema change backfill transactions might use a timestamp a bit
	// into the future. This is not a problem normally because a read
	// of schema data written into the impending future gets pushed,
	// but the reads being done here are at a specific timestamp through
	// AS OF SYSTEM TIME.
	time.Sleep(5 * time.Second)

	var nowString string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&nowString); err != nil {
		return err
	}
	var nowInNanos int64
	if _, err := fmt.Sscanf(nowString, "%d", &nowInNanos); err != nil {
		return err
	}
	now := timeutil.Unix(0, nowInNanos)

	// Validate the different schema changes
	var eCount int64
	for i := range validationQueries {
		var count int64
		q := fmt.Sprintf(validationQueries[i], nowString)
		if err := db.QueryRow(q).Scan(&count); err != nil {
			return err
		}
		l.Printf("query: %s, found %d rows\n", q, count)
		if count == 0 {
			return errors.Errorf("%s: %d rows found", q, count)
		}
		if eCount == 0 {
			eCount = count
			// Investigate index creation problems. Always run this so we know
			// it works.
			if indexValidationQueries != nil {
				sp := timeSpan{start: start, end: now}
				if err := findIndexProblem(
					ctx, l, db, sp, nowString, indexValidationQueries,
				); err != nil {
					return err
				}
			}
		} else if count != eCount {
			return errors.Errorf("%s: %d rows found, expected %d rows", q, count, eCount)
		}
	}
	return nil
}

type timeSpan struct {
	start, end time.Time
}

// Check index inconsistencies over the timeSpan and return true when
// problems are seen.
func checkIndexOverTimeSpan(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	s timeSpan,
	nowString string,
	indexValidationQueries []string,
) (bool, error) {
	var eCount int64
	q := fmt.Sprintf(indexValidationQueries[0], nowString)
	if err := db.QueryRow(q, s.start, s.end).Scan(&eCount); err != nil {
		return false, err
	}
	var count int64
	q = fmt.Sprintf(indexValidationQueries[1], nowString)
	if err := db.QueryRow(q, s.start, s.end).Scan(&count); err != nil {
		return false, err
	}
	l.Printf("counts seen %d, %d, over [%s, %s]\n", count, eCount, s.start, s.end)
	return count != eCount, nil
}

// Keep splitting the span of time passed and log where index
// inconsistencies are seen.
func findIndexProblem(
	ctx context.Context,
	l *logger.Logger,
	db *gosql.DB,
	s timeSpan,
	nowString string,
	indexValidationQueries []string,
) error {
	spans := []timeSpan{s}
	// process all the outstanding time spans.
	for len(spans) > 0 {
		s := spans[0]
		spans = spans[1:]
		// split span into two time ranges.
		leftSpan, rightSpan := s, s
		d := s.end.Sub(s.start) / 2
		if d < 50*time.Millisecond {
			l.Printf("problem seen over [%s, %s]\n", s.start, s.end)
			continue
		}
		m := s.start.Add(d)
		leftSpan.end = m
		rightSpan.start = m

		leftState, err := checkIndexOverTimeSpan(
			ctx, l, db, leftSpan, nowString, indexValidationQueries)
		if err != nil {
			return err
		}
		rightState, err := checkIndexOverTimeSpan(
			ctx, l, db, rightSpan, nowString, indexValidationQueries)
		if err != nil {
			return err
		}
		if leftState {
			spans = append(spans, leftSpan)
		}
		if rightState {
			spans = append(spans, rightSpan)
		}
		if !(leftState || rightState) {
			l.Printf("no problem seen over [%s, %s]\n", s.start, s.end)
		}
	}
	return nil
}

func registerSchemaChangeIndexTPCC800(r registry.Registry) {
	r.Add(makeIndexAddTpccTest(r.MakeClusterSpec(5, spec.CPU(16), spec.WorkloadNode()), 800, time.Hour*2))
}

func registerSchemaChangeIndexTPCC100(r registry.Registry) {
	r.Add(makeIndexAddTpccTest(r.MakeClusterSpec(5, spec.WorkloadNode()), 100, time.Minute*15))
}

func makeIndexAddTpccTest(
	spec spec.ClusterSpec, warehouses int, length time.Duration,
) registry.TestSpec {
	return registry.TestSpec{
		Name:             fmt.Sprintf("schemachange/indexschemachange/index/tpcc/w=%d", warehouses),
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          spec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.DefaultLeases,
		Timeout:          length * 3,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs: fmt.Sprintf("--wait=false --tolerate-errors --workers=%d", warehouses),
				During: func(ctx context.Context) error {
					return runAndLogStmts(ctx, t, c, "addindex", []string{
						`SET CLUSTER SETTING bulkio.index_backfill.ingest_concurrency = 2;`,
						`CREATE UNIQUE INDEX ON tpcc.order (o_entry_d, o_w_id, o_d_id, o_carrier_id, o_id);`,
						`CREATE INDEX ON tpcc.order (o_carrier_id);`,
						`CREATE INDEX ON tpcc.customer (c_last, c_first);`,
					})
				},
				DisableDefaultScheduledBackup: true,
				Duration:                      length,
				SetupType:                     usingImport,
			})
		},
	}
}

// bulkIngestSchemaChangeOp represents the type of schema change operation to
// perform  during bulk ingestion testing.
type bulkIngestSchemaChangeOp string

const (
	// createIndexOp runs CREATE INDEX, which adds keys in unsorted order.
	createIndexOp bulkIngestSchemaChangeOp = "create_index"
	// addColumnOp runs ADD COLUMN, which adds keys in sorted order.
	addColumnOp bulkIngestSchemaChangeOp = "add_column"
)

func registerSchemaChangeBulkIngest(r registry.Registry) {
	// Allow a long running time to account for runs that use a
	// cockroach build with runtime assertions enabled.
	r.Add(makeSchemaChangeBulkIngestTest(r, 12, 4_000_000_000, 5*time.Hour, createIndexOp))
	r.Add(makeSchemaChangeBulkIngestTest(r, 12, 4_000_000_000, 5*time.Hour, addColumnOp))
}

func makeSchemaChangeBulkIngestTest(
	r registry.Registry,
	numNodes, numRows int,
	length time.Duration,
	operation bulkIngestSchemaChangeOp,
) registry.TestSpec {
	return registry.TestSpec{
		Name:             fmt.Sprintf("schemachange/bulkingest/nodes=%d/rows=%d/%s", numNodes, numRows, operation),
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(numNodes, spec.WorkloadNode(), spec.Disks(4)),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.LeaderLeases,
		Timeout:          length * 2,
		PostProcessPerfMetrics: func(test string, histogram *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {
			// The histogram tracks the total elapsed time for the CREATE INDEX operation.
			totalElapsed := histogram.Elapsed

			// Calculate throughput in rows/sec per node.
			schemaChangeDuration := int64(totalElapsed / 1000) // Convert to seconds.
			if schemaChangeDuration == 0 {
				schemaChangeDuration = 1 // Avoid division by zero.
			}
			avgRatePerNode := roachtestutil.MetricPoint(float64(numRows) / float64(int64(numNodes)*schemaChangeDuration))

			return roachtestutil.AggregatedPerfMetrics{
				{
					Name:           fmt.Sprintf("%s_throughput", test),
					Value:          avgRatePerNode,
					Unit:           "rows/s/node",
					IsHigherBetter: true,
				},
			}, nil
		},
		// `fixtures import` (with the workload paths) is not supported in 2.1
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// Configure column a to have sequential ascending values. The payload
			// column will be randomized and thus uncorrelated with the primary key
			// (a, b, c).
			bNum := 1000
			cNum := 1000
			aNum := numRows / (bNum * cNum)
			if c.IsLocal() {
				aNum = 100000
				bNum = 1
				cNum = 1
			}
			payloadBytes := 40

			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

			// Don't add another index when importing.
			cmdWrite := fmt.Sprintf(
				// For fixtures import, use the version built into the cockroach binary
				// so the tpcc workload-versions match on release branches.
				"./cockroach workload fixtures import bulkingest {pgurl:1} --a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
				aNum, bNum, cNum, payloadBytes,
			)

			c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdWrite)

			// Set up histogram exporter for performance metrics.
			exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
			tickHistogram, perfBuf := initBulkJobPerfArtifacts(length*2, t, exporter)
			defer roachtestutil.CloseExporter(ctx, exporter, t, c, perfBuf, c.Node(1), "")

			m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())

			indexDuration := length
			if c.IsLocal() {
				indexDuration = time.Second * 30
			}
			cmdWriteAndRead := fmt.Sprintf(
				"./cockroach workload run bulkingest --duration %s {pgurl:1-%d} --a %d --b %d --c %d --payload-bytes %d",
				indexDuration.String(), c.Spec().NodeCount-1, aNum, bNum, cNum, payloadBytes,
			)
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdWriteAndRead)
				return nil
			})

			m.Go(func(ctx context.Context) error {
				db := c.Conn(ctx, t.L(), 1)
				defer db.Close()

				// Enable distributed merge for index backfills.
				if _, err := db.Exec("SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'enabled'"); err != nil {
					t.Fatal(err)
				}

				t.L().Printf("Computing table statistics manually")
				if _, err := db.Exec("CREATE STATISTICS stats from bulkingest.bulkingest"); err != nil {
					t.Fatal(err)
				}
				if !c.IsLocal() {
					// Wait for the load generator to run for a few minutes before performing the schema change.
					sleepInterval := time.Minute * 5
					maxSleep := length / 2
					if sleepInterval > maxSleep {
						sleepInterval = maxSleep
					}
					time.Sleep(sleepInterval)
				}

				// Tick once before starting the schema change.
				tickHistogram()
				before := timeutil.Now()

				var stmt string
				switch operation {
				case createIndexOp:
					t.L().Printf("Creating index")
					stmt = `CREATE INDEX payload_a ON bulkingest.bulkingest (payload, a)`
				case addColumnOp:
					t.L().Printf("Adding column")
					stmt = `ALTER TABLE bulkingest.bulkingest ADD COLUMN new_column INT NOT NULL DEFAULT 42`
				default:
					t.Fatalf("Unknown operation: %s", operation)
				}

				if _, err := db.Exec(stmt); err != nil {
					t.Fatal(err)
				}
				// Tick once after the schema change to capture the total elapsed time.
				tickHistogram()
				t.L().Printf("%s took %v\n", stmt, timeutil.Since(before))
				return nil
			})

			m.Wait()
		},
	}
}

// registerSchemaChangeBulkIngestScaleTest registers tests for investigating
// distributed merge behavior at varying dataset sizes.
func registerSchemaChangeBulkIngestScaleTest(r registry.Registry) {
	for rows := int64(5_000_000_000); rows <= 8_000_000_000; rows += 3_000_000_000 {
		// Each size runs in two modes: with and without distributed merge
		r.Add(makeSchemaChangeBulkIngestScaleTest(r, 12, rows, false)) // No distributed merge
		r.Add(makeSchemaChangeBulkIngestScaleTest(r, 12, rows, true))  // With distributed merge
	}
}

func makeSchemaChangeBulkIngestScaleTest(
	r registry.Registry, numNodes int, numRows int64, distributedMerge bool,
) registry.TestSpec {
	modeStr := "nodist"
	if distributedMerge {
		modeStr = "dist"
	}

	return registry.TestSpec{
		Name:             fmt.Sprintf("schemachange/bulkingest/scale/rows=%d/%s", numRows, modeStr),
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(numNodes, spec.WorkloadNode(), spec.Disks(4)),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.ManualOnly, // Manual-only for investigation
		Leases:           registry.LeaderLeases,
		Timeout:          18 * time.Hour, // Extended timeout for large datasets
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runBulkIngestScaleTest(ctx, t, c, numRows, distributedMerge)
		},
	}
}

func runBulkIngestScaleTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numRows int64, distributedMerge bool,
) {
	// Step 1: Start cluster
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

	// Step 2: Calculate workload parameters
	// Use same formula as existing test, with local override for testing
	var aNum, bNum, cNum int64
	var actualRows int64
	payloadBytes := 40

	if c.IsLocal() {
		// For local testing, use a small dataset to validate code paths quickly
		aNum = 100000
		bNum = 1
		cNum = 1
		actualRows = aNum * bNum * cNum // 100,000 rows
		t.L().Printf("Running in LOCAL mode: scaled down to %d rows", actualRows)
	} else {
		bNum = 1000
		cNum = 1000
		aNum = numRows / (bNum * cNum)
		actualRows = numRows
	}

	// Step 3: Import data (no secondary index)
	cmdImport := fmt.Sprintf(
		"./cockroach workload fixtures import bulkingest {pgurl:1} "+
			"--a %d --b %d --c %d --payload-bytes %d --index-b-c-a=false",
		aNum, bNum, cNum, payloadBytes,
	)
	t.L().Printf("Importing data: %d rows (%d x %d x %d)", actualRows, aNum, bNum, cNum)
	importStart := timeutil.Now()
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdImport)
	t.L().Printf("Import completed in %v", timeutil.Since(importStart))

	// Step 4: Connect to database
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Step 5: Gather statistics
	t.L().Printf("Creating table statistics")
	statsStart := timeutil.Now()
	if _, err := db.Exec("CREATE STATISTICS stats FROM bulkingest.bulkingest"); err != nil {
		t.Fatal(err)
	}
	t.L().Printf("Statistics created in %v", timeutil.Since(statsStart))

	// Step 6: Get logical data size using SHOW RANGES WITH DETAILS
	var logicalBytes int64
	if err := db.QueryRow(`
		SELECT COALESCE(sum(range_size), 0)
		FROM [SHOW RANGES FROM TABLE bulkingest.bulkingest WITH DETAILS]
	`).Scan(&logicalBytes); err != nil {
		t.Fatal(err)
	}
	logicalGB := float64(logicalBytes) / (1024 * 1024 * 1024)
	t.L().Printf("Logical data size: %.2f GB (%d bytes)", logicalGB, logicalBytes)

	// Step 7: Print lease holder distribution
	t.L().Printf("Table ranges after import:")
	leaseRows, err := db.Query(`
		WITH r AS (SHOW RANGES FROM TABLE bulkingest.bulkingest WITH DETAILS)
		SELECT lease_holder, count(*) AS range_count FROM r GROUP BY lease_holder ORDER BY lease_holder
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer leaseRows.Close()
	for leaseRows.Next() {
		var leaseHolder int
		var rangeCount int
		if err := leaseRows.Scan(&leaseHolder, &rangeCount); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("  lease_holder=%d, range_count=%d", leaseHolder, rangeCount)
	}
	if err := leaseRows.Err(); err != nil {
		t.Fatal(err)
	}

	// Step 8: Configure distributed merge mode
	if distributedMerge {
		t.L().Printf("Enabling distributed merge mode (declarative)")
		if _, err := db.Exec(
			"SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'declarative'",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			"SET CLUSTER SETTING bulkio.import.distributed_merge.enabled = true",
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			"SET CLUSTER SETTING server.debug.default_vmodule = 'merge=2'",
		); err != nil {
			t.Fatal(err)
		}
	} else {
		t.L().Printf("Disabling distributed merge mode")
		if _, err := db.Exec(
			"SET CLUSTER SETTING bulkio.index_backfill.distributed_merge.mode = 'disabled'",
		); err != nil {
			t.Fatal(err)
		}
	}

	// Step 9: Create index and measure time
	stmt := `CREATE INDEX payload_a ON bulkingest.bulkingest (payload, a)`
	t.L().Printf("Creating index (distributed_merge=%v)", distributedMerge)
	indexStart := timeutil.Now()

	_, err = db.Exec(stmt)
	indexDuration := timeutil.Since(indexStart)

	// Step 10: Report results
	var resultError string
	if err != nil {
		resultError = fmt.Sprintf(", error=%s", err.Error())
	}
	t.L().Printf("RESULT: rows=%d, dist=%v, size_gb=%.2f, duration_sec=%.2f, success=%v%s",
		actualRows, distributedMerge, logicalGB,
		indexDuration.Seconds(), err == nil, resultError)

	if err != nil {
		t.L().Printf("CREATE INDEX FAILED: %v", err)
		t.Fatal(err)
	}

	t.L().Printf("CREATE INDEX completed in %v", indexDuration)
}

func registerSchemaChangeDuringTPCC800(r registry.Registry) {
	r.Add(makeSchemaChangeDuringTPCC(r.MakeClusterSpec(5, spec.CPU(16), spec.WorkloadNode()), 800, time.Hour*3))
}

func makeSchemaChangeDuringTPCC(
	spec spec.ClusterSpec, warehouses int, length time.Duration,
) registry.TestSpec {
	return registry.TestSpec{
		Name:             "schemachange/during/tpcc",
		Owner:            registry.OwnerSQLFoundations,
		Benchmark:        true,
		Cluster:          spec,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Timeout:          length * 3,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCC(ctx, t, t.L(), c, tpccOptions{
				Warehouses: warehouses,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs: fmt.Sprintf("--wait=false --tolerate-errors --workers=%d", warehouses),
				During: func(ctx context.Context) error {
					if t.IsBuildVersion(`v19.2.0`) {
						if err := runAndLogStmts(ctx, t, c, "during-schema-changes-19.2", []string{
							// CREATE TABLE AS with a specified primary key was added in 19.2.
							`CREATE TABLE tpcc.orderpks (o_w_id, o_d_id, o_id, PRIMARY KEY(o_w_id, o_d_id, o_id)) AS select o_w_id, o_d_id, o_id FROM tpcc.order;`,
						}); err != nil {
							return err
						}
					} else {
						if err := runAndLogStmts(ctx, t, c, "during-schema-changes-19.1", []string{
							`CREATE TABLE tpcc.orderpks (o_w_id INT, o_d_id INT, o_id INT, PRIMARY KEY(o_w_id, o_d_id, o_id));`,
							// We can't populate the table with CREATE TABLE AS, so just
							// insert the rows. The limit exists to reduce contention.
							`INSERT INTO tpcc.orderpks SELECT o_w_id, o_d_id, o_id FROM tpcc.order LIMIT 10000;`,
						}); err != nil {
							return err
						}
					}
					return runAndLogStmts(ctx, t, c, "during-schema-changes", []string{
						`CREATE INDEX ON tpcc.order (o_carrier_id);`,

						`CREATE TABLE tpcc.customerpks (c_w_id INT, c_d_id INT, c_id INT, FOREIGN KEY (c_w_id, c_d_id, c_id) REFERENCES tpcc.customer (c_w_id, c_d_id, c_id));`,

						`ALTER TABLE tpcc.order ADD COLUMN orderdiscount INT DEFAULT 0;`,
						`ALTER TABLE tpcc.order ADD CONSTRAINT nodiscount CHECK (orderdiscount = 0);`,

						`ALTER TABLE tpcc.orderpks ADD CONSTRAINT warehouse_id FOREIGN KEY (o_w_id) REFERENCES tpcc.warehouse (w_id);`,

						// The FK constraint on tpcc.district referencing tpcc.warehouse is
						// unvalidated, thus this operation will not be a noop.
						`ALTER TABLE tpcc.district VALIDATE CONSTRAINT district_d_w_id_fkey;`,

						`ALTER TABLE tpcc.orderpks RENAME TO tpcc.readytodrop;`,
						`TRUNCATE TABLE tpcc.readytodrop CASCADE;`,
						`DROP TABLE tpcc.readytodrop CASCADE;`,
					})
				},
				Duration:  length,
				SetupType: usingImport,
			})
		},
	}
}

func runAndLogStmts(
	ctx context.Context, t test.Test, c cluster.Cluster, prefix string, stmts []string,
) error {
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()
	t.L().Printf("%s: running %d statements\n", prefix, len(stmts))
	start := timeutil.Now()
	for i, stmt := range stmts {
		// Let some traffic run before the schema change.
		time.Sleep(time.Minute)
		t.L().Printf("%s: running statement %d...\n", prefix, i+1)
		before := timeutil.Now()
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("%s: statement %d: %q took %v\n", prefix, i+1, stmt, timeutil.Since(before))
	}
	t.L().Printf("%s: ran %d statements in %v\n", prefix, len(stmts), timeutil.Since(start))
	return nil
}
