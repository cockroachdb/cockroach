// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

func registerImportCancellation(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             `import-cancellation`,
		Owner:            registry.OwnerSQLQueries,
		Benchmark:        true,
		Timeout:          4 * time.Hour,
		Cluster:          r.MakeClusterSpec(6, spec.CPU(32)),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Cloud() != spec.GCE && !c.IsLocal() {
				t.Skip("uses gs://cockroach-fixtures-us-east1; see https://github.com/cockroachdb/cockroach/issues/105968")
			}
			runImportCancellation(ctx, t, c)
		},
	})
}

func runImportCancellation(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.DeprecatedWorkload(), "./workload") // required for tpch
	startOpts := maybeUseMemoryBudget(t, 50)
	startOpts.RoachprodOpts.ScheduleBackups = true
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings())
	t.Status("starting csv servers")
	c.Run(ctx, c.All(), `./cockroach workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

	// Download the tpch queries file. After the import, we'll run tpch queries
	// against the imported tables.
	const queriesFilename = "tpch"
	const queriesURL = "https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/tpch-queries"
	t.Status(fmt.Sprintf("downloading %s query file from %s", queriesFilename, queriesURL))
	if err := c.RunE(ctx, c.Node(1), fmt.Sprintf("curl %s > %s", queriesURL, queriesFilename)); err != nil {
		t.Fatal(err)
	}
	numQueries, err := getNumQueriesInFile(queriesFilename, queriesURL)
	if err != nil {
		t.Fatal(err)
	}

	// Create the tables.
	conn := c.Conn(ctx, t.L(), 1)
	t.Status("creating SQL tables")
	if _, err := conn.Exec(`CREATE DATABASE csv;`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(`USE csv;`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(`SET CLUSTER SETTING kv.bulk_ingest.max_index_buffer_size = '2gb'`); err != nil {
		t.Fatal(err)
	}

	seed := int64(1666467482296309000)
	rng := randutil.NewTestRandWithSeed(seed)

	tablesToNumFiles := map[string]int{
		"region":   1,
		"nation":   1,
		"part":     8,
		"supplier": 8,
		"partsupp": 8,
		"customer": 8,
		"orders":   8,
		"lineitem": 2,
	}
	for tbl := range tablesToNumFiles {
		fixtureURL := fmt.Sprintf("gs://cockroach-fixtures-us-east1/tpch-csv/schema/%s.sql?AUTH=implicit", tbl)
		createStmt, err := readCreateTableFromFixture(fixtureURL, conn)
		if err != nil {
			t.Fatal(err)
		}
		// Create table to import into.
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			t.Fatal(err)
		}
		// Set a random GC TTL that is relatively short. We want to exercise GC
		// of MVCC range tombstones, and would like a mix of live MVCC Range
		// Tombstones and the Pebble RangeKeyUnset tombstones that clear them.
		ttl := randutil.RandIntInRange(rng, 10*60 /* 10m */, 60*20 /* 20m */)
		stmt := fmt.Sprintf(`ALTER TABLE csv.%s CONFIGURE ZONE USING gc.ttlseconds = $1`, tbl)
		_, err = conn.ExecContext(ctx, stmt, ttl)
		if err != nil {
			t.Fatal(err)
		}
	}

	test := importCancellationTest{
		Test:    t,
		c:       c,
		rootRng: rng,
		seed:    seed,
	}
	m := c.NewMonitor(ctx)
	t.Status("running imports with seed ", seed)
	var wg sync.WaitGroup
	wg.Add(len(tablesToNumFiles))
	for tableName, numFiles := range tablesToNumFiles {
		tableName, numFiles := tableName, numFiles // bind to iteration scope
		rng := rand.New(rand.NewSource(test.rootRng.Int63()))
		m.Go(func(ctx context.Context) error {
			defer wg.Done()
			t.WorkerStatus(`launching worker for `, tableName)
			defer t.WorkerStatus()

			test.runImportSequence(ctx, rng, tableName, numFiles)
			return nil
		})
	}
	wg.Wait()

	// Before running the TPCH workload, lift the GC TTL back up. Otherwise the
	// long-running analytical queries can fail due to reading at a timestamp
	// that becomes GC'd.
	for tbl := range tablesToNumFiles {
		stmt := fmt.Sprintf(`ALTER TABLE csv.%s CONFIGURE ZONE USING gc.ttlseconds = $1`, tbl)
		_, err = conn.ExecContext(ctx, stmt, 60*60*4 /* 4 hours */)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Run the TPCH workload. Note that the TPCH workload asserts equality for
	// query results. If the import cancellations corrupted table data, running
	// the TPCH workload should observe it.
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus(`running tpch workload`)
		// maxOps flag will allow us to exit the workload once all the queries
		// were run 2 times.
		const numRunsPerQuery = 2
		const maxLatency = 500 * time.Second
		maxOps := numRunsPerQuery * numQueries
		cmd := fmt.Sprintf(
			"./workload run querybench --db=csv --concurrency=1 --query-file=%s "+
				"--num-runs=%d --max-ops=%d {pgurl%s} "+
				"--histograms="+t.PerfArtifactsDir()+"/stats.json --histograms-max-latency=%s",
			queriesFilename, numRunsPerQuery, maxOps, c.All(), maxLatency.String())
		if err := c.RunE(ctx, c.Node(1), cmd); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	m.Wait()

	// TODO(jackson): Ensure that the number of RangeKeySet falls (—to zero?).
}

type importCancellationTest struct {
	test.Test
	c       cluster.Cluster
	rootRng *rand.Rand
	seed    int64
}

func (t *importCancellationTest) makeFilename(tableName string, number int, numFiles int) string {
	// Tables with more than one files have the number as a suffix on the
	// filename, `<tablename>.tbl.1`. Tables with a single file do not.
	if numFiles > 1 {
		return fmt.Sprintf(`'gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/%[1]s.tbl.%[2]d?AUTH=implicit'`, tableName, number)
	}
	return fmt.Sprintf(`'gs://cockroach-fixtures-us-east1/tpch-csv/sf-100/%[1]s.tbl?AUTH=implicit'`, tableName)
}

func (t *importCancellationTest) runImportSequence(
	ctx context.Context, rng *rand.Rand, tableName string, numFiles int,
) {
	n := t.c.All()[rng.Intn(len(t.c.All()))]
	conn := t.c.Conn(ctx, t.L(), n)
	if _, err := conn.Exec(`USE csv;`); err != nil {
		t.Fatal(err)
	}

	filesToImport := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		filesToImport[i] = t.makeFilename(tableName, i+1, numFiles)
	}

	// The following loop runs between 1-4 times. Each time, it selects a random
	// subset of the table's fixture files and begins importing them with an
	// IMPORT INTO. after a random duration, it cancels the import job. If the
	// job has not yet completed, the import will be reverted and a MVCC range
	// tombstone will be written over the imported data. If the job has already
	// completed, the successfully imported files are removed from
	// consideration.
	//
	// The loop stops when there's no longer any un-imported files remaining. On
	// the last run, all un-imported files are included in the import and the
	// import is not cancelled. As such, when this function returns all files
	// should have successfully been imported. There may be additional versions
	// of some of the keys remnant from failed imports, and MVCC range
	// tombstones at various timestamps deleting them.

	attempts := randutil.RandIntInRange(rng, 2, 5)
	for i := 0; i < attempts && len(filesToImport) > 0; i++ {
		t.WorkerStatus(fmt.Sprintf(`attempt %d/%d for table %q`, i+1, attempts, tableName))
		files := filesToImport
		// If not the last attempt, import a subset of the files available.
		// This will create MVCC range tombstones across separate regions of
		// the table's keyspace.
		if i != attempts-1 {
			rng.Shuffle(len(files), func(i, j int) {
				files[i], files[j] = files[j], files[i]
			})
			files = files[:randutil.RandIntInRange(rng, 1, len(files)+1)]
		}
		t.L().PrintfCtx(ctx, "Beginning import job for attempt %d/%d for table %s with %d files: %s",
			i+1, attempts, tableName, len(files), strings.Join(files, ", "))

		var jobID string
		importStmt := fmt.Sprintf(`
			IMPORT INTO %[1]s
			CSV DATA (
			%[2]s
			) WITH  delimiter='|', detached
		`, tableName, strings.Join(files, ", "))
		err := conn.QueryRowContext(ctx, importStmt).Scan(&jobID)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}

		// If not the last attempt, cancel the job after a random duration in [10s,3m).
		if i != attempts-1 {
			timeout := time.Duration(randutil.RandIntInRange(rng, 10, 60*3)) * time.Second
			select {
			case <-time.After(timeout):
				// fallthrough
			case <-ctx.Done():
				return
			}

			t.L().PrintfCtx(ctx, "Cancelling import job for attempt %d/%d for table %s.", i+1, attempts, tableName)
			_, err = conn.ExecContext(ctx, `CANCEL JOBS (
				WITH x AS (SHOW JOBS)
				SELECT job_id FROM x WHERE job_id = $1 AND status IN ('pending', 'running', 'retrying')
			)`, jobID)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Block until the job is complete. Afterwards, it either completed
		// succesfully before we cancelled it, or the cancellation has finished
		// reverting the keys it wrote prior to cancellation.
		var status string
		err = conn.QueryRowContext(
			ctx,
			`WITH x AS (SHOW JOBS WHEN COMPLETE (SELECT $1)) SELECT status FROM x`,
			jobID,
		).Scan(&status)
		if err != nil {
			t.Fatal(err)
		}
		t.L().PrintfCtx(ctx, "Import job for attempt %d/%d for table %s (%d files) completed with status %s.",
			i+1, attempts, tableName, len(files), status)

		// If the IMPORT was successful (eg, our cancellation came in too late),
		// remove the files that succeeded so we don't try to import them again.
		// If this was the last attempt, this should remove all the remaining
		// files and `filesToImport` should be empty.
		if status == "succeeded" {
			t.L().PrintfCtx(ctx, "Removing files [%s] from consideration; completed", strings.Join(files, ", "))
			filesToImport = removeStrings(filesToImport, files)
		} else if status == "failed" {
			t.Fatal(errors.Newf("Job %s failed.\n", jobID))
		}
	}
	if len(filesToImport) != 0 {
		t.Fatalf("Expected zero remaining %q files after final attempt, but %d remaining.",
			tableName, len(filesToImport))
	}

	// Kick off a stats collection job for the table. This serves a dual purpose.
	// Up-to-date statistics on the table helps the optimizer during the query
	// phase of the test. The stats job also requires scanning a large swath of
	// the keyspace, which results in greater test coverage.
	stmt := fmt.Sprintf(`ANALYZE csv.%s`, tableName)
	_, err := conn.ExecContext(ctx, stmt)
	if err != nil {
		t.Fatal(err)
	}
}

// remove removes all elements from a that exist in b.
func removeStrings(a, b []string) []string {
	rm := map[string]bool{}
	for _, s := range b {
		rm[s] = true
	}

	var ret []string
	for _, s := range a {
		if !rm[s] {
			ret = append(ret, s)
		}
	}
	return ret
}
