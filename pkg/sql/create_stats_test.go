// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TestStatsWithLowTTL simulates a CREATE STATISTICS run that takes longer than
// the TTL of a table; the purpose is to test the timestamp-advancing mechanism.
func TestStatsWithLowTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test depends on reasonable timings, so don't run under race.
	skip.UnderRace(t)

	rng, _ := randutil.NewTestRand()
	var blockTableReader atomic.Bool
	blockCh := make(chan struct{})

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				// Set the batch size small to avoid having to use a large
				// number of rows.
				//
				// We use a random bytes limit so that the scans have a chance
				// to stop at different points within the SQL row (in case of
				// multiple column families).
				TableReaderBatchBytesLimit: 50 + int64(rng.Intn(100)),
				TableReaderStartScanCb: func() {
					if blockTableReader.Load() {
						<-blockCh
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`)
	// Sometimes use single column family, sometimes use multiple.
	if rng.Intn(2) == 0 {
		r.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, FAMILY (k, a, b));`)
	} else {
		r.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, FAMILY (k), FAMILY (a), FAMILY (b));`)
	}
	const initialNumRows, maxNumRows = 20, 100
	r.Exec(t, `INSERT INTO t SELECT k, 2*k, 3*k FROM generate_series(0, $1) AS g(k)`, initialNumRows-1)

	// Start a goroutine that keeps modifying rows (updating, deleting,
	// inserting new ones) in the table and issues GCRequests simulating a 2
	// second TTL. While this is running, reading at a timestamp older than 2
	// seconds will likely error out.
	var goroutineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	stopCh := make(chan struct{})
	// onlyUpdates determines whether only UPDATE stmts are issued in the
	// goroutine - this behavior is used in the first part of the test where we
	// expect the stats collection to fail (if we allow INSERT and DELETE stmts,
	// we might never hit the GC threshold).
	var onlyUpdates atomic.Bool
	onlyUpdates.Store(true)

	go func() {
		defer wg.Done()

		// Open a separate connection to the database.
		db2 := s.SQLConn(t)

		nextPK := initialNumRows
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			switch rng.Intn(4) {
			case 0, 1:
				// In 50% cases try to update an existing row (it's ok if the
				// row doesn't exist).
				k := rng.Intn(nextPK)
				if _, err := db2.Exec(`UPDATE t SET a=a+1, b=b+2 WHERE k=$1`, k); err != nil {
					goroutineErr = err
					return
				}
			case 2:
				if onlyUpdates.Load() {
					continue
				}
				// In 25% cases try to delete a row (it's ok if the row doesn't
				// exist).
				k := rng.Intn(nextPK)
				if _, err := db2.Exec(`DELETE FROM t WHERE k=$1`, k); err != nil {
					goroutineErr = err
					return
				}
			case 3:
				if onlyUpdates.Load() {
					continue
				}
				// In 25% cases insert a new row, but don't insert too many rows
				// to allow for the stats collection to complete.
				if nextPK == maxNumRows {
					continue
				}
				if _, err := db2.Exec(`INSERT INTO t SELECT $1, 2*$1, 3*$1`, nextPK); err != nil {
					goroutineErr = err
					return
				}
				nextPK++
			}
			// Force a table GC of values older than 2 seconds.
			if err := s.ForceTableGC(
				context.Background(), "defaultdb", "t", s.Clock().Now().Add(-int64(2*time.Second), 0),
			); err != nil {
				goroutineErr = err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Sleep 500ms after every scanned row (note that we set the KV batch size to
	// a small limit above), to simulate a long-running operation.
	row.TestingInconsistentScanSleep = 500 * time.Millisecond
	defer func() { row.TestingInconsistentScanSleep = 0 }()

	// Sleep enough to ensure the table descriptor existed at AOST.
	time.Sleep(100 * time.Millisecond)

	// Creating statistics should fail now because the timestamp will get older
	// than 1s. In theory, we could get really lucky (wrt scheduling of the
	// goroutine above), so we try multiple times.
	for i := 0; ; i++ {
		_, err := db.Exec(`CREATE STATISTICS foo FROM t AS OF SYSTEM TIME '-0.1s'`)
		if err != nil {
			if !testutils.IsError(err, "batch timestamp .* must be after replica GC threshold") {
				// Unexpected error.
				t.Error(err)
			}
			break
		}
		if i > 5 {
			t.Error("expected CREATE STATISTICS to fail")
			break
		}
		t.Log("expected CREATE STATISTICS to fail, trying again")
	}

	// Set up timestamp advance to keep timestamps no older than 1s.
	r.Exec(t, `SET CLUSTER SETTING sql.stats.max_timestamp_age = '1s'`)
	onlyUpdates.Store(false)

	// Block start of the inconsistent scan for 2s so that the initial timestamp
	// becomes way too old.
	blockTableReader.Store(true)
	go func() {
		defer blockTableReader.Store(false)
		time.Sleep(2 * time.Second)
		close(blockCh)
	}()

	_, err := db.Exec(`CREATE STATISTICS foo FROM t AS OF SYSTEM TIME '-0.1s'`)
	if err != nil {
		t.Error(err)
	}

	close(stopCh)
	wg.Wait()
	if goroutineErr != nil {
		t.Fatal(goroutineErr)
	}
}

func TestStaleStatsForDeletedTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	stats.AutomaticStatisticsClusterMode.Override(ctx, &s.ClusterSettings().SV, false)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	sqlRunner.Exec(t, `CREATE TABLE t(a int PRIMARY KEY)`)
	sqlRunner.Exec(t, `CREATE STATISTICS s FROM t`)

	// Simulate a stale entry in system.table_statistics that references a
	// table descriptor that does not exist anywhere else.
	sqlRunner.Exec(t, `
INSERT INTO system.table_statistics (
	"tableID",
	"name",
	"columnIDs",
	"createdAt",
	"rowCount",
	"distinctCount",
	"nullCount",
	"avgSize"
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		201,
		"t_deleted",
		"{1}",
		timeutil.Now(),
		1, /* rowCount */
		1, /* distinctCount */
		0, /* nullCount */
		4, /* avgSize */
	)
	sqlRunner.CheckQueryResults(
		t,
		`SELECT stxrelid::regclass::text, stxname, stxnamespace::regnamespace::text FROM pg_catalog.pg_statistic_ext`,
		[][]string{{"t", "s", "public"}},
	)
}

// BenchmarkAnalyze runs a benchmark for the ANALYZE statement on a
// sysbench-like table.
func BenchmarkAnalyze(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	stats.AutomaticStatisticsClusterMode.Override(ctx, &s.ClusterSettings().SV, false)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	// Ensure that there's always a single range.
	sqlRunner.Exec(b, `SET CLUSTER SETTING kv.split_queue.enabled = false`)
	sqlRunner.Exec(b, `
		CREATE TABLE t (
			id INT8 PRIMARY KEY,
			k INT8 NOT NULL DEFAULT 0,
			c CHAR(120) NOT NULL DEFAULT '',
			pad CHAR(60) NOT NULL DEFAULT '',
			INDEX (k)
		)
	`)

	const (
		batchSize = 10_000
		numRows   = 100_000
	)

	for i, n := 0, numRows; i < n; i += batchSize {
		sqlRunner.Exec(b, `
			INSERT INTO t (id, k, c, pad)
			SELECT
				i,
				(random() * `+strconv.Itoa(numRows)+`)::INT, 
				substr(md5(random()::TEXT) || md5(random()::TEXT) || md5(random()::TEXT) || md5(random()::TEXT), 0, 120),
				substr(md5(random()::TEXT) || md5(random()::TEXT), 0, 60)
			FROM generate_series($1, $2) AS g(i)
		`, i, i+batchSize-1)
	}

	// Run a full table scan to resolve intents and reduce variance.
	sqlRunner.Exec(b, `SELECT count(*) FROM t`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlRunner.Exec(b, `ANALYZE t`)
	}
}

func TestAutoPartialStatsJobDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)

	// Disable automatic statistics collection.
	sqlRunner.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`)
	sqlRunner.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_partial_collection.enabled = false;`)

	// Create a test table.
	sqlRunner.Exec(t, `CREATE TABLE test (id INT PRIMARY KEY, value INT);`)

	// Insert some data into the table.
	sqlRunner.Exec(t, `INSERT INTO test SELECT i, i*100 FROM generate_series(1, 100) AS g(i);`)

	// First create full statistics to establish baseline stats.
	sqlRunner.Exec(t, `CREATE STATISTICS __auto__ FROM test`)

	// Explicitly create partial stats to ensure we have a job to check.
	sqlRunner.Exec(t, `CREATE STATISTICS __auto_partial__ FROM test USING EXTREMES`)

	// Wait for the partial stats job to complete.
	testutils.SucceedsSoon(t, func() error {
		var count int
		// Check specifically for AUTO CREATE PARTIAL STATS jobs related to our table.
		sqlRunner.QueryRow(t, `
			SELECT count(*) FROM system.jobs 
			WHERE job_type = 'AUTO CREATE PARTIAL STATS' 
			AND description LIKE '%test%'`).Scan(&count)
		if count == 0 {
			return errors.New("expected at least one AUTO CREATE PARTIAL STATS job")
		}
		return nil
	})

	// Verify job description contains the expected text.
	var description string
	sqlRunner.QueryRow(t, `
		SELECT description FROM system.jobs 
		WHERE job_type = 'AUTO CREATE PARTIAL STATS'
		AND description LIKE '%test%' 
		LIMIT 1`).Scan(&description)

	expectedDescription := "Partial statistics update for"
	if !strings.Contains(description, expectedDescription) {
		t.Errorf("expected description to contain: %s, got: %s", expectedDescription, description)
	}
}
