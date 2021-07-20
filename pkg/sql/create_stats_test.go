// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestStatsWithLowTTL simulates a CREATE STATISTICS run that takes longer than
// the TTL of a table; the purpose is to test the timestamp-advancing mechanism.
func TestStatsWithLowTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test depends on reasonable timings, so don't run under race.
	skip.UnderRace(t)

	// Set the KV batch size to 1 to avoid having to use a large number of rows.
	defer row.TestingSetKVBatchSize(1)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `
		SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;
		CREATE DATABASE test;
		USE test;
		CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT);
	`)
	const numRows = 5
	r.Exec(t, `INSERT INTO t SELECT k, 2*k, 3*k FROM generate_series(0, $1) AS g(k)`, numRows-1)

	pgURL, cleanupFunc := sqlutils.PGUrl(t,
		s.ServingSQLAddr(),
		"TestStatsWithLowTTL",
		url.User(security.RootUser),
	)
	defer cleanupFunc()

	// Start a goroutine that keeps updating rows in the table and issues
	// GCRequests simulating a 2 second TTL. While this is running, reading at a
	// timestamp older than 2 seconds will likely error out.
	var goroutineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	stopCh := make(chan struct{})

	go func() {
		defer wg.Done()

		// Open a separate connection to the database.
		db2, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			goroutineErr = err
			return
		}
		defer db2.Close()

		_, err = db2.Exec("USE test")
		if err != nil {
			goroutineErr = err
			return
		}
		rng, _ := randutil.NewPseudoRand()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			k := rng.Intn(numRows)
			if _, err := db2.Exec(`UPDATE t SET a=a+1, b=b+2 WHERE k=$1`, k); err != nil {
				goroutineErr = err
				return
			}
			// Force a table GC of values older than 2 seconds.
			if err := s.ForceTableGC(
				context.Background(), "test", "t", s.Clock().Now().Add(-int64(2*time.Second), 0),
			); err != nil {
				goroutineErr = err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Sleep 500ms after every scanned row (note that we set the KV batch size to
	// 1 above), to simulate a long-running operation.
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
