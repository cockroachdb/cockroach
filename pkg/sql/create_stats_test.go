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
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestStatsWithLowTTL simulates a CREATE STATISTICS run that takes longer than
// the TTL of a table; the purpose is to test the timestamp-advancing mechanism.
func TestStatsWithLowTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if util.RaceEnabled {
		// The test requires a bunch of data to be inserted, which is much slower in
		// race mode.
		t.Skip("skipping under race")
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `
		SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;
		CREATE DATABASE test;
		USE test;
		CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT);
	`)
	const numRows = 50000
	r.Exec(t, `INSERT INTO t SELECT k, 2*k, 3*k FROM generate_series(0, $1) AS g(k)`, numRows-1)

	pgURL, cleanupFunc := sqlutils.PGUrl(t,
		s.ServingSQLAddr(),
		"TestStatsWithLowTTL",
		url.User(security.RootUser),
	)
	defer cleanupFunc()

	// Start a goroutine that keeps updating rows in the table and issues
	// GCRequests simulating a 1 second TTL. While this is running, reading at a
	// timestamp older than 1 second will error out.
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
			// Force a table GC of values older than 1 second.
			if err := s.ForceTableGC(
				context.Background(), "test", "t", s.Clock().Now().Add(-int64(1*time.Second), 0),
			); err != nil {
				goroutineErr = err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Sleep 500ms after every 10k scanned rows, to simulate a long-running
	// operation.
	rowexec.TestingSamplerSleep = 500 * time.Millisecond
	defer func() { rowexec.TestingSamplerSleep = 0 }()

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

	// Set up timestamp advance to keep timestamps no older than 0.3s.
	r.Exec(t, `SET CLUSTER SETTING sql.stats.max_timestamp_age = '0.3s'`)

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
