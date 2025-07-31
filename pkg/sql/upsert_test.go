// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestUpsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if mutations.MaxBatchSize(false /* forceProductionMaxBatchSize */) == 1 {
		// The fast path requires that the max batch size is at least 2, so
		// we'll skip the test.
		skip.UnderMetamorphic(t)
	}

	// This filter increments scans and endTxn for every ScanRequest and
	// EndTxnRequest that hits user table data.
	var gets uint64
	var scans uint64
	var endTxn uint64
	var codecValue atomic.Value
	filter := func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
		codec := codecValue.Load()
		if codec == nil {
			return nil
		}
		if bytes.Compare(filterArgs.Req.Header().Key, bootstrap.TestingUserTableDataMin(codec.(keys.SQLCodec))) >= 0 {
			switch filterArgs.Req.Method() {
			case kvpb.Scan:
				atomic.AddUint64(&scans, 1)
			case kvpb.Get:
				atomic.AddUint64(&gets, 1)
			case kvpb.EndTxn:
				if filterArgs.Hdr.Txn.Status == roachpb.STAGING {
					// Ignore async explicit commits.
					return nil
				}
				atomic.AddUint64(&endTxn, 1)
			}
		}
		return nil
	}

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
				TestingEvalFilter: filter,
			},
		}},
	})
	defer srv.Stopper().Stop(context.Background())
	codecValue.Store(srv.ApplicationLayer().Codec())
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.kv (k INT PRIMARY KEY, v INT)`)

	// This should hit the fast path.
	atomic.StoreUint64(&gets, 0)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This could hit the fast path, but doesn't right now because of #14482.
	atomic.StoreUint64(&gets, 0)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `INSERT INTO d.kv VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET v=excluded.v`)
	if s := atomic.LoadUint64(&gets); s != 1 {
		t.Errorf("expected 1 get (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected 0 scans but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should not hit the fast path because it doesn't set every column.
	atomic.StoreUint64(&gets, 0)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv (k) VALUES (1)`)
	if s := atomic.LoadUint64(&gets); s != 1 {
		t.Errorf("expected 1 get (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected 0 scans but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should hit the fast path, but won't be a 1PC because of the explicit
	// transaction.
	atomic.StoreUint64(&gets, 0)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPSERT INTO d.kv VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s := atomic.LoadUint64(&gets); s != 0 {
		t.Errorf("expected no gets (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 1 {
		t.Errorf("expected 1 end-txn (no 1PC) but got %d", s)
	}

	// This should not hit the fast path because kv has a secondary index.
	sqlDB.Exec(t, `CREATE INDEX vidx ON d.kv (v)`)
	atomic.StoreUint64(&gets, 0)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&gets); s != 1 {
		t.Errorf("expected 1 get (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected 0 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}
}

func TestConcurrentUpsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)

	testCases := []struct {
		name       string
		updateStmt string
	}{
		// Upsert case.
		{
			name:       "upsert",
			updateStmt: `UPSERT INTO d.t VALUES (1, $1)`,
		},
		// Update case.
		{
			name:       "update",
			updateStmt: `UPDATE d.t SET b = $1 WHERE a = 1`,
		},
	}

	for _, test := range testCases {
		updateStmt := test.updateStmt
		t.Run(test.name, func(t *testing.T) {
			g, ctx := errgroup.WithContext(context.Background())
			for i := 0; i < 2; i++ {
				g.Go(func() error {
					for j := 0; j < 100; j++ {
						if _, err := sqlDB.DB.ExecContext(ctx, updateStmt, j); err != nil {
							return err
						}
					}
					return nil
				})
			}
			// We select on both the primary key and the secondary
			// index to highlight the lost update anomaly, which used
			// to occur on 1PC snapshot-isolation upserts (and updates).
			// See #14099.
			if err := g.Wait(); err != nil {
				t.Errorf(`%+v
SELECT * FROM d.t@primary = %s
SELECT * FROM d.t@b_idx   = %s
`,
					err,
					sqlDB.QueryStr(t, `SELECT * FROM d.t@primary`),
					sqlDB.QueryStr(t, `SELECT * FROM d.t@b_idx`),
				)
			}
		})
	}
}

// TestConcurrentUpsertAddDropVirtualComputedColumn tests DML statements with a
// RETURNING clause running concurrently with an ALTER TABLE ADD/DROP COLUMN
// which adds or drops a virtual computed column.
func TestConcurrentUpsertAddDropVirtualComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT, b INT, PRIMARY KEY(a, b), INDEX b_idx (b))`)

	var mu syncutil.Mutex
	const numRows = 12

	testCases := []struct {
		name            string
		updateStmt      string
		checkVirtualCol bool
		returningBDelta int
		alterStmt       string
		testQuery       string
		result          []string
		colAValue       int
	}{
		// Upsert case.
		{
			name:            "upsert add",
			updateStmt:      `UPSERT INTO d.t VALUES ($1, $2) RETURNING a, b`,
			returningBDelta: 0,
			alterStmt:       `ALTER TABLE d.t ADD COLUMN b_plus_one INT AS (b+1) VIRTUAL UNIQUE`,
			testQuery:       `SELECT count(*) FROM d.t@primary WHERE b_plus_one IS NOT NULL`,
			result:          []string{fmt.Sprint(numRows)},
			colAValue:       1,
		},
		{
			name:            "upsert drop",
			updateStmt:      `UPSERT INTO d.t VALUES ($1, $2+100) RETURNING a, b`,
			returningBDelta: 100,
			alterStmt:       `ALTER TABLE d.t DROP COLUMN b_plus_one`,
			testQuery:       `SELECT a, b FROM d.t@primary ORDER BY 1 DESC, 2 LIMIT 1`,
			result:          []string{"2", "101"},
			colAValue:       2,
		},
		// Update cases.
		{
			name:            "update add",
			updateStmt:      `UPDATE d.t SET b = b-1 WHERE a = $1 AND b = $2 RETURNING a, b`,
			returningBDelta: -1,
			alterStmt:       `ALTER TABLE d.t ADD COLUMN b_plus_2 INT AS (b+2) VIRTUAL UNIQUE`,
			testQuery:       `SELECT count(*) FROM d.t@primary WHERE b_plus_2 IS NOT NULL`,
			result:          []string{fmt.Sprint(2 * numRows)},
			colAValue:       1,
		},
		{
			name:            "update drop",
			updateStmt:      `UPDATE d.t SET b = b-1 WHERE a = $1 AND b = $2-1 RETURNING a, b`,
			returningBDelta: -2,
			alterStmt:       `ALTER TABLE d.t DROP COLUMN b_plus_2`,
			testQuery:       `SELECT a, b FROM d.t@primary ORDER BY 1,2 LIMIT 1`,
			result:          []string{"1", "-1"},
			colAValue:       1,
		},
		// Delete cases.
		{
			name:            "delete add",
			updateStmt:      `DELETE FROM d.t WHERE a = $1 AND b = $2+100 RETURNING a, b`,
			returningBDelta: 100,
			alterStmt:       `ALTER TABLE d.t ADD COLUMN b_plus_one INT AS (b+1) VIRTUAL UNIQUE`,
			testQuery:       `SELECT count(*) FROM d.t@primary WHERE b_plus_one IS NOT NULL`,
			result:          []string{fmt.Sprint(numRows)},
			colAValue:       2,
		},
		{
			name:            "delete drop",
			updateStmt:      `DELETE FROM d.t WHERE a = $1 AND b = $2-2 RETURNING a, b`,
			returningBDelta: -2,
			alterStmt:       `ALTER TABLE d.t DROP COLUMN b_plus_one`,
			testQuery:       `SELECT count(*) FROM d.t@primary`,
			result:          []string{"0"},
			colAValue:       1,
		},
		// Insert cases.
		{
			name:            "insert add",
			updateStmt:      `INSERT INTO d.t VALUES ($1, $2) ON CONFLICT (a, b) DO UPDATE SET b=d.t.b+100 RETURNING a, b`,
			returningBDelta: 0,
			alterStmt:       `ALTER TABLE d.t ADD COLUMN b_plus_one INT AS (b+1) VIRTUAL UNIQUE`,
			testQuery:       `SELECT count(*) FROM d.t@primary WHERE b_plus_one IS NOT NULL`,
			result:          []string{fmt.Sprint(numRows)},
			colAValue:       2,
		},
		{
			name:            "insert drop",
			updateStmt:      `INSERT INTO d.t VALUES ($1, $2) ON CONFLICT (a, b) DO UPDATE SET b=d.t.b+100 RETURNING a, b`,
			returningBDelta: 100,
			alterStmt:       `ALTER TABLE d.t DROP COLUMN b_plus_one`,
			testQuery:       `SELECT * FROM d.t@primary ORDER BY 1, 2 LIMIT 1`,
			result:          []string{"2", "101"},
			colAValue:       2,
		},
	}

	for _, test := range testCases {
		updateStmt := test.updateStmt
		alterStmt := test.alterStmt
		colAValue := test.colAValue
		returningBDelta := test.returningBDelta
		testQuery := test.testQuery
		result := test.result
		// Serialize the tests because the next test relies on the results of
		// the previous one.
		mu.Lock()
		t.Run(test.name, func(t *testing.T) {
			defer mu.Unlock()
			g, _ := errgroup.WithContext(context.Background())
			g.Go(func() error {
				for j := 1; j <= numRows; j++ {
					rows := sqlDB.QueryStr(t, updateStmt, colAValue, j)
					expected := []string{fmt.Sprint(colAValue), fmt.Sprint(j + returningBDelta)}
					require.Equal(t, expected, rows[0])
				}
				return nil
			})
			g.Go(func() error {
				// Let the updates start before the ALTER TABLE is issued.
				time.Sleep(500 * time.Microsecond)
				if _, err := sqlDB.DB.ExecContext(ctx, alterStmt); err != nil {
					return err
				}
				return nil
			})
			if err := g.Wait(); err != nil {
				t.Errorf(`%+v
SELECT * FROM d.t@primary = %s
SELECT * FROM d.t@b_idx   = %s
`,
					err,
					sqlDB.QueryStr(t, `SELECT * FROM d.t@primary`),
					sqlDB.QueryStr(t, `SELECT * FROM d.t@b_idx`),
				)
			}
			results := sqlDB.QueryStr(t, testQuery)
			require.Equal(t, result, results[0])
		})
	}
}
