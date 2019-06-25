// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/sync/errgroup"
)

func TestUpsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This filter increments scans and endTxn for every ScanRequest and
	// EndTransactionRequest that hits user table data.
	var scans uint64
	var endTxn uint64
	filter := func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if bytes.Compare(filterArgs.Req.Header().Key, keys.UserTableDataMin) >= 0 {
			switch filterArgs.Req.Method() {
			case roachpb.Scan:
				atomic.AddUint64(&scans, 1)
			case roachpb.EndTransaction:
				if filterArgs.Hdr.Txn.Status == roachpb.STAGING {
					// Ignore async explicit commits.
					return nil
				}
				atomic.AddUint64(&endTxn, 1)
			}
		}
		return nil
	}

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storage.StoreTestingKnobs{
			EvalKnobs: storagebase.BatchEvalTestingKnobs{
				TestingEvalFilter: filter,
			},
		}},
	})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.kv (k INT PRIMARY KEY, v INT)`)

	// This should hit the fast path.
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
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `INSERT INTO d.kv VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET v=excluded.v`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should not hit the fast path because it doesn't set every column.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv (k) VALUES (1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}

	// This should hit the fast path, but won't be a 1PC because of the explicit
	// transaction.
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
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 1 {
		t.Errorf("expected 1 end-txn (no 1PC) but got %d", s)
	}

	// This should not hit the fast path because kv has a secondary index.
	sqlDB.Exec(t, `CREATE INDEX vidx ON d.kv (v)`)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&endTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&endTxn); s != 0 {
		t.Errorf("expected no end-txn (1PC) but got %d", s)
	}
}

func TestConcurrentUpsert(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
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
		t.Run(test.name, func(t *testing.T) {
			g, ctx := errgroup.WithContext(context.Background())
			for i := 0; i < 2; i++ {
				g.Go(func() error {
					for j := 0; j < 100; j++ {
						if _, err := sqlDB.DB.ExecContext(ctx, test.updateStmt, j); err != nil {
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
