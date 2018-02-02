// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUpsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This filter increments scans and beginTxn for every ScanRequest and
	// BeginTransactionRequest that hits user table data.
	var scans uint64
	var beginTxn uint64
	filter := func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if bytes.Compare(filterArgs.Req.Header().Key, keys.UserTableDataMin) >= 0 {
			switch filterArgs.Req.Method() {
			case roachpb.Scan:
				atomic.AddUint64(&scans, 1)
			case roachpb.BeginTransaction:
				atomic.AddUint64(&beginTxn, 1)
			}
		}
		return nil
	}

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storage.StoreTestingKnobs{
			EvalKnobs: batcheval.TestingKnobs{
				TestingEvalFilter: filter,
			},
		}},
	})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.public.kv (k INT PRIMARY KEY, v INT)`)

	// This should hit the fast path.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&beginTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.public.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&beginTxn); s != 0 {
		t.Errorf("expected no begin-txn (1PC) but got %d", s)
	}

	// This could hit the fast path, but doesn't right now because of #14482.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&beginTxn, 0)
	sqlDB.Exec(t, `INSERT INTO d.public.kv VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET v=excluded.v`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&beginTxn); s != 0 {
		t.Errorf("expected no begin-txn (1PC) but got %d", s)
	}

	// This should not hit the fast path because it doesn't set every column.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&beginTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.public.kv (k) VALUES (1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&beginTxn); s != 0 {
		t.Errorf("expected no begin-txn (1PC) but got %d", s)
	}

	// This should hit the fast path, but won't be a 1PC because of the explicit
	// transaction.
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&beginTxn, 0)
	tx, err := sqlDB.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPSERT INTO d.public.kv VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s := atomic.LoadUint64(&scans); s != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&beginTxn); s != 1 {
		t.Errorf("expected 1 begin-txn (no 1PC) but got %d", s)
	}

	// This should not hit the fast path because kv has a secondary index.
	sqlDB.Exec(t, `CREATE INDEX vidx ON d.public.kv (v)`)
	atomic.StoreUint64(&scans, 0)
	atomic.StoreUint64(&beginTxn, 0)
	sqlDB.Exec(t, `UPSERT INTO d.public.kv VALUES (1, 1)`)
	if s := atomic.LoadUint64(&scans); s != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", s)
	}
	if s := atomic.LoadUint64(&beginTxn); s != 0 {
		t.Errorf("expected no begin-txn (1PC) but got %d", s)
	}
}

func TestConcurrentUpsertWithSnapshotIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.public.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)
	sqlDB.Exec(t, `SET DEFAULT_TRANSACTION_ISOLATION TO SNAPSHOT`)

	testCases := []struct {
		name       string
		updateStmt string
	}{
		// Upsert case.
		{
			name:       "upsert",
			updateStmt: `UPSERT INTO d.public.t VALUES (1, $1)`,
		},
		// Update case.
		{
			name:       "update",
			updateStmt: `UPDATE d.public.t SET b = $1 WHERE a = 1`,
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
SELECT * FROM d.public.t@primary = %s
SELECT * FROM d.public.t@b_idx   = %s
`,
					err,
					sqlDB.QueryStr(t, `SELECT * FROM d.public.t@primary`),
					sqlDB.QueryStr(t, `SELECT * FROM d.public.t@b_idx`),
				)
			}
		})
	}
}
