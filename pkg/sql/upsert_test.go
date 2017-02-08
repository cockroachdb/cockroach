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
//
// Author: Daniel Harrison (dan@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestUpsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This filter increments scans for every ScanRequest that hits user table
	// data.
	var scans int64
	filter := func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if filterArgs.Req.Method() == roachpb.Scan &&
			bytes.Compare(filterArgs.Req.Header().Key, keys.UserTableDataMin) >= 0 {
			scans++
		}
		return nil
	}

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storage.StoreTestingKnobs{
			TestingCommandFilter: filter,
		}},
	})
	defer s.Stopper().Stop()
	sqlDB := sqlutils.MakeSQLRunner(t, conn)
	sqlDB.Exec(`CREATE DATABASE d`)
	sqlDB.Exec(`CREATE TABLE d.kv (k INT PRIMARY KEY, v INT)`)

	// This should hit the fast path.
	scans = 0
	sqlDB.Exec(`UPSERT INTO d.kv VALUES (1, 1)`)
	if scans != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", scans)
	}

	// This should hit the fast path.
	scans = 0
	sqlDB.Exec(`INSERT INTO d.kv VALUES (1, 1) ON CONFLICT (k) DO UPDATE SET v=excluded.v`)
	if scans != 0 {
		t.Errorf("expected no scans (the upsert fast path) but got %d", scans)
	}

	// This should not hit the fast path because it doesn't set every column.
	scans = 0
	sqlDB.Exec(`UPSERT INTO d.kv (k) VALUES (1)`)
	if scans != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", scans)
	}

	// This should not hit the fast path because kv has a secondary index.
	sqlDB.Exec(`CREATE INDEX vidx ON d.kv (v)`)
	scans = 0
	sqlDB.Exec(`UPSERT INTO d.kv VALUES (1, 1)`)
	if scans != 1 {
		t.Errorf("expected 1 scans (no upsert fast path) but got %d", scans)
	}
}
