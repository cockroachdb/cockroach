// Copyright 2018 The Cockroach Authors.
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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	ie := s.InternalExecutor().(*sql.InternalExecutor)
	row, err := ie.QueryRow(ctx, "test", nil /* txn */, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 1 {
		t.Fatalf("expected 1 col, got: %d", len(row))
	}
	r, ok := row[0].(*tree.DInt)
	if !ok || *r != 1 {
		t.Fatalf("expected a DInt == 1, got: %T:%s", r, r)
	}

	// Test that auto-retries work.
	if _, err := db.Exec("create database test; create sequence test.seq start with 1"); err != nil {
		t.Fatal(err)
	}
	// The following statement will succeed on the 2nd try.
	row, err = ie.QueryRow(
		ctx, "test", nil, /* txn */
		"select case nextval('test.seq') when 1 then crdb_internal.force_retry('1h') else 99 end",
	)
	if err != nil {
		t.Fatal(err)
	}
	r, ok = row[0].(*tree.DInt)
	if !ok || *r != 99 {
		t.Fatalf("expected a DInt == 99, got: %T:%s", r, r)
	}

	// Reset the sequence to a clear value. Next nextval() will return 2.
	if _, err := db.Exec("SELECT setval('test.seq', 1)"); err != nil {
		t.Fatal(err)
	}

	// Test the auto-retries work inside an external transaction too. In this
	// case, the executor cannot retry internally.
	cnt := 0
	err = s.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		cnt++
		row, err = ie.QueryRow(
			ctx, "test", txn,
			"select case nextval('test.seq') when 2 then crdb_internal.force_retry('1h') else 99 end",
		)
		if err != nil {
			return err
		}
		r, ok = row[0].(*tree.DInt)
		if !ok || *r != 99 {
			t.Fatalf("expected a DInt == 99, got: %T:%s", r, r)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("expected 2 iterations, got: %d", cnt)
	}
}

func TestSessionBoundInternalExecutor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := db.Exec("create database foo"); err != nil {
		t.Fatal(err)
	}

	expDB := "foo"
	ie := sql.MakeSessionBoundInternalExecutor(
		ctx,
		&sessiondata.SessionData{
			Database:      expDB,
			SequenceState: &sessiondata.SequenceState{},
		},
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)

	row, err := ie.QueryRow(ctx, "test", nil /* txn */, "show database")
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 1 {
		t.Fatalf("expected 1 col, got: %d", len(row))
	}
	r, ok := row[0].(*tree.DString)
	if !ok || string(*r) != expDB {
		t.Fatalf("expected a DString == %s, got: %T: %s", expDB, r, r)
	}
}

// TODO(andrei): Test that descriptor leases are released by the
// InternalExecutor, with and without a higher-level txn. When there is no
// higher-level txn, the leases are released normally by the txn finishing. When
// there is, they are released by the resetExtraTxnState() call in the
// InternalExecutor. Unfortunately at the moment we don't have a great way to
// test lease releases.
