// Copyright 2016 The Cockroach Authors.
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
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
)

func TestErrorCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	count1 := telemetry.GetRawFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	_, err := db.Query("SELECT 1+")
	if err == nil {
		t.Fatal("expected error, got no error")
	}

	count2 := telemetry.GetRawFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	if count2-count1 != 1 {
		t.Fatalf("expected 1 syntax error, got %d", count2-count1)
	}

	rows, err := db.Query(`SHOW SYNTAX 'SELECT 1+'`)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	for rows.Next() {
		// Do nothing. SHOW SYNTAX itself is tested elsewhere.
		// We just check the counts below.
	}
	rows.Close()

	count3 := telemetry.GetRawFeatureCounts()["errorcodes."+pgerror.CodeSyntaxError]

	if count3-count2 != 1 {
		t.Fatalf("expected 1 syntax error, got %d", count3-count2)
	}
}

func TestUnimplementedCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec("CREATE TABLE t(x INT8)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("ALTER TABLE t ALTER COLUMN x SET DATA TYPE STRING USING x::STRING"); err == nil {
		t.Fatal("expected error, got no error")
	}

	if telemetry.GetRawFeatureCounts()["unimplemented.#9851.INT8->STRING"] == 0 {
		t.Fatal("expected unimplemented telemetry, got nothing")
	}
}

func TestTransactionRetryErrorCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	// Transaction retry errors aren't given a pg error code until deep
	// in pgwire (pgwire.convertToErrWithPGCode). Make sure we're
	// reporting errors at a level that allows this code to be recorded.

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec("CREATE TABLE accounts (id INT8 PRIMARY KEY, balance INT8)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO accounts VALUES (1, 100)"); err != nil {
		t.Fatal(err)
	}

	txn1, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	txn2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	for _, txn := range []*gosql.Tx{txn1, txn2} {
		rows, err := txn.Query("SELECT * FROM accounts WHERE id = 1")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		rows.Close()
	}

	for _, txn := range []*gosql.Tx{txn1, txn2} {
		if _, err := txn.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil && err.(*pq.Error).Code != "40001" {
			t.Fatal(err)
		}
	}

	if telemetry.GetRawFeatureCounts()["errorcodes.40001"] == 0 {
		t.Fatal("expected error code telemetry, got nothing")
	}
}
