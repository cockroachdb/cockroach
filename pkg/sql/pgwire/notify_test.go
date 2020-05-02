// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestListenNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE DATABASE testing`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE testing.f (v INT)`); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare(`SELECT * FROM testing.f`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE testing.f ADD COLUMN u int`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO testing.f VALUES (1, 2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(); !testutils.IsError(err, "must not change result type") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}

	// Test that an INSERT RETURNING will not commit data.
	stmt, err = db.Prepare(`INSERT INTO testing.f VALUES ($1, $2) RETURNING *`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE testing.f ADD COLUMN t int`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM testing.f`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.Exec(3, 4); !testutils.IsError(err, "must not change result type") {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	var countAfter int
	if err := db.QueryRow(`SELECT count(*) FROM testing.f`).Scan(&countAfter); err != nil {
		t.Fatal(err)
	}
	if count != countAfter {
		t.Fatalf("expected %d rows, got %d", count, countAfter)
	}
}
