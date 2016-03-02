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
//
// Author: David Taylor (david@cockroachlabs.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/tracing"
)

func SetupMultinodeTestCluster(t testing.TB, nodes int, name string) ([]*sql.DB, func()) {
	var servers []*server.TestServer
	first := server.StartTestServer(t)
	servers = append(servers, first)
	for i := 1; i < nodes; i++ {
		servers = append(servers, server.StartTestServerJoining(t, first))
	}

	var conns []*sql.DB
	var closes []func() error
	var cleanups []func()

	for i, s := range servers {
		pgURL, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, fmt.Sprintf("node%d", i))
		pgURL.Path = name
		db, err := sql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		closes = append(closes, db.Close)
		cleanups = append(cleanups, cleanupFn)
		conns = append(conns, db)
	}

	if _, err := conns[0].Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}

	f := func() {
		for _, fn := range closes {
			_ = fn()
		}
		for _, s := range servers {
			s.Stop()
		}
		for _, fn := range cleanups {
			fn()
		}
	}

	return conns, f
}

func TestMultinodeCockroack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer tracing.Disable()()

	conns, cleanup := SetupMultinodeTestCluster(t, 3, "Testing")
	defer cleanup()

	if _, err := conns[0].Exec(`CREATE TABLE testing (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	if _, err := conns[0].Exec(`INSERT INTO testing VALUES (5, 1), (4, 2), (1, 2)`); err != nil {
		t.Fatal(err)
	}

	if r, err := conns[1].Query(`SELECT * FROM testing WHERE k = 5`); err != nil {
		t.Fatal(err)
	} else if !r.Next() {
		t.Fatal("no rows")
	}

	if r, err := conns[2].Exec(`DELETE FROM testing`); err != nil {
		t.Fatal(err)
	} else if rows, err := r.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if expected, actual := int64(3), rows; expected != actual {
		t.Fatalf("wrong row count deleted: expected %d actual %d", expected, actual)
	}
}
