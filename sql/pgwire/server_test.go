// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package pgwire

import (
	"bytes"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	_ "github.com/lib/pq"
)

type pgTestServer struct {
	testServer *server.TestServer
	pgServer   *Server
	db         *sql.DB
}

func newPGTestServer(t *testing.T) *pgTestServer {
	ts := server.StartTestServer(t)
	pgServer := NewServer(&Context{
		Context:  &ts.Ctx.Context,
		Executor: ts.SQLExecutor(),
		Stopper:  ts.Stopper(),
	})
	if err := pgServer.Start(util.CreateTestAddr("tcp")); err != nil {
		t.Fatal(err)
	}
	s := &pgTestServer{
		testServer: ts,
		pgServer:   pgServer,
	}
	db, err := sql.Open("postgres", s.datasource())
	if err != nil {
		t.Fatal(err)
	}
	s.db = db
	return s
}

func (s *pgTestServer) stop() {
	if r := recover(); r != nil {
		return
	}
	s.db.Close()
	s.testServer.Stop()
}

func (s *pgTestServer) datasource() string {
	host, port, err := net.SplitHostPort(s.pgServer.Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("sslmode=disable host=%s port=%s", host, port)
}

func TestSimpleQuery(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := newPGTestServer(t)
	defer s.stop()

	rows, err := s.db.Query("SELECT true, 42, 3.14, 'data'::blob, 'hello', '2015-09-12 15:30:00'::timestamp")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var b bool
		var i int
		var f float64
		var d []byte
		var s string
		var tm time.Time
		if err := rows.Scan(&b, &i, &f, &d, &s, &tm); err != nil {
			t.Fatal(err)
		}
		if !b {
			t.Errorf("expected true but got false")
		}
		if i != 42 {
			t.Errorf("expected 42 but got %d", i)
		}
		if f != 3.14 {
			t.Errorf("expected.3.14 but got %f", f)
		}
		if bytes.Compare(d, []byte("data")) != 0 {
			t.Errorf("expected 'data' but got %s", d)
		}
		if s != "hello" {
			t.Errorf("expected 'hello' but got %s", s)
		}
		if a, e := tm.Format(time.RFC3339), "2015-09-12T15:30:00Z"; a != e {
			t.Errorf("expected %s but got %s", a, e)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestSimpleExec(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := newPGTestServer(t)
	defer s.stop()

	if _, err := s.db.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE test.foo (a INT)"); !testutils.IsError(
		err, "table must contain a primary key") {
		t.Fatalf("expected 'must contain a primary key' error; got %s", err)
	}
	if _, err := s.db.Exec("CREATE TABLE test.foo (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
}
