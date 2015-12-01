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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func trivialQuery(datasource string) error {
	db, err := sql.Open("postgres", datasource)
	if err != nil {
		return err
	}
	defer db.Close()
	{
		_, err := db.Exec("SELECT 1")
		return err
	}
}

func TestPGWire(t *testing.T) {
	defer leaktest.AfterTest(t)

	for _, insecure := range [...]bool{true, false} {
		ctx := server.NewTestContext()
		ctx.Insecure = insecure
		s := setupTestServerWithContext(t, ctx)

		host, port, err := net.SplitHostPort(s.PGAddr())
		if err != nil {
			t.Fatal(err)
		}

		if err := trivialQuery(fmt.Sprintf("host=%s port=%s", host, port)); err != nil {
			if !(insecure && err == pq.ErrSSLNotSupported) {
				t.Fatal(err)
			}
		}

		{
			err := trivialQuery(fmt.Sprintf("sslmode=disable host=%s port=%s", host, port))
			if insecure {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, pgwire.ErrSSLRequired) {
					t.Fatal(err)
				}
			}
		}

		{
			err := trivialQuery(fmt.Sprintf("sslmode=require host=%s port=%s", host, port))
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		cleanupTestServer(s)
	}
}
