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

package bench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver to gosql
	_ "github.com/lib/pq"              // registers the pg driver to gosql

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

// Disable benchmarking with the experimental optimizer for now.
const enableCockroachOpt = false

func benchmarkCockroach(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}

	if enableCockroachOpt {
		// If benchmarking against new optimizer, disable DistSQL, so that it's
		// apples-to-apples, since the new optimizer uses local execution.
		if _, err := db.Exec(`SET DISTSQL=OFF`); err != nil {
			b.Fatal(err)
		}
	}

	f(b, db)
}

func benchmarkCockroachOpt(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	if !enableCockroachOpt {
		b.Skipf("Benchmark with experimental optimizer is disabled")
	}

	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}

	if _, err := db.Exec(`SET EXPERIMENTAL_OPT=ON`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMultinodeCockroach(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	tc := testcluster.StartTestCluster(b, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "bench",
			},
		})
	if _, err := tc.Conns[0].Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}
	defer tc.Stopper().Stop(context.TODO())

	f(b, tc.Conns[0])
}

func benchmarkPostgres(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	// Note: the following uses SSL. To run this, make sure your local
	// Postgres server has SSL enabled. To use Cockroach's checked-in
	// testing certificates for Postgres' SSL, first determine the
	// location of your Postgres server's configuration file:
	// ```
	// $ psql -h localhost -p 5432 -c 'SHOW config_file'
	//                config_file
	// -----------------------------------------
	//  /usr/local/var/postgres/postgresql.conf
	// (1 row)
	//```
	//
	// Now open this file and set the following values:
	// ```
	// $ grep ^ssl /usr/local/var/postgres/postgresql.conf
	// ssl = on # (change requires restart)
	// ssl_cert_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/node.crt' # (change requires restart)
	// ssl_key_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/node.key' # (change requires restart)
	// ssl_ca_file = '$GOPATH/src/github.com/cockroachdb/cockroach/pkg/security/securitytest/test_certs/ca.crt' # (change requires restart)
	// ```
	// Where `$GOPATH/src/github.com/cockroachdb/cockroach`
	// is replaced with your local Cockroach source directory.
	// Be sure to restart Postgres for this to take effect.

	pgURL := url.URL{
		Scheme:   "postgres",
		Host:     "localhost:5432",
		RawQuery: "sslmode=require&dbname=postgres",
	}
	if conn, err := net.Dial("tcp", pgURL.Host); err != nil {
		b.Skipf("unable to connect to postgres server on %s: %s", pgURL.Host, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

func benchmarkMySQL(b *testing.B, f func(b *testing.B, db *gosql.DB)) {
	const addr = "localhost:3306"
	if conn, err := net.Dial("tcp", addr); err != nil {
		b.Skipf("unable to connect to mysql server on %s: %s", addr, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("mysql", fmt.Sprintf("root@tcp(%s)/", addr))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	f(b, db)
}

// ForEachDB iterates the given benchmark over multiple database engines.
func ForEachDB(b *testing.B, fn func(*testing.B, *gosql.DB)) {
	for _, dbFn := range []func(*testing.B, func(*testing.B, *gosql.DB)){
		benchmarkCockroach,
		benchmarkCockroachOpt,
		benchmarkMultinodeCockroach,
		benchmarkPostgres,
		benchmarkMySQL,
	} {
		dbName := runtime.FuncForPC(reflect.ValueOf(dbFn).Pointer()).Name()
		dbName = strings.TrimPrefix(dbName, "github.com/cockroachdb/cockroach/pkg/bench.benchmark")
		b.Run(dbName, func(b *testing.B) {
			dbFn(b, fn)
		})
	}
}
