// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver to gosql
	_ "github.com/lib/pq"              // registers the pg driver to gosql
)

// BenchmarkFn is a function that runs a benchmark using the given SQLRunner.
type BenchmarkFn func(b *testing.B, db *sqlutils.SQLRunner)

func benchmarkCockroach(b *testing.B, f BenchmarkFn) {
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}

	f(b, sqlutils.MakeSQLRunner(db))
}

func benchmarkMultinodeCockroach(b *testing.B, f BenchmarkFn) {
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

	f(b, sqlutils.MakeRoundRobinSQLRunner(tc.Conns[0], tc.Conns[1], tc.Conns[2]))
}

func benchmarkPostgres(b *testing.B, f BenchmarkFn) {
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
		skip.IgnoreLintf(b, "unable to connect to postgres server on %s: %s", pgURL.Host, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(b, `CREATE SCHEMA IF NOT EXISTS bench`)

	f(b, r)
}

func benchmarkMySQL(b *testing.B, f BenchmarkFn) {
	const addr = "localhost:3306"
	if conn, err := net.Dial("tcp", addr); err != nil {
		skip.IgnoreLintf(b, "unable to connect to mysql server on %s: %s", addr, err)
	} else {
		conn.Close()
	}

	db, err := gosql.Open("mysql", fmt.Sprintf("root@tcp(%s)/", addr))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(b, `CREATE DATABASE IF NOT EXISTS bench`)

	f(b, r)
}

// ForEachDB iterates the given benchmark over multiple database engines.
func ForEachDB(b *testing.B, fn BenchmarkFn) {
	for _, dbFn := range []func(*testing.B, BenchmarkFn){
		benchmarkCockroach,
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
