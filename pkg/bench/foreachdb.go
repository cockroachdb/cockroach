// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bench

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/errors"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver to gosql
	_ "github.com/lib/pq"              // registers the pg driver to gosql
	"github.com/stretchr/testify/require"
)

var runSepProcessTenant = flag.Bool("run-sep-process-tenant", false, "run separate process tenant benchmarks (these may freeze due to tenant limits)")

// BenchmarkFn is a function that runs a benchmark using the given SQLRunner.
type BenchmarkFn func(b *testing.B, db *sqlutils.SQLRunner)

// timerUtil is a helper method that should be called right before the
// invocation of BenchmarkFn and the returned function should be deferred.
func timerUtil(b *testing.B) func() {
	b.ResetTimer()
	b.StartTimer()
	return b.StopTimer
}

func benchmarkCockroach(b *testing.B, f BenchmarkFn) {
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{
			UseDatabase:       "bench",
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(83461),
		})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}

	defer timerUtil(b)()
	f(b, sqlutils.MakeSQLRunner(db))
}

// benchmarkSharedProcessTenantCockroach runs the benchmark against a
// shared process tenant server in a single-node cluster. The tenant
// runs in the same process as the KV host.
func benchmarkSharedProcessTenantCockroach(b *testing.B, f BenchmarkFn) {
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
	defer s.Stopper().Stop(ctx)

	// Create our own test tenant with a known name.
	tenantName := "benchtenant"
	_, tenantDB, err := s.TenantController().StartSharedProcessTenant(ctx,
		base.TestSharedProcessTenantArgs{
			TenantName:  roachpb.TenantName(tenantName),
			UseDatabase: "bench",
		})
	require.NoError(b, err)

	// Exempt the tenant from rate limiting. We expect most
	// shared-process tenants will run without rate limiting in
	// the near term.
	_, err = db.Exec(`ALTER TENANT benchtenant GRANT CAPABILITY exempt_from_rate_limiting`)
	require.NoError(b, err)

	var tenantID uint64
	require.NoError(b, db.QueryRow(`SELECT id FROM [SHOW TENANT benchtenant]`).Scan(&tenantID))

	err = testutils.SucceedsSoonError(func() error {
		capabilities, found := s.StorageLayer().TenantCapabilitiesReader().GetCapabilities(roachpb.MustMakeTenantID(tenantID))
		if !found {
			return errors.Newf("capabilities not yet ready")
		}
		if !tenantcapabilities.MustGetBoolByID(
			capabilities, tenantcapabilities.ExemptFromRateLimiting,
		) {
			return errors.Newf("capabilities not yet ready")
		}
		return nil
	})
	require.NoError(b, err)

	_, err = tenantDB.Exec(`CREATE DATABASE bench`)
	require.NoError(b, err)

	defer timerUtil(b)()
	f(b, sqlutils.MakeSQLRunner(tenantDB))
}

// benchmarkSepProcessTenantCockroach runs the benchmark against a tenant with a
// single SQL pod and a single-node KV host cluster. The tenant runs in a
// separate process from the KV host.
func benchmarkSepProcessTenantCockroach(b *testing.B, f BenchmarkFn) {
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
	defer s.Stopper().Stop(ctx)

	// Create our own test tenant with a known name.
	_, tenantDB := serverutils.StartTenant(b, s, base.TestTenantArgs{
		TenantName:  "benchtenant",
		TenantID:    roachpb.MustMakeTenantID(10),
		UseDatabase: "bench",
	})

	// The benchmarks sometime hit the default span limit, so we increase it.
	// NOTE(andrei): Benchmarks drop the tables they're creating, so I'm not sure
	// if hitting this limit is expected.
	_, err := db.Exec(`SET CLUSTER SETTING spanconfig.virtual_cluster.max_spans = 10000000`)
	require.NoError(b, err)

	_, err = tenantDB.Exec(`CREATE DATABASE bench`)
	require.NoError(b, err)

	defer timerUtil(b)()
	f(b, sqlutils.MakeSQLRunner(tenantDB))
}

func benchmarkMultinodeCockroach(b *testing.B, f BenchmarkFn) {
	tc := testcluster.StartTestCluster(b, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				UseDatabase:       "bench",
				DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(83461),
			},
		})
	if _, err := tc.Conns[0].Exec(`CREATE DATABASE bench`); err != nil {
		b.Fatal(err)
	}
	defer tc.Stopper().Stop(context.TODO())

	defer timerUtil(b)()
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
	// ```
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

	defer timerUtil(b)()
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

	defer timerUtil(b)()
	f(b, r)
}

// ForEachDB iterates the given benchmark over multiple database engines.
func ForEachDB(b *testing.B, fn BenchmarkFn) {

	dbFns := []func(*testing.B, BenchmarkFn){
		benchmarkCockroach,
		benchmarkSharedProcessTenantCockroach,
	}

	if *runSepProcessTenant {
		dbFns = append(dbFns, benchmarkSepProcessTenantCockroach)
	}

	dbFns = append(dbFns,
		benchmarkMultinodeCockroach,
		benchmarkPostgres,
		benchmarkMySQL,
	)

	for _, dbFn := range dbFns {
		dbName := runtime.FuncForPC(reflect.ValueOf(dbFn).Pointer()).Name()
		dbName = strings.TrimPrefix(dbName, "github.com/cockroachdb/cockroach/pkg/bench.benchmark")
		b.Run(dbName, func(b *testing.B) {
			dbFn(b, fn)
		})
	}
}
