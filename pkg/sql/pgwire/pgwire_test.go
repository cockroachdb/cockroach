// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire_test

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func wrongArgCountString(want, got int) string {
	return fmt.Sprintf("sql: expected %d arguments, got %d", want, got)
}

func trivialQuery(pgURL url.URL) error {
	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		return err
	}
	defer db.Close()
	{
		_, err := db.Exec("SELECT 1")
		return err
	}
}

// TestPGWireDrainClient makes sure that in draining mode, the server refuses
// new connections and allows sessions with ongoing transactions to finish.
func TestPGWireDrainClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params := base.TestServerArgs{Insecure: true}
	s, _, _ := serverutils.StartServer(t, params)

	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	host, port, err := net.SplitHostPort(s.ServingSQLAddr())
	if err != nil {
		t.Fatal(err)
	}

	pgBaseURL := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		User:     url.User(security.RootUser),
		RawQuery: "sslmode=disable",
	}

	db, err := gosql.Open("postgres", pgBaseURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Draining runs in a separate goroutine since it won't return until the
	// connection with an ongoing transaction finishes.
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- func() error {
			return s.(*server.TestServer).DrainClients(ctx)
		}()
	}()

	// Ensure server is in draining mode and rejects new connections.
	testutils.SucceedsSoon(t, func() error {
		if err := trivialQuery(pgBaseURL); !testutils.IsError(err, pgwire.ErrDrainingNewConn) {
			return errors.Errorf("unexpected error: %v", err)
		}
		return nil
	})

	if _, err := txn.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	for err := range errChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	if !s.(*server.TestServer).PGServer().IsDraining() {
		t.Fatal("server should be draining, but is not")
	}
}

// TestPGWireDrainOngoingTxns tests that connections with open transactions are
// canceled when they go on for too long.
func TestPGWireDrainOngoingTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params := base.TestServerArgs{Insecure: true}
	s, _, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	host, port, err := net.SplitHostPort(s.ServingSQLAddr())
	if err != nil {
		t.Fatal(err)
	}

	pgBaseURL := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		User:     url.User(security.RootUser),
		RawQuery: "sslmode=disable",
	}

	db, err := gosql.Open("postgres", pgBaseURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pgServer := s.(*server.TestServer).PGServer()

	// Make sure that the server reports correctly the case in which a
	// connection did not respond to cancellation in time.
	t.Run("CancelResponseFailure", func(t *testing.T) {
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		// Overwrite the pgServer's cancel map to avoid race conditions in
		// which the connection is canceled and closes itself before the
		// pgServer stops waiting for connections to respond to cancellation.
		realCancels := pgServer.OverwriteCancelMap()

		// Set draining with no queryWait or cancelWait timeout. The expected
		// behavior is that the ongoing session is immediately canceled but
		// since we overwrote the context.CancelFunc, this cancellation will
		// not have any effect. The pgServer will not bother to wait for the
		// connection to close properly and should notify the caller that a
		// session did not respond to cancellation.
		if err := pgServer.DrainImpl(
			ctx, 0 /* queryWait */, 0, /* cancelWait */
		); !testutils.IsError(err, "some sessions did not respond to cancellation") {
			t.Fatalf("unexpected error: %v", err)
		}

		// Actually cancel the connection.
		for _, cancel := range realCancels {
			cancel()
		}

		// Make sure that the connection was disrupted. A retry loop is needed
		// because we must wait (since we told the pgServer not to) until the
		// connection registers the cancellation and closes itself.
		testutils.SucceedsSoon(t, func() error {
			if _, err := txn.Exec("SELECT 1"); !errors.Is(err, driver.ErrBadConn) {
				return errors.Errorf("unexpected error: %v", err)
			}
			return nil
		})

		if err := txn.Commit(); !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("unexpected error: %v", err)
		}

		pgServer.Undrain()
	})

	// Make sure that a connection gets canceled and correctly responds to this
	// cancellation by closing itself.
	t.Run("CancelResponseSuccess", func(t *testing.T) {
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		// Set draining with no queryWait timeout and a 2s cancelWait timeout.
		// The expected behavior is for the pgServer to immediately cancel any
		// ongoing sessions and wait for 2s for the cancellation to take effect.
		if err := pgServer.DrainImpl(
			ctx, 0 /* queryWait */, 2*time.Second, /* cancelWait */
		); err != nil {
			t.Fatal(err)
		}

		if err := txn.Commit(); err == nil ||
			(!errors.Is(err, driver.ErrBadConn) &&
				!strings.Contains(err.Error(), "connection reset by peer")) {
			t.Fatalf("unexpected error: %v", err)
		}

		pgServer.Undrain()
	})
}

// We want to ensure that despite use of errors.{Wrap,Wrapf}, we are surfacing a
// pq.Error.
func TestPGUnwrapError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// This is just a statement that is known to utilize errors.Wrap.
	stmt := "SELECT COALESCE(2, 'foo')"

	if _, err := db.Exec(stmt); err == nil {
		t.Fatalf("expected %s to error", stmt)
	} else {
		if !errors.HasType(err, (*pq.Error)(nil)) {
			t.Fatalf("pgwire should be surfacing a pq.Error")
		}
	}
}

func TestPGPrepareFail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testFailures := map[string]string{
		"SELECT $1 = $1":                            "pq: could not determine data type of placeholder $1",
		"SELECT $1":                                 "pq: could not determine data type of placeholder $1",
		"SELECT $1 + $1":                            "pq: could not determine data type of placeholder $1",
		"SELECT CASE WHEN TRUE THEN $1 END":         "pq: could not determine data type of placeholder $1",
		"SELECT CASE WHEN TRUE THEN $1 ELSE $2 END": "pq: could not determine data type of placeholder $1",
		"SELECT $1 > 0 AND NOT $1":                  "pq: placeholder $1 already has type int, cannot assign bool",
		"CREATE TABLE $1 (id INT)":                  "pq: at or near \"1\": syntax error",
		"UPDATE d.t SET s = i + $1":                 "pq: unsupported binary operator: <int> + <anyelement> (desired <string>)",
		"SELECT $0 > 0":                             "pq: lexical error: placeholder index must be between 1 and 65536",
		"SELECT $2 > 0":                             "pq: could not determine data type of placeholder $1",
		"SELECT 3 + CASE (4) WHEN 4 THEN $1 END":    "pq: could not determine data type of placeholder $1",
		"SELECT ($1 + $1) + current_date()":         "pq: could not determine data type of placeholder $1",
		"SELECT $1 + $2, $2::FLOAT":                 "pq: could not determine data type of placeholder $1",
		"SELECT $1[2]":                              "pq: could not determine data type of placeholder $1",
		"SELECT ($1 + 2) + ($1 + 2.5::FLOAT)":       "pq: unsupported binary operator: <int> + <float>",
	}

	if _, err := db.Exec(`CREATE DATABASE d; CREATE TABLE d.t (i INT, s STRING, d INT)`); err != nil {
		t.Fatal(err)
	}

	for query, reason := range testFailures {
		if stmt, err := db.Prepare(query); err == nil {
			t.Errorf("expected error: %s", query)
			if err := stmt.Close(); err != nil {
				t.Fatal(err)
			}
		} else if err.Error() != reason {
			t.Errorf(`%s: got: %q, expected: %q`, query, err, reason)
		}
	}
}

// Run a Prepare referencing a table created or dropped in the same
// transaction.
func TestPGPrepareWithCreateDropInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	{
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Exec(`
	CREATE DATABASE d;
	CREATE TABLE d.kv (k VARCHAR PRIMARY KEY, v VARCHAR);
`); err != nil {
			t.Fatal(err)
		}

		stmt, err := tx.Prepare(`INSERT INTO d.kv (k,v) VALUES ($1, $2);`)
		if err != nil {
			t.Fatal(err)
		}

		res, err := stmt.Exec('a', 'b')
		if err != nil {
			t.Fatal(err)
		}
		stmt.Close()
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		if affected != 1 {
			t.Fatalf("unexpected number of rows affected: %d", affected)
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	{
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Exec(`
	DROP TABLE d.kv;
`); err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Prepare(`
INSERT INTO d.kv (k,v) VALUES ($1, $2);
`); !testutils.IsError(err, "relation \"d.kv\" does not exist") {
			t.Fatalf("err = %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

type preparedQueryTest struct {
	qargs   []interface{}
	results [][]interface{}
	others  int
	error   string
	// preparedError determines the error to expect upon stmt.Query()
	// (executing a prepared statement), as opposed to db.Query()
	// (direct query without prepare). If left empty, error above is
	// used for both.
	preparedError string
}

func (p preparedQueryTest) SetArgs(v ...interface{}) preparedQueryTest {
	p.qargs = v
	return p
}

func (p preparedQueryTest) Results(v ...interface{}) preparedQueryTest {
	p.results = append(p.results, v)
	return p
}

func (p preparedQueryTest) Others(o int) preparedQueryTest {
	p.others = o
	return p
}

func (p preparedQueryTest) Error(err string) preparedQueryTest {
	p.error = err
	return p
}

func (p preparedQueryTest) PreparedError(err string) preparedQueryTest {
	p.preparedError = err
	return p
}

func TestPGPreparedQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var baseTest preparedQueryTest

	queryTests := []struct {
		sql   string
		ptest []preparedQueryTest
	}{
		{"SELECT $1 > 0", []preparedQueryTest{
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs("1").Results(true),
			baseTest.SetArgs(1.1).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "1.1": invalid syntax`).Results(true),
			baseTest.SetArgs("1.0").Error(`pq: error in argument for $1: strconv.ParseInt: parsing "1.0": invalid syntax`),
			baseTest.SetArgs(true).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "true": invalid syntax`),
		}},
		{"SELECT ($1) > 0", []preparedQueryTest{
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs(-1).Results(false),
		}},
		{"SELECT ((($1))) > 0", []preparedQueryTest{
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs(-1).Results(false),
		}},
		{"SELECT TRUE AND $1", []preparedQueryTest{
			baseTest.SetArgs(true).Results(true),
			baseTest.SetArgs(false).Results(false),
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs("").Error(`pq: error in argument for $1: strconv.ParseBool: parsing "": invalid syntax`),
			// Make sure we can run another after a failure.
			baseTest.SetArgs(true).Results(true),
		}},
		{"SELECT $1::bool", []preparedQueryTest{
			baseTest.SetArgs(true).Results(true),
			baseTest.SetArgs("true").Results(true),
			baseTest.SetArgs("false").Results(false),
			baseTest.SetArgs("1").Results(true),
			baseTest.SetArgs(2).Error(`pq: error in argument for $1: strconv.ParseBool: parsing "2": invalid syntax`),
			baseTest.SetArgs(3.1).Error(`pq: error in argument for $1: strconv.ParseBool: parsing "3.1": invalid syntax`),
			baseTest.SetArgs("").Error(`pq: error in argument for $1: strconv.ParseBool: parsing "": invalid syntax`),
		}},
		{"SELECT CASE 40+2 WHEN 42 THEN 51 ELSE $1::INT END", []preparedQueryTest{
			baseTest.Error(
				"pq: no value provided for placeholder: $1",
			).PreparedError(
				wrongArgCountString(1, 0),
			),
		}},
		{"SELECT $1::int > $2::float", []preparedQueryTest{
			baseTest.SetArgs(2, 1).Results(true),
			baseTest.SetArgs("2", 1).Results(true),
			baseTest.SetArgs(1, "2").Results(false),
			baseTest.SetArgs("2", "1.0").Results(true),
			baseTest.SetArgs("2.0", "1").Error(`pq: error in argument for $1: strconv.ParseInt: parsing "2.0": invalid syntax`),
			baseTest.SetArgs(2.1, 1).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "2.1": invalid syntax`),
		}},
		{"SELECT greatest($1, 0, $2), $2", []preparedQueryTest{
			baseTest.SetArgs(1, -1).Results(1, -1),
			baseTest.SetArgs(-1, 10).Results(10, 10),
			baseTest.SetArgs("-2", "-1").Results(0, -1),
			baseTest.SetArgs(1, 2.1).Error(`pq: error in argument for $2: strconv.ParseInt: parsing "2.1": invalid syntax`),
		}},
		{"SELECT $1::int, $1::float", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1, 1.0),
			baseTest.SetArgs("1").Results(1, 1.0),
		}},
		{"SELECT 3 + $1, $1 + $2", []preparedQueryTest{
			baseTest.SetArgs("1", "2").Results(4, 3),
			baseTest.SetArgs(3, "4").Results(6, 7),
			baseTest.SetArgs(0, "a").Error(`pq: error in argument for $2: strconv.ParseInt: parsing "a": invalid syntax`),
		}},
		// Check for name resolution.
		{"SELECT count(*)", []preparedQueryTest{
			baseTest.Results(1),
		}},
		{"SELECT CASE WHEN $1 THEN 1-$3 WHEN $2 THEN 1+$3 END", []preparedQueryTest{
			baseTest.SetArgs(true, false, 2).Results(-1),
			baseTest.SetArgs(false, true, 3).Results(4),
			baseTest.SetArgs(false, false, 2).Results(gosql.NullBool{}),
		}},
		{"SELECT CASE 1 WHEN $1 THEN $2 ELSE 2 END", []preparedQueryTest{
			baseTest.SetArgs(1, 3).Results(3),
			baseTest.SetArgs(2, 3).Results(2),
			baseTest.SetArgs(true, 0).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "true": invalid syntax`),
		}},
		{"SELECT $1[2] LIKE 'b'", []preparedQueryTest{
			baseTest.SetArgs(pq.Array([]string{"a", "b", "c"})).Results(true),
			baseTest.SetArgs(pq.Array([]gosql.NullString{{String: "a", Valid: true}, {Valid: false}, {String: "c", Valid: true}})).Results(gosql.NullBool{Valid: false}),
		}},
		{"SET application_name = $1", []preparedQueryTest{
			baseTest.SetArgs("hello world"),
		}},
		{"SET CLUSTER SETTING cluster.organization = $1", []preparedQueryTest{
			baseTest.SetArgs("hello world"),
		}},
		{"SHOW DATABASE", []preparedQueryTest{
			baseTest.Results("defaultdb"),
		}},
		{"SHOW COLUMNS FROM system.users", []preparedQueryTest{
			baseTest.
				Results("username", "STRING", false, gosql.NullBool{}, "", "{primary}", false).
				Results("hashedPassword", "BYTES", true, gosql.NullBool{}, "", "{primary}", false).
				Results("isRole", "BOOL", false, false, "", "{primary}", false),
		}},
		{"SELECT database_name, owner FROM [SHOW DATABASES]", []preparedQueryTest{
			baseTest.Results("d", security.RootUser).
				Results("defaultdb", security.RootUser).
				Results("postgres", security.RootUser).
				Results("system", security.NodeUser),
		}},
		{"SHOW GRANTS ON system.users", []preparedQueryTest{
			baseTest.Results("system", "public", "users", security.AdminRole, "DELETE").
				Results("system", "public", "users", security.AdminRole, "GRANT").
				Results("system", "public", "users", security.AdminRole, "INSERT").
				Results("system", "public", "users", security.AdminRole, "SELECT").
				Results("system", "public", "users", security.AdminRole, "UPDATE").
				Results("system", "public", "users", security.RootUser, "DELETE").
				Results("system", "public", "users", security.RootUser, "GRANT").
				Results("system", "public", "users", security.RootUser, "INSERT").
				Results("system", "public", "users", security.RootUser, "SELECT").
				Results("system", "public", "users", security.RootUser, "UPDATE"),
		}},
		{"SHOW INDEXES FROM system.users", []preparedQueryTest{
			baseTest.Results("users", "primary", false, 1, "username", "ASC", false, false).
				Results("users", "primary", false, 2, "hashedPassword", "N/A", true, false).
				Results("users", "primary", false, 3, "isRole", "N/A", true, false),
		}},
		{"SHOW TABLES FROM system", []preparedQueryTest{
			baseTest.Results("public", "comments", "table", gosql.NullString{}, 0, gosql.NullString{}).Others(31),
		}},
		{"SHOW SCHEMAS FROM system", []preparedQueryTest{
			baseTest.Results("crdb_internal", gosql.NullString{}).Others(4),
		}},
		{"SHOW CONSTRAINTS FROM system.users", []preparedQueryTest{
			baseTest.Results("users", "primary", "PRIMARY KEY", "PRIMARY KEY (username ASC)", true),
		}},
		{"SHOW TIME ZONE", []preparedQueryTest{
			baseTest.Results("UTC"),
		}},
		{"CREATE USER IF NOT EXISTS $1 WITH PASSWORD $2", []preparedQueryTest{
			baseTest.SetArgs("abc", "def"),
			baseTest.SetArgs("woo", "waa"),
		}},
		{"ALTER USER IF EXISTS $1 WITH PASSWORD $2", []preparedQueryTest{
			baseTest.SetArgs("abc", "def"),
			baseTest.SetArgs("woo", "waa"),
		}},
		{"SHOW USERS", []preparedQueryTest{
			baseTest.Results("abc", "", "{}").
				Results("admin", "", "{}").
				Results("root", "", "{admin}").
				Results("woo", "", "{}"),
		}},
		{"DROP USER $1", []preparedQueryTest{
			baseTest.SetArgs("abc"),
			baseTest.SetArgs("woo"),
		}},
		{"SELECT (SELECT 1+$1)", []preparedQueryTest{
			baseTest.SetArgs(1).Results(2),
		}},
		{"SELECT CASE WHEN $1 THEN $2 ELSE 3 END", []preparedQueryTest{
			baseTest.SetArgs(true, 2).Results(2),
			baseTest.SetArgs(false, 2).Results(3),
		}},
		{"SELECT CASE WHEN TRUE THEN 1 ELSE $1 END", []preparedQueryTest{
			baseTest.SetArgs(2).Results(1),
		}},
		{"SELECT CASE $1 WHEN 1 THEN 1 END", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(2).Results(gosql.NullInt64{}),
		}},
		{"SELECT $1::timestamp, $2::date", []preparedQueryTest{
			baseTest.SetArgs("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		}},
		{"SELECT $1::date, $2::timestamp", []preparedQueryTest{
			baseTest.SetArgs(
				time.Date(2006, 7, 8, 0, 0, 0, 9, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6000, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
				time.Date(2001, 1, 2, 3, 4, 5, 6000, time.FixedZone("", 0)),
			),
		}},
		{"INSERT INTO d.ts VALUES($1, $2) RETURNING *", []preparedQueryTest{
			baseTest.SetArgs("2001-01-02 03:04:05", "2006-07-08").Results(
				time.Date(2001, 1, 2, 3, 4, 5, 0, time.FixedZone("", 0)),
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		}},
		{"INSERT INTO d.ts VALUES(current_timestamp(), $1) RETURNING b", []preparedQueryTest{
			baseTest.SetArgs("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		}},
		{"INSERT INTO d.ts VALUES(statement_timestamp(), $1) RETURNING b", []preparedQueryTest{
			baseTest.SetArgs("2006-07-08").Results(
				time.Date(2006, 7, 8, 0, 0, 0, 0, time.FixedZone("", 0)),
			),
		}},
		{"INSERT INTO d.ts (a) VALUES ($1) RETURNING a", []preparedQueryTest{
			baseTest.SetArgs(
				time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("", 0)),
			).Results(
				time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("", 0)),
			),
		}},
		{"INSERT INTO d.T VALUES ($1) RETURNING 1", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(nil).Results(1),
		}},
		{"INSERT INTO d.T VALUES ($1::INT) RETURNING 1", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1),
		}},
		{"INSERT INTO d.T VALUES ($1) RETURNING $1", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1),
			baseTest.SetArgs(3).Results(3),
		}},
		{"INSERT INTO d.T VALUES ($1) RETURNING $1, 1 + $1", []preparedQueryTest{
			baseTest.SetArgs(1).Results(1, 2),
			baseTest.SetArgs(3).Results(3, 4),
		}},
		{"INSERT INTO d.T VALUES (greatest(42, $1)) RETURNING a", []preparedQueryTest{
			baseTest.SetArgs(40).Results(42),
			baseTest.SetArgs(45).Results(45),
		}},
		// TODO(justin): match this with the optimizer. Currently we only report
		// one placeholder not being filled in, since we only detect so at eval
		// time, #26901.
		// {"SELECT a FROM d.T WHERE a = $1 AND (SELECT a >= $2 FROM d.T WHERE a = $1)",  []preparedQueryTest{
		// 	baseTest.SetArgs(10, 5).Results(10),
		// 	baseTest.Error(
		// 		"pq: no value provided for placeholders: $1, $2",
		// 	).PreparedError(
		// 		wrongArgCountString(2, 0),
		// 	),
		// }},
		{"SELECT * FROM (VALUES (1), (2), (3), (4)) AS foo (a) LIMIT $1 OFFSET $2", []preparedQueryTest{
			baseTest.SetArgs(1, 0).Results(1),
			baseTest.SetArgs(1, 1).Results(2),
			baseTest.SetArgs(1, 2).Results(3),
		}},
		{"SELECT * FROM (VALUES (1), (2), (3), (4)) AS foo (a) FETCH FIRST $1 ROWS ONLY OFFSET $2 ROWS", []preparedQueryTest{
			baseTest.SetArgs(1, 0).Results(1),
			baseTest.SetArgs(1, 1).Results(2),
			baseTest.SetArgs(1, 2).Results(3),
		}},
		{"SELECT 3 + CASE (4) WHEN 4 THEN $1 ELSE 42 END", []preparedQueryTest{
			baseTest.SetArgs(12).Results(15),
			baseTest.SetArgs(-12).Results(-9),
		}},
		{"SELECT DATE '2001-01-02' + ($1 + $1:::int)", []preparedQueryTest{
			baseTest.SetArgs(12).Results("2001-01-26T00:00:00Z"),
		}},
		// Hint for INT type to distinguish from ~INET functionality.
		{"SELECT to_hex(~(~$1:::INT))", []preparedQueryTest{
			baseTest.SetArgs(12).Results("c"),
		}},
		{"SELECT $1::INT", []preparedQueryTest{
			baseTest.SetArgs(12).Results(12),
		}},
		{"SELECT ANNOTATE_TYPE($1, int)", []preparedQueryTest{
			baseTest.SetArgs(12).Results(12),
		}},
		{"SELECT $1 + $2, ANNOTATE_TYPE($2, float)", []preparedQueryTest{
			baseTest.SetArgs(12, 23).Results(35, 23),
		}},
		{"INSERT INTO d.T VALUES ($1 + 1) RETURNING a", []preparedQueryTest{
			baseTest.SetArgs(1).Results(2),
			baseTest.SetArgs(11).Results(12),
		}},
		{"INSERT INTO d.T VALUES (-$1) RETURNING a", []preparedQueryTest{
			baseTest.SetArgs(1).Results(-1),
			baseTest.SetArgs(-999).Results(999),
		}},
		{"INSERT INTO d.two (a, b) VALUES (~$1, $1 + $2) RETURNING a, b", []preparedQueryTest{
			baseTest.SetArgs(5, 6).Results(-6, 11),
		}},
		{"INSERT INTO d.str (s) VALUES (left($1, 3)) RETURNING s", []preparedQueryTest{
			baseTest.SetArgs("abcdef").Results("abc"),
			baseTest.SetArgs("123456").Results("123"),
		}},
		{"INSERT INTO d.str (b) VALUES (COALESCE($1, 'strLit')) RETURNING b", []preparedQueryTest{
			baseTest.SetArgs(nil).Results("strLit"),
			baseTest.SetArgs("123456").Results("123456"),
		}},
		{"INSERT INTO d.intStr VALUES ($1, 'hello ' || $1::TEXT) RETURNING *", []preparedQueryTest{
			baseTest.SetArgs(123).Results(123, "hello 123"),
		}},
		{"SELECT * from d.T WHERE a = ANY($1)", []preparedQueryTest{
			baseTest.SetArgs(pq.Array([]int{10})).Results(10),
		}},
		{"SELECT s from (VALUES ('foo'), ('bar')) as t(s) WHERE s = ANY($1)", []preparedQueryTest{
			baseTest.SetArgs(pq.StringArray([]string{"foo"})).Results("foo"),
		}},
		// #13725
		{"SELECT * FROM d.emptynorows", []preparedQueryTest{
			baseTest.SetArgs(),
		}},
		{"SELECT * FROM d.emptyrows", []preparedQueryTest{
			baseTest.SetArgs().Results().Results().Results(),
		}},
		// #14238
		{"EXPLAIN SELECT 1", []preparedQueryTest{
			baseTest.SetArgs().
				Results("distribution: local").
				Results("vectorized: true").
				Results("").
				Results("• values").
				Results("  size: 1 column, 1 row"),
		}},
		// #14245
		{"SELECT 1::oid = $1", []preparedQueryTest{
			baseTest.SetArgs(1).Results(true),
			baseTest.SetArgs(2).Results(false),
			baseTest.SetArgs("1").Results(true),
			baseTest.SetArgs("2").Results(false),
		}},
		{"SELECT * FROM d.pg_catalog.pg_class WHERE relnamespace = $1", []preparedQueryTest{
			baseTest.SetArgs(1),
		}},
		{"SELECT $1::UUID", []preparedQueryTest{
			baseTest.SetArgs("63616665-6630-3064-6465-616462656562").Results("63616665-6630-3064-6465-616462656562"),
		}},
		{"SELECT $1::INET", []preparedQueryTest{
			baseTest.SetArgs("192.168.0.1/32").Results("192.168.0.1"),
		}},
		{"SELECT $1::TIME", []preparedQueryTest{
			baseTest.SetArgs("12:00:00").Results("0000-01-01T12:00:00Z"),
		}},
		{"SELECT $1::TIMETZ", []preparedQueryTest{
			baseTest.SetArgs("12:00:00+0330").Results("0000-01-01T12:00:00+03:30"),
		}},
		{"SELECT $1::BOX2D", []preparedQueryTest{
			baseTest.SetArgs("BOX(1 2,3 4)").Results("BOX(1 2,3 4)"),
		}},
		{"SELECT $1::GEOGRAPHY", []preparedQueryTest{
			baseTest.SetArgs("POINT(1.0 1.0)").Results("0101000020E6100000000000000000F03F000000000000F03F"),
		}},
		{"SELECT $1::GEOMETRY", []preparedQueryTest{
			baseTest.SetArgs("POINT(1.0 1.0)").Results("0101000000000000000000F03F000000000000F03F"),
		}},
		{"SELECT $1:::FLOAT[]", []preparedQueryTest{
			baseTest.SetArgs("{}").Results("{}"),
			baseTest.SetArgs("{1.0,2.0,3.0}").Results("{1.0,2.0,3.0}"),
		}},
		{"SELECT $1:::DECIMAL[]", []preparedQueryTest{
			baseTest.SetArgs("{1.000}").Results("{1.000}"),
		}},
		{"SELECT $1:::STRING[]", []preparedQueryTest{
			baseTest.SetArgs(`{aaa}`).Results(`{aaa}`),
			baseTest.SetArgs(`{"aaa"}`).Results(`{aaa}`),
			baseTest.SetArgs(`{aaa,bbb,ccc}`).Results(`{aaa,bbb,ccc}`),
		}},
		{"SELECT $1:::JSON", []preparedQueryTest{
			baseTest.SetArgs(`true`).Results(`true`),
			baseTest.SetArgs(`"hello"`).Results(`"hello"`),
		}},
		{"SELECT $1:::BIT(4)", []preparedQueryTest{
			baseTest.SetArgs(`1101`).Results(`1101`),
		}},
		{"SELECT $1:::VARBIT", []preparedQueryTest{
			baseTest.SetArgs(`1101`).Results(`1101`),
			baseTest.SetArgs(`1101001`).Results(`1101001`),
		}},
		{"SELECT $1::INT[]", []preparedQueryTest{
			baseTest.SetArgs(pq.Array([]int64{10})).Results(pq.Array([]int64{10})),
		}},
		{"INSERT INTO d.arr VALUES($1, $2)", []preparedQueryTest{
			baseTest.SetArgs(pq.Array([]int64{}), pq.Array([]string{})),
		}},
		{"EXPERIMENTAL SCRUB TABLE system.locations", []preparedQueryTest{
			baseTest.SetArgs(),
		}},
		{"ALTER RANGE liveness CONFIGURE ZONE = $1", []preparedQueryTest{
			baseTest.SetArgs("num_replicas: 1"),
		}},
		{"ALTER RANGE liveness CONFIGURE ZONE USING num_replicas = $1", []preparedQueryTest{
			baseTest.SetArgs(1),
		}},
		{"ALTER RANGE liveness CONFIGURE ZONE = $1", []preparedQueryTest{
			baseTest.SetArgs(gosql.NullString{}),
		}},
		{"TRUNCATE TABLE d.str", []preparedQueryTest{
			baseTest.SetArgs(),
		}},

		// TODO(nvanbenschoten): Same class of limitation as that in logic_test/typing:
		//   Nested constants are not exposed to the same constant type resolution rules
		//   as top-level constants, and instead are simply resolved to their natural type.
		//{"SELECT (CASE a WHEN 10 THEN 'one' WHEN 11 THEN (CASE 'en' WHEN 'en' THEN $1 END) END) AS ret FROM d.T ORDER BY ret DESC LIMIT 2",  []preparedQueryTest{
		// 	baseTest.SetArgs("hello").Results("one").Results("hello"),
		//}},
	}

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Update the default AS OF time for querying the system.table_statistics
	// table to create the crdb_internal.table_row_statistics table.
	if _, err := db.Exec(
		"SET CLUSTER SETTING sql.crdb_internal.table_row_statistics.as_of_time = '-1µs'",
	); err != nil {
		t.Fatal(err)
	}

	runTests := func(
		t *testing.T,
		query string,
		prepared bool,
		tests []preparedQueryTest,
		queryFunc func(...interface{}) (*gosql.Rows, error),
	) {
		for idx, test := range tests {
			t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
				if testing.Verbose() || log.V(1) {
					log.Infof(context.Background(), "query: %s", query)
				}
				rows, err := queryFunc(test.qargs...)
				if err != nil {
					if test.error == "" {
						t.Errorf("%s: %v: unexpected error: %s", query, test.qargs, err)
					} else {
						expectedErr := test.error
						if prepared && test.preparedError != "" {
							expectedErr = test.preparedError
						}
						if err.Error() != expectedErr {
							t.Errorf("%s: %v: expected error: %s, got %s", query, test.qargs, expectedErr, err)
						}
					}
					return
				}
				defer rows.Close()

				if test.error != "" {
					t.Fatalf("expected error: %s: %v", query, test.qargs)
				}

				for _, expected := range test.results {
					if !rows.Next() {
						t.Fatalf("expected row: %s: %v", query, test.qargs)
					}
					dst := make([]interface{}, len(expected))
					for i, d := range expected {
						dst[i] = reflect.New(reflect.TypeOf(d)).Interface()
					}
					if err := rows.Scan(dst...); err != nil {
						t.Error(err)
					}
					for i, d := range dst {
						dst[i] = reflect.Indirect(reflect.ValueOf(d)).Interface()
					}
					if len(dst) > 0 && len(expected) > 0 && !reflect.DeepEqual(dst, expected) {
						t.Errorf("%s: %v: expected %v, got %v", query, test.qargs, expected, dst)
					}
				}
				for rows.Next() {
					if test.others > 0 {
						test.others--
						continue
					}
					cols, err := rows.Columns()
					if err != nil {
						t.Errorf("%s: %s", query, err)
						continue
					}
					// Unexpected line. Get and print out the details.
					dst := make([]interface{}, len(cols))
					for i := range dst {
						dst[i] = new(interface{})
					}
					if err := rows.Scan(dst...); err != nil {
						t.Errorf("%s: %s", query, err)
						continue
					}
					b, err := json.Marshal(dst)
					if err != nil {
						t.Errorf("%s: %s", query, err)
						continue
					}
					t.Errorf("%s: unexpected row: %s", query, b)
				}
				if test.others > 0 {
					t.Fatalf("%s: expected %d more row(s)", query, test.others)
				}
			})
		}
	}

	initStmt := `
CREATE DATABASE d;
CREATE TABLE d.t (a INT);
INSERT INTO d.t VALUES (10),(11);
CREATE TABLE d.ts (a TIMESTAMP, b DATE);
CREATE TABLE d.two (a INT, b INT);
CREATE TABLE d.intStr (a INT, s STRING);
CREATE TABLE d.str (s STRING, b BYTES);
CREATE TABLE d.arr (a INT[], b TEXT[]);
CREATE TABLE d.emptynorows (); -- zero columns, zero rows
CREATE TABLE d.emptyrows (x INT);
INSERT INTO d.emptyrows VALUES (1),(2),(3);
ALTER TABLE d.emptyrows DROP COLUMN x; -- zero columns, 3 rows
`
	if _, err := db.Exec(initStmt); err != nil {
		t.Fatal(err)
	}

	t.Run("exec", func(t *testing.T) {
		for _, test := range queryTests {
			query := test.sql
			tests := test.ptest
			t.Run(query, func(t *testing.T) {
				runTests(t, query, false, tests, func(args ...interface{}) (*gosql.Rows, error) {
					return db.Query(query, args...)
				})
			})
		}
	})

	t.Run("prepare", func(t *testing.T) {
		for _, test := range queryTests {
			query := test.sql
			tests := test.ptest
			t.Run(query, func(t *testing.T) {
				if stmt, err := db.Prepare(query); err != nil {
					t.Errorf("%s: prepare error: %s", query, err)
				} else {
					defer stmt.Close()

					runTests(t, query, true, tests, stmt.Query)
				}
			})
		}
	})
}

type preparedExecTest struct {
	qargs           []interface{}
	rowsAffected    int64
	error           string
	rowsAffectedErr string
}

func (p preparedExecTest) SetArgs(v ...interface{}) preparedExecTest {
	p.qargs = v
	return p
}

func (p preparedExecTest) RowsAffected(rowsAffected int64) preparedExecTest {
	p.rowsAffected = rowsAffected
	return p
}

func (p preparedExecTest) Error(err string) preparedExecTest {
	p.error = err
	return p
}

func (p preparedExecTest) RowsAffectedErr(err string) preparedExecTest {
	p.rowsAffectedErr = err
	return p
}

// Verify that bound dates are evaluated using session timezone.
func TestPGPrepareDate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec("CREATE TABLE test (t TIMESTAMPTZ)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("SET TIME ZONE +08"); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("INSERT INTO test VALUES ($1)")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stmt.Exec("2018-01-01 12:34:56"); err != nil {
		t.Fatal(err)
	}

	// Reset to UTC for the query.
	if _, err := db.Exec("SET TIME ZONE UTC"); err != nil {
		t.Fatal(err)
	}

	var ts time.Time
	if err := db.QueryRow("SELECT t FROM test").Scan(&ts); err != nil {
		t.Fatal(err)
	}

	exp := time.Date(2018, 1, 1, 4, 34, 56, 0, time.UTC)
	if !exp.Equal(ts) {
		t.Fatalf("expected %s, got %s", exp, ts)
	}
}

func TestPGPreparedExec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var baseTest preparedExecTest
	execTests := []struct {
		query string
		tests []preparedExecTest
	}{
		{
			"CREATE DATABASE d",
			[]preparedExecTest{
				baseTest,
			},
		},
		{
			"CREATE TABLE d.public.t (i INT, s STRING, d INT)",
			[]preparedExecTest{
				baseTest,
				baseTest.Error(`pq: relation "d.public.t" already exists`),
			},
		},
		{
			"INSERT INTO d.public.t VALUES ($1, $2, $3)",
			[]preparedExecTest{
				baseTest.SetArgs(1, "one", 2).RowsAffected(1),
				baseTest.SetArgs("two", 2, 2).Error(`pq: error in argument for $1: strconv.ParseInt: parsing "two": invalid syntax`),
			},
		},
		{
			"UPDATE d.public.t SET s = $1, i = i + $2, d = 1 + $3 WHERE i = $4",
			[]preparedExecTest{
				baseTest.SetArgs(4, 3, 2, 1).RowsAffected(1),
			},
		},
		{
			"UPDATE d.public.t SET i = $1 WHERE (i, s) = ($2, $3)",
			[]preparedExecTest{
				baseTest.SetArgs(8, 4, "4").RowsAffected(1),
			},
		},
		{
			"DELETE FROM d.public.t WHERE s = $1 and i = $2 and d = 2 + $3",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2, 3).RowsAffected(0),
			},
		},
		{
			"INSERT INTO d.public.t VALUES ($1), ($2)",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2).RowsAffected(2),
			},
		},
		{
			"INSERT INTO d.public.t VALUES ($1), ($2) RETURNING $3 + 1",
			[]preparedExecTest{
				baseTest.SetArgs(3, 4, 5).RowsAffected(2),
			},
		},
		{
			"UPDATE d.public.t SET i = CASE WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				baseTest.SetArgs(true, true, 3).RowsAffected(5),
			},
		},
		{
			"UPDATE d.public.t SET i = CASE i WHEN $1 THEN i-$3 WHEN $2 THEN i+$3 END",
			[]preparedExecTest{
				baseTest.SetArgs(1, 2, 3).RowsAffected(5),
			},
		},
		{
			"UPDATE d.public.t SET d = CASE WHEN TRUE THEN $1 END",
			[]preparedExecTest{
				baseTest.SetArgs(2).RowsAffected(5),
			},
		},
		{
			"DELETE FROM d.public.t RETURNING $1+1",
			[]preparedExecTest{
				baseTest.SetArgs(1).RowsAffected(5),
			},
		},
		{
			"DROP TABLE d.public.t",
			[]preparedExecTest{
				baseTest,
				baseTest.Error(`pq: relation "d.public.t" does not exist`),
			},
		},
		{
			"CREATE TABLE d.public.t AS SELECT $1+1 AS x",
			[]preparedExecTest{
				baseTest.SetArgs(1),
			},
		},
		{
			"CREATE TABLE d.public.types (i int, f float, s string, b bytes, d date, m timestamp, z timestamp with time zone, n interval, o bool, e decimal)",
			[]preparedExecTest{
				baseTest,
			},
		},
		{
			"INSERT INTO d.public.types VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
			[]preparedExecTest{
				baseTest.RowsAffected(1).SetArgs(
					int64(0),
					float64(0),
					"",
					[]byte{},
					time.Time{}, // date
					time.Time{}, // timestamp
					time.Time{}, // timestamptz
					time.Hour.String(),
					true,
					"0.0", // decimal
				),
			},
		},
		{
			"DROP DATABASE d CASCADE",
			[]preparedExecTest{
				baseTest,
			},
		},
		{
			"CANCEL JOB $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"CANCEL JOBS SELECT $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"RESUME JOB $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"RESUME JOBS SELECT $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"PAUSE JOB $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"PAUSE JOBS SELECT $1",
			[]preparedExecTest{
				baseTest.SetArgs(123).Error("pq: job with ID 123 does not exist"),
			},
		},
		{
			"CANCEL QUERY $1",
			[]preparedExecTest{
				baseTest.SetArgs("01").Error("pq: could not cancel query 00000000000000000000000000000001: query ID 00000000000000000000000000000001 not found"),
			},
		},
		{
			"CANCEL QUERIES SELECT $1",
			[]preparedExecTest{
				baseTest.SetArgs("01").Error("pq: could not cancel query 00000000000000000000000000000001: query ID 00000000000000000000000000000001 not found"),
			},
		},
		{
			"CANCEL SESSION $1",
			[]preparedExecTest{
				baseTest.SetArgs("01").Error("pq: could not cancel session 00000000000000000000000000000001: session ID 00000000000000000000000000000001 not found"),
			},
		},
		{
			"CANCEL SESSIONS SELECT $1",
			[]preparedExecTest{
				baseTest.SetArgs("01").Error("pq: could not cancel session 00000000000000000000000000000001: session ID 00000000000000000000000000000001 not found"),
			},
		},
		// An empty string is valid in postgres.
		{
			"",
			[]preparedExecTest{
				baseTest.RowsAffectedErr("no RowsAffected available after the empty statement"),
			},
		},
		// Empty statements are permitted.
		{
			";",
			[]preparedExecTest{
				baseTest.RowsAffectedErr("no RowsAffected available after the empty statement"),
			},
		},
		// Any number of empty statements are permitted with a single statement
		// anywhere.
		{
			"; ; SET DATABASE = system; ;",
			[]preparedExecTest{
				baseTest,
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	runTests := func(
		t *testing.T, query string, tests []preparedExecTest, execFunc func(...interface{},
		) (gosql.Result, error)) {
		for idx, test := range tests {
			t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
				if testing.Verbose() || log.V(1) {
					log.Infof(context.Background(), "exec: %s", query)
				}
				if result, err := execFunc(test.qargs...); err != nil {
					if test.error == "" {
						t.Errorf("%s: %v: unexpected error: %s", query, test.qargs, err)
					} else if err.Error() != test.error {
						t.Errorf("%s: %v: expected error: %s, got %s", query, test.qargs, test.error, err)
					}
				} else {
					rowsAffected, err := result.RowsAffected()
					if !testutils.IsError(err, test.rowsAffectedErr) {
						t.Errorf("%s: %v: expected %q, got %v", query, test.qargs, test.rowsAffectedErr, err)
					} else if rowsAffected != test.rowsAffected {
						t.Errorf("%s: %v: expected %v, got %v", query, test.qargs, test.rowsAffected, rowsAffected)
					}
				}
			})
		}
	}

	t.Run("exec", func(t *testing.T) {
		for _, execTest := range execTests {
			t.Run(execTest.query, func(t *testing.T) {
				runTests(t, execTest.query, execTest.tests, func(args ...interface{}) (gosql.Result, error) {
					return db.Exec(execTest.query, args...)
				})
			})
		}
	})

	t.Run("prepare", func(t *testing.T) {
		for _, execTest := range execTests {
			t.Run(execTest.query, func(t *testing.T) {
				if testing.Verbose() || log.V(1) {
					log.Infof(context.Background(), "prepare: %s", execTest.query)
				}
				if stmt, err := db.Prepare(execTest.query); err != nil {
					t.Errorf("%s: prepare error: %s", execTest.query, err)
				} else {
					defer stmt.Close()

					runTests(t, execTest.query, execTest.tests, stmt.Exec)
				}
			})
		}
	})
}

// Names should be qualified automatically during Prepare when a database name
// was given in the connection string.
func TestPGPrepareNameQual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`); err != nil {
		t.Fatal(err)
	}

	pgURL.Path = "/testing"
	db2, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	statements := []string{
		`CREATE TABLE IF NOT EXISTS f (v INT)`,
		`INSERT INTO f VALUES (42)`,
		`SELECT * FROM f`,
		`DELETE FROM f WHERE v = 42`,
		`DROP TABLE IF EXISTS f`,
	}

	for _, stmtString := range statements {
		if _, err = db2.Exec(stmtString); err != nil {
			t.Fatal(err)
		}

		stmt, err := db2.Prepare(stmtString)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = stmt.Exec(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPGPrepareInvalidate ensures that changing table schema triggers recompile
// of a prepared query.
func TestPGPrepareInvalidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	testCases := []struct {
		stmt    string
		prep    bool
		numCols int
	}{
		{
			stmt: `CREATE DATABASE IF NOT EXISTS testing`,
		},
		{
			stmt: `CREATE TABLE IF NOT EXISTS ab (a INT PRIMARY KEY, b INT)`,
		},
		{
			stmt:    `INSERT INTO ab (a, b) VALUES (1, 10)`,
			prep:    true,
			numCols: 2,
		},
		{
			stmt:    `ALTER TABLE ab ADD COLUMN c INT`,
			numCols: 3,
		},
		{
			stmt:    `ALTER TABLE ab DROP COLUMN c`,
			numCols: 2,
		},
	}

	var prep *gosql.Stmt
	for _, tc := range testCases {
		if _, err = db.Exec(tc.stmt); err != nil {
			t.Fatal(err)
		}

		// Create the prepared statement.
		if tc.prep {
			if prep, err = db.Prepare(`SELECT * FROM ab WHERE b=10`); err != nil {
				t.Fatal(err)
			}
		}

		if prep != nil {
			rows, _ := prep.Query()
			defer rows.Close()
			cols, _ := rows.Columns()
			if len(cols) != tc.numCols {
				t.Fatalf("expected %d cols, got %d cols", tc.numCols, len(cols))
			}
		}
	}
}

// A DDL should return "CommandComplete", not "EmptyQuery" Response.
func TestCmdCompleteVsEmptyStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// lib/pq handles the empty query response by returning a nil driver.Result.
	// Unfortunately gosql.Exec wraps that, nil or not, in a gosql.Result which doesn't
	// expose the underlying driver.Result.
	// gosql.Result does however have methods which attempt to dereference the underlying
	// driver.Result and can thus be used to determine if it is nil.
	// TODO(dt): This would be prettier and generate better failures with testify/assert's helpers.

	// Result of a DDL (command complete) yields a non-nil underlying driver result.
	nonempty, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = nonempty.RowsAffected() // should not panic if lib/pq returned a non-nil result.

	empty, err := db.Exec(" ; ; ;")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := empty.RowsAffected()
	if rows != 0 {
		t.Fatalf("expected 0 rows, got %d", rows)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

// Unfortunately lib/pq doesn't expose returned command tags directly, but we can test
// the methods where it depends on their values (Begin, Commit, RowsAffected for INSERTs).
func TestPGCommandTags(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS testing`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE testing.tags (k INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatal(err)
	}

	// Begin will error if the returned tag is not BEGIN.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Commit also checks the correct tag is returned.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO testing.tags VALUES (4, 1)"); err != nil {
		t.Fatal(err)
	}
	// Rollback also checks the correct tag is returned.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// An error will abort the server's transaction.
	if _, err := tx.Exec("INSERT INTO testing.tags VALUES (4, 1), (4, 1)"); err == nil {
		t.Fatal("expected an error on duplicate k")
	}
	// Rollback, even of an aborted txn, should also return the correct tag.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// lib/pq has a special-case for INSERT (due to oids), so test insert and update statements.
	res, err := db.Exec("INSERT INTO testing.tags VALUES (1, 1), (2, 2)")
	if err != nil {
		t.Fatal(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatal("unexpected number of rows affected:", affected)
	}

	res, err = db.Exec("INSERT INTO testing.tags VALUES (3, 3)")
	if err != nil {
		t.Fatal(err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatal("unexpected number of rows affected:", affected)
	}

	res, err = db.Exec("UPDATE testing.tags SET v = 3")
	if err != nil {
		t.Fatal(err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 3 {
		t.Fatal("unexpected number of rows affected:", affected)
	}
}

// checkSQLNetworkMetrics returns the server's pgwire bytesIn/bytesOut and an
// error if the bytesIn/bytesOut don't satisfy the given minimums and maximums.
func checkSQLNetworkMetrics(
	s serverutils.TestServerInterface, minBytesIn, minBytesOut, maxBytesIn, maxBytesOut int64,
) (int64, int64, error) {
	if err := s.WriteSummaries(); err != nil {
		return -1, -1, err
	}

	bytesIn := s.MustGetSQLNetworkCounter(pgwire.MetaBytesIn.Name)
	bytesOut := s.MustGetSQLNetworkCounter(pgwire.MetaBytesOut.Name)
	if a, min := bytesIn, minBytesIn; a < min {
		return bytesIn, bytesOut, errors.Errorf("bytesin %d < expected min %d", a, min)
	}
	if a, min := bytesOut, minBytesOut; a < min {
		return bytesIn, bytesOut, errors.Errorf("bytesout %d < expected min %d", a, min)
	}
	if a, max := bytesIn, maxBytesIn; a > max {
		return bytesIn, bytesOut, errors.Errorf("bytesin %d > expected max %d", a, max)
	}
	if a, max := bytesOut, maxBytesOut; a > max {
		return bytesIn, bytesOut, errors.Errorf("bytesout %d > expected max %d", a, max)
	}
	return bytesIn, bytesOut, nil
}

func TestSQLNetworkMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Setup pgwire client.
	pgURL, cleanupFn := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	const minbytes = 20
	const maxbytes = 2 * 1024

	// Make sure we're starting at 0.
	if _, _, err := checkSQLNetworkMetrics(s, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}

	// A single query should give us some I/O.
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}
	bytesIn, bytesOut, err := checkSQLNetworkMetrics(s, minbytes, minbytes, maxbytes, maxbytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}

	// A second query should give us more I/O.
	_, _, err = checkSQLNetworkMetrics(s, bytesIn+minbytes, bytesOut+minbytes, maxbytes, maxbytes)
	if err != nil {
		t.Fatal(err)
	}

	// Verify connection counter.
	expectConns := func(n int) {
		testutils.SucceedsSoon(t, func() error {
			if conns := s.MustGetSQLNetworkCounter(pgwire.MetaConns.Name); conns != int64(n) {
				return errors.Errorf("connections %d != expected %d", conns, n)
			}
			return nil
		})
	}

	var conns [10]*gosql.DB
	for i := range conns {
		var err error
		if conns[i], err = gosql.Open("postgres", pgURL.String()); err != nil {
			t.Fatal(err)
		}
		defer conns[i].Close()

		rows, err := conns[i].Query("SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
		expectConns(i + 1)
	}

	for i := len(conns) - 1; i >= 0; i-- {
		conns[i].Close()
		expectConns(i)
	}
}

func TestPGWireOverUnixSocket(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if runtime.GOOS == "windows" {
		skip.IgnoreLint(t, "unix sockets not support on windows")
	}

	// We need a temp directory in which we'll create the unix socket.
	//
	// On BSD, binding to a socket is limited to a path length of 104 characters
	// (including the NUL terminator). In glibc, this limit is 108 characters.
	//
	// macOS has a tendency to produce very long temporary directory names, so
	// we are careful to keep all the constants involved short.
	tempDir, err := ioutil.TempDir("", "PGSQL")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	const port = "6"

	socketFile := filepath.Join(tempDir, ".s.PGSQL."+port)

	params := base.TestServerArgs{
		Insecure:   true,
		SocketFile: socketFile,
	}
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// We can't pass socket paths as url.Host to libpq, use ?host=/... instead.
	options := url.Values{
		"host": []string{tempDir},
	}
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(security.RootUser),
		Host:     net.JoinHostPort("", port),
		RawQuery: options.Encode(),
	}
	if err := trivialQuery(pgURL); err != nil {
		t.Fatal(err)
	}
}

func TestPGWireResultChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

func TestSessionParameters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestServerArgs{Insecure: true}
	s, _, _ := serverutils.StartServer(t, params)

	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	host, ports, _ := net.SplitHostPort(s.ServingSQLAddr())
	port, _ := strconv.Atoi(ports)

	connCfg := pgx.ConnConfig{
		Host:      host,
		Port:      uint16(port),
		User:      security.RootUser,
		TLSConfig: nil, // insecure
		Logger:    pgxTestLogger{},
	}

	testData := []struct {
		varName        string
		val            string
		expectedStatus bool
		expectedSet    bool
		expectedErr    string
	}{
		// Unknown parameters are tolerated without error (a warning will be logged).
		{"foo", "bar", false, false, ``},
		// Known parameters are checked to actually be set, even session vars which
		// are not valid server status params can be set.
		{"extra_float_digits", "3", false, true, ``},
		{"extra_float_digits", "-3", false, true, ``},
		{"distsql", "off", false, true, ``},
		{"distsql", "auto", false, true, ``},
		// Case does not matter to set, but the server will reply with special cased
		// variables.
		{"timezone", "Europe/Paris", false, true, ``},
		{"TimeZone", "Europe/Amsterdam", true, true, ``},
		{"datestyle", "ISO, MDY", false, true, ``},
		{"DateStyle", "ISO, MDY", true, true, ``},
		// Known parameters that definitely cannot be set will cause an error.
		{"server_version", "bar", false, false, `parameter "server_version" cannot be changed.*55P02`},
		// Erroneous values are also rejected.
		{"extra_float_digits", "42", false, false, `42 is outside the valid range for parameter "extra_float_digits".*22023`},
		{"datestyle", "woo", false, false, `invalid value for parameter "DateStyle".*22023`},
	}

	for _, test := range testData {
		t.Run(test.varName+"="+test.val, func(t *testing.T) {
			cfg := connCfg
			cfg.RuntimeParams = map[string]string{test.varName: test.val}
			db, err := pgx.Connect(cfg)
			t.Logf("conn error: %v", err)
			if !testutils.IsError(err, test.expectedErr) {
				t.Fatalf("expected %q, got %v", test.expectedErr, err)
			}
			if err != nil {
				return
			}
			defer func() { _ = db.Close() }()

			for k, v := range db.RuntimeParams {
				t.Logf("received runtime param %s = %q", k, v)
			}

			// If the session var is also a valid status param, then check
			// the requested value was processed.
			if test.expectedStatus {
				serverVal := db.RuntimeParams[test.varName]
				if serverVal != test.val {
					t.Fatalf("initial server status %v: got %q, expected %q",
						test.varName, serverVal, test.val)
				}
			}

			// Check the value also inside the session.
			rows, err := db.Query("SHOW " + test.varName)
			if err != nil {
				// Check that the value was not expected to be settable.
				// (The set was ignored).
				if !test.expectedSet && strings.Contains(err.Error(), "unrecognized configuration parameter") {
					return
				}
				t.Fatal(err)
			}
			// Check that the value set was the value sent by the client.
			if !rows.Next() {
				t.Fatal("too short")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			var gotVal string
			if err := rows.Scan(&gotVal); err != nil {
				t.Fatal(err)
			}
			if rows.Next() {
				_ = rows.Scan(&gotVal)
				t.Fatalf("expected no more rows, got %v", gotVal)
			}
			t.Logf("server says %s = %q", test.varName, gotVal)
			if gotVal != test.val {
				t.Fatalf("expected %q, got %q", test.val, gotVal)
			}
		})
	}
}

type pgxTestLogger struct{}

func (l pgxTestLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	log.Infof(context.Background(), "pgx log [%s] %s - %s", level, msg, data)
}

// pgxTestLogger implements pgx.Logger.
var _ pgx.Logger = pgxTestLogger{}

func TestCancelRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		params := base.TestServerArgs{Insecure: insecure}
		s, _, _ := serverutils.StartServer(t, params)

		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", s.ServingSQLAddr())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// Reset telemetry so we get a deterministic count below.
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

		fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
		// versionCancel is the special code sent as header for cancel requests.
		// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
		// and the explanation in server.go.
		const versionCancel = 80877102
		if err := fe.Send(&pgproto3.StartupMessage{ProtocolVersion: versionCancel}); err != nil {
			t.Fatal(err)
		}
		if _, err := fe.Receive(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("unexpected: %v", err)
		}
		if count := telemetry.GetRawFeatureCounts()["pgwire.unimplemented.cancel_request"]; count != 1 {
			t.Fatalf("expected 1 cancel request, got %d", count)
		}
	})
}

func TestUnsupportedGSSEnc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		params := base.TestServerArgs{Insecure: insecure}
		s, _, _ := serverutils.StartServer(t, params)

		ctx := context.Background()
		defer s.Stopper().Stop(ctx)

		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", s.ServingSQLAddr())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// Reset telemetry so we get a deterministic count below.
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

		fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
		// versionCancel is the special code sent as header for cancel requests.
		// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
		// and the explanation in server.go.
		const versionGSSENC = 80877104
		if err := fe.Send(&pgproto3.StartupMessage{ProtocolVersion: versionGSSENC}); err != nil {
			t.Fatal(err)
		}
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}

		res, ok := msg.(*pgproto3.ErrorResponse)
		if !ok {
			t.Fatalf("expected pgproto3.ErrorResponse, got %T", msg)
		}

		require.Equal(t, res.Severity, "ERROR")
		require.Equal(t, res.Code, pgcode.ProtocolViolation.String())

		if count := telemetry.GetRawFeatureCounts()["othererror."+pgcode.ProtocolViolation.String()+".#52184"]; count != 1 {
			t.Fatalf("expected 1 cancel request, got %d", count)
		}
	})
}

func TestFailPrepareFailsTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Prepare("select fail"); err == nil {
		t.Fatal("Got no error, expected one")
	}

	// This should also fail, since the txn should be destroyed.
	if _, err := tx.Query("select 1"); err == nil {
		t.Fatal("got no error, expected one")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
