// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"math"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/errors"
)

func installSensitiveAccessLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	bt := true
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"sql-audit": {
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{Auditable: &bt},
			},
			Channels: logconfig.ChannelList{Channels: []log.Channel{channel.SENSITIVE_ACCESS}},
		}}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return cleanup
}

// TestAdminAuditLogBasic verifies that after enabling the admin audit log,
// a SELECT statement executed by the admin user is logged to the audit log.
func TestAdminAuditLogBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installSensitiveAccessLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.log.admin_audit.enabled = true;`)
	db.Exec(t, `SELECT 1;`)

	var selectAdminRe = regexp.MustCompile(`"EventType":"admin_query","Statement":"‹SELECT 1›","Tag":"SELECT","User":"root"`)

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 10000, selectAdminRe,
		log.WithMarkedSensitiveData)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.New("no entries found"))
	}
}

// TestAdminAuditLogRegularUser verifies that after enabling the admin audit log,
// a SELECT statement executed by the a regular user does not appear in the audit log.
func TestAdminAuditLogRegularUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installSensitiveAccessLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.log.admin_audit.enabled = true;`)

	db.Exec(t, `CREATE USER testuser`)
	pgURL, testuserCleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "TestImportPrivileges-testuser",
		url.User("testuser"),
	)

	defer testuserCleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	defer testuser.Close()

	if _, err = testuser.Exec(`SELECT 1`); err != nil {
		t.Fatal(err)
	}

	var selectRe = regexp.MustCompile(`SELECT 1`)

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 10000, selectRe,
		log.WithMarkedSensitiveData)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatal(errors.New("unexpected SELECT 1 entry found"))
	}
}

// TestAdminAuditLogTransaction verifies that every statement in a transaction
// is logged to the admin audit log.
func TestAdminAuditLogTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installSensitiveAccessLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.log.admin_audit.enabled = true;`)
	db.Exec(t, `
BEGIN;
CREATE TABLE t();
SELECT * FROM t;
SELECT 1;
COMMIT;
`)

	testCases := []struct {
		name string
		exp  string
	}{
		{
			"select-1-query",
			`"EventType":"admin_query","Statement":"‹SELECT 1›"`,
		},
		{
			"select-*-from-table-query",
			`"EventType":"admin_query","Statement":"‹SELECT * FROM \"\".\"\".t›"`,
		},
		{
			"create-table-query",
			`"EventType":"admin_query","Statement":"‹CREATE TABLE defaultdb.public.t ()›"`,
		},
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"admin_query"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, tc := range testCases {
		found := false
		for _, e := range entries {
			if !strings.Contains(e.Message, tc.exp) {
				continue
			}
			found = true
		}
		if !found {
			t.Fatal(errors.Newf("no matching entry found for test case %s", tc.name))
		}
	}
}

// TestAdminAuditLogMultipleTransactions verifies that after enabling the admin
// audit log, statements executed in a transaction by a user with admin privilege
// are all logged. Once the user loses admin privileges, the statements
// should no longer be logged.
func TestAdminAuditLogMultipleTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installSensitiveAccessLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `SET CLUSTER SETTING sql.log.admin_audit.enabled = true;`)

	db.Exec(t, `CREATE USER testuser`)
	pgURL, testuserCleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "TestImportPrivileges-testuser",
		url.User("testuser"),
	)

	defer testuserCleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	defer testuser.Close()

	db.Exec(t, `GRANT admin TO testuser`)

	if _, err := testuser.Exec(`
BEGIN;
CREATE TABLE t();
SELECT * FROM t;
SELECT 1;
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name string
		exp  string
	}{
		{
			"select-1-query",
			`"EventType":"admin_query","Statement":"‹SELECT 1›"`,
		},
		{
			"select-*-from-table-query",
			`"EventType":"admin_query","Statement":"‹SELECT * FROM \"\".\"\".t›"`,
		},
		{
			"create-table-query",
			`"EventType":"admin_query","Statement":"‹CREATE TABLE defaultdb.public.t ()›"`,
		},
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"admin_query"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, tc := range testCases {
		found := false
		for _, e := range entries {
			if !strings.Contains(e.Message, tc.exp) {
				continue
			}
			found = true
		}
		if !found {
			t.Fatal(errors.Newf("no matching entry found for test case %s", tc.name))
		}
	}

	// Remove admin from testuser, statements should no longer appear in the
	// log.
	db.Exec(t, "REVOKE admin FROM testuser")

	// SELECT 2 should not appear in the log.
	if _, err := testuser.Exec(`
BEGIN;
SELECT 2;
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	log.Flush()

	entries, err = log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`SELECT 2`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatal(errors.Newf("unexpected entries found"))
	}
}
