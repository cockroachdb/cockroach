// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func installSensitiveAccessLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	bt := true
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"sql-audit": {
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{Auditable: &bt},
			},
			Channels: logconfig.SelectChannels(channel.SENSITIVE_ACCESS)},
	}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfigForReconfig(cfg, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */)
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

	var selectAdminRe = regexp.MustCompile(`"EventType":"admin_query","Statement":"SELECT ‹1›","Tag":"SELECT","User":"root"`)
	log.FlushFiles()

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

	testuser := s.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))

	if _, err := testuser.Exec(`SELECT 1`); err != nil {
		t.Fatal(err)
	}

	var selectRe = regexp.MustCompile(`SELECT 1`)

	log.FlushFiles()

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
CREATE TABLE t(a INT8 PRIMARY KEY DEFAULT 2);
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
			`"EventType":"admin_query","Statement":"SELECT ‹1›"`,
		},
		{
			"select-*-from-table-query",
			`"EventType":"admin_query","Statement":"SELECT * FROM \"\".\"\".t"`,
		},
		{
			"create-table-query",
			`"EventType":"admin_query","Statement":"CREATE TABLE defaultdb.public.t (a INT8 PRIMARY KEY DEFAULT ‹2›)"`,
		},
	}

	log.FlushFiles()

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

// TestAuditLogZoneConfig verifies that zone config changes produce
// zone_config_audit events on the SENSITIVE_ACCESS channel for both admin
// and non-admin users, across all CONFIGURE ZONE syntax variants.
//
// Regression test for CRDB-1180: zone config changes are security-sensitive
// operations that control data placement (for compliance/geo-partitioning).
// zone_config_audit events are emitted unconditionally, without requiring
// sql.log.admin_audit.enabled.
func TestAuditLogZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installSensitiveAccessLogFileSink(sc, t)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)

	// Intentionally NOT setting sql.log.admin_audit.enabled to prove that
	// zone_config_audit events are emitted unconditionally.

	// Set up a non-admin user with ZONECONFIG privilege on all test objects.
	db.Exec(t, `CREATE USER testuser`)
	db.Exec(t, `CREATE DATABASE audit_zone_db`)
	db.Exec(t, `CREATE TABLE audit_zone_test(id INT PRIMARY KEY, val STRING)`)
	db.Exec(t, `CREATE INDEX audit_zone_idx ON audit_zone_test(val)`)
	db.Exec(t, `GRANT ZONECONFIG ON TABLE audit_zone_test TO testuser`)
	db.Exec(t, `GRANT ZONECONFIG ON DATABASE audit_zone_db TO testuser`)

	// Create a table that testuser does NOT have ZONECONFIG privilege on,
	// to test that failed zone config changes are also audited.
	db.Exec(t, `CREATE TABLE audit_zone_noperm(id INT PRIMARY KEY)`)

	testuserConn := s.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))

	testCases := []struct {
		name      string
		useAdmin  bool
		setup     string
		stmt      string
		user      string // expected username in log entry
		expectErr bool   // if true, the statement is expected to fail
	}{
		{
			name:     "admin/table/using",
			useAdmin: true,
			stmt:     `ALTER TABLE audit_zone_test CONFIGURE ZONE USING num_replicas = 5`,
			user:     "root",
		},
		{
			name:     "non-admin/table/using",
			useAdmin: false,
			stmt:     `ALTER TABLE audit_zone_test CONFIGURE ZONE USING num_replicas = 3`,
			user:     "testuser",
		},
		{
			name:     "admin/table/discard",
			useAdmin: true,
			stmt:     `ALTER TABLE audit_zone_test CONFIGURE ZONE DISCARD`,
			user:     "root",
		},
		{
			name:     "non-admin/table/discard",
			useAdmin: false,
			setup:    `ALTER TABLE audit_zone_test CONFIGURE ZONE USING num_replicas = 5`,
			stmt:     `ALTER TABLE audit_zone_test CONFIGURE ZONE DISCARD`,
			user:     "testuser",
		},
		{
			name:     "admin/table/using-default",
			useAdmin: true,
			setup:    `ALTER TABLE audit_zone_test CONFIGURE ZONE USING num_replicas = 5`,
			stmt:     `ALTER TABLE audit_zone_test CONFIGURE ZONE USING DEFAULT`,
			user:     "root",
		},
		{
			name:     "admin/database/using",
			useAdmin: true,
			stmt:     `ALTER DATABASE audit_zone_db CONFIGURE ZONE USING gc.ttlseconds = 7200`,
			user:     "root",
		},
		{
			name:     "non-admin/database/using",
			useAdmin: false,
			stmt:     `ALTER DATABASE audit_zone_db CONFIGURE ZONE USING gc.ttlseconds = 3600`,
			user:     "testuser",
		},
		{
			name:     "admin/index/using",
			useAdmin: true,
			stmt:     `ALTER INDEX audit_zone_test@audit_zone_idx CONFIGURE ZONE USING gc.ttlseconds = 3600`,
			user:     "root",
		},
		{
			name:      "non-admin/table/permission-denied",
			useAdmin:  false,
			stmt:      `ALTER TABLE audit_zone_noperm CONFIGURE ZONE USING num_replicas = 5`,
			user:      "testuser",
			expectErr: true,
		},
		{
			name:      "admin/database/alter-locality",
			useAdmin:  true,
			stmt:      `ALTER DATABASE audit_zone_db ALTER LOCALITY GLOBAL CONFIGURE ZONE USING num_replicas = 5`,
			user:      "root",
			expectErr: true, // database is not multi-region
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != "" {
				db.Exec(t, tc.setup)
			}

			beforeExec := timeutil.Now().UnixNano()

			if tc.useAdmin {
				if tc.expectErr {
					if _, err := sqlDB.Exec(tc.stmt); err == nil {
						t.Fatal("expected error but statement succeeded")
					}
				} else {
					db.Exec(t, tc.stmt)
				}
			} else {
				_, err := testuserConn.Exec(tc.stmt)
				if tc.expectErr && err == nil {
					t.Fatal("expected error but statement succeeded")
				}
				if !tc.expectErr && err != nil {
					t.Fatal(err)
				}
			}

			log.FlushFiles()

			entries, err := log.FetchEntriesFromFiles(
				beforeExec, math.MaxInt64, 10000,
				regexp.MustCompile(`"EventType":"zone_config_audit".*`+tc.user),
				log.WithMarkedSensitiveData,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) == 0 {
				t.Fatalf("no zone_config_audit entry found for %q (user=%s)", tc.stmt, tc.user)
			}
		})
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

	testuser := s.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))

	db.Exec(t, `GRANT admin TO testuser`)

	if _, err := testuser.Exec(`
BEGIN;
CREATE TABLE t(a INT8 PRIMARY KEY DEFAULT 2);
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
			`"EventType":"admin_query","Statement":"SELECT ‹1›"`,
		},
		{
			"select-*-from-table-query",
			`"EventType":"admin_query","Statement":"SELECT * FROM \"\".\"\".t"`,
		},
		{
			"create-table-query",
			`"EventType":"admin_query","Statement":"CREATE TABLE defaultdb.public.t (a INT8 PRIMARY KEY DEFAULT ‹2›)"`,
		},
	}

	log.FlushFiles()

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

	log.FlushFiles()

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
