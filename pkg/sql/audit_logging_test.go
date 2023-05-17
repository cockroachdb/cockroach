// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRoleBasedAuditLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	// Dummy table/user used by tests.
	db.Exec(t, `CREATE TABLE u(x int)`)
	db.Exec(t, `CREATE USER test_user`)

	t.Run("testSingleRoleAuditLogging", func(t *testing.T) {
		testSingleRoleAuditLogging(t, db)
	})
	// Reset audit config between tests.
	db.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = ''`)
	t.Run("testMultiRoleAuditLogging", func(t *testing.T) {
		testMultiRoleAuditLogging(t, db)
	})
}

func testSingleRoleAuditLogging(t *testing.T, db *sqlutils.SQLRunner) {
	db.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		all_stmt_types ALL
		no_stmt_types NONE
		root ALL
	'`)

	allStmtTypesRole := "all_stmt_types"
	noStmtTypeRole := "no_stmt_types"

	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", allStmtTypesRole))
	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", noStmtTypeRole))

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO test_user`,
		// DML statement
		`SELECT * FROM u`,
	}
	testData := []struct {
		name            string
		role            string
		queries         []string
		expectedNumLogs int
	}{
		{
			name:            "test-all-stmt-types",
			role:            allStmtTypesRole,
			queries:         testQueries,
			expectedNumLogs: 4,
		},
		{
			name:            "test-no-stmt-types",
			role:            noStmtTypeRole,
			queries:         testQueries,
			expectedNumLogs: 0,
		},
		// Test match on username
		{
			name:    "test-username",
			role:    "root",
			queries: testQueries,
			// 1 set cluster setting, 2 create roles, 3 test queries, 3 revoke queries
			expectedNumLogs: 9,
		},
	}

	for _, td := range testData {
		// Grant the audit role
		if td.role != "root" {
			db.Exec(t, fmt.Sprintf("GRANT %s to root", td.role))
		}
		// Run queries
		for idx := range td.queries {
			db.Exec(t, td.queries[idx])
		}
		// Revoke the audit role.
		db.Exec(t, fmt.Sprintf("REVOKE %s from root", td.role))
	}

	log.FlushFileSinks()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"role_based_audit_event"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	roleToLogs := make(map[string]int)
	for _, entry := range entries {
		for _, td := range testData {
			if strings.Contains(entry.Message, `"Role":"‹`+td.role+`›"`) {
				roleToLogs[td.role]++
			}
		}
	}
	for _, td := range testData {
		numLogs, exists := roleToLogs[td.role]
		if !exists && td.expectedNumLogs != 0 {
			t.Errorf("found no entries for role: %s", td.role)
		}
		require.Equal(t, td.expectedNumLogs, numLogs, "incorrect number of entries for role : %s", td.role)
	}
}

// testMultiRoleAuditLogging tests that we emit the expected audit logs when a user belongs to
// multiple roles that correspond to audit settings. We ensure that the expected audit logs
// correspond to *only* the *first matching audit setting* of the user's roles.
func testMultiRoleAuditLogging(t *testing.T, db *sqlutils.SQLRunner) {
	startTimestamp := timeutil.Now().Unix()
	roleA := "roleA"
	roleB := "roleB"

	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleA))
	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleB))
	db.Exec(t, fmt.Sprintf("GRANT %s, %s to root", roleA, roleB))

	db.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		roleA ALL
		roleB ALL
	'`)

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO test_user`,
		// DML statement
		`SELECT * FROM u`,
	}
	testData := struct {
		name               string
		expectedRoleToLogs map[string]int
	}{
		name: "test-multi-role-user",
		expectedRoleToLogs: map[string]int{
			// Expect logs from all queries and 1 for setting the cluster setting.
			roleA: 4,
			// Expect no logs from roleB as we match on roleA first.
			roleB: 0,
		},
	}

	for _, query := range testQueries {
		db.Exec(t, query)
	}

	log.FlushFileSinks()

	entries, err := log.FetchEntriesFromFiles(
		startTimestamp,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"role_based_audit_event"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	roleToLogs := make(map[string]int)
	for role, expectedNumLogs := range testData.expectedRoleToLogs {
		for _, entry := range entries {
			// Lowercase the role string as we normalize it for logs.
			if strings.Contains(entry.Message, strings.ToLower(role)) {
				roleToLogs[role]++
			}
		}
		require.Equal(t, expectedNumLogs, roleToLogs[role], "unexpected number of logs for role: '%s'", role)
	}
}
