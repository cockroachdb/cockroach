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
	gosql "database/sql"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
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
	rootRunner := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	// Dummy table/user used by tests.
	rootRunner.Exec(t, `CREATE TABLE u(x int)`)
	rootRunner.Exec(t, `CREATE USER testuser`)

	rootRunner.Exec(t, `GRANT SYSTEM MODIFYCLUSTERSETTING TO testuser`)
	rootRunner.Exec(t, `GRANT ALL ON * TO testuser WITH GRANT OPTION`)

	testUserURL, cleanupFn := sqlutils.PGUrl(t,
		s.ServingSQLAddr(), t.Name(), url.User(username.TestUser))
	defer cleanupFn()

	testUserDb, err := gosql.Open("postgres", testUserURL.String())
	require.NoError(t, err)
	defer testUserDb.Close()
	testUserRunner := sqlutils.MakeSQLRunner(testUserDb)

	t.Run("testSingleRoleAuditLogging", func(t *testing.T) {
		testSingleRoleAuditLogging(t, rootRunner, testUserRunner)
	})
	// Reset audit config between tests.
	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = ''`)
	t.Run("testMultiRoleAuditLogging", func(t *testing.T) {
		testMultiRoleAuditLogging(t, rootRunner, testUserRunner)
	})
}

func testSingleRoleAuditLogging(
	t *testing.T, rootRunner *sqlutils.SQLRunner, testRunner *sqlutils.SQLRunner,
) {
	allStmtTypesRole := "all_stmt_types"
	noStmtTypeRole := "no_stmt_types"

	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", allStmtTypesRole))
	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", noStmtTypeRole))

	testRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		all_stmt_types ALL
		no_stmt_types NONE
		testuser ALL
	'`)

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO root`,
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
			expectedNumLogs: 3,
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
			role:    "testuser",
			queries: testQueries,
			// One for each test query, another one for setting the initial cluster setting
			expectedNumLogs: 4,
		},
	}

	for _, td := range testData {
		// Grant the audit role
		if td.role != username.TestUser {
			rootRunner.Exec(t, fmt.Sprintf("GRANT %s to testuser", td.role))
		}
		// Run queries
		for idx := range td.queries {
			testRunner.Exec(t, td.queries[idx])
		}
		// Revoke the audit role.
		rootRunner.Exec(t, fmt.Sprintf("REVOKE %s from testuser", td.role))
	}

	log.Flush()

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
func testMultiRoleAuditLogging(
	t *testing.T, rootRunner *sqlutils.SQLRunner, testRunner *sqlutils.SQLRunner,
) {
	roleA := "roleA"
	roleB := "roleB"

	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleA))
	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleB))
	rootRunner.Exec(t, fmt.Sprintf("GRANT %s, %s to testuser", roleA, roleB))

	testRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		roleA ALL
		roleB ALL
	'`)

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO root`,
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
		testRunner.Exec(t, query)
	}

	log.Flush()

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
