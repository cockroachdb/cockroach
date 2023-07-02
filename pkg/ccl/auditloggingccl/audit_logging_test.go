// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package auditloggingccl

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
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRoleBasedAuditEnterpriseGated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	rootRunner := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	// Disable enterprise.
	enableEnterprise := utilccl.TestingDisableEnterprise()

	// Enable auditing.
	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = 'ALL ALL'`)

	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		rootRunner.QueryRow(t,
			"SHOW CLUSTER SETTING sql.log.user_audit",
		).Scan(&currentVal)
		if currentVal == "" {
			return errors.Newf("waiting for cluster setting to be set")
		}
		return nil
	})

	// Run a test query.
	rootRunner.Exec(t, `SHOW CLUSTER SETTING sql.log.user_audit`)

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

	// Enterprise is disabled, expect the number of entries to be 0.
	if len(entries) != 0 {
		t.Fatal(errors.Newf("enterprise is disabled, found unexpected entries"))
	}

	// Enable enterprise
	enableEnterprise()

	// Run a test query.
	rootRunner.Exec(t, `SHOW CLUSTER SETTING sql.log.user_audit`)

	log.Flush()

	entries, err = log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"Statement":"SHOW CLUSTER SETTING \\"sql.log.user_audit\\""`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	// Enterprise is enabled, expect an audit log.
	if len(entries) != 1 {
		t.Fatal(errors.Newf("enterprise is enabled, expected 1 entry, got %d", len(entries)))
	}
}

func TestSingleRoleAuditLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	rootRunner := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	testUserURL, cleanupFn := sqlutils.PGUrl(t,
		s.ServingSQLAddr(), t.Name(), url.User(username.TestUser))
	defer cleanupFn()

	testUserDb, err := gosql.Open("postgres", testUserURL.String())
	require.NoError(t, err)
	defer testUserDb.Close()
	testRunner := sqlutils.MakeSQLRunner(testUserDb)

	// Dummy table/user used by tests.
	setupQueries(t, rootRunner)

	allStmtTypesRole := "all_stmt_types"
	noStmtTypeRole := "no_stmt_types"

	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", allStmtTypesRole))
	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", noStmtTypeRole))

	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		all_stmt_types ALL
		no_stmt_types NONE
		testuser ALL
	'`)

	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		rootRunner.QueryRow(t,
			"SHOW CLUSTER SETTING sql.log.user_audit",
		).Scan(&currentVal)
		if currentVal == "" {
			return errors.Newf("waiting for cluster setting to be set")
		}
		return nil
	})

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
			// One for each test query.
			expectedNumLogs: 3,
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

func TestMultiRoleAuditLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	rootRunner := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	testUserURL, cleanupFn := sqlutils.PGUrl(t,
		s.ServingSQLAddr(), t.Name(), url.User(username.TestUser))
	defer cleanupFn()

	testUserDb, err := gosql.Open("postgres", testUserURL.String())
	require.NoError(t, err)
	defer testUserDb.Close()
	testRunner := sqlutils.MakeSQLRunner(testUserDb)

	// Dummy table/user used by tests.
	setupQueries(t, rootRunner)

	roleA := "roleA"
	roleB := "roleB"

	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleA))
	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleB))
	rootRunner.Exec(t, fmt.Sprintf("GRANT %s, %s to testuser", roleA, roleB))

	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		roleA ALL
		roleB ALL
	'`)

	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		rootRunner.QueryRow(t,
			"SHOW CLUSTER SETTING sql.log.user_audit",
		).Scan(&currentVal)

		if currentVal == "" {
			return errors.Newf("waiting for cluster setting to be set")
		}
		return nil
	})

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
			// Expect logs from all queries.
			roleA: 3,
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

func TestReducedAuditConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	rootRunner := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	// Dummy table/user used by tests.
	setupQueries(t, rootRunner)

	// Enable reduced config.
	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit.reduced_config.enabled = true`)
	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		rootRunner.QueryRow(t,
			"SHOW CLUSTER SETTING sql.log.user_audit.reduced_config.enabled",
		).Scan(&currentVal)

		if currentVal == "false" {
			return errors.Newf("waiting for reduced config cluster setting to be true")
		}
		return nil
	})

	testUserURL, cleanupFn := sqlutils.PGUrl(t,
		s.ServingSQLAddr(), t.Name(), url.User(username.TestUser))
	defer cleanupFn()

	testUserDb, err := gosql.Open("postgres", testUserURL.String())
	require.NoError(t, err)
	defer testUserDb.Close()
	testRunner := sqlutils.MakeSQLRunner(testUserDb)

	// Set a cluster configuration.
	roleA := "roleA"
	rootRunner.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		roleA ALL
	'`)

	testutils.SucceedsSoon(t, func() error {
		var currentVal string
		rootRunner.QueryRow(t,
			"SHOW CLUSTER SETTING sql.log.user_audit",
		).Scan(&currentVal)

		if currentVal == "" {
			return errors.Newf("waiting for cluster setting to be set")
		}
		return nil
	})

	// Run a query. This initializes the reduced audit configuration for the user.
	// Currently, there are no corresponding roles for the user in the audit configuration.
	// Consequently, the user's reduced audit config will be nil.
	testQuery := `SELECT * FROM u`
	testRunner.Exec(t, testQuery)

	// Grant a role the user that corresponds to an audit setting.
	rootRunner.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleA))
	rootRunner.Exec(t, fmt.Sprintf("GRANT %s to testuser", roleA))

	// Run the query again. We expect no log from this query even though the user now has a corresponding role
	// as the reduced audit configuration has already been computed, and there were no corresponding audit settings
	// for the user at that time.
	testRunner.Exec(t, testQuery)

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

	// Expect the number of entries to be 0.
	if len(entries) != 0 {
		t.Fatal(errors.Newf("unexpected entries found"))
	}

	// Open 2nd connection for the test user.
	testUserDb2, err := gosql.Open("postgres", testUserURL.String())
	require.NoError(t, err)
	defer testUserDb2.Close()
	testRunner2 := sqlutils.MakeSQLRunner(testUserDb2)

	// Run a query on the new connection. The new connection will cause the reduced audit config to be re-computed.
	// The user now has a corresponding audit setting. We use a new query here to differentiate.
	testRunner2.Exec(t, `GRANT SELECT ON TABLE u TO root`)

	log.Flush()

	entries, err = log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`GRANT SELECT ON TABLE ‹u› TO root`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	// Expect the number of entries to be 1.
	if len(entries) != 1 {
		t.Fatal(errors.Newf("unexpected number of entries found (not 1)"))
	}
}

func setupQueries(t *testing.T, rootRunner *sqlutils.SQLRunner) {
	// Dummy table/user used by tests.
	rootRunner.Exec(t, `CREATE TABLE u(x int)`)
	rootRunner.Exec(t, `CREATE USER testuser`)

	rootRunner.Exec(t, `GRANT SYSTEM MODIFYCLUSTERSETTING TO testuser`)
	rootRunner.Exec(t, `GRANT ALL ON * TO testuser WITH GRANT OPTION`)
}
