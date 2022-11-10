// Copyright 2022 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const createTable = "CREATE TABLE t(i int PRIMARY KEY);"
const rangeErrorMessage = "RangeIterator failed to seek"

func createTestServer(t *testing.T) (serverutils.TestServerInterface, func()) {
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	return testCluster.Server(0), func() {
		testCluster.Stopper().Stop(context.Background())
	}
}

func createSystemTenantDB(t *testing.T, testServer serverutils.TestServerInterface) *gosql.DB {
	return serverutils.OpenDBConn(
		t,
		testServer.ServingSQLAddr(),
		"",    /* useDatabase */
		false, /* insecure */
		testServer.Stopper(),
	)
}

func createSecondaryTenantDB(
	t *testing.T,
	testServer serverutils.TestServerInterface,
	allowSplitAndScatter bool,
	skipSQLSystemTenantCheck bool,
) *gosql.DB {
	_, db := serverutils.StartTenant(
		t, testServer, base.TestTenantArgs{
			TestingKnobs: base.TestingKnobs{
				TenantTestingKnobs: &TenantTestingKnobs{
					AllowSplitAndScatter:      allowSplitAndScatter,
					SkipSQLSystemTentantCheck: skipSQLSystemTenantCheck,
				},
			},
			TenantID: serverutils.TestTenantID(),
		},
	)
	return db
}

func TestMultiTenantAdminFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc                     string
		setup                    string
		setups                   []string
		query                    string
		errorMessage             string
		allowSplitAndScatter     bool
		skipSQLSystemTenantCheck bool
	}{
		{
			desc:  "ALTER RANGE x RELOCATE LEASE",
			query: "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
		},
		{
			desc:                     "ALTER RANGE x RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE LEASE TO 1;",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER RANGE RELOCATE LEASE",
			query: "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
		},
		{
			desc:                     "ALTER RANGE RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE LEASE TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER RANGE x RELOCATE VOTERS",
			query: "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
		},
		{
			desc:                     "ALTER RANGE x RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]) RELOCATE VOTERS FROM 1 TO 1;",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER RANGE RELOCATE VOTERS",
			query: "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
		},
		{
			desc:                     "ALTER RANGE RELOCATE VOTERS SkipSQLSystemTenantCheck",
			query:                    "ALTER RANGE RELOCATE VOTERS FROM 1 TO 1 FOR (SELECT min(range_id) FROM [SHOW RANGES FROM TABLE t]);",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE",
			query: "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
		},
		{
			desc:                     "ALTER TABLE x EXPERIMENTAL_RELOCATE LEASE SkipSQLSystemTenantCheck",
			query:                    "ALTER TABLE t EXPERIMENTAL_RELOCATE LEASE SELECT 1, 1;",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER TABLE x EXPERIMENTAL_RELOCATE VOTERS",
			query: "ALTER TABLE t EXPERIMENTAL_RELOCATE VOTERS SELECT ARRAY[1], 1;",
		},
		{
			desc:         "ALTER TABLE x SPLIT AT",
			query:        "ALTER TABLE t SPLIT AT VALUES (1);",
			errorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:         "ALTER INDEX x SPLIT AT",
			setup:        "CREATE INDEX idx on t(i);",
			query:        "ALTER INDEX t@idx SPLIT AT VALUES (1);",
			errorMessage: "request [1 AdmSplit] not permitted",
		},
		{
			desc:                 "ALTER TABLE x UNSPLIT AT",
			setup:                "ALTER TABLE t SPLIT AT VALUES (1);",
			query:                "ALTER TABLE t UNSPLIT AT VALUES (1);",
			errorMessage:         "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc: "ALTER INDEX x UNSPLIT AT",
			// Multiple setup statements required because of https://github.com/cockroachdb/cockroach/issues/90535.
			setups: []string{
				"CREATE INDEX idx on t(i);",
				"ALTER INDEX t@idx SPLIT AT VALUES (1);",
			},
			query:                "ALTER INDEX t@idx UNSPLIT AT VALUES (1);",
			errorMessage:         "request [1 AdmUnsplit] not permitted",
			allowSplitAndScatter: true,
		},
		{
			desc:  "ALTER TABLE x UNSPLIT ALL",
			query: "ALTER TABLE t UNSPLIT ALL;",
		},
		{
			desc:                     "ALTER TABLE x UNSPLIT ALL SkipSQLSystemTenantCheck",
			query:                    "ALTER TABLE t UNSPLIT ALL;",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:  "ALTER INDEX x UNSPLIT ALL",
			setup: "CREATE INDEX idx on t(i);",
			query: "ALTER INDEX t@idx UNSPLIT ALL;",
		},
		{
			desc:                     "ALTER INDEX x UNSPLIT ALL SkipSQLSystemTenantCheck",
			setup:                    "CREATE INDEX idx on t(i);",
			query:                    "ALTER INDEX t@idx UNSPLIT ALL;",
			errorMessage:             rangeErrorMessage,
			skipSQLSystemTenantCheck: true,
		},
		{
			desc:         "ALTER TABLE x SCATTER",
			query:        "ALTER TABLE t SCATTER;",
			errorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:         "ALTER INDEX x SCATTER",
			setup:        "CREATE INDEX idx on t(i);",
			query:        "ALTER INDEX t@idx SCATTER;",
			errorMessage: "request [1 AdmScatter] not permitted",
		},
		{
			desc:  "CONFIGURE ZONE",
			query: "ALTER TABLE t CONFIGURE ZONE DISCARD;",
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			execQueries := func(db *gosql.DB) error {
				setups := tc.setups
				setup := tc.setup
				if setup != "" {
					setups = []string{setup}
				}
				setups = append([]string{createTable}, setups...)
				for _, setup := range setups {
					_, err := db.ExecContext(ctx, setup)
					require.NoError(t, err, setup)
				}
				_, err := db.ExecContext(ctx, tc.query)
				return err
			}

			// Test system tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSystemTenantDB(t, testServer)
				err := execQueries(db)
				require.NoError(t, err)
			}()

			// Test secondary tenant.
			func() {
				testServer, cleanup := createTestServer(t)
				defer cleanup()
				db := createSecondaryTenantDB(t, testServer, tc.allowSplitAndScatter, tc.skipSQLSystemTenantCheck)
				err := execQueries(db)
				require.Error(t, err)
				errorMessage := tc.errorMessage
				if errorMessage == "" {
					errorMessage = errorutil.UnsupportedWithMultiTenancyMessage
				}
				require.Contains(t, err.Error(), errorMessage)
			}()
		})
	}
}

func TestTruncateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	execQueries := func(db *gosql.DB) error {
		_, err := db.ExecContext(ctx, createTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "ALTER TABLE t SPLIT AT VALUES (1);")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "TRUNCATE TABLE t;")
		require.NoError(t, err)
		_, err = db.QueryContext(ctx, "SHOW RANGES FROM TABLE t;")
		// TODO(ewall): Verify splits are not retained after `SHOW RANGES` is supported by secondary tenants.
		return err
	}

	// Test system tenant.
	func() {
		testServer, cleanup := createTestServer(t)
		defer cleanup()
		db := createSystemTenantDB(t, testServer)
		err := execQueries(db)
		require.NoError(t, err)
	}()

	// Test secondary tenant.
	func() {
		testServer, cleanup := createTestServer(t)
		defer cleanup()
		db := createSecondaryTenantDB(
			t,
			testServer,
			true,  /* allowSplitAndScatter */
			false, /* skipSQLSystemTenantCheck */
		)
		err := execQueries(db)
		require.Error(t, err)
		require.Contains(t, err.Error(), rangeErrorMessage)
	}()
}
