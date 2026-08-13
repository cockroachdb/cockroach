// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := DebugZipTableRegistry{
		"table_with_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{"x", "y", "z"},
		},
		"table_with_empty_sensitive_cols": {
			nonSensitiveCols: NonSensitiveColumns{},
		},
		"table_with_custom_queries": {
			customQueryUnredacted: "SELECT * FROM table_with_custom_queries",
			customQueryRedacted:   "SELECT a, b, c FROM table_with_custom_queries",
		},
		"table_with_non_sensitive_cols_and_custom_unredacted_query": {
			nonSensitiveCols:      NonSensitiveColumns{"x", "crdb_internal.pretty_key(y, 0) as y", "z"},
			customQueryUnredacted: "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query",
		},
		"table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback": {
			nonSensitiveCols:              NonSensitiveColumns{"x", "crdb_internal.pretty_key(y, 0) as y", "z"},
			customQueryUnredacted:         "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
			customQueryUnredactedFallback: "SELECT x FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
		},
	}

	t.Run("errors if no table config present in registry", func(t *testing.T) {
		actual, err := reg.QueryForTable("does_not_exist", false /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no entry found")
		assert.Empty(t, actual)
	})

	t.Run("produces `TABLE` query when unredacted with no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := TableQuery{query: "TABLE table_with_sensitive_cols"}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom query when unredacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := TableQuery{query: "SELECT * FROM table_with_custom_queries"}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces query with only non-sensitive columns when redacted and no custom query", func(t *testing.T) {
		table := "table_with_sensitive_cols"
		expected := TableQuery{query: `SELECT x, y, z FROM table_with_sensitive_cols`}
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("produces custom when redacted and custom query supplied", func(t *testing.T) {
		table := "table_with_custom_queries"
		expected := TableQuery{query: "SELECT a, b, c FROM table_with_custom_queries"}
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("returns error when no custom queries and no non-sensitive columns supplied", func(t *testing.T) {
		table := "table_with_empty_sensitive_cols"
		actual, err := reg.QueryForTable(table, true /* redact */)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no non-sensitive columns defined")
		assert.Empty(t, actual)
	})

	t.Run("produces query when a combination of nonSensitiveCols and customQueryUnredacted is supplied", func(t *testing.T) {
		table := "table_with_non_sensitive_cols_and_custom_unredacted_query"
		expected := TableQuery{query: "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query"}

		t.Run("with redact flag", func(t *testing.T) {
			actual, err := reg.QueryForTable(table, true /* redact */)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)
		})

		t.Run("without redact flag", func(t *testing.T) {
			actual, err := reg.QueryForTable(table, false /* redact */)
			assert.NoError(t, err)
			assert.Equal(t, expected, actual)
		})
	})

	t.Run("with fallback query", func(t *testing.T) {
		table := "table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback"
		expected := TableQuery{
			query:    "SELECT x, crdb_internal.pretty_key(y, 0) as y, z FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
			fallback: "SELECT x FROM table_with_non_sensitive_cols_and_custom_unredacted_query_with_fallback",
		}
		actual, err := reg.QueryForTable(table, false /* redact */)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}

func TestZipContainsAllSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	rows := sqlDB.Query(t, `SELECT table_name FROM [SHOW TABLES FROM system] WHERE type = 'table' ORDER BY table_name`)
	defer rows.Close()

	var allSystemTables []string
	for rows.Next() {
		var tableName string
		require.NoError(t, rows.Scan(&tableName))
		allSystemTables = append(allSystemTables, "system."+tableName)
	}
	require.NoError(t, rows.Err())

	// Verify that every system table is either in zipSystemTables or in the
	// disabled list.
	var missingTables []string
	for _, fullTableName := range allSystemTables {
		_, inRegistry := zipSystemTables[fullTableName]
		_, inDisabled := disabledSystemTables[fullTableName]
		if !inRegistry && !inDisabled {
			missingTables = append(missingTables, fullTableName)
		}
		// Ensure tables are not in both lists (would be redundant).
		require.Falsef(t, inRegistry && inDisabled, "system table %q is in both zipSystemTables and disabledSystemTables registries", fullTableName)
	}
	require.Falsef(t, len(missingTables) > 0, "the following system tables are neither in zipSystemTables nor in disabledSystemTables registries:\n%s", strings.Join(missingTables, "\n"))

	// Verify that disabled tables are indeed not in the registry.
	for disabledTable := range disabledSystemTables {
		tableQuery, err := zipSystemTables.QueryForTable(disabledTable, false /* redact */)
		require.Equal(t, "", tableQuery.query)
		require.Error(t, err)
	}
}

func TestTableRegistryConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	validate := func(table string, regConfig TableRegistryConfig) {
		if regConfig.customQueryRedacted == "" && len(regConfig.nonSensitiveCols) == 0 {
			t.Fatalf("table %q contains no redacted query configuration", table)
		}
		if regConfig.customQueryRedacted != "" && len(regConfig.nonSensitiveCols) > 0 {
			t.Fatalf(
				"table %q has both customQueryRedacted and nonSensitiveCols. These fields are mutually exclusive.",
				table)
		}
		// Redacted zips must stay redacted even when a query times out or
		// fails: a fallback that runs in place of the redacted query could
		// silently reintroduce sensitive data. If a redacted fallback ever
		// becomes necessary, it must redact at least as much as the primary
		// query, and this check should be replaced with one that verifies
		// that property.
		if regConfig.customQueryRedactedFallback != "" {
			t.Fatalf("table %q has a customQueryRedactedFallback; redacted queries must not have fallbacks", table)
		}
	}

	for table, regConfig := range zipInternalTablesPerCluster {
		validate(table, regConfig)
	}
	for table, regConfig := range zipInternalTablesPerNode {
		validate(table, regConfig)
	}
	for table, regConfig := range zipSystemTables {
		validate(table, regConfig)
	}
}

func executeAllCustomQuerys(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableRegistry DebugZipTableRegistry,
) {
	for table, regConfig := range tableRegistry {
		if regConfig.customQueryRedacted != "" {
			rows := sqlDB.Query(t, regConfig.customQueryRedacted)
			require.NoError(t, rows.Err(), "failed to select for table %s redacted", table)
		}

		if regConfig.customQueryUnredacted != "" {
			rows := sqlDB.Query(t, regConfig.customQueryUnredacted)
			require.NoError(t, rows.Err(), "failed to select for table %s unredacted", table)
		}

		if regConfig.customQueryUnredactedFallback != "" {
			rows := sqlDB.Query(t, regConfig.customQueryUnredactedFallback)
			require.NoError(t, rows.Err(), "failed to select for table %s unredacted fallback", table)
		}
	}
}

func TestCustomQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster := serverutils.StartCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// The zip queries include queries that are only meant to work
			// in a system tenant. These would fail if pointed to a
			// secondary tenant.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer cluster.Stopper().Stop(context.Background())
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	executeAllCustomQuerys(t, sqlDB, zipInternalTablesPerCluster)
	executeAllCustomQuerys(t, sqlDB, zipInternalTablesPerNode)
	executeAllCustomQuerys(t, sqlDB, zipSystemTables)
}

// requireNoSecretInQuery runs query and fails if secret appears in any column
// of any row it returns.
func requireNoSecretInQuery(t *testing.T, sqlDB *sqlutils.SQLRunner, secret string, query string) {
	t.Helper()
	rows := sqlDB.Query(t, query)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	for rows.Next() {
		require.NoError(t, rows.Scan(vals...))
		for i, v := range vals {
			raw := *(v.(*interface{}))
			// BYTES columns scan as []byte, which fmt renders as a list of
			// decimal byte values - the secret would slip through the
			// substring check below.
			s, ok := raw.([]byte)
			if !ok {
				s = []byte(fmt.Sprint(raw))
			}
			require.NotContainsf(t, string(s), secret,
				"secret leaked in column %s of query:\n%s", cols[i], query)
		}
	}
	require.NoError(t, rows.Err())
}

// TestSensitiveSettingScrubbedFromZipDumps verifies that the unredacted zip
// queries never emit the values of sensitive cluster settings: neither from
// historical eventlog rows (written before values were redacted at write
// time, including the raw statement text in the info payload), nor from
// events for retired/renamed settings that no longer match the registry, nor
// from session dumps whose last_active_query holds the raw SET statement.
func TestSensitiveSettingScrubbedFromZipDumps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	const secret = "hunter2-zip-secret"

	// Simulate pre-fix eventlog rows: one for a sensitive setting with the
	// raw value and statement text, one for a setting unknown to the
	// registry (retired/renamed), and one for a non-sensitive setting.
	sqlDB.Exec(t, "SET allow_unsafe_internals = true")
	insertEvent := func(name, value string) {
		info := fmt.Sprintf(
			`{"EventType": "set_cluster_setting", "SettingName": "%s", "Value": "%s", `+
				`"Statement": "SET CLUSTER SETTING %s = '%s'", "PlaceholderValues": ["'%s'"], "User": "root"}`,
			name, value, name, value, value)
		sqlDB.Exec(t,
			`INSERT INTO system.eventlog (timestamp, "eventType", "targetID", "reportingID", info) VALUES (now(), 'set_cluster_setting', 0, 1, $1)`,
			info)
	}
	insertEvent("cloudstorage.http.custom_ca", secret)
	insertEvent("some.retired.setting", secret)
	insertEvent("cluster.label", "not-a-secret")

	// Populate last_active_query with a sensitive SET, on a dedicated
	// connection so the test's own queries don't displace it. The session
	// stays open (idle) for the rest of the test, which is exactly the
	// leak scenario: last_active_query lingers until the next statement.
	setter := sqlutils.MakeSQLRunner(srv.SQLConn(t))
	setter.Exec(t, fmt.Sprintf(
		"SET CLUSTER SETTING cloudstorage.http.custom_ca = '%s'", secret))

	// Populate system.tenant_settings with a sensitive override.
	sqlDB.Exec(t, fmt.Sprintf(
		"ALTER TENANT ALL SET CLUSTER SETTING cloudstorage.http.custom_ca = '%s'", secret))

	for _, table := range []string{
		"system.eventlog",
		"system.settings",
		"system.tenant_settings",
		"cluster_settings_history",
		"crdb_internal.cluster_sessions",
		"crdb_internal.cluster_queries",
		"crdb_internal.node_sessions",
		"crdb_internal.node_queries",
	} {
		var regConfig TableRegistryConfig
		var ok bool
		for _, reg := range []DebugZipTableRegistry{
			zipInternalTablesPerCluster, zipInternalTablesPerNode, zipSystemTables,
		} {
			if regConfig, ok = reg[table]; ok {
				break
			}
		}
		require.Truef(t, ok, "table %s not found in any registry", table)
		require.NotEmptyf(t, regConfig.customQueryUnredacted, "no unredacted query for %s", table)
		requireNoSecretInQuery(t, sqlDB, secret, regConfig.customQueryUnredacted)
		// The fallback runs whenever the primary query fails - most often
		// against a server predating crdb_internal.cluster_settings.sensitive,
		// but also when the primary times out on a busy cluster. It must hold
		// the secret back just as tightly.
		if regConfig.customQueryUnredactedFallback != "" {
			requireNoSecretInQuery(t, sqlDB, secret, regConfig.customQueryUnredactedFallback)
		}
	}

	// The non-sensitive event passes through intact, including its statement.
	var info string
	sqlDB.QueryRow(t,
		"SELECT info FROM ("+zipSystemTables["system.eventlog"].customQueryUnredacted+
			`) WHERE info LIKE '%cluster.label%' AND info LIKE '%not-a-secret%'`,
	).Scan(&info)
	require.Contains(t, info, `SET CLUSTER SETTING cluster.label = 'not-a-secret'`)
}

// TestSettingsZipDumpsJoinOnInternalKey verifies that the system.settings and
// system.tenant_settings dumps match rows against the setting registry by
// internal key. Their `name` column holds the key, not the user-visible name,
// so joining on the name silently loses every setting renamed via WithName():
// the row is either dropped from the dump or conservatively redacted.
func TestSettingsZipDumpsJoinOnInternalKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	// A non-sensitive setting whose user-visible name differs from its
	// internal key. The assertion below keeps the test honest if the setting
	// is ever un-renamed - pick another renamed setting rather than dropping
	// the coverage.
	const name = "kv.transaction.write_pipelining.enabled"
	const key = "kv.transaction.write_pipelining_enabled"
	var gotKey string
	sqlDB.QueryRow(t,
		"SELECT key FROM crdb_internal.cluster_settings WHERE variable = $1", name).Scan(&gotKey)
	require.Equalf(t, key, gotKey,
		"%s is no longer a renamed setting; this test needs one to be meaningful", name)

	sqlDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = false", name))
	sqlDB.Exec(t, fmt.Sprintf("ALTER TENANT ALL SET CLUSTER SETTING %s = false", name))

	for _, tc := range []struct {
		table string
		query string
	}{
		{"system.settings", zipSystemTables["system.settings"].customQueryUnredacted},
		{"system.settings (redacted)", zipSystemTables["system.settings"].customQueryRedacted},
		{"system.tenant_settings", zipSystemTables["system.tenant_settings"].customQueryUnredacted},
	} {
		t.Run(tc.table, func(t *testing.T) {
			rows := sqlDB.QueryStr(t,
				"SELECT value FROM ("+tc.query+") WHERE name = $1", key)
			require.Lenf(t, rows, 1, "row for %s missing from the %s dump", key, tc.table)
			require.Equal(t, "false", rows[0][0])
		})
	}
}

// TestStoredCredentialsScrubbedFromZipDumps verifies that the unredacted zip
// queries do not emit the columns that hold endpoint credentials: the encoded
// connection URI in system.external_connections, and the scheduled statement
// in system.scheduled_jobs. Both carry cloud storage keys in URI query params.
func TestStoredCredentialsScrubbedFromZipDumps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(srv.SQLConn(t))

	const secret = "hunter2-zip-access-key"

	// The rows are written directly rather than through CREATE EXTERNAL
	// CONNECTION / CREATE SCHEDULE, which would try to reach the endpoint. All
	// the dumps see is the encoded proto, so the bytes only need to contain
	// the secret, not be a well-formed one.
	sqlDB.Exec(t, "SET allow_unsafe_internals = true")
	sqlDB.Exec(t, `INSERT INTO system.external_connections
		(connection_name, connection_type, connection_details, owner, owner_id)
		VALUES ('backup-target', 'STORAGE', $1, 'root', 1)`,
		[]byte("s3://bucket/path?AWS_SECRET_ACCESS_KEY="+secret))
	sqlDB.Exec(t, `INSERT INTO system.scheduled_jobs
		(schedule_id, schedule_name, owner, executor_type, execution_args)
		VALUES (1, 'nightly', 'root', 'scheduled-backup-executor', $1)`,
		[]byte("BACKUP INTO 's3://bucket/path?AWS_SECRET_ACCESS_KEY="+secret+"'"))

	for _, tc := range []struct {
		table string
		// key identifies the row inserted above, so the check cannot pass
		// just because the dump returned nothing.
		key   string
		value string
	}{
		{"system.external_connections", "connection_name = 'backup-target'", "connection_details"},
		{"system.scheduled_jobs", "schedule_id = 1", "execution_args"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			// Go through QueryForTable rather than reading the registry entry
			// directly: without an unredacted query it hands back a plain
			// `TABLE`, which is exactly the leak being guarded against.
			tableQuery, err := zipSystemTables.QueryForTable(tc.table, false /* redact */)
			require.NoError(t, err)
			query := tableQuery.query
			requireNoSecretInQuery(t, sqlDB, secret, query)

			rows := sqlDB.QueryStr(t,
				fmt.Sprintf("SELECT %s FROM (%s) WHERE %s", tc.value, query, tc.key))
			require.Lenf(t, rows, 1, "row missing from the %s dump", tc.table)
			require.Equal(t, "<redacted>", rows[0][0])
		})
	}
}

func executeSelectOnNonSensitiveColumns(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableRegistry DebugZipTableRegistry,
) {

	for table, regConfig := range tableRegistry {
		if len(regConfig.nonSensitiveCols) != 0 {
			columns := strings.Join(regConfig.nonSensitiveCols[:], ",")
			rows := sqlDB.Query(t, fmt.Sprintf("SELECT %s FROM %s", columns, table))
			require.NoError(t, rows.Err(), "failed to select non sensitive columns on table %s", table)
		}
	}
}

func TestNonSensitiveColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cluster := serverutils.StartCluster(t, 1 /* numNodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// The zip queries include queries that are only meant to work
			// in a system tenant. These would fail if pointed to a
			// secondary tenant.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			//We are seeing certificate issue in CI test job. Hence,we are
			//running cluster in insecure mode.
			Insecure: true,
		},
	})
	defer cluster.Stopper().Stop(context.Background())
	testConn := cluster.ServerConn(0 /* idx */)
	sqlDB := sqlutils.MakeSQLRunner(testConn)

	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerCluster)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipInternalTablesPerNode)
	executeSelectOnNonSensitiveColumns(t, sqlDB, zipSystemTables)
}
