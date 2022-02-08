// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	tc, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	kvDB := tc.Server(0).DB()
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()
	sqlDB.Exec(t, `
SET CLUSTER SETTING sql.cross_db_fks.enabled = TRUE;
CREATE TYPE data.welcome AS ENUM ('hello', 'hi');
USE data; CREATE SCHEMA sc;
CREATE TABLE data.sc.t1 (a INT);
CREATE TABLE data.sc.t2 (a data.welcome);
`)

	const full, inc, inc2 = localFoo + "/full", localFoo + "/inc", localFoo + "/inc2"

	beforeTS := sqlDB.QueryStr(t, `SELECT now()::timestamp::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, beforeTS), full)

	res := sqlDB.QueryStr(t, `
SELECT
  database_name, parent_schema_name, object_name, object_type, backup_type,
  start_time::string, end_time::string, rows, is_full_cluster
FROM
	[SHOW BACKUP $1]
ORDER BY object_type, object_name`, full)
	expectedObjects := [][]string{
		{"NULL", "NULL", "data", "database", "full", "NULL", beforeTS, "NULL", "false"},
		{"data", "NULL", "public", "schema", "full", "NULL", beforeTS, "NULL", "false"},
		{"data", "NULL", "sc", "schema", "full", "NULL", beforeTS, "NULL", "false"},
		{"data", "public", "bank", "table", "full", "NULL", beforeTS, strconv.Itoa(numAccounts), "false"},
		{"data", "sc", "t1", "table", "full", "NULL", beforeTS, strconv.Itoa(0), "false"},
		{"data", "sc", "t2", "table", "full", "NULL", beforeTS, strconv.Itoa(0), "false"},
		{"data", "public", "_welcome", "type", "full", "NULL", beforeTS, "NULL", "false"},
		{"data", "public", "welcome", "type", "full", "NULL", beforeTS, "NULL", "false"},
	}
	require.Equal(t, expectedObjects, res)

	// Mess with half the rows.
	affectedRows, err := sqlDB.Exec(t,
		`UPDATE data.bank SET id = -1 * id WHERE id > $1`, numAccounts/2,
	).RowsAffected()
	require.NoError(t, err)
	require.Equal(t, numAccounts/2, int(affectedRows))

	// Backup the changes by appending to the base and by making a separate
	// inc backup.
	incTS := sqlDB.QueryStr(t, `SELECT now()::timestamp::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, incTS), full)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2`, incTS), inc, full)

	// Check the appended base backup.
	res = sqlDB.QueryStr(t, `SELECT object_name, backup_type, start_time::string, end_time::string, rows, is_full_cluster FROM [SHOW BACKUP $1]`, full)
	require.Equal(t, [][]string{
		// Full.
		{"data", "full", "NULL", beforeTS, "NULL", "false"},
		{"public", "full", "NULL", beforeTS, "NULL", "false"},
		{"bank", "full", "NULL", beforeTS, strconv.Itoa(numAccounts), "false"},
		{"welcome", "full", "NULL", beforeTS, "NULL", "false"},
		{"_welcome", "full", "NULL", beforeTS, "NULL", "false"},
		{"sc", "full", "NULL", beforeTS, "NULL", "false"},
		{"t1", "full", "NULL", beforeTS, "0", "false"},
		{"t2", "full", "NULL", beforeTS, "0", "false"},
		// Incremental.
		{"data", "incremental", beforeTS, incTS, "NULL", "false"},
		{"public", "incremental", beforeTS, incTS, "NULL", "false"},
		{"bank", "incremental", beforeTS, incTS, strconv.Itoa(int(affectedRows * 2)), "false"},
		{"welcome", "incremental", beforeTS, incTS, "NULL", "false"},
		{"_welcome", "incremental", beforeTS, incTS, "NULL", "false"},
		{"sc", "incremental", beforeTS, incTS, "NULL", "false"},
		{"t1", "incremental", beforeTS, incTS, "0", "false"},
		{"t2", "incremental", beforeTS, incTS, "0", "false"},
	}, res)

	// Check the separate inc backup.
	res = sqlDB.QueryStr(t, `SELECT start_time::string, end_time::string, rows FROM [SHOW BACKUP $1] WHERE object_name='bank'`, inc)
	require.Equal(t, [][]string{
		{beforeTS, incTS, strconv.Itoa(int(affectedRows * 2))},
	}, res)

	// Create two new tables, alphabetically on either side of bank.
	sqlDB.Exec(t, `CREATE TABLE data.auth (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `CREATE TABLE data.users (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.users VALUES (1, 'one'), (2, 'two'), (3, 'three')`)

	// Backup the changes again, by appending to the base and by making a
	// separate inc backup.
	inc2TS := sqlDB.QueryStr(t, `SELECT now()::timestamp::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, inc2TS), full)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2, $3`, inc2TS), inc2, full, inc)

	// Check the appended base backup.
	res = sqlDB.QueryStr(t, `SELECT object_name, backup_type, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1] WHERE object_type='table'`, full)
	require.Equal(t, [][]string{
		{"bank", "full", "NULL", beforeTS, strconv.Itoa(numAccounts)},
		{"t1", "full", "NULL", beforeTS, "0"},
		{"t2", "full", "NULL", beforeTS, "0"},
		{"bank", "incremental", beforeTS, incTS, strconv.Itoa(int(affectedRows * 2))},
		{"t1", "incremental", beforeTS, incTS, "0"},
		{"t2", "incremental", beforeTS, incTS, "0"},
		{"bank", "incremental", incTS, inc2TS, "0"},
		{"t1", "incremental", incTS, inc2TS, "0"},
		{"t2", "incremental", incTS, inc2TS, "0"},
		{"auth", "incremental", incTS, inc2TS, "0"},
		{"users", "incremental", incTS, inc2TS, "3"},
	}, res)

	// Check the separate inc backup.
	res = sqlDB.QueryStr(t, `SELECT object_name, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1] WHERE object_type='table'`, inc2)
	require.Equal(t, [][]string{
		{"bank", incTS, inc2TS, "0"},
		{"t1", incTS, inc2TS, "0"},
		{"t2", incTS, inc2TS, "0"},
		{"auth", incTS, inc2TS, "0"},
		{"users", incTS, inc2TS, "3"},
	}, res)

	const details = localFoo + "/details"
	sqlDB.Exec(t, `CREATE TABLE data.details1 (c INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.details1 (SELECT generate_series(1, 100))`)
	sqlDB.Exec(t, `ALTER TABLE data.details1 SPLIT AT VALUES (1), (42)`)
	sqlDB.Exec(t, `CREATE TABLE data.details2()`)
	sqlDB.Exec(t, `BACKUP data.details1, data.details2 TO $1;`, details)

	details1Desc := desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), keys.SystemSQLCodec, "data", "details1")
	details2Desc := desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), keys.SystemSQLCodec, "data", "details2")
	d1ID := details1Desc.GetID()
	d2ID := details2Desc.GetID()
	details1Key := roachpb.Key(rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, d1ID, details1Desc.GetPrimaryIndexID()))
	details2Key := roachpb.Key(rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, d2ID, details2Desc.GetPrimaryIndexID()))

	sqlDBRestore.CheckQueryResults(t, fmt.Sprintf(`SHOW BACKUP RANGES '%s'`, details), [][]string{
		{
			fmt.Sprintf("/Table/%d/1", d1ID),
			fmt.Sprintf("/Table/%d/2", d1ID),
			string(details1Key),
			string(details1Key.PrefixEnd())},
		{
			fmt.Sprintf("/Table/%d/1", d2ID),
			fmt.Sprintf("/Table/%d/2", d2ID),
			string(details2Key),
			string(details2Key.PrefixEnd()),
		},
	})

	var showFiles = fmt.Sprintf(`SELECT start_pretty, end_pretty, size_bytes, rows
		FROM [SHOW BACKUP FILES '%s']`, details)
	sqlDBRestore.CheckQueryResults(t, showFiles, [][]string{
		{
			fmt.Sprintf("/Table/%d/1/1", d1ID),
			fmt.Sprintf("/Table/%d/1/42", d1ID),
			"410", "41",
		},
		{
			fmt.Sprintf("/Table/%d/1/42", d1ID),
			fmt.Sprintf("/Table/%d/2", d1ID),
			"590", "59",
		},
	})
	sstMatcher := regexp.MustCompile(`\d+\.sst`)
	pathRows := sqlDB.QueryStr(t, `SELECT path FROM [SHOW BACKUP FILES $1]`, details)
	for _, row := range pathRows {
		path := row[0]
		if matched := sstMatcher.MatchString(path); !matched {
			t.Errorf("malformatted path in SHOW BACKUP FILES: %s", path)
		}
	}
	if len(pathRows) != 2 {
		t.Fatalf("expected 2 files, but got %d", len(pathRows))
	}

	// SCHEMAS: Test the creation statement.
	var showBackupRows [][]string
	var expected []string

	// Test that tables, views and sequences are all supported.
	{
		viewTableSeq := localFoo + "/tableviewseq"
		sqlDB.Exec(t, `CREATE TABLE data.tableA (a int primary key, b int, INDEX tableA_b_idx (b ASC))`)
		sqlDB.Exec(t, `CREATE VIEW data.viewA AS SELECT a from data.tableA`)
		sqlDB.Exec(t, `CREATE SEQUENCE data.seqA START 1 INCREMENT 2 MAXVALUE 20`)
		sqlDB.Exec(t, `BACKUP data.tableA, data.viewA, data.seqA TO $1;`, viewTableSeq)

		// Create tables with the same ID as data.tableA to ensure that comments
		// from different tables in the restoring cluster don't appear.
		tableA := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "data", "tablea")
		for i := bootstrap.TestingUserDescID(0); i < uint32(tableA.GetID()); i++ {
			tableName := fmt.Sprintf("foo%d", i)
			sqlDBRestore.Exec(t, fmt.Sprintf("CREATE TABLE %s ();", tableName))
			sqlDBRestore.Exec(t, fmt.Sprintf("COMMENT ON TABLE %s IS 'table comment'", tableName))
		}

		expectedCreateTable := `CREATE TABLE tablea (
		a INT8 NOT NULL,
		b INT8 NULL,
		CONSTRAINT tablea_pkey PRIMARY KEY (a ASC),
		INDEX tablea_b_idx (b ASC)
	)`
		expectedCreateView := "CREATE VIEW viewa (\n\ta\n) AS SELECT a FROM data.public.tablea"
		expectedCreateSeq := `CREATE SEQUENCE seqa MINVALUE 1 MAXVALUE 20 INCREMENT 2 START 1`

		showBackupRows = sqlDBRestore.QueryStr(t,
			fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS '%s'] WHERE object_type='table'`, viewTableSeq))
		expected = []string{
			expectedCreateTable,
			expectedCreateView,
			expectedCreateSeq,
		}
		for i, row := range showBackupRows {
			createStmt := row[0]
			if !eqWhitespace(createStmt, expected[i]) {
				t.Fatalf("mismatched create statement: %s, want %s", createStmt, expected[i])
			}
		}
	}

	// Test that foreign keys that reference tables that are in the backup
	// are included.
	{
		includedFK := localFoo + "/includedFK"
		sqlDB.Exec(t, `CREATE TABLE data.FKSrc (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE data.FKRefTable (a INT PRIMARY KEY, B INT REFERENCES data.FKSrc(a))`)
		sqlDB.Exec(t, `CREATE DATABASE data2`)
		sqlDB.Exec(t, `CREATE TABLE data2.FKRefTable (a INT PRIMARY KEY, B INT REFERENCES data.FKSrc(a))`)
		sqlDB.Exec(t, `BACKUP data.FKSrc, data.FKRefTable, data2.FKRefTable TO $1;`, includedFK)

		wantSameDB := `CREATE TABLE fkreftable (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT fkreftable_pkey PRIMARY KEY (a ASC),
				CONSTRAINT fkreftable_b_fkey FOREIGN KEY (b) REFERENCES public.fksrc(a)
			)`
		wantDiffDB := `CREATE TABLE fkreftable (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT fkreftable_pkey PRIMARY KEY (a ASC),
				CONSTRAINT fkreftable_b_fkey FOREIGN KEY (b) REFERENCES data.public.fksrc(a)
			)`

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS '%s'] WHERE object_type='table'`, includedFK))
		createStmtSameDB := showBackupRows[1][0]
		if !eqWhitespace(createStmtSameDB, wantSameDB) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmtSameDB, wantSameDB)
		}

		createStmtDiffDB := showBackupRows[2][0]
		if !eqWhitespace(createStmtDiffDB, wantDiffDB) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmtDiffDB, wantDiffDB)
		}
	}

	// Foreign keys that were not included in the backup are not mentioned in
	// the create statement.
	{
		missingFK := localFoo + "/missingFK"
		sqlDB.Exec(t, `BACKUP data2.FKRefTable TO $1;`, missingFK)

		want := `CREATE TABLE fkreftable (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT fkreftable_pkey PRIMARY KEY (a ASC)
			)`

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS '%s'] WHERE object_type='table'`, missingFK))
		createStmt := showBackupRows[0][0]
		if !eqWhitespace(createStmt, want) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmt, want)
		}
	}

	{
		fullCluster := localFoo + "/full_cluster"
		sqlDB.Exec(t, `BACKUP TO $1;`, fullCluster)

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT is_full_cluster FROM [SHOW BACKUP '%s']`, fullCluster))
		isFullCluster := showBackupRows[0][0]
		if !eqWhitespace(isFullCluster, "true") {
			t.Fatal("expected show backup to indicate that backup was full cluster")
		}

		fullClusterInc := localFoo + "/full_cluster_inc"
		sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2;`, fullClusterInc, fullCluster)

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT is_full_cluster FROM [SHOW BACKUP '%s']`, fullCluster))
		isFullCluster = showBackupRows[0][0]
		if !eqWhitespace(isFullCluster, "true") {
			t.Fatal("expected show backup to indicate that backup was full cluster")
		}
	}

	// Show privileges of descriptors that are backed up.
	{
		showPrivs := localFoo + "/show_privs"
		sqlDB.Exec(t, `
CREATE DATABASE mi5; USE mi5;
CREATE SCHEMA locator;
CREATE TYPE locator.continent AS ENUM ('amer', 'eu', 'afr', 'asia', 'au', 'ant');
CREATE TABLE locator.agent_locations (id INT PRIMARY KEY, location locator.continent);
CREATE TABLE top_secret (id INT PRIMARY KEY, name STRING);

CREATE USER agent_bond;
CREATE USER agent_thomas;
CREATE USER m;
CREATE ROLE agents;

GRANT agents TO agent_bond;
GRANT agents TO agent_thomas;

GRANT ALL ON DATABASE mi5 TO agents;
--REVOKE UPDATE ON DATABASE mi5 FROM agents;

GRANT ALL ON SCHEMA locator TO m;
GRANT ALL ON SCHEMA locator TO agent_bond;
REVOKE USAGE ON SCHEMA locator FROM agent_bond;

GRANT ALL ON TYPE locator.continent TO m;
GRANT ALL ON TYPE locator.continent TO agent_bond;
REVOKE USAGE ON TYPE locator.continent FROM agent_bond;

GRANT ALL ON TABLE locator.agent_locations TO m;
GRANT UPDATE ON locator.agent_locations TO agents;
GRANT SELECT ON locator.agent_locations TO agent_bond;

GRANT ALL ON top_secret TO m;
GRANT INSERT ON top_secret TO agents;
GRANT SELECT ON top_secret TO agent_bond;
GRANT UPDATE ON top_secret TO agent_bond;
`)
		sqlDB.Exec(t, `BACKUP DATABASE mi5 TO $1;`, showPrivs)

		want := [][]string{
			{`mi5`, `database`, `GRANT ALL ON mi5 TO admin; GRANT ALL ` +
				`ON mi5 TO agents; GRANT CONNECT ON mi5 TO public; GRANT ALL ON mi5 TO root; `, `root`},
			{`public`, `schema`, `GRANT ALL ON public TO admin; GRANT CREATE, USAGE ON public TO public; GRANT ALL ON public TO root; `, `admin`},
			{`locator`, `schema`, `GRANT ALL ON locator TO admin; GRANT CREATE, GRANT ON locator TO agent_bond; GRANT ALL ON locator TO m; ` +
				`GRANT ALL ON locator TO root; `, `root`},
			{`continent`, `type`, `GRANT ALL ON continent TO admin; GRANT GRANT ON continent TO agent_bond; GRANT ALL ON continent TO m; GRANT USAGE ON continent TO public; GRANT ALL ON continent TO root; `, `root`},
			{`_continent`, `type`, `GRANT ALL ON _continent TO admin; GRANT USAGE ON _continent TO public; GRANT ALL ON _continent TO root; `, `root`},
			{`agent_locations`, `table`, `GRANT ALL ON agent_locations TO admin; ` +
				`GRANT SELECT ON agent_locations TO agent_bond; GRANT UPDATE ON agent_locations TO agents; ` +
				`GRANT ALL ON agent_locations TO m; GRANT ALL ON agent_locations TO root; `, `root`},
			{`top_secret`, `table`, `GRANT ALL ON top_secret TO admin; ` +
				`GRANT SELECT, UPDATE ON top_secret TO agent_bond; GRANT INSERT ON top_secret TO agents; ` +
				`GRANT ALL ON top_secret TO m; GRANT ALL ON top_secret TO root; `, `root`},
		}

		showQuery := fmt.Sprintf(`SELECT object_name, object_type, privileges, owner FROM [SHOW BACKUP '%s' WITH privileges]`, showPrivs)
		sqlDBRestore.CheckQueryResults(t, showQuery, want)

		// Change the owner and expect the changes to be reflected in a new backup
		showOwner := localFoo + "/show_owner"
		sqlDB.Exec(t, `
ALTER DATABASE mi5 OWNER TO agent_thomas;
ALTER SCHEMA locator OWNER TO agent_thomas;
ALTER TYPE locator.continent OWNER TO agent_bond;
ALTER TABLE locator.agent_locations OWNER TO agent_bond;
`)
		sqlDB.Exec(t, `BACKUP DATABASE mi5 TO $1;`, showOwner)

		want = [][]string{
			{`agent_thomas`},
			{`admin`},
			{`agent_thomas`},
			{`agent_bond`},
			{`agent_bond`},
			{`agent_bond`},
			{`root`},
		}

		showQuery = fmt.Sprintf(`SELECT owner FROM [SHOW BACKUP '%s' WITH privileges]`, showOwner)
		sqlDBRestore.CheckQueryResults(t, showQuery, want)
	}
}

func eqWhitespace(a, b string) bool {
	return strings.Replace(a, "\t", "", -1) == strings.Replace(b, "\t", "", -1)
}

func TestShowBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	const full = localFoo + "/full"
	const remoteInc = localFoo + "/inc"

	// Make an initial backup.
	sqlDB.Exec(t, `BACKUP data.bank INTO $1`, full)
	// Add Incremental changes to it 3 times.
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	// Make a second full backup, add changes to it twice.
	sqlDB.Exec(t, `BACKUP data.bank INTO $1`, full)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	// Make a third full backup, add changes to it.
	sqlDB.Exec(t, `BACKUP data.bank INTO $1`, full)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1`, full)
	// Make 2 remote incremental backups, chaining to the third full backup
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1 WITH incremental_location = $2`, full, remoteInc)
	sqlDB.Exec(t, `BACKUP data.bank INTO LATEST IN $1 WITH incremental_location = $2`, full, remoteInc)

	rows := sqlDBRestore.QueryStr(t, `SHOW BACKUPS IN $1`, full)

	// assert that we see the three, and only the three, full backups.
	require.Equal(t, 3, len(rows))

	// check that we can show the inc layers in the individual full backups.
	b1 := sqlDBRestore.QueryStr(t, `SELECT * FROM [SHOW BACKUP $1 IN $2] WHERE object_type='table'`, rows[0][0], full)
	require.Equal(t, 4, len(b1))
	b2 := sqlDBRestore.QueryStr(t, `SELECT * FROM [SHOW BACKUP $1 IN $2] WHERE object_type='table'`, rows[1][0], full)
	require.Equal(t, 3, len(b2))

	require.Equal(t,
		sqlDBRestore.QueryStr(t, `SHOW BACKUP $1 IN $2`, rows[2][0], full),
		sqlDBRestore.QueryStr(t, `SHOW BACKUP LATEST IN $1`, full),
	)

	// check that full and remote incremental backups appear
	b3 := sqlDBRestore.QueryStr(t,
		`SELECT * FROM [SHOW BACKUP LATEST IN $1 WITH incremental_location= 'nodelocal://0/foo/inc'] WHERE object_type='table'`, full)
	require.Equal(t, 3, len(b3))

}

func TestShowBackupTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	tc, systemDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, 20 are embedded. See:
	_ = security.EmbeddedTenantIDs()

	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)
	beforeTS := systemDB.QueryStr(t, `SELECT now()::timestamp::string`)[0][0]

	systemDB.Exec(t, fmt.Sprintf(`BACKUP TENANT 10 TO 'nodelocal://1/t10' AS OF SYSTEM TIME '%s'`, beforeTS))

	res := systemDB.QueryStr(t, `SELECT object_name, object_type, start_time::string, end_time::string, rows FROM [SHOW BACKUP 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL", beforeTS, "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT object_name, object_type, privileges FROM [SHOW BACKUP 'nodelocal://1/t10' WITH privileges]`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT object_name, object_type, create_statement FROM [SHOW BACKUP SCHEMAS 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT start_pretty, end_pretty FROM [SHOW BACKUP RANGES 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"/Tenant/10", "/Tenant/11"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT start_pretty, end_pretty FROM [SHOW BACKUP FILES 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"/Tenant/10", "/Tenant/11"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT database_id, parent_schema_id, object_id FROM [SHOW BACKUP 'nodelocal://1/t10' WITH debug_ids]`)
	require.Equal(t, [][]string{
		{"NULL", "NULL", "10"},
	}, res)
}

func TestShowBackupPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER testuser`)
	sqlDB.Exec(t, `CREATE TABLE privs (a INT)`)

	pgURL, cleanup := sqlutils.PGUrl(t, srv.ServingSQLAddr(),
		"TestShowBackupPrivileges-testuser", url.User("testuser"))
	defer cleanup()
	testuser, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, testuser.Close())
	}()

	// Make an initial backup.
	const full = localFoo + "/full"
	sqlDB.Exec(t, `BACKUP privs INTO $1`, full)
	// Add an incremental backup to it.
	sqlDB.Exec(t, `BACKUP privs INTO LATEST IN $1`, full)
	// Make a second full backup using non into syntax.
	sqlDB.Exec(t, `BACKUP TO $1;`, full)

	_, err = testuser.Exec(`SHOW BACKUPS IN $1`, full)
	require.True(t, testutils.IsError(err,
		"only users with the admin role are allowed to SHOW BACKUP from the specified nodelocal URI"))

	_, err = testuser.Exec(`SHOW BACKUP $1`, full)
	require.True(t, testutils.IsError(err,
		"only users with the admin role are allowed to SHOW BACKUP from the specified nodelocal URI"))

	sqlDB.Exec(t, `GRANT admin TO testuser`)
	_, err = testuser.Exec(`SHOW BACKUPS IN $1`, full)
	require.NoError(t, err)

	_, err = testuser.Exec(`SHOW BACKUP $1`, full)
	require.NoError(t, err)
}

func TestShowUpgradedForeignKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		testdataBase = testutils.TestDataPath(t, "restore_old_versions")
		fkRevDirs    = testdataBase + "/fk-rev-history"
	)

	dirs, err := ioutil.ReadDir(fkRevDirs)
	require.NoError(t, err)
	for _, dir := range dirs {
		require.True(t, dir.IsDir())
		exportDir, err := filepath.Abs(filepath.Join(fkRevDirs, dir.Name()))
		require.NoError(t, err)
		t.Run(dir.Name(), showUpgradedForeignKeysTest(exportDir))
	}
}

func showUpgradedForeignKeysTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)

		type testCase struct {
			table                     string
			expectedForeignKeyPattern string
		}
		for _, tc := range []testCase{
			{
				"circular",
				"CONSTRAINT self_fk FOREIGN KEY \\(selfid\\) REFERENCES public\\.circular\\(selfid\\) NOT VALID",
			},
			{
				"child",
				"CONSTRAINT \\w+ FOREIGN KEY \\(\\w+\\) REFERENCES public\\.parent\\(\\w+\\)",
			},
			{
				"child_pk",
				"CONSTRAINT \\w+ FOREIGN KEY \\(\\w+\\) REFERENCES public\\.parent\\(\\w+\\)",
			},
		} {
			results := sqlDB.QueryStr(t, `
				SELECT
					create_statement
				FROM
					[SHOW BACKUP SCHEMAS $1]
				WHERE
					object_type = 'table' AND object_name = $2
				`, localFoo, tc.table)
			require.NotEmpty(t, results)
			require.Regexp(t, regexp.MustCompile(tc.expectedForeignKeyPattern), results[0][0])
		}
	}
}

func TestShowBackupWithDebugIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	// Create test database with bank table
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// add 1 type, 1 schema, and 2 tables to the database
	sqlDB.Exec(t, `
		SET CLUSTER SETTING sql.cross_db_fks.enabled = TRUE;
		CREATE TYPE data.welcome AS ENUM ('hello', 'hi');
		USE data; CREATE SCHEMA sc;
		CREATE TABLE data.sc.t1 (a INT);
		CREATE TABLE data.sc.t2 (a data.welcome);
  `)

	const full = localFoo + "/full"

	beforeTS := sqlDB.QueryStr(t, `SELECT now()::timestamp::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, beforeTS), full)

	// extract the object IDs for the database and public schema
	databaseRow := sqlDB.QueryStr(t, `
		SELECT database_name, database_id, parent_schema_name, parent_schema_id, object_name, object_id, object_type
		FROM [SHOW BACKUP $1 WITH debug_ids]
		WHERE object_name = 'bank'`, full)
	require.NotEmpty(t, databaseRow)
	dbID, err := strconv.Atoi(databaseRow[0][1])
	require.NoError(t, err)
	publicID, err := strconv.Atoi(databaseRow[0][3])
	require.NoError(t, err)

	require.Greater(t, dbID, 0)
	require.Greater(t, publicID, 0)

	res := sqlDB.QueryStr(t, `
		SELECT database_name, database_id, parent_schema_name, parent_schema_id, object_name, object_id, object_type
		FROM [SHOW BACKUP $1 WITH debug_ids]
		ORDER BY object_id`, full)

	dbIDStr := strconv.Itoa(dbID)
	publicIDStr := strconv.Itoa(publicID)
	schemaIDStr := strconv.Itoa(dbID + 5)

	expectedObjects := [][]string{
		{"NULL", "NULL", "NULL", "NULL", "data", dbIDStr, "database"},
		{"data", dbIDStr, "NULL", "NULL", "public", strconv.Itoa(dbID + 1), "schema"},
		{"data", dbIDStr, "public", publicIDStr, "bank", strconv.Itoa(dbID + 2), "table"},
		{"data", dbIDStr, "public", publicIDStr, "welcome", strconv.Itoa(dbID + 3), "type"},
		{"data", dbIDStr, "public", publicIDStr, "_welcome", strconv.Itoa(dbID + 4), "type"},
		{"data", dbIDStr, "NULL", "NULL", "sc", schemaIDStr, "schema"},
		{"data", dbIDStr, "sc", schemaIDStr, "t1", strconv.Itoa(dbID + 6), "table"},
		{"data", dbIDStr, "sc", schemaIDStr, "t2", strconv.Itoa(dbID + 7), "table"},
	}

	require.Equal(t, expectedObjects, res)

}

func TestShowBackupPathIsCollectionRoot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11

	// Create test database with bank table.
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// Make an initial backup.
	sqlDB.Exec(t, `BACKUP data.bank INTO $1`, localFoo)

	// Ensure proper error gets returned from back SHOW BACKUP Path
	sqlDB.ExpectErr(t, "The specified path is the root of a backup collection.",
		"SHOW BACKUP $1", localFoo)
}
