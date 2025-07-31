// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TODO (msbutler): when refactoring these tests to data driven tests, keep the
// original go test on 22.1 and only use new backup syntax in data driven test

func TestShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	tc, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	kvDB := tc.Server(0).ApplicationLayer().DB()
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()
	sqlDB.ExecMultiple(t, strings.Split(`
SET CLUSTER SETTING sql.cross_db_fks.enabled = TRUE;
SET CLUSTER SETTING bulkio.backup.file_size = '1';
CREATE TYPE data.welcome AS ENUM ('hello', 'hi');
USE data; CREATE SCHEMA sc;
CREATE TABLE data.sc.t1 (a INT);
CREATE TABLE data.sc.t2 (a data.welcome);
`,
		`;`)...)

	const full, inc = localFoo + "/full", localFoo + "/inc"

	beforeTS := sqlDB.QueryStr(t, `SELECT now()::timestamptz::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data INTO $1 AS OF SYSTEM TIME '%s'`, beforeTS), full)

	res := sqlDB.QueryStr(t, `
SELECT
  database_name, parent_schema_name, object_name, object_type, backup_type,
  start_time::string, end_time::string, rows, is_full_cluster
FROM
	[SHOW BACKUP FROM LATEST IN $1]
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
	incTS := sqlDB.QueryStr(t, `SELECT now()::timestamptz::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data INTO LATEST IN $1 AS OF SYSTEM TIME '%s' WITH incremental_location = $2`, incTS), full, inc)

	// Check the appended base backup.
	res = sqlDB.QueryStr(t, `SELECT object_name, backup_type, start_time::string, end_time::string, rows, is_full_cluster FROM [SHOW BACKUP FROM LATEST IN $1 WITH incremental_location = $2]`, full, inc)
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

	// Create two new tables, alphabetically on either side of bank.
	sqlDB.Exec(t, `CREATE TABLE data.auth (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `CREATE TABLE data.users (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.users VALUES (1, 'one'), (2, 'two'), (3, 'three')`)

	// Backup the changes again, by making a new inc backup.
	inc2TS := sqlDB.QueryStr(t, `SELECT now()::timestamptz::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data INTO LATEST IN $1 AS OF SYSTEM TIME '%s' WITH incremental_location = $2`, inc2TS), full, inc)

	// Check the backup.
	res = sqlDB.QueryStr(t, `SELECT object_name, backup_type, start_time::string, end_time::string, rows FROM [SHOW BACKUP FROM LATEST IN $1 WITH incremental_location = $2] WHERE object_type='table'`, full, inc)
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

	const details = localFoo + "/details"
	sqlDB.Exec(t, `CREATE TABLE data.details1 (c INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.details1 (SELECT generate_series(1, 100))`)
	sqlDB.Exec(t, `ALTER TABLE data.details1 SPLIT AT VALUES (1), (42)`)
	sqlDB.Exec(t, `CREATE TABLE data.details2()`)
	sqlDB.Exec(t, `BACKUP data.details1, data.details2 INTO $1;`, details)

	codec := tc.ApplicationLayer(0).Codec()
	details1Desc := desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), codec, "data", "details1")
	details2Desc := desctestutils.TestingGetPublicTableDescriptor(tc.Server(0).DB(), codec, "data", "details2")
	d1ID := details1Desc.GetID()
	d2ID := details2Desc.GetID()
	details1Key := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, d1ID, details1Desc.GetPrimaryIndexID()))
	details2Key := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, d2ID, details2Desc.GetPrimaryIndexID()))

	var prefix string
	if !tc.ApplicationLayer(0).Codec().ForSystemTenant() {
		prefix = tc.ApplicationLayer(0).Codec().TenantPrefix().String()
	}
	sqlDBRestore.CheckQueryResults(t, fmt.Sprintf(`SHOW BACKUP RANGES FROM LATEST IN '%s'`, details), [][]string{
		{
			fmt.Sprintf(prefix+"/Table/%d/1", d1ID),
			fmt.Sprintf(prefix+"/Table/%d/2", d1ID),
			string(details1Key),
			string(details1Key.PrefixEnd())},
		{
			fmt.Sprintf(prefix+"/Table/%d/1", d2ID),
			fmt.Sprintf(prefix+"/Table/%d/2", d2ID),
			string(details2Key),
			string(details2Key.PrefixEnd()),
		},
	})

	fileSize1 := "410"
	fileSize2 := "590"
	if !codec.ForSystemTenant() {
		fileSize1 = "492"
		fileSize2 = "708"
	}
	var showFiles = fmt.Sprintf(`SELECT start_pretty, end_pretty, size_bytes, rows
		FROM [SHOW BACKUP FILES FROM LATEST IN '%s']`, details)
	sqlDBRestore.CheckQueryResults(t, showFiles, [][]string{
		{
			fmt.Sprintf(prefix+"/Table/%d/1/1", d1ID),
			fmt.Sprintf(prefix+"/Table/%d/1/42", d1ID),
			fileSize1, "41",
		},
		{
			fmt.Sprintf(prefix+"/Table/%d/1/42", d1ID),
			fmt.Sprintf(prefix+"/Table/%d/2", d1ID),
			fileSize2, "59",
		},
	})
	sstMatcher := regexp.MustCompile(`\d+\.sst`)
	pathRows := sqlDB.QueryStr(t, `SELECT path FROM [SHOW BACKUP FILES FROM LATEST IN $1]`, details)
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
		sqlDB.Exec(t, `BACKUP data.tableA, data.viewA, data.seqA INTO $1;`, viewTableSeq)

		// Create tables with the same ID as data.tableA to ensure that comments
		// from different tables in the restoring cluster don't appear.
		tableA := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "data", "tablea")
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
			fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS FROM LATEST IN '%s'] WHERE object_type='table'`, viewTableSeq))
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
		sqlDB.Exec(t, `BACKUP data.FKSrc, data.FKRefTable, data2.FKRefTable INTO $1;`, includedFK)

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

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS FROM LATEST IN '%s'] WHERE object_type='table'`, includedFK))
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
		sqlDB.Exec(t, `BACKUP data2.FKRefTable INTO $1;`, missingFK)

		want := `CREATE TABLE fkreftable (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT fkreftable_pkey PRIMARY KEY (a ASC)
			)`

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW BACKUP SCHEMAS FROM LATEST IN '%s'] WHERE object_type='table'`, missingFK))
		createStmt := showBackupRows[0][0]
		if !eqWhitespace(createStmt, want) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmt, want)
		}
	}

	{
		fullCluster := localFoo + "/full_cluster"
		sqlDB.Exec(t, `BACKUP INTO $1;`, fullCluster)

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT is_full_cluster FROM [SHOW BACKUP FROM LATEST IN '%s']`, fullCluster))
		isFullCluster := showBackupRows[0][0]
		if !eqWhitespace(isFullCluster, "true") {
			t.Fatal("expected show backup to indicate that backup was full cluster")
		}

		fullClusterInc := localFoo + "/full_cluster_inc"
		sqlDB.Exec(t, `BACKUP INTO LATEST IN $1 WITH incremental_location = $2;`, fullCluster, fullClusterInc)

		showBackupRows = sqlDBRestore.QueryStr(t, fmt.Sprintf(`SELECT is_full_cluster FROM [SHOW BACKUP FROM LATEST IN '%s']`, fullCluster))
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
		sqlDB.Exec(t, `BACKUP DATABASE mi5 INTO $1;`, showPrivs)

		want := [][]string{
			{`mi5`, `database`, `GRANT ALL ON DATABASE mi5 TO admin WITH GRANT OPTION; ` +
				`GRANT ALL ON DATABASE mi5 TO agents; ` +
				`GRANT CONNECT ON DATABASE mi5 TO public; ` +
				`GRANT ALL ON DATABASE mi5 TO root WITH GRANT OPTION; `, `root`},
			{`public`, `schema`, `GRANT ALL ON SCHEMA public TO admin WITH GRANT OPTION; ` +
				`GRANT CREATE, USAGE ON SCHEMA public TO public; ` +
				`GRANT ALL ON SCHEMA public TO root WITH GRANT OPTION; `, `root`},
			{`locator`, `schema`, `GRANT ALL ON SCHEMA locator TO admin WITH GRANT OPTION; ` +
				`GRANT CREATE ON SCHEMA locator TO agent_bond; ` +
				`GRANT ALL ON SCHEMA locator TO m; ` +
				`GRANT ALL ON SCHEMA locator TO root WITH GRANT OPTION; `, `root`},
			{`continent`, `type`, `GRANT ALL ON TYPE continent TO admin WITH GRANT OPTION; ` +
				`GRANT ALL ON TYPE continent TO m; ` +
				`GRANT USAGE ON TYPE continent TO public; ` +
				`GRANT ALL ON TYPE continent TO root WITH GRANT OPTION; `, `root`},
			{`_continent`, `type`, `GRANT ALL ON TYPE _continent TO admin WITH GRANT OPTION; ` +
				`GRANT USAGE ON TYPE _continent TO public; ` +
				`GRANT ALL ON TYPE _continent TO root WITH GRANT OPTION; `, `root`},
			{`agent_locations`, `table`, `GRANT ALL ON TABLE agent_locations TO admin WITH GRANT OPTION; ` +
				`GRANT SELECT ON TABLE agent_locations TO agent_bond; ` +
				`GRANT UPDATE ON TABLE agent_locations TO agents; ` +
				`GRANT ALL ON TABLE agent_locations TO m; ` +
				`GRANT ALL ON TABLE agent_locations TO root WITH GRANT OPTION; `, `root`},
			{`top_secret`, `table`, `GRANT ALL ON TABLE top_secret TO admin WITH GRANT OPTION; ` +
				`GRANT SELECT, UPDATE ON TABLE top_secret TO agent_bond; ` +
				`GRANT INSERT ON TABLE top_secret TO agents; GRANT ALL ON TABLE top_secret TO m; ` +
				`GRANT ALL ON TABLE top_secret TO root WITH GRANT OPTION; `, `root`},
		}

		showQuery := fmt.Sprintf(`SELECT object_name, object_type, privileges, owner FROM [SHOW BACKUP FROM LATEST IN '%s' WITH privileges]`, showPrivs)
		sqlDBRestore.CheckQueryResults(t, showQuery, want)

		// Change the owner and expect the changes to be reflected in a new backup
		showOwner := localFoo + "/show_owner"
		sqlDB.Exec(t, `
ALTER DATABASE mi5 OWNER TO agent_thomas;
ALTER SCHEMA locator OWNER TO agent_thomas;
ALTER TYPE locator.continent OWNER TO agent_bond;
ALTER TABLE locator.agent_locations OWNER TO agent_bond;
`)
		sqlDB.Exec(t, `BACKUP DATABASE mi5 INTO $1;`, showOwner)

		want = [][]string{
			{`mi5`, `agent_thomas`},
			{`public`, `root`},
			{`locator`, `agent_thomas`},
			{`continent`, `agent_bond`},
			{`_continent`, `agent_bond`},
			{`agent_locations`, `agent_bond`},
			{`top_secret`, `root`},
		}

		showQuery = fmt.Sprintf(`SELECT object_name, owner FROM [SHOW BACKUP FROM LATEST IN '%s' WITH privileges]`, showOwner)
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
	b1 := sqlDBRestore.QueryStr(t, `SELECT * FROM [SHOW BACKUP FROM $1 IN $2] WHERE object_type='table'`, rows[0][0], full)
	require.Equal(t, 4, len(b1))
	b2 := sqlDBRestore.QueryStr(t,
		`SELECT * FROM [SHOW BACKUP FROM $1 IN $2] WHERE object_type='table'`, rows[1][0], full)
	require.Equal(t, 3, len(b2))

	require.Equal(t,
		sqlDBRestore.QueryStr(t, `SHOW BACKUP FROM $1 IN $2`, rows[2][0], full),
		sqlDBRestore.QueryStr(t, `SHOW BACKUP FROM LATEST IN $1`, full),
	)

	// check that full and remote incremental backups appear
	b3 := sqlDBRestore.QueryStr(t,
		`SELECT * FROM [SHOW BACKUP FROM LATEST IN $1 WITH incremental_location = $2 ] WHERE object_type ='table'`, full, remoteInc)
	require.Equal(t, 3, len(b3))

}

func TestShowNonDefaultBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	const full = localFoo + "/full"
	const remoteInc = localFoo + "/inc"

	// Make an initial backup.
	fullNonDefault := full + "NonDefault"
	incNonDefault := remoteInc + "NonDefault"
	sqlDB.Exec(t, `BACKUP DATABASE data INTO $1`, fullNonDefault)

	// Get base number of files, schemas, and ranges in the backup
	var oldCount [4]int
	for i, typ := range []string{"FILES", "SCHEMAS", "RANGES", "VALIDATE"} {
		query := fmt.Sprintf(`SELECT count(*) FROM [SHOW BACKUP %s FROM LATEST IN '%s']`, typ,
			fullNonDefault)
		count, err := strconv.Atoi(sqlDB.QueryStr(t, query)[0][0])
		require.NoError(t, err, "error converting original count to integer")
		oldCount[i] = count
	}

	// Increase the number of files,schemas, and ranges that will be in the backup chain
	sqlDB.Exec(t, `CREATE TABLE data.blob (a INT PRIMARY KEY); INSERT INTO data.blob VALUES (0)`)
	sqlDB.Exec(t, `BACKUP INTO LATEST IN $1`, fullNonDefault)
	sqlDB.Exec(t, `BACKUP INTO LATEST IN $1 WITH incremental_location=$2`, fullNonDefault, incNonDefault)

	// Show backup should contain more rows as new files/schemas/ranges were
	// added in the incremental backup
	for i, typ := range []string{"FILES", "SCHEMAS", "RANGES"} {
		query := fmt.Sprintf(`SELECT count(*) FROM [SHOW BACKUP %s FROM LATEST IN '%s']`, typ,
			fullNonDefault)
		newCount, err := strconv.Atoi(sqlDB.QueryStr(t, query)[0][0])
		require.NoError(t, err, "error converting new count to integer")
		require.Greater(t, newCount, oldCount[i])

		queryInc := fmt.Sprintf(`SELECT count(*) FROM [SHOW BACKUP %s FROM LATEST IN '%s' WITH incremental_location='%s']`, typ,
			fullNonDefault, incNonDefault)
		newCountInc, err := strconv.Atoi(sqlDB.QueryStr(t, queryInc)[0][0])
		require.NoError(t, err, "error converting new count to integer")
		require.Greater(t, newCountInc, oldCount[i])
	}
}

// TestShowBackupTenantView ensures that SHOW BACKUP on a tenant backup returns the same results
// as a SHOW BACKUP on a cluster backup that contains the same data.
// TODO(msbutler): convert to data driven test, and test cluster and database
// level backups once tenants support nodelocal or once http external storage supports listing.
func TestShowBackupTenantView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	tc, systemDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	srv := tc.Server(0)

	_ = securitytest.EmbeddedTenantIDs()

	_, conn2 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(2)})
	defer conn2.Close()

	tenant2 := sqlutils.MakeSQLRunner(conn2)
	dataQuery := `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`
	backupQuery := `BACKUP TABLE foo.bar INTO $1`
	showBackupQuery := "SELECT object_name, object_type, rows FROM [SHOW BACKUP FROM LATEST IN $1]"
	tenant2.Exec(t, dataQuery)

	// First, assert that SHOW BACKUPS on a tenant backup returns the same results if
	// either the system tenant or tenant2 calls it.
	tenantAddr, httpServerCleanup := makeInsecureHTTPServer(t)
	defer httpServerCleanup()

	tenant2.Exec(t, backupQuery, tenantAddr)
	systemTenantShowRes := systemDB.QueryStr(t, showBackupQuery, tenantAddr)
	require.Equal(t, systemTenantShowRes, tenant2.QueryStr(t, showBackupQuery, tenantAddr))

	// If the system tenant created the same data, and conducted the same backup,
	// the row counts should look the same.
	systemAddr, httpServerCleanup2 := makeInsecureHTTPServer(t)
	defer httpServerCleanup2()

	systemDB.Exec(t, dataQuery)
	systemDB.Exec(t, backupQuery, systemAddr)
	require.Equal(t, systemTenantShowRes, systemDB.QueryStr(t, showBackupQuery, systemAddr))
}

func TestShowBackupTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	tc, systemDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer cleanupFn()
	srv := tc.Server(0)

	// NB: tenant certs for 10, 11, 20 are embedded. See:
	_ = securitytest.EmbeddedTenantIDs()

	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)
	beforeTS := systemDB.QueryStr(t, `SELECT now()::timestamptz::string`)[0][0]

	systemDB.Exec(t, fmt.Sprintf(`BACKUP TENANT 10 INTO 'nodelocal://1/t10' AS OF SYSTEM TIME '%s'`, beforeTS))

	res := systemDB.QueryStr(t, `SELECT object_name, object_type, start_time::string, end_time::string, rows FROM [SHOW BACKUP FROM LATEST IN 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL", beforeTS, "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT object_name, object_type, privileges FROM [SHOW BACKUP FROM LATEST IN 'nodelocal://1/t10' WITH privileges]`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT object_name, object_type, create_statement FROM [SHOW BACKUP SCHEMAS FROM LATEST IN 'nodelocal://1/t10']`)
	require.Equal(t, [][]string{
		{"10", "TENANT", "NULL"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT start_pretty FROM [SHOW BACKUP FILES FROM LATEST IN 'nodelocal://1/t10'] ORDER BY start_key LIMIT 1`)
	require.Equal(t, [][]string{
		{"/Tenant/10"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT end_pretty FROM [SHOW BACKUP FILES FROM LATEST IN 'nodelocal://1/t10'] ORDER BY end_key DESC LIMIT 1`)
	require.Equal(t, [][]string{
		{"/Tenant/10/Max"},
	}, res)

	res = systemDB.QueryStr(t, `SELECT database_id, parent_schema_id, object_id FROM [SHOW BACKUP FROM LATEST IN 'nodelocal://1/t10' WITH debug_ids]`)
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

	testuser := srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))

	// Make an initial backup.
	const full = localFoo + "/full"
	sqlDB.Exec(t, `BACKUP privs INTO $1`, full)
	// Add an incremental backup to it.
	sqlDB.Exec(t, `BACKUP privs INTO LATEST IN $1`, full)
	// Make a second full backup using non into syntax.
	sqlDB.Exec(t, `BACKUP INTO $1;`, full)

	_, err := testuser.Exec(`SHOW BACKUPS IN $1`, full)
	require.True(t, testutils.IsError(err,
		"only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access the specified nodelocal URI"))

	_, err = testuser.Exec(`SHOW BACKUP FROM LATEST IN $1`, full)
	require.True(t, testutils.IsError(err,
		"only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access the specified nodelocal URI"))

	sqlDB.Exec(t, `GRANT admin TO testuser`)
	_, err = testuser.Exec(`SHOW BACKUPS IN $1`, full)
	require.NoError(t, err)

	_, err = testuser.Exec(`SHOW BACKUP FROM LATEST IN $1`, full)
	require.NoError(t, err)
}

func TestShowBackupWithDebugIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 11
	// Create test database with bank table
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// add 1 type, 1 schema, and 2 tables to the database
	sqlDB.ExecMultiple(t, strings.Split(`
		SET CLUSTER SETTING sql.cross_db_fks.enabled = TRUE;
		CREATE TYPE data.welcome AS ENUM ('hello', 'hi');
		USE data; CREATE SCHEMA sc;
		CREATE TABLE data.sc.t1 (a INT);
		CREATE TABLE data.sc.t2 (a data.welcome);`,
		`;`)...)

	const full = localFoo + "/full"

	beforeTS := sqlDB.QueryStr(t, `SELECT now()::timestamptz::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data INTO $1 AS OF SYSTEM TIME '%s'`, beforeTS), full)

	// extract the object IDs for the database and public schema
	databaseRow := sqlDB.QueryStr(t, `
		SELECT database_name, database_id, parent_schema_name, parent_schema_id, object_name, object_id, object_type
		FROM [SHOW BACKUP FROM LATEST IN $1 WITH debug_ids]
		WHERE object_name = 'bank'`, full)
	require.NotEmpty(t, databaseRow)
	dbID, err := strconv.Atoi(databaseRow[0][1])
	require.NoError(t, err)
	publicID, err := strconv.Atoi(databaseRow[0][3])
	require.NoError(t, err)

	require.Greater(t, dbID, 0)
	require.Greater(t, publicID, 0)
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
		"SHOW BACKUP '' IN $1", localFoo)
}

// TestShowBackupCheckFiles verifies the check_files option catches a corrupt
// backup file in 3 scenarios: 1. SST from a full backup; 2. SST from a default
// incremental backup; 3. SST from an incremental backup created with the
// incremental_location parameter. The first two scenarios also get checked with
// locality aware backups. The test also sanity checks the new file_bytes column
// in SHOW BACKUP with check_files, which displays the physical size of each
// table in the backup.
func TestShowBackupCheckFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numAccounts = 11
	skip.UnderRace(t, "multinode cluster setup times out under race, likely due to resource starvation.")

	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()

	collectionRoot := "full"
	incLocRoot := "inc"
	const c1, c2, c3 = `nodelocal://1/full`, `nodelocal://2/full`, `nodelocal://3/full`
	const i1, i2, i3 = `nodelocal://1/inc`, `nodelocal://2/inc`, `nodelocal://3/inc`
	localities := []string{"default", "dc=dc1", "dc=dc2"}

	collections := []string{
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c1, url.QueryEscape(localities[0])),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c2, url.QueryEscape(localities[1])),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", c3, url.QueryEscape(localities[2])),
	}

	incrementals := []string{
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i1, url.QueryEscape(localities[0])),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i2, url.QueryEscape(localities[1])),
		fmt.Sprintf("'%s?COCKROACH_LOCALITY=%s'", i3, url.QueryEscape(localities[2])),
	}
	tests := []struct {
		dest       []string
		inc        []string
		localities []string
	}{
		{dest: []string{collections[0]}, inc: []string{incrementals[0]}},
		{dest: collections, inc: incrementals, localities: localities},
	}

	sqlDB.Exec(t, `CREATE DATABASE fkdb`)
	sqlDB.Exec(t, `CREATE TABLE fkdb.fk (ind INT)`)

	for _, test := range tests {
		dest := strings.Join(test.dest, ", ")
		inc := strings.Join(test.inc, ", ")

		if len(test.dest) > 1 {
			dest = "(" + dest + ")"
			inc = "(" + inc + ")"
		}

		for i := 0; i < 10; i++ {
			sqlDB.Exec(t, `INSERT INTO fkdb.fk (ind) VALUES ($1)`, i)
		}
		fb := fmt.Sprintf("BACKUP DATABASE fkdb INTO %s", dest)
		sqlDB.Exec(t, fb)

		sqlDB.Exec(t, `INSERT INTO fkdb.fk (ind) VALUES ($1)`, 200)

		sib := fmt.Sprintf("BACKUP DATABASE fkdb INTO LATEST IN %s WITH incremental_location = %s", dest, inc)
		sqlDB.Exec(t, sib)

		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE fkdb INTO LATEST IN %s", dest))

		// breakCheckFiles validates that moving an SST will cause SHOW BACKUP with check_files to
		// error.
		breakCheckFiles := func(
			// rootDir identifies the root of the collection or incremental backup dir
			// of the target backup file.
			rootDir string,

			// backupDest contains the collection or incremental URIs.
			backupDest []string,

			// file contains the path to the target file staring from the rootDir.
			file string,

			// fileLocality contains the expected locality of the target file.
			fileLocality string,

			// checkQuery contains the 'SHOW BACKUP with check_files' query.
			checkQuery string) {

			// Ensure no errors first
			sqlDB.Exec(t, checkQuery)

			fullPath := filepath.Join(tempDir, rootDir, file)
			badPath := fullPath + "t"
			if err := os.Rename(fullPath, badPath); err != nil {
				require.NoError(t, err, "failed to corrupt SST")
			}

			// If the file is from a locality aware backup, check its locality info. Note
			// that locality aware URIs have the following structure:
			// `someURI?COCKROACH_LOCALITY ='locality'`.
			if fileLocality == "NULL" {
			} else {
				var locality string
				fileLocality = url.QueryEscape(fileLocality)
				for _, destURI := range backupDest {
					// Using the locality, match for the proper URI.
					destLocality := strings.Split(destURI, "?")
					if strings.Contains(destLocality[1], fileLocality) {
						locality = destLocality[1]
						break
					}
				}
				require.NotEmpty(t, locality, "could not find file locality")
			}

			// Note that the expected error message excludes the nodelocal portion of
			// the file path (nodelocal://1/) to avoid a test flake for locality aware
			// backups where two different nodelocal URI's read to the same place. In
			// this scenario, the test expects the backup to be in nodelocal://2/foo
			// and the actual error message resolves the uri to nodelocal://1/foo.
			// While both are correct, the test fails.
			errorMsg := fmt.Sprintf("The following files are missing from the backup:\n\t.*%s",
				filepath.Join(rootDir, file))
			sqlDB.ExpectErr(t, errorMsg, checkQuery)

			if err := os.Rename(badPath, fullPath); err != nil {
				require.NoError(t, err, "failed to de-corrupt SST")
			}
		}
		fileInfo := sqlDB.QueryStr(t,
			fmt.Sprintf(`SELECT path, locality, file_bytes FROM [SHOW BACKUP FILES FROM LATEST IN %s with check_files]`, dest))

		checkQuery := fmt.Sprintf(`SHOW BACKUP FROM LATEST IN %s WITH check_files`, dest)

		// Break on full backup.
		breakCheckFiles(collectionRoot, test.dest, fileInfo[0][0], fileInfo[0][1], checkQuery)

		// Break on default inc backup.
		breakCheckFiles(collectionRoot, test.dest, fileInfo[len(fileInfo)-1][0], fileInfo[len(fileInfo)-1][1], checkQuery)

		// Check that each file size is positive.
		for _, file := range fileInfo {
			sz, err := strconv.Atoi(file[2])
			require.NoError(t, err, "could not get file size")
			require.Greater(t, sz, 0, "file size is not positive")
		}

		// Check that the returned file size is consistent across flavors of SHOW BACKUP.
		fileSum := sqlDB.QueryStr(t,
			fmt.Sprintf(`SELECT sum(file_bytes) FROM [SHOW BACKUP FILES FROM LATEST IN %s with check_files]`, dest))

		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT sum(file_bytes) FROM [SHOW BACKUP FROM LATEST IN %s with check_files]`, dest),
			fileSum)

		if len(test.dest) == 1 {
			// Break on an incremental backup stored at incremental_location.
			fileInfo := sqlDB.QueryStr(t,
				fmt.Sprintf(`SELECT path, locality FROM [SHOW BACKUP FILES FROM LATEST IN %s WITH incremental_location = %s]`,
					dest, inc))

			incCheckQuery := fmt.Sprintf(`SHOW BACKUP FROM LATEST IN %s WITH check_files, incremental_location = %s`, dest, inc)
			breakCheckFiles(incLocRoot, test.inc, fileInfo[len(fileInfo)-1][0], fileInfo[len(fileInfo)-1][1], incCheckQuery)
		}
	}
}
