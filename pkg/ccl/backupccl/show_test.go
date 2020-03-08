// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 11
	_, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	const full, inc, inc2 = localFoo + "/full", localFoo + "/inc", localFoo + "/inc2"

	beforeTS := sqlDB.QueryStr(t, `SELECT now()::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, beforeTS), full)

	res := sqlDB.QueryStr(t, `SELECT table_name, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1]`, full)
	require.Equal(t, [][]string{{"bank", "NULL", beforeTS, strconv.Itoa(numAccounts)}}, res)

	// Mess with half the rows.
	affectedRows, err := sqlDB.Exec(t,
		`UPDATE data.bank SET id = -1 * id WHERE id > $1`, numAccounts/2,
	).RowsAffected()
	require.NoError(t, err)
	require.Equal(t, numAccounts/2, int(affectedRows))

	// Backup the changes by appending to the base and by making a separate
	// inc backup.
	incTS := sqlDB.QueryStr(t, `SELECT now()::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, incTS), full)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2`, incTS), inc, full)

	// Check the appended base backup.
	res = sqlDB.QueryStr(t, `SELECT table_name, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1]`, full)
	require.Equal(t, [][]string{
		{"bank", "NULL", beforeTS, strconv.Itoa(numAccounts)},
		{"bank", beforeTS, incTS, strconv.Itoa(int(affectedRows * 2))},
	}, res)

	// Check the separate inc backup.
	res = sqlDB.QueryStr(t, `SELECT start_time::string, end_time::string, rows FROM [SHOW BACKUP $1]`, inc)
	require.Equal(t, [][]string{
		{beforeTS, incTS, strconv.Itoa(int(affectedRows * 2))},
	}, res)

	// Create two new tables, alphabetically on either side of bank.
	sqlDB.Exec(t, `CREATE TABLE data.auth (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `CREATE TABLE data.users (id INT PRIMARY KEY, name STRING)`)
	sqlDB.Exec(t, `INSERT INTO data.users VALUES (1, 'one'), (2, 'two'), (3, 'three')`)

	// Backup the changes again, by appending to the base and by making a
	// separate inc backup.
	inc2TS := sqlDB.QueryStr(t, `SELECT now()::string`)[0][0]
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s'`, inc2TS), full)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE data TO $1 AS OF SYSTEM TIME '%s' INCREMENTAL FROM $2, $3`, inc2TS), inc2, full, inc)

	// Check the appended base backup.
	res = sqlDB.QueryStr(t, `SELECT table_name, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1]`, full)
	require.Equal(t, [][]string{
		{"bank", "NULL", beforeTS, strconv.Itoa(numAccounts)},
		{"bank", beforeTS, incTS, strconv.Itoa(int(affectedRows * 2))},
		{"bank", incTS, inc2TS, "0"},
		{"auth", incTS, inc2TS, "0"},
		{"users", incTS, inc2TS, "3"},
	}, res)

	// Check the separate inc backup.
	res = sqlDB.QueryStr(t, `SELECT table_name, start_time::string, end_time::string, rows FROM [SHOW BACKUP $1]`, inc2)
	require.Equal(t, [][]string{
		{"bank", incTS, inc2TS, "0"},
		{"auth", incTS, inc2TS, "0"},
		{"users", incTS, inc2TS, "3"},
	}, res)

	const details = localFoo + "/details"
	sqlDB.Exec(t, `CREATE TABLE data.details1 (c INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO data.details1 (SELECT generate_series(1, 100))`)
	sqlDB.Exec(t, `ALTER TABLE data.details1 SPLIT AT VALUES (1), (42)`)
	sqlDB.Exec(t, `CREATE TABLE data.details2()`)
	sqlDB.Exec(t, `BACKUP data.details1, data.details2 TO $1;`, details)

	details1Desc := sqlbase.GetTableDescriptor(tc.Server(0).DB(), "data", "details1")
	details2Desc := sqlbase.GetTableDescriptor(tc.Server(0).DB(), "data", "details2")
	details1Key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(details1Desc, details1Desc.PrimaryIndex.ID))
	details2Key := roachpb.Key(sqlbase.MakeIndexKeyPrefix(details2Desc, details2Desc.PrimaryIndex.ID))

	sqlDB.CheckQueryResults(t, fmt.Sprintf(`SHOW BACKUP RANGES '%s'`, details), [][]string{
		{"/Table/56/1", "/Table/56/2", string(details1Key), string(details1Key.PrefixEnd())},
		{"/Table/57/1", "/Table/57/2", string(details2Key), string(details2Key.PrefixEnd())},
	})

	var showFiles = fmt.Sprintf(`SELECT start_pretty, end_pretty, size_bytes, rows
		FROM [SHOW BACKUP FILES '%s']`, details)
	sqlDB.CheckQueryResults(t, showFiles, [][]string{
		{"/Table/56/1/1", "/Table/56/1/42", "369", "41"},
		{"/Table/56/1/42", "/Table/56/2", "531", "59"},
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
		sqlDB.Exec(t, `COMMENT ON TABLE data.tableA IS 'table'`)
		sqlDB.Exec(t, `COMMENT ON COLUMN data.tableA.a IS 'column'`)
		sqlDB.Exec(t, `COMMENT ON INDEX data.tableA_b_idx IS 'index'`)
		sqlDB.Exec(t, `CREATE VIEW data.viewA AS SELECT a from data.tableA`)
		sqlDB.Exec(t, `CREATE SEQUENCE data.seqA START 1 INCREMENT 2 MAXVALUE 20`)
		sqlDB.Exec(t, `BACKUP data.tableA, data.viewA, data.seqA TO $1;`, viewTableSeq)

		expectedCreateTable := `CREATE TABLE tablea (
	a INT8 NOT NULL,
	b INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (a ASC),
	INDEX tablea_b_idx (b ASC),
	FAMILY "primary" (a, b)
);
COMMENT ON TABLE tablea IS 'table';
COMMENT ON COLUMN tablea.a IS 'column';
COMMENT ON INDEX tablea_b_idx IS 'index'`
		expectedCreateView := `CREATE VIEW viewa (a) AS SELECT a FROM data.public.tablea`
		expectedCreateSeq := `CREATE SEQUENCE seqa MINVALUE 1 MAXVALUE 20 INCREMENT 2 START 1`

		showBackupRows = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW BACKUP SCHEMAS '%s'`, viewTableSeq))
		expected = []string{
			expectedCreateTable,
			expectedCreateView,
			expectedCreateSeq,
		}
		for i, row := range showBackupRows {
			createStmt := row[6]
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
				CONSTRAINT "primary" PRIMARY KEY (a ASC),
				CONSTRAINT fk_b_ref_fksrc FOREIGN KEY (b) REFERENCES fksrc(a),
				INDEX fkreftable_auto_index_fk_b_ref_fksrc (b ASC),
				FAMILY "primary" (a, b)
			)`
		wantDiffDB := `CREATE TABLE fkreftable (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT "primary" PRIMARY KEY (a ASC),
				CONSTRAINT fk_b_ref_fksrc FOREIGN KEY (b) REFERENCES data.public.fksrc(a),
				INDEX fkreftable_auto_index_fk_b_ref_fksrc (b ASC),
				FAMILY "primary" (a, b)
			)`

		showBackupRows = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW BACKUP SCHEMAS '%s'`, includedFK))
		createStmtSameDB := showBackupRows[1][6]
		if !eqWhitespace(createStmtSameDB, wantSameDB) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmtSameDB, wantSameDB)
		}

		createStmtDiffDB := showBackupRows[2][6]
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
				CONSTRAINT "primary" PRIMARY KEY (a ASC),
				INDEX fkreftable_auto_index_fk_b_ref_fksrc (b ASC),
				FAMILY "primary" (a, b)
			)`

		showBackupRows = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW BACKUP SCHEMAS '%s'`, missingFK))
		createStmt := showBackupRows[0][6]
		if !eqWhitespace(createStmt, want) {
			t.Fatalf("mismatched create statement: %s, want %s", createStmt, want)
		}
	}

}

func eqWhitespace(a, b string) bool {
	return strings.Replace(a, "\t", "", -1) == strings.Replace(b, "\t", "", -1)
}
