// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 11
	_, tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	full, inc, details := localFoo+"/full", localFoo+"/inc", localFoo+"/details"

	beforeFull := timeutil.Now()
	sqlDB.Exec(t, `BACKUP data.bank TO $1`, full)

	var unused driver.Value
	var start, end *time.Time
	var dataSize, rows uint64
	sqlDB.QueryRow(t, `SELECT * FROM [SHOW BACKUP $1] WHERE table_name = 'bank'`, full).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start != nil {
		t.Errorf("expected null start time on full backup, got %v", *start)
	}
	if !(*end).After(beforeFull) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeFull, start, end)
	}
	if rows != numAccounts {
		t.Errorf("expected %d got: %d", numAccounts, rows)
	}

	// Mess with half the rows.
	affectedRows, err := sqlDB.Exec(t,
		`UPDATE data.bank SET id = -1 * id WHERE id > $1`, numAccounts/2,
	).RowsAffected()
	if err != nil {
		t.Fatal(err)
	} else if affectedRows != numAccounts/2 {
		t.Fatalf("expected to update %d rows, got %d", numAccounts/2, affectedRows)
	}

	beforeInc := timeutil.Now()
	sqlDB.Exec(t, `BACKUP data.bank TO $1 INCREMENTAL FROM $2`, inc, full)

	sqlDB.QueryRow(t, `SELECT * FROM [SHOW BACKUP $1] WHERE table_name = 'bank'`, inc).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start == nil {
		t.Errorf("expected start time on inc backup, got %v", *start)
	}
	if !(*end).After(beforeInc) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeInc, start, end)
	}
	// We added affectedRows and removed affectedRows, so there should be 2*
	// affectedRows in the backup.
	if expected := affectedRows * 2; rows != uint64(expected) {
		t.Errorf("expected %d got: %d", expected, rows)
	}

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
		{"/Table/54/1", "/Table/54/2", string(details1Key), string(details1Key.PrefixEnd())},
		{"/Table/55/1", "/Table/55/2", string(details2Key), string(details2Key.PrefixEnd())},
	})

	var showFiles = fmt.Sprintf(`SELECT start_pretty, end_pretty, size_bytes, rows
		FROM [SHOW BACKUP FILES '%s']`, details)
	sqlDB.CheckQueryResults(t, showFiles, [][]string{
		{"/Table/54/1/1", "/Table/54/1/42", "369", "41"},
		{"/Table/54/1/42", "/Table/54/2", "531", "59"},
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
		sqlDB.Exec(t, `CREATE TABLE data.tableA (a int primary key, b int)`)
		sqlDB.Exec(t, `CREATE VIEW data.viewA AS SELECT a from data.tableA`)
		sqlDB.Exec(t, `CREATE SEQUENCE data.seqA START 1 INCREMENT 2 MAXVALUE 20`)
		sqlDB.Exec(t, `BACKUP data.tableA, data.viewA, data.seqA TO $1;`, viewTableSeq)

		expectedCreateTable := `CREATE TABLE tablea (
				a INT8 NOT NULL,
				b INT8 NULL,
				CONSTRAINT "primary" PRIMARY KEY (a ASC),
				FAMILY "primary" (a, b)
			)`
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
