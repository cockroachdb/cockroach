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
}
