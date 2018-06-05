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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 11
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	full, inc := localFoo+"/full", localFoo+"/inc"

	beforeFull := timeutil.Now()
	sqlDB.Exec(t, `BACKUP data.bank TO $1`, full)

	var unused driver.Value
	var start, end *time.Time
	var dataSize, rows uint64
	sqlDB.QueryRow(t, `SELECT * FROM [SHOW BACKUP $1] WHERE "table" = 'bank'`, full).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start != nil {
		t.Errorf("expected null start time on full backup, got %v", *start)
	}
	if !(*end).After(beforeFull) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeFull, start, end)
	}
	if dataSize <= 0 {
		t.Errorf("expected dataSize to be >0 got : %d", dataSize)
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

	sqlDB.QueryRow(t, `SELECT * FROM [SHOW BACKUP $1] WHERE "table" = 'bank'`, inc).Scan(
		&unused, &unused, &start, &end, &dataSize, &rows,
	)
	if start == nil {
		t.Errorf("expected start time on inc backup, got %v", *start)
	}
	if !(*end).After(beforeInc) {
		t.Errorf("expected now (%s) to be in (%s, %s)", beforeInc, start, end)
	}
	if dataSize <= 0 {
		t.Errorf("expected dataSize to be >0 got : %d", dataSize)
	}
	// We added affectedRows and removed affectedRows, so there should be 2*
	// affectedRows in the backup.
	if expected := affectedRows * 2; rows != uint64(expected) {
		t.Errorf("expected %d got: %d", expected, rows)
	}
}
