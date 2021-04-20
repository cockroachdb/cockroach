// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestShowSummary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	ctx := context.Background()
	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)

	const dbOnlyBackupPath = "nodelocal://0/dbOnlyFooFolder"
	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME `+ts1.AsOfSystemTime(), dbOnlyBackupPath)

	sqlDB.Exec(t, `CREATE SCHEMA testDB.testschema`)
	sqlDB.Exec(t, `CREATE TYPE fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TYPE testDB.testschema.fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TABLE fooTable (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)
	const backupPath = "nodelocal://0/fooFolder"
	ts2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME `+ts2.AsOfSystemTime(), backupPath)

	t.Run("show-summary-without-types-or-tables", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup show %s --external-io-dir=%s", dbOnlyBackupPath, dir))
		require.NoError(t, err)
		expectedOutput := fmt.Sprintf(
			`{
	"StartTime": "1970-01-01T00:00:00Z",
	"EndTime": "%s",
	"DataSize": "0 B",
	"Rows": 0,
	"IndexEntries": 0,
	"FormatVersion": 1,
	"ClusterID": "%s",
	"NodeID": 0,
	"BuildInfo": "%s",
	"Files": [],
	"Spans": "[]",
	"DatabaseDescriptors": {
		"52": "testdb"
	},
	"TableDescriptors": {},
	"TypeDescriptors": {},
	"SchemaDescriptors": {
		"29": "public"
	}
}
`, ts1.GoTime().Format(time.RFC3339), srv.ClusterID(), build.GetInfo().Short())
		checkExpectedOutput(t, expectedOutput, out)
	})

	t.Run("show-summary-with-full-information", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup show %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)

		var sstFile string
		rows := sqlDB.Query(t, `select path from [show backup files $1]`, backupPath)
		defer rows.Close()
		if !rows.Next() {
			require.NoError(t, rows.Err())
			t.Fatal("expected at least 1 row")
		}
		err = rows.Scan(&sstFile)
		require.NoError(t, err)

		expectedOutput := fmt.Sprintf(
			`{
	"StartTime": "1970-01-01T00:00:00Z",
	"EndTime": "%s",
	"DataSize": "20 B",
	"Rows": 1,
	"IndexEntries": 0,
	"FormatVersion": 1,
	"ClusterID": "%s",
	"NodeID": 0,
	"BuildInfo": "%s",
	"Files": [
		{
			"Path": "%s",
			"Span": "/Table/59/{1-2}",
			"DataSize": "20 B",
			"IndexEntries": 0,
			"Rows": 1
		}
	],
	"Spans": "[/Table/58/{1-2} /Table/59/{1-2}]",
	"DatabaseDescriptors": {
		"52": "testdb"
	},
	"TableDescriptors": {
		"58": "testdb.public.footable",
		"59": "testdb.testschema.footable"
	},
	"TypeDescriptors": {
		"54": "testdb.public.footype",
		"55": "testdb.public._footype",
		"56": "testdb.testschema.footype",
		"57": "testdb.testschema._footype"
	},
	"SchemaDescriptors": {
		"29": "public",
		"53": "testdb.testschema"
	}
}
`, ts2.GoTime().Format(time.RFC3339), srv.ClusterID(), build.GetInfo().Short(), sstFile)
		checkExpectedOutput(t, expectedOutput, out)
	})
}

func TestListBackups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	ctx := context.Background()
	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)
	const backupPath = "nodelocal://0/fooFolder"

	ts := generateBackupTimestamps(3)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[0].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[1].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[2].AsOfSystemTime()), backupPath)

	t.Run("show-backups-with-backups-in-collection", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup list-backups %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)

		var buf bytes.Buffer
		rows := [][]string{
			{"." + ts[0].GoTime().Format(backupccl.DateBasedIntoFolderName)},
			{"." + ts[1].GoTime().Format(backupccl.DateBasedIntoFolderName)},
			{"." + ts[2].GoTime().Format(backupccl.DateBasedIntoFolderName)},
		}
		cols := []string{"path"}
		rowSliceIter := cli.NewRowSliceIter(rows, "l" /*align*/)
		if err := cli.PrintQueryOutput(&buf, cols, rowSliceIter); err != nil {
			t.Fatalf("TestListBackups: PrintQueryOutput: %v", err)
		}
		checkExpectedOutput(t, buf.String(), out)
	})
}

func TestListIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	ctx := context.Background()
	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)
	const backupPath = "nodelocal://0/fooFolder"
	ts := generateBackupTimestamps(3)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, ts[0].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, ts[1].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, ts[2].AsOfSystemTime()), backupPath)

	out, err := c.RunWithCapture(fmt.Sprintf("debug backup list-incremental %s --external-io-dir=%s", backupPath, dir))
	require.NoError(t, err)
	expectedIncFolder := ts[1].GoTime().Format(backupccl.DateBasedIncFolderName)
	expectedIncFolder2 := ts[2].GoTime().Format(backupccl.DateBasedIncFolderName)

	var buf bytes.Buffer
	rows := [][]string{
		{"/fooFolder", "-", ts[0].GoTime().Format(time.RFC3339)},
		{"/fooFolder" + expectedIncFolder, ts[0].GoTime().Format(time.RFC3339), ts[1].GoTime().Format(time.RFC3339)},
		{"/fooFolder" + expectedIncFolder2, ts[1].GoTime().Format(time.RFC3339), ts[2].GoTime().Format(time.RFC3339)},
	}
	cols := []string{"path", "start time", "end time"}
	rowSliceIter := cli.NewRowSliceIter(rows, "lll" /*align*/)
	if err := cli.PrintQueryOutput(&buf, cols, rowSliceIter); err != nil {
		t.Fatalf("TestListIncremental: PrintQueryOutput: %v", err)
	}
	checkExpectedOutput(t, buf.String(), out)
}

func TestShowData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	ctx := context.Background()
	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)
	sqlDB.Exec(t, `CREATE SCHEMA testDB.testschema`)
	sqlDB.Exec(t, `CREATE TABLE fooTable (id INT PRIMARY KEY, value INT, tag STRING)`)
	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (1, 123, 'cat')`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (id INT PRIMARY KEY, value INT, tag STRING)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (2, 223, 'dog')`)

	const backupPublicSchemaPath = "nodelocal://0/fooFolder/public"
	sqlDB.Exec(t, `BACKUP TABLE testDB.public.fooTable TO $1 `, backupPublicSchemaPath)

	const backupTestSchemaPath = "nodelocal://0/fooFolder/test"
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE testDB.testschema.fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts.AsOfSystemTime()), backupTestSchemaPath)

	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (3, 333, 'mickey mouse')`)
	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE testDB.testschema.fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts1.AsOfSystemTime()), backupTestSchemaPath)

	testCasesOnError := []struct {
		name           string
		tableName      string
		backupPaths    []string
		expectedOutput string
	}{
		{
			"show-data-with-not-qualified-name",
			"fooTable",
			[]string{backupTestSchemaPath},
			"ERROR: fetching entry: table name should be specified in format databaseName.schemaName.tableName\n",
		}, {
			"show-data-fail-with-not-found-table-of-public-schema",
			"testDB.public.fooTable",
			[]string{backupTestSchemaPath},
			"ERROR: fetching entry: table testdb.public.footable not found\n",
		}, {
			"show-data-fail-with-not-found-table-of-user-defined-schema",
			"testDB.testschema.fooTable",
			[]string{backupPublicSchemaPath},
			"ERROR: fetching entry: table testdb.testschema.footable not found\n",
		}, {
			"show-data-fail-without-table-specified",
			"",
			[]string{backupPublicSchemaPath},
			"ERROR: export data requires table name specified by --table flag\n",
		},
	}
	for _, tc := range testCasesOnError {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --external-io-dir=%s",
				strings.Join(tc.backupPaths, " "),
				tc.tableName,
				dir))
			require.NoError(t, err)
			checkExpectedOutput(t, tc.expectedOutput, out)
		})
	}

	testCasesDatumOutput := []struct {
		name           string
		tableName      string
		backupPaths    []string
		expectedDatums string
	}{
		{
			"show-data-with-qualified-table-name-of-user-defined-schema",
			"testDB.testschema.fooTable",
			[]string{backupTestSchemaPath},
			"2,223,'dog'\n",
		},
		{
			"show-data-with-qualified-table-name-of-public-schema",
			"testDB.public.fooTable",
			[]string{backupPublicSchemaPath},
			"1,123,'cat'\n",
		}, {
			"show-data-of-incremental-backup",
			"testDB.testschema.fooTable",
			[]string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName)},
			"2,223,'dog'\n3,333,'mickey mouse'\n",
		},
	}

	for _, tc := range testCasesDatumOutput {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --external-io-dir=%s",
				strings.Join(tc.backupPaths, " "),
				tc.tableName,
				dir))
			require.NoError(t, err)
			checkExpectedOutput(t, tc.expectedDatums, out)
		})
	}
}

func TestShowDataAOST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	ctx := context.Background()
	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)
	sqlDB.Exec(t, `CREATE TABLE fooTable (id INT PRIMARY KEY, value INT, tag STRING)`)

	sqlDB.Exec(t, `CREATE SCHEMA fooschema`)
	sqlDB.Exec(t, `CREATE TABLE fooschema.fooTable (id INT PRIMARY KEY, value INT, tag STRING, FAMILY f1 (value, tag))`)

	const backupPath = "nodelocal://0/fooFolder"
	const backupPathWithRev = "nodelocal://0/fooFolderRev"

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (1, 123, 'cat')`)
	sqlDB.Exec(t, `INSERT INTO fooschema.fooTable VALUES (1, 123, 'foo cat'),(7, 723, 'cockroach')`)
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (2, 223, 'dog')`)
	sqlDB.Exec(t, `DELETE FROM fooschema.fooTable WHERE id=7`)
	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s'`, ts1.AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts1.AsOfSystemTime()), backupPathWithRev)

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (3, 323, 'mickey mouse')`)
	sqlDB.Exec(t, `INSERT INTO fooschema.fooTable VALUES (3, 323, 'foo mickey mouse')`)
	ts2BeforeSchemaChange := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `ALTER TABLE fooTable ADD COLUMN active BOOL`)
	ts2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s'`, ts2.AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts2.AsOfSystemTime()), backupPathWithRev)

	sqlDB.Exec(t, `DELETE FROM fooTable WHERE id=3`)
	ts3AfterDeletion := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `UPDATE fooTable SET active=(TRUE) WHERE id = 1`)
	ts3 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s'`, ts3.AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts3.AsOfSystemTime()), backupPathWithRev)

	t.Run("show-data-as-of-a-uncovered-timestamp", func(t *testing.T) {
		tsNotCovered := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s --as-of=%s --external-io-dir=%s",
			backupPath,
			"testDB.public.fooTable",
			tsNotCovered.AsOfSystemTime(),
			dir))
		require.NoError(t, err)
		expectedError := fmt.Sprintf(
			"ERROR: fetching entry: supplied backups do not cover requested time %s\n",
			timeutil.Unix(0, tsNotCovered.WallTime).UTC())
		checkExpectedOutput(t, expectedError, out)
	})

	t.Run("show-data-as-of-non-backup-ts-should-return-error", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --as-of=%s --external-io-dir=%s",
			backupPath,
			"testDB.public.fooTable",
			ts.AsOfSystemTime(),
			dir))
		require.NoError(t, err)
		expectedError := fmt.Sprintf(
			"ERROR: fetching entry: unknown read time: %s\n"+
				"HINT: reading data for requested time requires that BACKUP was created with \"revision_history\" "+
				"or should specify the time to be an exact backup time, nearest backup time is %s\n",
			timeutil.Unix(0, ts.WallTime).UTC(),
			timeutil.Unix(0, ts1.WallTime).UTC())
		checkExpectedOutput(t, expectedError, out)
	})

	testCases := []struct {
		name         string
		tableName    string
		backupPaths  []string
		asof         string
		expectedData string
	}{
		{
			"show-data-of-public-schema-without-as-of-time",
			"testDB.public.fooTable",
			[]string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			"", /*asof*/
			"1,123,'cat',true\n2,223,'dog',null\n",
		},
		{
			"show-data-as-of-a-single-full-backup-timestamp",
			"testDB.public.fooTable",
			[]string{
				backupPath,
			},
			ts1.AsOfSystemTime(),
			"1,123,'cat'\n2,223,'dog'\n",
		},
		{
			"show-data-of-public-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts1.AsOfSystemTime(),
			"1,123,'cat'\n2,223,'dog'\n",
		},
		{
			"show-data-of-public-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts2.AsOfSystemTime(),
			"1,123,'cat',null\n2,223,'dog',null\n3,323,'mickey mouse',null\n",
		},
		{
			"show-data-as-of-foo-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			"testDB.fooschema.fooTable",
			[]string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts2.AsOfSystemTime(),
			"1,123,'foo cat'\n3,323,'foo mickey mouse'\n",
		},
		{
			"show-data-as-of-public-schema-as-of-the-third-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts3.AsOfSystemTime(),
			"1,123,'cat',true\n2,223,'dog',null\n",
		},
		{
			"show-data-with-rev-history-as-of-time-after-first-insertion-should-work-in-a-single-full-backup",
			"testDB.fooschema.fooTable",
			[]string{
				backupPathWithRev,
			},
			ts.AsOfSystemTime(),
			"1,123,'foo cat'\n7,723,'cockroach'\n",
		},
		{
			"show-data-with-rev-history-as-of-time-after-deteletion-should-work-in-a-single-full-backup",
			"testDB.fooschema.fooTable",
			[]string{
				backupPathWithRev,
			},
			ts1.AsOfSystemTime(),
			"1,123,'foo cat'\n",
		},
		{
			"show-data-with-rev-history-as-of-time-after-first-insertion-should-work-in-a-chain-of-backups",
			"testDB.fooschema.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts.AsOfSystemTime(),
			"1,123,'foo cat'\n7,723,'cockroach'\n",
		},
		{
			"show-data-with-rev-history-as-of-time-after-deteletion-should-work-in-a-chain-of-backups",
			"testDB.fooschema.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts1.AsOfSystemTime(),
			"1,123,'foo cat'\n",
		},
		{
			"show-data-with-rev-history-as-of-time-before-schema-changes-should-work-in-a-chain-of-backups",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts2BeforeSchemaChange.AsOfSystemTime(),
			"1,123,'cat'\n2,223,'dog'\n3,323,'mickey mouse'\n",
		},
		{
			"show-data-with-rev-history-history-as-of-time-after-schema-changes-should-work-in-a-chain-of-backups",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts2.AsOfSystemTime(),
			"1,123,'cat',null\n2,223,'dog',null\n3,323,'mickey mouse',null\n",
		},
		{
			"show-data-with-rev-history-as-of-time-after-deletion-should-work-in-a-chain-of-backups",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts3AfterDeletion.AsOfSystemTime(),
			"1,123,'cat',null\n2,223,'dog',null\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s --as-of=%s --external-io-dir=%s ",
				strings.Join(tc.backupPaths, " "),
				tc.tableName,
				tc.asof,
				dir))
			require.NoError(t, err)
			checkExpectedOutput(t, tc.expectedData, out)
		})
	}
}

func checkExpectedOutput(t *testing.T, expected string, out string) {
	endOfCmd := strings.Index(out, "\n")
	output := out[endOfCmd+1:]
	require.Equal(t, expected, output)
}

// generateBackupTimestamps creates n Timestamps with minimal
// interval of 10 milliseconds to be used in incremental
// backup tests.
// Because incremental backup collections are stored in
// a sub-directory structure that assumes that backups
// are taken at least 10 milliseconds apart.
func generateBackupTimestamps(n int) []hlc.Timestamp {
	timestamps := make([]hlc.Timestamp, 0, n)
	for i := 0; i < n; i++ {
		timestamps = append(timestamps, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		time.Sleep(10 * time.Millisecond)
	}
	return timestamps
}
