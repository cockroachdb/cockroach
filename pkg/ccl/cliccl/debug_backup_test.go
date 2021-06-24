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
		setDebugContextDefault()
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
		setDebugContextDefault()
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
		setDebugContextDefault()
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

func TestExportData(t *testing.T) {
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

	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable(id) SELECT * FROM generate_series(4,30)`)

	sqlDB.Exec(t, `ALTER TABLE fooTable SPLIT AT VALUES (10), (20)`)
	var rangeNum int
	sqlDB.QueryRow(t, `SELECT count(*) from [SHOW RANGES from TABLE fooTable]`).Scan(&rangeNum)
	require.Equal(t, 3, rangeNum)

	ts2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE testDB.testschema.fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts2.AsOfSystemTime()), backupTestSchemaPath)

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
			setDebugContextDefault()
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
		flags          string
	}{
		{
			name:           "show-data-with-qualified-table-name-of-user-defined-schema",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath},
			expectedDatums: "2,223,'dog'\n",
		},
		{
			name:           "show-data-with-qualified-table-name-of-public-schema",
			tableName:      "testDB.public.fooTable",
			backupPaths:    []string{backupPublicSchemaPath},
			expectedDatums: "1,123,'cat'\n",
		}, {
			name:           "show-data-of-incremental-backup",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: "2,223,'dog'\n3,333,'mickey mouse'\n",
		}, {
			name:           "show-data-of-incremental-backup-with-maxRows-flag",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: "2,223,'dog'\n3,333,'mickey mouse'\n4,null,null\n",
			flags:          "--max-rows=3",
		}, {
			name:           "show-data-of-incremental-backup-with-maxRows-larger-than-total-rows-of-data",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: "2,223,'dog'\n3,333,'mickey mouse'\n" + generateRows(4, 27),
			flags:          "--max-rows=300",
		}, {
			name:           "show-data-of-incremental-backup-with-start-key-specified",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: generateRows(5, 26),
			flags:          "--start-key=raw:\\xbf\\x89\\x8c\\x8c",
		}, {
			name:           "show-data-of-incremental-backup-with-start-key-and-max-rows-specified",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: generateRows(5, 6),
			flags:          "--start-key=raw:\\xbf\\x89\\x8c\\x8c --max-rows=6",
		}, {
			name:           "show-data-of-incremental-backup-of-multiple-entries-with-start-key-and-max-rows-specified",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: generateRows(5, 20),
			flags:          "--start-key=raw:\\xbf\\x89\\x8c\\x8c --max-rows=20",
		},
		{
			name:           "show-data-of-incremental-backup-with-start-key-of-bytekey-format-and-max-rows-specified",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: generateRows(5, 2),
			flags:          "--start-key=bytekey:\\x8c\\x8c --max-rows=2",
		}, {
			name:           "show-data-of-incremental-backup-with-start-key-of-hex-format-and-max-rows-specified",
			tableName:      "testDB.testschema.fooTable",
			backupPaths:    []string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName), backupTestSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedDatums: generateRows(5, 2),
			flags:          "--start-key=hex:bf898c8c --max-rows=2",
		},
	}

	for _, tc := range testCasesDatumOutput {
		t.Run(tc.name, func(t *testing.T) {
			setDebugContextDefault()
			out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --external-io-dir=%s %s",
				strings.Join(tc.backupPaths, " "),
				tc.tableName,
				dir,
				tc.flags))
			require.NoError(t, err)
			checkExpectedOutput(t, tc.expectedDatums, out)
		})
	}
}

func TestExportDataWithMultipleRanges(t *testing.T) {
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
	// the small test-case will get entirely buffered/merged by small-file merging
	// and mean there one only be a single file.
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_size = '0'`)
	sqlDB.Exec(t, `CREATE DATABASE testDB`)
	sqlDB.Exec(t, `USE testDB`)
	sqlDB.Exec(t, `CREATE TABLE fooTable(id int PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO fooTable select * from generate_series(1,10)`)
	sqlDB.Exec(t, `ALTER TABLE fooTable SPLIT AT VALUES (2), (5), (7)`)

	const backupPath = "nodelocal://0/fooFolder"
	sqlDB.Exec(t, `BACKUP TABLE fooTable TO $1 `, backupPath)

	var rangeNum int
	sqlDB.QueryRow(t, `SELECT count(*) from [SHOW RANGES from TABLE fooTable]`).Scan(&rangeNum)
	require.Equal(t, 4, rangeNum)
	sqlDB.QueryRow(t, `SELECT count(*) from [SHOW BACKUP FILES $1]`, backupPath).Scan(&rangeNum)
	require.Equal(t, 4, rangeNum)

	sqlDB.Exec(t, `ALTER TABLE fooTable ADD COLUMN active BOOL DEFAULT false`)
	sqlDB.Exec(t, `INSERT INTO fooTable select * from generate_series(11,15)`)
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts.AsOfSystemTime()), backupPath)

	sqlDB.QueryRow(t, `SELECT count(*) from [SHOW RANGES from TABLE fooTable]`).Scan(&rangeNum)
	require.Equal(t, 4, rangeNum)
	sqlDB.QueryRow(t, `SELECT count(*) from [SHOW BACKUP FILES $1]`, backupPath).Scan(&rangeNum)
	require.Equal(t, 8, rangeNum)

	t.Run("export-data-with-multiple-ranges", func(t *testing.T) {
		setDebugContextDefault()
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=testDB.public.fooTable  --external-io-dir=%s",
			backupPath,
			dir))
		require.NoError(t, err)
		var expectedOut string
		for i := 1; i <= 10; i++ {
			expectedOut = fmt.Sprintf("%s%d\n", expectedOut, i)
		}
		checkExpectedOutput(t, expectedOut, out)
	})

	t.Run("export-data-with-multiple-ranges-in-incremental-backups", func(t *testing.T) {
		setDebugContextDefault()
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s %s --table=testDB.public.fooTable  --external-io-dir=%s",
			backupPath, backupPath+ts.GoTime().Format(backupccl.DateBasedIncFolderName),
			dir))
		require.NoError(t, err)
		var expectedOut string
		for i := 1; i <= 15; i++ {
			expectedOut = fmt.Sprintf("%s%d,false\n", expectedOut, i)
		}
		checkExpectedOutput(t, expectedOut, out)
	})
}

func TestExportDataAOST(t *testing.T) {
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
		setDebugContextDefault()
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
		setDebugContextDefault()
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --as-of=%s --external-io-dir=%s",
			backupPath,
			"testDB.public.fooTable",
			ts.AsOfSystemTime(),
			dir))
		require.NoError(t, err)
		expectedError := fmt.Sprintf(
			"ERROR: fetching entry: unknown read time: %s\n"+
				"HINT: reading data for requested time requires that BACKUP was created with %q "+
				"or should specify the time to be an exact backup time, nearest backup time is %s\n",
			timeutil.Unix(0, ts.WallTime).UTC(),
			backupOptRevisionHistory,
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
			name:      "show-data-of-public-schema-without-as-of-time",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			expectedData: "1,123,'cat',true\n2,223,'dog',null\n",
		},
		{
			name:         "show-data-as-of-a-single-full-backup-timestamp",
			tableName:    "testDB.public.fooTable",
			backupPaths:  []string{backupPath},
			asof:         ts1.AsOfSystemTime(),
			expectedData: "1,123,'cat'\n2,223,'dog'\n",
		},
		{
			name:      "show-data-of-public-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts1.AsOfSystemTime(),
			expectedData: "1,123,'cat'\n2,223,'dog'\n",
		},
		{
			name:      "show-data-of-public-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			asof:         ts2.AsOfSystemTime(),
			expectedData: "1,123,'cat',null\n2,223,'dog',null\n3,323,'mickey mouse',null\n",
		},
		{
			name:      "show-data-as-of-foo-schema-as-of-the-second-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			tableName: "testDB.fooschema.fooTable",
			backupPaths: []string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			asof:         ts2.AsOfSystemTime(),
			expectedData: "1,123,'foo cat'\n3,323,'foo mickey mouse'\n",
		},
		{
			name:      "show-data-as-of-public-schema-as-of-the-third-backup-timestamp-should-work-in-a-chain-of-incremental-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPath,
				backupPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPath + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			asof:         ts3.AsOfSystemTime(),
			expectedData: "1,123,'cat',true\n2,223,'dog',null\n",
		},
		{
			name:         "show-data-with-rev-history-as-of-time-after-first-insertion-should-work-in-a-single-full-backup",
			tableName:    "testDB.fooschema.fooTable",
			backupPaths:  []string{backupPathWithRev},
			asof:         ts.AsOfSystemTime(),
			expectedData: "1,123,'foo cat'\n7,723,'cockroach'\n",
		},
		{
			name:         "show-data-with-rev-history-as-of-time-after-deteletion-should-work-in-a-single-full-backup",
			tableName:    "testDB.fooschema.fooTable",
			backupPaths:  []string{backupPathWithRev},
			asof:         ts1.AsOfSystemTime(),
			expectedData: "1,123,'foo cat'\n",
		},
		{
			name:      "show-data-with-rev-history-as-of-time-after-first-insertion-should-work-in-a-chain-of-backups",
			tableName: "testDB.fooschema.fooTable",
			backupPaths: []string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts.AsOfSystemTime(),
			expectedData: "1,123,'foo cat'\n7,723,'cockroach'\n",
		},
		{
			name:      "show-data-with-rev-history-as-of-time-after-deteletion-should-work-in-a-chain-of-backups",
			tableName: "testDB.fooschema.fooTable",
			backupPaths: []string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts1.AsOfSystemTime(),
			expectedData: "1,123,'foo cat'\n",
		},
		{
			name:      "show-data-with-rev-history-as-of-time-before-schema-changes-should-work-in-a-chain-of-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts2BeforeSchemaChange.AsOfSystemTime(),
			expectedData: "1,123,'cat'\n2,223,'dog'\n3,323,'mickey mouse'\n",
		},
		{
			name:      "show-data-with-rev-history-history-as-of-time-after-schema-changes-should-work-in-a-chain-of-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts2.AsOfSystemTime(),
			expectedData: "1,123,'cat',null\n2,223,'dog',null\n3,323,'mickey mouse',null\n",
		},
		{
			name:      "show-data-with-rev-history-as-of-time-after-deletion-should-work-in-a-chain-of-backups",
			tableName: "testDB.public.fooTable",
			backupPaths: []string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName)},
			asof:         ts3AfterDeletion.AsOfSystemTime(),
			expectedData: "1,123,'cat',null\n2,223,'dog',null\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setDebugContextDefault()
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

func TestExportDataWithRevisions(t *testing.T) {
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

	const backupPath = "nodelocal://0/fooFolder"
	const backupPathWithRev = "nodelocal://0/fooFolderRev"

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (1, 123, 'cat')`)
	var tsInsert time.Time
	sqlDB.QueryRow(t, `SELECT crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) from fooTable where id=1`).Scan(&tsInsert)

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (2, 223, 'dog')`)
	var tsInsert2 time.Time
	sqlDB.QueryRow(t, `SELECT crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) from fooTable where id=2`).Scan(&tsInsert2)

	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s'`, ts1.AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts1.AsOfSystemTime()), backupPathWithRev)

	sqlDB.Exec(t, `ALTER TABLE fooTable ADD COLUMN active BOOL`)
	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (3, 323, 'mickey mouse', true)`)
	var tsInsert3 time.Time
	sqlDB.QueryRow(t, `SELECT crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) from fooTable where id=3`).Scan(&tsInsert3)
	ts2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts2.AsOfSystemTime()), backupPathWithRev)

	sqlDB.Exec(t, `SET sql_safe_updates=false`)
	sqlDB.Exec(t, `ALTER TABLE fooTable DROP COLUMN value`)
	var tsDropColumn time.Time
	sqlDB.QueryRow(t, `SELECT crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) from fooTable where id=3`).Scan(&tsDropColumn)

	sqlDB.Exec(t, `UPDATE fooTable SET tag=('lion') WHERE id = 1`)
	var tsUpdate time.Time
	sqlDB.QueryRow(t, `SELECT crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) from fooTable where id=1`).Scan(&tsUpdate)

	ts3 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts3.AsOfSystemTime()), backupPathWithRev)

	sqlDB.Exec(t, `CREATE INDEX extra ON fooTable (id)`)
	ts4 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO $1 AS OF SYSTEM TIME '%s' WITH revision_history`, ts4.AsOfSystemTime()), backupPathWithRev)

	t.Run("show-data-revisions-of-backup-without-revision-history", func(t *testing.T) {
		setDebugContextDefault()
		out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s  --with-revisions --external-io-dir=%s",
			backupPath,
			"testDB.public.fooTable",
			dir))
		require.NoError(t, err)
		expectedError := "ERROR: invalid flag: with-revisions\nHINT: requires backup created with \"revision_history\"\n"
		checkExpectedOutput(t, expectedError, out)
	})

	testCases := []struct {
		name          string
		tableName     string
		backupPaths   []string
		expectedData  string
		upToTimestamp string
	}{
		{
			"show-data-revisions-of-a-single-full-backup",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
			},
			fmt.Sprintf("1,123,'cat',%s\n2,223,'dog',%s\n", tsInsert.UTC(), tsInsert2.UTC()),
			"",
		},
		{
			"show-data-revisions-after-adding-an-colum",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			fmt.Sprintf("3,323,'mickey mouse',true,%s\n", tsInsert3.UTC()),
			ts2.AsOfSystemTime(),
		},
		{
			"show-data-revisions-after-dropping-an-colum-and-update-value",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			fmt.Sprintf("1,'lion',null,%s\n1,'cat',null,%s\n2,'dog',null,%s\n3,'mickey mouse',true,%s\n",
				tsUpdate.UTC(), tsDropColumn.UTC(), tsDropColumn.UTC(), tsDropColumn.UTC()),
			ts3.AsOfSystemTime(),
		}, {
			"show-data-revisions-after-adding-index",
			"testDB.public.fooTable",
			[]string{
				backupPathWithRev,
				backupPathWithRev + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts3.GoTime().Format(backupccl.DateBasedIncFolderName),
				backupPathWithRev + ts4.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			fmt.Sprintf("1,'lion',null,%s\n1,'cat',null,%s\n2,'dog',null,%s\n3,'mickey mouse',true,%s\n",
				tsUpdate.UTC(), tsDropColumn.UTC(), tsDropColumn.UTC(), tsDropColumn.UTC()),
			ts4.AsOfSystemTime(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setDebugContextDefault()
			out, err := c.RunWithCapture(fmt.Sprintf("debug backup export %s --table=%s --with-revisions --up-to=%s --external-io-dir=%s",
				strings.Join(tc.backupPaths, " "),
				tc.tableName,
				tc.upToTimestamp,
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

// generateRows generates rows of pattern "%d,null,null\n"
// used to verify that --max-rows captures correct number of rows
func generateRows(start int, rowCount int) string {
	var res string
	for i := 0; i < rowCount; i++ {
		res += fmt.Sprintf("%d,null,null\n", start+i)
	}
	return res
}
