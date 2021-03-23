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
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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

func TestLoadShowSummary(t *testing.T) {
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
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1`, dbOnlyBackupPath)

	sqlDB.Exec(t, `CREATE SCHEMA testDB.testschema`)
	sqlDB.Exec(t, `CREATE TYPE fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TYPE testDB.testschema.fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TABLE fooTable (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)
	const backupPath = "nodelocal://0/fooFolder"
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1`, backupPath)

	t.Run("show-summary-without-types-or-tables", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show summary %s --external-io-dir=%s", dbOnlyBackupPath, dir))
		require.NoError(t, err)
		expectedMetadataOutputSubstr := []string{"StartTime:", "EndTime:", "DataSize: 0 (0 B)", "Rows: 0", "IndexEntries: 0", "FormatVersion: 1", "ClusterID:", "NodeID: 0", "BuildInfo:"}
		expectedOutputSubstr := append(expectedMetadataOutputSubstr, `
Spans:
	(No spans included in the specified backup path.)
Files:
	(No sst files included in the specified backup path.)
Databases:
	52: testdb
Schemas:
	29: public
Types:
	(No user-defined types included in the specified backup path.)
Tables:
	(No tables included in the specified backup path.)`)
		for _, substr := range expectedOutputSubstr {
			require.Contains(t, out, substr)
		}
	})

	t.Run("show-summary-with-full-information", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show summary %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		expectedMetadataOutputSubstr := []string{"StartTime:", "EndTime:", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0", "FormatVersion: 1", "ClusterID:", "NodeID: 0", "BuildInfo:"}
		expectedSpansOutput := "Spans:\n\t/Table/58/{1-2}\n\t/Table/59/{1-2}\n"
		expectedFilesOutputSubstr := []string{"Files:\n\t", ".sst", "Span: /Table/59/{1-2}", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0"}
		expectedDescOutput := `
Databases:
	52: testdb
Schemas:
	29: public
	53: testdb.testschema
Types:
	54: testdb.public.footype
	55: testdb.public._footype
	56: testdb.testschema.footype
	57: testdb.testschema._footype
Tables:
	58: testdb.public.footable
	59: testdb.testschema.footable`
		expectedOutputSubstr := append(expectedMetadataOutputSubstr, expectedSpansOutput)
		expectedOutputSubstr = append(expectedOutputSubstr, expectedFilesOutputSubstr...)
		expectedOutputSubstr = append(expectedOutputSubstr, expectedDescOutput)
		for _, substr := range expectedOutputSubstr {
			require.Contains(t, out, substr)
		}
	})
}

func TestLoadShowBackups(t *testing.T) {
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
	t.Run("show-backups-without-backups-in-collection", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show backups %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		expectedOutput := "no backups found.\n"
		checkExpectedOutput(t, expectedOutput, out)
	})

	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[0].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[1].AsOfSystemTime()), backupPath)
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, ts[2].AsOfSystemTime()), backupPath)

	t.Run("show-backups-with-backups-in-collection", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show backups %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)

		expectedOutput := fmt.Sprintf(".%s\n.%s\n.%s\n",
			ts[0].GoTime().Format(backupccl.DateBasedIntoFolderName),
			ts[1].GoTime().Format(backupccl.DateBasedIntoFolderName),
			ts[2].GoTime().Format(backupccl.DateBasedIntoFolderName))
		checkExpectedOutput(t, expectedOutput, out)
	})
}

func TestLoadShowIncremental(t *testing.T) {
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

	out, err := c.RunWithCapture(fmt.Sprintf("load show incremental %s --external-io-dir=%s", backupPath, dir))
	require.NoError(t, err)
	expectedIncFolder := ts[1].GoTime().Format(backupccl.DateBasedIncFolderName)
	expectedIncFolder2 := ts[2].GoTime().Format(backupccl.DateBasedIncFolderName)

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 28 /*minwidth*/, 1 /*tabwidth*/, 2 /*padding*/, ' ' /*padchar*/, 0 /*flags*/)
	fmt.Fprintf(w, "/fooFolder	-	%s\n", ts[0].GoTime().Format(time.RFC3339))
	fmt.Fprintf(w, "/fooFolder%s	%s	%s\n", expectedIncFolder, ts[0].GoTime().Format(time.RFC3339), ts[1].GoTime().Format(time.RFC3339))
	fmt.Fprintf(w, "/fooFolder%s	%s	%s\n", expectedIncFolder2, ts[2].GoTime().Format(time.RFC3339), ts[2].GoTime().Format(time.RFC3339))
	if err := w.Flush(); err != nil {
		t.Fatalf("TestLoadShowIncremental: flush: %v", err)
	}
	checkExpectedOutput(t, buf.String(), out)
}

func TestLoadShowData(t *testing.T) {
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
	sqlDB.Exec(t, `CREATE TABLE fooTable (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (123)`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)

	const backupPublicSchemaPath = "nodelocal://0/fooFolder/public"
	sqlDB.Exec(t, `BACKUP TABLE testDB.public.fooTable TO $1 `, backupPublicSchemaPath)

	const backupTestSchemaPath = "nodelocal://0/fooFolder/test"
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE testDB.testschema.fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts.AsOfSystemTime()), backupTestSchemaPath)

	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (333)`)
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
		},
	}
	for _, tc := range testCasesOnError {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("load show data %s %s  --external-io-dir=%s",
				tc.tableName,
				strings.Join(tc.backupPaths, " "),
				dir))
			require.NoError(t, err)
			checkExpectedOutput(t, tc.expectedOutput, out)
		})
	}

	testCasesOnKVOutput := []struct {
		name        string
		tableName   string
		backupPaths []string
		expectedKV  []struct{ key, value string }
	}{
		{
			"show-data-with-qualified-table-name-of-user-defined-schema",
			"testDB.testschema.fooTable",
			[]string{backupTestSchemaPath},
			[]struct{ key, value string }{{"/Table/55/1/123/0", "/TUPLE/"}},
		},
		{
			"show-data-with-qualified-table-name-of-public-schema",
			"testDB.public.fooTable",
			[]string{backupPublicSchemaPath},
			[]struct{ key, value string }{{"/Table/54/1/123/0", "/TUPLE/"}},
		}, {
			"show-data-of-incremental-backup",
			"testDB.testschema.fooTable",
			[]string{backupTestSchemaPath, backupTestSchemaPath + ts1.GoTime().Format(backupccl.DateBasedIncFolderName)},
			[]struct{ key, value string }{{"/Table/55/1/123/0", "/TUPLE/"}, {"/Table/55/1/333/0", "/TUPLE/"}},
		},
	}

	for _, tc := range testCasesOnKVOutput {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("load show data %s %s  --external-io-dir=%s",
				tc.tableName,
				strings.Join(tc.backupPaths, " "),
				dir))
			require.NoError(t, err)
			checkExpectedKV(t, tc.expectedKV, out)
		})
	}
}

func TestLoadShowDataAOST(t *testing.T) {
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
	sqlDB.Exec(t, `CREATE TABLE fooTable (a INT PRIMARY KEY)`)

	const backupPublicSchemaPath = "nodelocal://0/fooFolder/public"

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (123)`)
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (223)`)
	ts1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts1.AsOfSystemTime()), backupPublicSchemaPath)

	sqlDB.Exec(t, `INSERT INTO fooTable VALUES (323)`)
	ts2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP TABLE fooTable TO $1 AS OF SYSTEM TIME '%s'`, ts2.AsOfSystemTime()), backupPublicSchemaPath)

	t.Run("show-data-as-of-a-future-timestamp", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show data %s %s  --as-of=10s --external-io-dir=%s",
			"testDB.public.fooTable",
			backupPublicSchemaPath,
			dir))
		require.NoError(t, err)
		expectedError := "ERROR: eval as of timestamp 10s: AS OF SYSTEM TIME: cannot specify timestamp in the future"
		require.Contains(t, out, expectedError)
	})

	testCases := []struct {
		name        string
		tableName   string
		backupPaths []string
		asof        string
		expectedKV  []struct{ key, value string }
	}{
		{
			"show-data-without-as-of-time",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
				backupPublicSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			"", /*asof*/
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
				{"/Table/53/1/223/0", "/TUPLE/"},
				{"/Table/53/1/323/0", "/TUPLE/"},
			},
		},
		{
			"show-data-as-of-time-after-first-insertion-should-work-in-a-single-full-backup",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
			},
			ts.AsOfSystemTime(),
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
			},
		},
		{
			"show-data-as-of-time-after-second-insertion-should-work-in-a-single-full-backup",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
			},
			ts1.AsOfSystemTime(),
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
				{"/Table/53/1/223/0", "/TUPLE/"},
			},
		},
		{
			"show-data-as-of-time-after-first-insertion-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
				backupPublicSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts.AsOfSystemTime(),
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
			},
		},
		{
			"show-data-as-of-time-after-second-insertion-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
				backupPublicSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts1.AsOfSystemTime(),
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
				{"/Table/53/1/223/0", "/TUPLE/"},
			},
		},
		{
			"show-data-as-of-time-after-third-insertion-should-work-in-a-chain-of-incremental-backups",
			"testDB.public.fooTable",
			[]string{
				backupPublicSchemaPath,
				backupPublicSchemaPath + ts2.GoTime().Format(backupccl.DateBasedIncFolderName),
			},
			ts2.AsOfSystemTime(),
			[]struct{ key, value string }{
				{"/Table/53/1/123/0", "/TUPLE/"},
				{"/Table/53/1/223/0", "/TUPLE/"},
				{"/Table/53/1/323/0", "/TUPLE/"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := c.RunWithCapture(fmt.Sprintf("load show data %s %s --as-of=%s --external-io-dir=%s ",
				tc.tableName,
				strings.Join(tc.backupPaths, " "),
				tc.asof,
				dir))
			require.NoError(t, err)
			checkExpectedKV(t, tc.expectedKV, out)
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

func checkExpectedKV(t *testing.T, expectedKV []struct{ key, value string }, out string) {
	lines := strings.Split(out, "\n")
	outkvs := lines[1 : len(lines)-1]
	require.Equal(t, len(expectedKV), len(outkvs))
	for i, line := range outkvs {
		require.True(t, strings.HasPrefix(line, expectedKV[i].key))
		require.True(t, strings.HasSuffix(line, expectedKV[i].value))
	}
}
