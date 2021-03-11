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

	t.Run("show-backups-without-backups-in-collection", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show backups %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		expectedOutput := "no backups found.\n"
		checkExpectedOutput(t, expectedOutput, out)
	})

	intoTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	initAOST := intoTS.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, initAOST), backupPath)
	intoTS1 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	intoAOST1 := intoTS1.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, intoAOST1), backupPath)
	intoTS2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	intoAOST2 := intoTS2.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB INTO $1 AS OF SYSTEM TIME '%s'`, intoAOST2), backupPath)

	t.Run("show-backups-with-backups-in-collection", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show backups %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)

		expectedOutput := fmt.Sprintf(".%s\n.%s\n.%s\n",
			intoTS.GoTime().Format(backupccl.DateBasedIntoFolderName),
			intoTS1.GoTime().Format(backupccl.DateBasedIntoFolderName),
			intoTS2.GoTime().Format(backupccl.DateBasedIntoFolderName))
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
	initTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	initAOST := initTS.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, initAOST), backupPath)

	// We do two more incremental backups on the full backups.
	incTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	incAOST := incTS.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, incAOST), backupPath)
	incTS2 := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	incAOST2 := incTS2.AsOfSystemTime()
	sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME '%s'`, incAOST2), backupPath)

	out, err := c.RunWithCapture(fmt.Sprintf("load show incremental %s --external-io-dir=%s", backupPath, dir))
	require.NoError(t, err)
	expectedIncFolder := incTS.GoTime().Format(backupccl.DateBasedIncFolderName)
	expectedIncFolder2 := incTS2.GoTime().Format(backupccl.DateBasedIncFolderName)

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 28, 1, 2, ' ', 0)
	fmt.Fprintf(w, "/fooFolder	-	%s\n", initTS.GoTime().Format(time.RFC3339))
	fmt.Fprintf(w, "/fooFolder%s	%s	%s\n", expectedIncFolder, initTS.GoTime().Format(time.RFC3339), incTS.GoTime().Format(time.RFC3339))
	fmt.Fprintf(w, "/fooFolder%s	%s	%s\n", expectedIncFolder2, incTS.GoTime().Format(time.RFC3339), incTS2.GoTime().Format(time.RFC3339))
	if err := w.Flush(); err != nil {
		t.Fatalf("TestLoadShowIncremental: flush: %v", err)
	}
	checkExpectedOutput(t, buf.String(), out)
}

func checkExpectedOutput(t *testing.T, expected string, out string) {
	endOfCmd := strings.Index(out, "\n")
	output := out[endOfCmd+1:]
	require.Equal(t, expected, output)
}
