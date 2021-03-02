// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLoadShow(t *testing.T) {
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
	sqlDB.Exec(t, `CREATE SCHEMA testDB.testschema`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)
	const backupPath = "nodelocal://0/fooFolder"
	sqlDB.Exec(t, `BACKUP testDB.testSchema.fooTable TO $1`, backupPath)

	// Test load show with metadata option
	expectedMetadataOutputSubstr := []string{"StartTime:", "EndTime:", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0", "FormatVersion: 1", "ClusterID:", "NodeID: 0", "BuildInfo:"}
	t.Run("show-metadata", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show %s metadata --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		for _, substr := range expectedMetadataOutputSubstr {
			require.True(t, strings.Contains(out, substr))
		}
	})

	// Test load show with spans option
	expectedSpansOutput := "/Table/54/{1-2}\n"
	t.Run("show-spans", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show %s spans --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		checkExpectedOutput(t, expectedSpansOutput, out)
	})

	// Test load show with files option
	expectedFilesOutputSubstr := []string{".sst", "Span: /Table/54/{1-2}", "Sha512:", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0"}
	t.Run("show-files", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show %s files --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		for _, substr := range expectedFilesOutputSubstr {
			require.Contains(t, out, substr)
		}
	})

	// Test load show with descriptors option
	expectedDescOutput :=
		`Databases:
	testdb
Schemas:
	public
	testschema
Tables:
	testdb.testschema.footable
`
	t.Run("show-descriptors", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show %s descriptors --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		checkExpectedOutput(t, expectedDescOutput, out)
	})

	// Test load show without options should output all information
	t.Run("show-without-options", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)
		expectedOutputSubstr := append(expectedMetadataOutputSubstr, "Spans:\n\t"+expectedSpansOutput)
		expectedOutputSubstr = append(expectedOutputSubstr, "Files:\n\t")
		expectedOutputSubstr = append(expectedOutputSubstr, expectedFilesOutputSubstr...)
		for _, substr := range expectedOutputSubstr {
			require.Contains(t, out, substr)
		}
	})
}

func checkExpectedOutput(t *testing.T, expected string, out string) {
	endOfCmd := strings.Index(out, "\n")
	out = out[endOfCmd+1:]
	require.Equal(t, expected, out)
}
