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

	c := cli.NewCliTest(cli.TestCliParams{T: t, NoServer: true})
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
	sqlDB.Exec(t, `CREATE TYPE fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TYPE testDB.testschema.fooType AS ENUM ()`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)
	const backupPath = "nodelocal://0/fooFolder"
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1`, backupPath)

	t.Run("show-summary", func(t *testing.T) {
		out, err := c.RunWithCapture(fmt.Sprintf("load show summary %s --external-io-dir=%s", backupPath, dir))
		require.NoError(t, err)

		expectedMetadataOutputSubstr := []string{"StartTime:", "EndTime:", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0", "FormatVersion: 1", "ClusterID:", "NodeID: 0", "BuildInfo:"}
		expectedSpansOutput := "Spans:\n\t/Table/58/{1-2}\n"
		expectedFilesOutputSubstr := []string{"Files:\n\t", ".sst", "Span: /Table/58/{1-2}", "Sha512:", "DataSize: 20 (20 B)", "Rows: 1", "IndexEntries: 0"}
		expectedDescOutput := `
Databases:
	52.testdb
Schemas:
	29.public
	53.testdb.testschema
Types:
	54.testdb.public.footype
	55.testdb.public._footype
	56.testdb.testschema.footype
	57.testdb.testschema._footype`
		expectedOutputSubstr := append(expectedMetadataOutputSubstr, expectedSpansOutput)
		expectedOutputSubstr = append(expectedOutputSubstr, expectedFilesOutputSubstr...)
		expectedOutputSubstr = append(expectedOutputSubstr, expectedDescOutput)
		for _, substr := range expectedOutputSubstr {
			require.Contains(t, out, substr)
		}
	})
}
