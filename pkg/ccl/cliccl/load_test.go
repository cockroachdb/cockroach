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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestBackupManifestMarshalJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	sqlDB.Exec(t, `CREATE TABLE fooTable (a INT)`)
	sqlDB.Exec(t, `CREATE TABLE testDB.testschema.fooTable (a INT)`)
	sqlDB.Exec(t, `INSERT INTO testDB.testschema.fooTable VALUES (123)`)
	const backupPath = "nodelocal://0/fooFolder"
	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `BACKUP DATABASE testDB TO $1 AS OF SYSTEM TIME `+ts.AsOfSystemTime(), backupPath)

	t.Run("marshall-backup-manifest", func(t *testing.T) {
		clientFactory := blobs.TestBlobServiceClient(dir)
		externalStorageFromURI := func(ctx context.Context, uri string, user security.SQLUsername) (cloud.ExternalStorage,
			error) {
			conf, err := cloudimpl.ExternalStorageConfFromURI(uri, user)
			require.NoError(t, err)
			return cloudimpl.TestingMakeLocalStorage(ctx, conf.LocalFile, cluster.MakeTestingClusterSettings(), clientFactory, base.ExternalIODirConfig{})
		}
		backupManifest, err := backupccl.ReadBackupManifestFromURI(ctx, backupPath, security.TestUserName(), externalStorageFromURI, nil)
		require.NoError(t, err)
		var meta = backupMetaDisplayMsg(backupManifest)
		jsonBytes, err := json.MarshalIndent(meta, "" /*prefix*/, "\t" /*indent*/)
		require.NoError(t, err)
		jsonString := string(jsonBytes)
		require.NoError(t, err)

		store, err := externalStorageFromURI(ctx, backupPath, security.TestUserName())
		require.NoError(t, err)
		files, err := store.ListFiles(ctx, "*.sst" /*patternSuffix*/)
		require.NoError(t, err)
		require.Equal(t, 1 /*expected*/, len(files))
		sstFile := files[0]

		expected := fmt.Sprintf(
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
}`, ts.GoTime().Format(time.RFC3339), srv.ClusterID(), build.GetInfo().Short(), sstFile)
		require.Equal(t, expected, jsonString)
	})
}
