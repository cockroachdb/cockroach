// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func generateMockSSTs(
	t *testing.T,
	ctx context.Context,
	storage cloud.ExternalStorage,
	filename string,
	returnedSSTs chan returnedSST,
) {
	t.Helper()
	reader, err := storage.ReadFile(ctx, filename)
	require.NoError(t, err)

	cr := csv.NewReader(reader)
	for {
		record, err := cr.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Read startKey, endKey, byteSize.
		require.Len(t, record, 3)
		startKey, err := base64.StdEncoding.DecodeString(record[0])
		require.NoError(t, err)
		endKey, err := base64.StdEncoding.DecodeString(record[1])
		require.NoError(t, err)
		byteSize, err := strconv.Atoi(record[2])
		require.NoError(t, err)

		// Generate random data for sst.
		sst := make([]byte, byteSize)
		require.NoError(t, err)
		retSST := returnedSST{
			f: BackupManifest_File{
				Span:        roachpb.Span{Key: startKey, EndKey: endKey},
				Path:        "",
				EntryCounts: RowCount{},
				StartTime:   hlc.Timestamp{},
				EndTime:     hlc.Timestamp{},
				LocalityKV:  "",
			},
			sst:            sst,
			revStart:       hlc.Timestamp{},
			completedSpans: 0,
			atKeyBoundary:  false,
			skipWrite:      true,
		}
		fmt.Println(retSST.f.Span.String())
		returnedSSTs <- retSST
	}
}

func TestBackupSSTSinkFlushBehavior(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseDir := "testdata"
	args := base.TestServerArgs{ExternalIODir: baseDir}
	params := base.TestClusterArgs{ServerArgs: args}

	ctx, tc, sqlDB, cleanupFn := backupRestoreTestSetupEmptyWithParams(t, singleNode, baseDir, InitManualReplication, params)
	defer cleanupFn()

	sqlDB.Exec(t, `SELECT crdb_internal.set_vmodule('backup_sst_sink=2');`)

	server := tc.Server(0)
	sinkConf := sstSinkConf{
		id:       base.SQLInstanceID(1),
		enc:      &roachpb.FileEncryptionOptions{},
		progCh:   nil,
		settings: &server.ClusterSettings().SV,
	}

	storageFactory := server.DistSQLServer().(*distsql.ServerImpl).ExternalStorageFromURI
	storage, err := storageFactory(ctx, "nodelocal://1/mocksst", security.RootUserName())
	require.NoError(t, err)

	sink := &sstSink{conf: sinkConf, dest: storage}

	defer func() {
		err := sink.Close()
		err = errors.CombineErrors(storage.Close(), err)
		if err != nil {
			log.Warningf(ctx, "failed to close backup sink(s): %+v", err)
		}
	}()

	ssts := make(chan returnedSST, 1)

	// Start generating mock SSTs.
	go func() {
		defer close(ssts)
		storage, err := storageFactory(ctx, "nodelocal://1/", security.RootUserName())
		require.NoError(t, err)
		// TODO(adityamaru): add CSV filename to read keys from.
		generateMockSSTs(t, ctx, storage, "backuplog.csv", ssts)
	}()

	for sst := range ssts {
		err := sink.push(ctx, sst)
		require.NoError(t, err)
	}
	require.NoError(t, sink.flush(ctx))

	// Print sstSink stats.
	log.Infof(ctx, "sink stats: %+v", sink.stats)
}
