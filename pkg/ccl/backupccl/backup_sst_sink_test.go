// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func generateMockSSTs(
	t *testing.T, ctx context.Context, storage cloud.ExternalStorage, returnedSSTs chan returnedSST,
) {
	t.Helper()
	reader, err := storage.ReadFile(ctx, "")
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
			skipWrite:      true,
		}
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

	runTest := func() {
		server := tc.Server(0)
		storageFactory := server.DistSQLServer().(*distsql.ServerImpl).ExternalStorageFromURI
		storage, err := storageFactory(ctx, "nodelocal://1/csvs/default/full", security.RootUserName())
		require.NoError(t, err)
		var files []string
		require.NoError(t, storage.List(ctx, "/", "", func(f string) error {
			files = append(files, f)
			return nil
		}))

		sinks := make([]*sstSink, len(files))
		sinksCh := make([]chan returnedSST, len(files))
		sinksStore := make([]cloud.ExternalStorage, len(files))
		for i := range files {
			sinkConf := sstSinkConf{
				id:       base.SQLInstanceID(i),
				enc:      &roachpb.FileEncryptionOptions{},
				progCh:   nil,
				settings: &server.ClusterSettings().SV,
			}

			storage, err := storageFactory(ctx, fmt.Sprintf("nodelocal://1/mocksst%d", i),
				security.RootUserName())
			require.NoError(t, err)

			st := cluster.MakeTestingClusterSettings()
			m := mon.NewUnlimitedMonitor(
				ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
			)
			sink, err := makeSSTSink(ctx, sinkConf, storage, newMemoryAccumulator(m))
			require.NoError(t, err)
			sinks[i] = sink
			sinksCh[i] = make(chan returnedSST, 1)
			sinksStore[i], err = storageFactory(ctx, fmt.Sprintf("nodelocal://1/csvs/default/full/%s", files[i]),
				security.RootUserName())
			require.NoError(t, err)
		}

		// Start generating mock SSTs.
		grp := ctxgroup.WithContext(ctx)
		for i := range files {
			index := i
			grp.GoCtx(func(ctx context.Context) error {
				sstChan := sinksCh[index]
				defer close(sstChan)
				generateMockSSTs(t, ctx, sinksStore[index], sstChan)
				return nil
			})
		}

		for i := range files {
			index := i
			grp.GoCtx(func(ctx context.Context) error {
				sinkCh := sinksCh[index]
				for sst := range sinkCh {
					err := sinks[index].push(ctx, sst)
					require.NoError(t, err)
				}
				require.NoError(t, sinks[index].flush(ctx))
				err := sinks[index].Close()
				err = errors.CombineErrors(sinksStore[index].Close(), err)
				require.NoError(t, err)
				return nil
			})
		}

		require.NoError(t, grp.Wait())

		// Print sstSink stats.
		for _, sink := range sinks {
			log.Info(ctx, "****NODE****\n\n")
			log.Infof(ctx, "sink stats: %+v\n\n", sink.stats)
		}
	}

	for _, bufferSize := range []string{"64"} {
		log.Infof(ctx, "merge_file_buffer_size  = %s MiB;\n\n", bufferSize)
		sqlDB.Exec(t, fmt.Sprintf(`SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '%sMiB'`, bufferSize))
		runTest()
	}
}
