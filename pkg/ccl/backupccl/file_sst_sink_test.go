// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestFileSSTSinkExtendOneFile is a regression test for a bug in fileSSTSink in
// which the sink fails to extend its last span added if there's only one file
// in the sink so far.
func TestFileSSTSinkExtendOneFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanup()

	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///0",
		base.ExternalIODirConfig{},
		tc.Servers[0].ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		tc.Servers[0].InternalDB().(isql.DB),
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)
	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '20B'`)

	// Never block.
	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress, 10)

	sinkConf := sstSinkConf{
		id:       1,
		enc:      nil,
		progCh:   progCh,
		settings: &tc.Servers[0].ClusterSettings().SV,
	}

	sink := makeFileSSTSink(sinkConf, store)

	getKeys := func(prefix string, n int) []byte {
		var b bytes.Buffer
		sst := storage.MakeBackupSSTWriter(ctx, nil, &b)
		for i := 0; i < n; i++ {
			require.NoError(t, sst.PutUnversioned([]byte(fmt.Sprintf("%s%08d", prefix, i)), nil))
		}
		sst.Close()
		return b.Bytes()
	}

	exportResponse1 := exportedSpan{
		metadata: backuppb.BackupManifest_File{
			Span: roachpb.Span{
				Key:    []byte("b"),
				EndKey: []byte("b"),
			},
			EntryCounts: roachpb.RowCount{
				DataSize: 100,
				Rows:     1,
			},
			StartTime:  hlc.Timestamp{},
			EndTime:    hlc.Timestamp{},
			LocalityKV: "",
		},
		dataSST:        getKeys("b", 100),
		revStart:       hlc.Timestamp{},
		completedSpans: 1,
		atKeyBoundary:  false,
	}

	exportResponse2 := exportedSpan{
		metadata: backuppb.BackupManifest_File{
			Span: roachpb.Span{
				Key:    []byte("b"),
				EndKey: []byte("z"),
			},
			EntryCounts: roachpb.RowCount{
				DataSize: 100,
				Rows:     1,
			},
			StartTime:  hlc.Timestamp{},
			EndTime:    hlc.Timestamp{},
			LocalityKV: "",
		},
		dataSST:        getKeys("c", 100),
		revStart:       hlc.Timestamp{},
		completedSpans: 1,
		atKeyBoundary:  true,
	}

	require.NoError(t, sink.write(ctx, exportResponse1))
	require.NoError(t, sink.write(ctx, exportResponse2))

	close(progCh)

	var progs []execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	for p := range progCh {
		progs = append(progs, p)
	}

	require.Equal(t, 1, len(progs))
	var progDetails backuppb.BackupManifest_Progress
	if err := types.UnmarshalAny(&progs[0].ProgressDetails, &progDetails); err != nil {
		t.Fatal(err)
	}

	// Verify that the file in the sink was properly extended and there is only 1
	// file in the progress details.
	require.Equal(t, 1, len(progDetails.Files))
}
