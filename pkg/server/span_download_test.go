// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestSpanDownloadLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalIODir:     dir,
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	accessor := srv.ExternalStorageAccessor().(*cloud.ExternalStorageAccessor)
	eng := storage.NewDefaultInMemForTesting(storage.RemoteStorageFactory(accessor))
	defer eng.Close()

	k := func(s string) roachpb.Key {
		k, err := keys.RewriteKeyToTenantPrefix(roachpb.Key(s), srv.Codec().TenantPrefix())
		require.NoError(t, err)
		return k
	}
	key := storage.MVCCKey{Key: k("bb"), Timestamp: hlc.Timestamp{WallTime: 1}}
	value := roachpb.MakeValueFromString("1")
	value.InitChecksum([]byte("foo"))

	var sstFile bytes.Buffer
	w := storage.MakeBackupSSTWriter(ctx, srv.ClusterSettings(), &sstFile)
	defer w.Close()
	require.NoError(t, w.Put(key, value.RawBytes))
	require.NoError(t, w.Finish())

	sstFileName := "foo"
	sstPath := "test"

	// Write this file to dir/foo with name test.
	if err := os.Mkdir(filepath.Join(dir, sstPath), 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, sstPath, sstFileName), sstFile.Bytes(), 0644); err != nil {
		t.Fatalf("%+v", err)
	}

	_, err := eng.IngestExternalFiles(ctx, []pebble.ExternalFile{
		{
			Locator:         "nodelocal://1/test",
			ObjName:         "foo",
			Size:            100,
			SmallestUserKey: roachpb.KeyMin,
			LargestUserKey:  roachpb.KeyMax,
			HasPointKey:     true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, eng.Download(ctx, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}))
}
