// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageccl

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSSTReaderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var openCalls, expectedOpenCalls int
	const sz, suffix = 100, 10
	raw := &sstReader{
		sz:   sizeStat(sz),
		body: ioctx.NopCloser(ioctx.ReaderAdapter(bytes.NewReader(nil))),
		openAt: func(offset int64) (ioctx.ReadCloserCtx, error) {
			openCalls++
			return ioctx.NopCloser(ioctx.ReaderAdapter(bytes.NewReader(make([]byte, sz-int(offset))))), nil
		},
	}

	require.Equal(t, 0, openCalls)
	_ = raw.readAndCacheSuffix(suffix)
	expectedOpenCalls++

	discard := make([]byte, 5)

	// Reading in the suffix doesn't make another call.
	_, _ = raw.ReadAt(discard, 90)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading in the suffix again doesn't make another call.
	_, _ = raw.ReadAt(discard, 95)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading outside the suffix makes a new call.
	_, _ = raw.ReadAt(discard, 85)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Reading at same offset, outside the suffix, does make a new call to rewind.
	_, _ = raw.ReadAt(discard, 85)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at new pos does makes a new call.
	_, _ = raw.ReadAt(discard, 0)
	expectedOpenCalls++
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at cur pos (where last read stopped) does not reposition.
	_, _ = raw.ReadAt(discard, 5)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at in suffix between non-suffix reads does not make a call.
	_, _ = raw.ReadAt(discard, 92)
	require.Equal(t, expectedOpenCalls, openCalls)

	// Read at where prior non-suffix read finished does not make a new call.
	_, _ = raw.ReadAt(discard, 10)
	require.Equal(t, expectedOpenCalls, openCalls)
}

// TestNewExternalSSTReader ensures that ExternalSSTReader properly reads and
// iterates through semi-overlapping SSTs stored in different external storage
// base directories. The SSTs created have the following spans:
//
// t3               a500--------------------a10000
//
// t2      a50--------------a1000
//
// t1   a0----a100
func TestNewExternalSSTReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tempDir, dirCleanupFn := testutils.TempDir(t)
	defer nodelocal.ReplaceNodeLocalForTesting(tempDir)()
	defer dirCleanupFn()
	clusterSettings := cluster.MakeTestingClusterSettings()

	const localFoo = "nodelocal://1/foo"

	subdirs := []string{"a", "b", "c"}
	fileStores := make([]StoreFile, len(subdirs))
	sstSize := []int{100, 1000, 1000}
	for i, subdir := range subdirs {

		// Create a store rooted in the file's subdir
		store, err := cloud.EarlyBootExternalStorageFromURI(
			ctx,
			localFoo+subdir+"/",
			base.ExternalIODirConfig{},
			clusterSettings,
			nil, /* limiters */
			cloud.NilMetrics,
		)
		require.NoError(t, err)
		fileStores[i].Store = store

		// Create the sst at timestamp i+1, and overlap it with the previous SST
		ts := i + 1
		startKey := 0
		if i > 0 {
			startKey = sstSize[i-1] / 2
		}
		kvs := make(storageutils.KVs, 0, sstSize[i])

		for j := startKey; j < sstSize[i]; j++ {
			suffix := string(encoding.EncodeVarintAscending([]byte{}, int64(j)))
			kvs = append(kvs, storageutils.PointKV("a"+suffix, ts, "1"))
		}

		fileName := subdir + "DistinctFileName.sst"
		fileStores[i].FilePath = fileName

		sst, _, _ := storageutils.MakeSST(t, clusterSettings, kvs)

		w, err := store.Writer(ctx, fileName)
		require.NoError(t, err)
		_, err = w.Write(sst)
		require.NoError(t, err)
		w.Close()
	}

	var iterOpts = storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}

	iter, err := ExternalSSTReader(ctx, fileStores, nil, iterOpts)
	require.NoError(t, err)
	for iter.SeekGE(storage.MVCCKey{Key: keys.LocalMax}); ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
	}
}

func TestNewExternalSSTReaderFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	metrics := cloud.MakeMetrics(cidr.NewLookup(&settings.SV))

	const localFoo = "nodelocal://1/foo"

	store, err := cloud.EarlyBootExternalStorageFromURI(ctx,
		localFoo,
		base.ExternalIODirConfig{},
		settings,
		nil, /* limiters */
		metrics)
	require.NoError(t, err)

	fileName := "ExistingFile.sst"
	sst, _, _ := storageutils.MakeSST(t, settings, []interface{}{})
	w, err := store.Writer(ctx, fileName)
	require.NoError(t, err)
	_, err = w.Write(sst)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	fileStores := []StoreFile{
		{
			Store:    store,
			FilePath: "ExistingFile.sst",
		},
		{
			Store:    store,
			FilePath: "DoesNotExist.sst",
		},
	}
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}

	_, err = ExternalSSTReader(ctx, fileStores, nil, iterOpts)
	require.Error(t, err)
	require.Equal(t,
		int64(0),
		metrics.(*cloud.Metrics).OpenReaders.Value(),
		"unexpected open readers")
}
