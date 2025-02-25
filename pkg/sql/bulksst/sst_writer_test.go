// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func encodeKey(strKey string) []byte {
	key := storage.MVCCKey{
		Key: []byte(strKey),
	}
	return storage.EncodeMVCCKeyToBuf(nil, key)
}

func TestBulkSSTWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const writePath = "test_gsst"
	fs := s.StorageLayer().Engines()[0].Env()

	fileAllocator := bulksst.NewVFSFileAllocator(writePath, fs)
	// Create a new batcher
	bulksst.BatchSize.Override(ctx, &s.ClusterSettings().SV, 1024)
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)

	// Intentionally go in an unsorted order.
	expectedSet := intsets.MakeFast()
	for i := 8192; i > 0; i-- {
		require.NoError(t, batcher.AddMVCCKey(ctx, storage.MVCCKey{
			Timestamp: s.Clock().Now(),
			Key:       encodeKey(fmt.Sprintf("key-%d", i)),
		},
			[]byte(fmt.Sprintf("value-%d", i))))
		expectedSet.Add(i)
	}
	require.NoError(t, batcher.CloseWithError(ctx))

	// Next validate each SST file.
	set := intsets.MakeFast()
	lastFileMin := -1
	for _, fileName := range fileAllocator.GetFileList() {
		currFileMin := -1
		currFileMax := -1
		values := readKeyValuesFromSST(t, fs, fileName)
		for _, value := range values {
			keyString := string(value.Key.Key)
			var num int
			scanned, err := fmt.Sscanf(strings.Split(keyString, "-")[1], "%d", &num)
			require.NoError(t, err)
			require.Equal(t, 1, scanned)
			set.Add(num)
			if currFileMin == -1 || currFileMin > num {
				currFileMin = num
			}
			if currFileMax == -1 || currFileMax < num {
				currFileMax = num
			}
		}
		// Ensure continuity between SSTs, where the minimum on the
		// previous file should continue to this file.
		if lastFileMin > 0 {
			require.Equal(t, lastFileMin-1, currFileMax)
		}
		lastFileMin = currFileMin
	}
	// Ensure we have all the inserted key / values.
	require.Equal(t, 8192, set.Len())
	require.Greaterf(t, len(fileAllocator.GetFileList()), 100, "expected multiple files")
	require.Zero(t, expectedSet.Difference(set).Len())
}

func readKeyValuesFromSST(t *testing.T, fs vfs.FS, filename string) []storage.MVCCKeyValue {
	file, err := fs.Open(filename, nil)
	require.NoError(t, err)
	readable, err := sstable.NewSimpleReadable(file)
	require.NoError(t, err)

	reader, err := sstable.NewReader(
		context.Background(),
		readable,
		storage.DefaultPebbleOptions().MakeReaderOptions())
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil)
	if err != nil {

	}
	defer func() {
		require.NoError(t, iter.Close())
	}()

	result := make([]storage.MVCCKeyValue, 0)
	for internalKV := iter.First(); internalKV != nil; internalKV = iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(internalKV.K.UserKey)
		require.NoError(t, err)
		rawValue, _, err := internalKV.V.Value(nil)
		require.NoError(t, err)
		result = append(result, storage.MVCCKeyValue{
			Key:   mvccKey.Clone(),
			Value: rawValue,
		})
	}
	return result
}
