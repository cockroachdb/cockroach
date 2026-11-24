// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// makeTestSSTIterator creates a SimpleMVCCIterator from test key-values using a real SST.
func makeTestSSTIterator(t *testing.T, kvs []interface{}) storage.SimpleMVCCIterator {
	t.Helper()
	st := cluster.MakeTestingClusterSettings()
	sstData, _, _ := storageutils.MakeSST(t, st, kvs)
	iter, err := storage.NewMemSSTIterator(sstData, false /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	return iter
}

// TestKeySuffixRoundTrip verifies that keys can be suffixed and then have the
// suffix removed to recover the original key.
func TestKeySuffixRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name    string
		key     string
		sstPath string
	}{
		{
			name:    "simple key",
			key:     "key-1",
			sstPath: "nodelocal://1/merge/sst-001.sst",
		},
		{
			name:    "another key",
			key:     "another/path/key",
			sstPath: "nodelocal://2/merge/sst-003.sst",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create an SST iterator with the key.
			iter := makeTestSSTIterator(t, []interface{}{
				storageutils.PointKV(tc.key, 1, "value"),
			})

			// Wrap with suffixing iterator. The suffixingIter.Close() will close
			// the underlying iterator, so we don't defer iter.Close() separately.
			suffixingIter := newSuffixingIterator(iter, tc.sstPath)
			defer suffixingIter.Close()

			// Position the iterator at the first key.
			suffixingIter.SeekGE(storage.MVCCKey{})
			ok, err := suffixingIter.Valid()
			require.NoError(t, err)
			require.True(t, ok)

			// Get the suffixed key and the original key from the underlying iterator.
			suffixedKey := suffixingIter.UnsafeKey()
			originalKey := iter.UnsafeKey()

			// Verify the suffixed key is longer than the original.
			require.Greater(t, len(suffixedKey.Key), len(originalKey.Key),
				"suffixed key should be longer than original")
			require.Equal(t, len(originalKey.Key)+keySuffixLen, len(suffixedKey.Key),
				"suffix should add exactly %d bytes", keySuffixLen)

			// Verify timestamp is preserved.
			require.Equal(t, originalKey.Timestamp, suffixedKey.Timestamp,
				"timestamp should be preserved")

			// Remove suffix and verify we get the original key back.
			baseKey, err := removeKeySuffix(suffixedKey.Key)
			require.NoError(t, err)
			require.Equal(t, originalKey.Key, baseKey, "base key should match original")
		})
	}
}

// TestDifferentSSTPaths verifies that different SST paths produce different
// suffixes, making keys distinguishable.
func TestDifferentSSTPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sstPaths := []string{
		"nodelocal://1/merge/sst-001.sst",
		"nodelocal://1/merge/sst-002.sst",
		"nodelocal://2/merge/sst-001.sst",
		"nodelocal://1/node-1/job-123/sst-001.sst",
		"nodelocal://1/node-2/job-123/sst-001.sst",
	}

	suffixedKeys := make([]roachpb.Key, len(sstPaths))
	var originalKey roachpb.Key
	for i, path := range sstPaths {
		iter := makeTestSSTIterator(t, []interface{}{
			storageutils.PointKV("key-1", 1, "value"),
		})
		// Wrap with suffixing iterator. suffixingIter.Close() will close iter.
		suffixingIter := newSuffixingIterator(iter, path)
		suffixingIter.SeekGE(storage.MVCCKey{})
		ok, err := suffixingIter.Valid()
		require.NoError(t, err)
		require.True(t, ok)
		suffixedKeys[i] = suffixingIter.UnsafeKey().Key.Clone()
		if originalKey == nil {
			originalKey = iter.UnsafeKey().Key.Clone()
		}
		suffixingIter.Close()
	}

	// Verify all suffixed keys are different.
	for i := 0; i < len(suffixedKeys); i++ {
		for j := i + 1; j < len(suffixedKeys); j++ {
			require.NotEqual(t, suffixedKeys[i], suffixedKeys[j],
				"keys from different SST paths should have different suffixes")
		}
	}

	// Verify all base keys are the same.
	for i, suffixedKey := range suffixedKeys {
		baseKey, err := removeKeySuffix(suffixedKey)
		require.NoError(t, err)
		require.Equal(t, originalKey, baseKey, "base key should be the same for path %d", i)
	}
}

// TestRemoveKeySuffixErrors verifies that removeKeySuffix returns errors for
// invalid keys.
func TestRemoveKeySuffixErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name string
		key  roachpb.Key
	}{
		{
			name: "key too short",
			key:  roachpb.Key{0x01, 0x02, 0x03},
		},
		{
			name: "key just under suffix length",
			key:  roachpb.Key{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := removeKeySuffix(tc.key)
			require.Error(t, err, "should return error for invalid key")
		})
	}
}

// TestMergingIteratorSurfacesAllKeys verifies that the merging iterator
// surfaces all keys from all input iterators, including duplicates.
func TestMergingIteratorSurfacesAllKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create two SST iterators with overlapping keys.
	// Iterator 1: keys a, b, c.
	// Iterator 2: keys b, c, d.
	iter1 := makeTestSSTIterator(t, []interface{}{
		storageutils.PointKV("a", 1, "v1"),
		storageutils.PointKV("b", 1, "v2"),
		storageutils.PointKV("c", 1, "v3"),
	})

	iter2 := makeTestSSTIterator(t, []interface{}{
		storageutils.PointKV("b", 1, "v4"),
		storageutils.PointKV("c", 1, "v5"),
		storageutils.PointKV("d", 1, "v6"),
	})

	// Wrap with suffixing iterators.
	suffixedIter1 := newSuffixingIterator(iter1, "sst-1")
	suffixedIter2 := newSuffixingIterator(iter2, "sst-2")

	// Merge them. The mergingIter.Close() will close all child iterators.
	mergingIter := newMergingIterator(
		[]storage.SimpleMVCCIterator{suffixedIter1, suffixedIter2},
		storage.IterOptions{},
	)
	defer mergingIter.Close()

	// Collect all base keys (with suffix removed).
	var baseKeys []string
	mergingIter.SeekGE(storage.MVCCKey{})
	for {
		ok, err := mergingIter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		key := mergingIter.UnsafeKey()
		baseKey, err := removeKeySuffix(key.Key)
		require.NoError(t, err)
		// The base key includes the tenant prefix, so extract just the user key
		// portion for comparison. The user key is the last part after the prefix.
		baseKeys = append(baseKeys, string(baseKey[len(baseKey)-1:]))
		mergingIter.NextKey()
	}

	// Expect all 6 keys: a, b(1), b(2), c(1), c(2), d.
	require.Len(t, baseKeys, 6, "should see all keys including duplicates")

	// Verify order: a, b, b, c, c, d.
	expectedKeys := []string{"a", "b", "b", "c", "c", "d"}
	require.Equal(t, expectedKeys, baseKeys, "keys should be in expected order")
}

// TestMergingIteratorDuplicateDetection verifies that consecutive base keys
// can be used to detect duplicates.
func TestMergingIteratorDuplicateDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create two SST iterators with a duplicate key "b".
	iter1 := makeTestSSTIterator(t, []interface{}{
		storageutils.PointKV("a", 1, "v1"),
		storageutils.PointKV("b", 1, "v2"),
	})

	iter2 := makeTestSSTIterator(t, []interface{}{
		storageutils.PointKV("b", 1, "v3"),
		storageutils.PointKV("c", 1, "v4"),
	})

	// Wrap with suffixing iterators.
	suffixedIter1 := newSuffixingIterator(iter1, "sst-1")
	suffixedIter2 := newSuffixingIterator(iter2, "sst-2")

	// Merge them. The mergingIter.Close() will close all child iterators.
	mergingIter := newMergingIterator(
		[]storage.SimpleMVCCIterator{suffixedIter1, suffixedIter2},
		storage.IterOptions{},
	)
	defer mergingIter.Close()

	// Simulate duplicate detection logic like in processMergedData.
	var prevBaseKey roachpb.Key
	duplicateFound := false
	var duplicateUserKey string

	mergingIter.SeekGE(storage.MVCCKey{})
	for {
		ok, err := mergingIter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}

		key := mergingIter.UnsafeKey()
		baseKey, err := removeKeySuffix(key.Key)
		require.NoError(t, err)

		if prevBaseKey != nil && baseKey.Equal(prevBaseKey) {
			duplicateFound = true
			// Extract just the user key portion (last byte) for comparison.
			duplicateUserKey = string(baseKey[len(baseKey)-1:])
			break
		}
		prevBaseKey = baseKey.Clone()
		mergingIter.NextKey()
	}

	require.True(t, duplicateFound, "should detect duplicate key")
	require.Equal(t, "b", duplicateUserKey, "duplicate key should be 'b'")
}
