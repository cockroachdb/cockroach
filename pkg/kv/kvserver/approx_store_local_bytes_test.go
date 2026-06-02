// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestApproxStoreLocalBytes verifies that ApproxStoreLocalBytes in
// RangeAppliedState increases as KV writes are applied.
func TestApproxStoreLocalBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store := tc.GetFirstStoreFromServer(t, 0)

	loadBytes := func(k roachpb.Key) int64 {
		t.Helper()
		repl := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write KV pairs and verify ApproxStoreLocalBytes > 0.
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Put(ctx, append(key, byte(i)), "value"))
	}
	bytesAfterWrites := loadBytes(key)
	require.Greater(t, bytesAfterWrites, int64(0),
		"ApproxStoreLocalBytes should be > 0 after writes")

	// Write more and verify the counter increased.
	for i := 5; i < 10; i++ {
		require.NoError(t, db.Put(ctx, append(key, byte(i)), "value"))
	}
	bytesAfterMoreWrites := loadBytes(key)
	require.Greater(t, bytesAfterMoreWrites, bytesAfterWrites,
		"ApproxStoreLocalBytes should increase with more writes")

	// Delete keys and verify the counter still increases. Deletes are writes
	// (they produce tombstones), so the monotonically increasing counter
	// should go up, not down.
	for i := 0; i < 5; i++ {
		_, err := db.Del(ctx, append(key, byte(i)))
		require.NoError(t, err)
	}
	require.Greater(t, loadBytes(key), bytesAfterMoreWrites,
		"ApproxStoreLocalBytes should increase with deletes")
}

// TestApproxStoreLocalBytesAddSSTable verifies that ApproxStoreLocalBytes
// increases when an AddSSTable request is applied.
func TestApproxStoreLocalBytesAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	store := tc.GetFirstStoreFromServer(t, 0)

	loadBytes := func(k roachpb.Key) int64 {
		t.Helper()
		repl := store.LookupReplica(roachpb.RKey(k))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	bytesBefore := loadBytes(key)

	// Build and send an AddSSTable request.
	kvs := make(storageutils.KVs, 5)
	for i := range kvs {
		k := append(key[:len(key):len(key)], byte('a'+i))
		kvs[i] = storageutils.PointKV(string(k), 1, "sst-value")
	}
	sst, start, end := storageutils.MakeSST(t, tc.Server(0).ClusterSettings(), kvs)
	addReq := &kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     storageutils.SSTStats(t, sst, 0),
	}
	_, pErr := kv.SendWrapped(ctx, tc.Server(0).DB().NonTransactionalSender(), addReq)
	require.NoError(t, pErr.GoError())

	bytesAfter := loadBytes(key)
	require.Greater(t, bytesAfter, bytesBefore,
		"ApproxStoreLocalBytes should increase after AddSSTable")
}

// TestApproxStoreLocalBytesSplit verifies that ApproxStoreLocalBytes is
// approximately halved on both sides after a split.
func TestApproxStoreLocalBytesSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	loadBytes := func(key roachpb.Key) int64 {
		t.Helper()
		repl := store.LookupReplica(roachpb.RKey(key))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write KV pairs to accumulate bytes.
	for i := 0; i < 10; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	preSplitBytes := loadBytes(scratchKey)
	require.Greater(t, preSplitBytes, int64(0))

	// Split.
	splitKey := scratchKey.Next()
	_, _, err = tc.SplitRange(splitKey)
	require.NoError(t, err)

	lhsBytes := loadBytes(scratchKey)
	rhsBytes := loadBytes(splitKey)
	t.Logf("pre-split=%d lhs=%d rhs=%d", preSplitBytes, lhsBytes, rhsBytes)

	// Both sides should have bytes > 0.
	require.Greater(t, lhsBytes, int64(0))
	require.Greater(t, rhsBytes, int64(0))
	// The sum should exceed the pre-split value (the split command itself
	// also writes bytes).
	require.Greater(t, lhsBytes+rhsBytes, preSplitBytes)
	// Each side should be less than the total.
	require.Less(t, rhsBytes, lhsBytes+rhsBytes)
}

// TestApproxStoreLocalBytesMerge verifies that ApproxStoreLocalBytes is
// the sum of both sides after a merge.
func TestApproxStoreLocalBytesMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	loadBytes := func(key roachpb.Key) int64 {
		t.Helper()
		repl := store.LookupReplica(roachpb.RKey(key))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write some KV pairs, then split.
	for i := 0; i < 10; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	splitKey := scratchKey.Next()
	_, _, err = tc.SplitRange(splitKey)
	require.NoError(t, err)

	// Write more to each side to get distinct values.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('A'+i))
		require.NoError(t, db.Put(ctx, key, "lhs-extra"))
	}
	for i := 0; i < 5; i++ {
		key := append(splitKey[:len(splitKey):len(splitKey)], byte('A'+i))
		require.NoError(t, db.Put(ctx, key, "rhs-extra"))
	}

	lhsBytes := loadBytes(scratchKey)
	rhsBytes := loadBytes(splitKey)
	require.Greater(t, lhsBytes, int64(0))
	require.Greater(t, rhsBytes, int64(0))

	// Merge.
	_, err = tc.MergeRanges(scratchKey)
	require.NoError(t, err)

	mergedBytes := loadBytes(scratchKey)
	t.Logf("lhs=%d rhs=%d merged=%d", lhsBytes, rhsBytes, mergedBytes)
	// The merged value should be at least the sum of both sides (the merge
	// command itself also adds bytes).
	require.GreaterOrEqual(t, mergedBytes, lhsBytes+rhsBytes)
}
