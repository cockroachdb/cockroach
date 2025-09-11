// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCommittedWritesSurviveSplitWithStaleRangeCache is a regression test for
// cockroachlabs/cockroach#16173. A committed transaction has intents on both
// sides of a split that the node resolving them (the leaseholder of the
// transaction record's range) doesn't know about yet. That node's intent
// resolver puts the point ResolveIntent requests of both new ranges into one
// batch with a 4 MiB TargetBytes. Committing a large intent whose timestamp is
// below the commit timestamp rewrites the value and uses up the budget in the
// left-hand range, so the DistSender never sends the right-hand requests. They
// used to come back without a ResumeSpan, which the intent resolver took as
// done: it garbage collected the transaction record, and the next reader of
// the right-hand intent treated the transaction as aborted and removed the
// committed value.
func TestCommittedWritesSurviveSplitWithStaleRangeCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "split", func(t *testing.T, split bool) {
		testutils.RunTrueAndFalse(t, "large", func(t *testing.T, large bool) {
			testCommittedWritesSurviveSplitWithStaleRangeCache(t, split, large)
		})
	})
}

func testCommittedWritesSurviveSplitWithStaleRangeCache(t *testing.T, split bool, large bool) {
	ctx := context.Background()
	// n1 holds the lease of the transaction's anchor range, so it stores the
	// transaction record and resolves the transaction's intents. n2 holds the
	// lease of the data range and runs the split. n3 is the gateway.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)
	n1, n2, n3 := tc.Server(0), tc.Server(1), tc.Server(2)

	scratch := tc.ScratchRange(t)
	key := func(s string) roachpb.Key {
		return append(scratch[:len(scratch):len(scratch)], s...)
	}
	anchor1, anchor2 := key("0-anchor1"), key("0-anchor2")
	keyA, keyB, keyC := key("1-a"), key("1-b"), key("1-c")
	_, dataDesc := tc.SplitRangeOrFatal(t, key("1"))
	dataDesc = tc.AddVotersOrFatal(t, dataDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.TransferRangeLeaseOrFatal(t, dataDesc, tc.Target(1))

	oldVal := []byte("old")
	size := 16
	if large {
		// Large enough that committing a single intent uses up the intent
		// resolver's 4 MiB TargetBytes.
		size = 5 << 20
	}
	newVal := bytes.Repeat([]byte("x"), size)
	// Writing through n1 also leaves the data range's descriptor in n1's range
	// cache.
	require.NoError(t, n1.DB().Put(ctx, keyA, oldVal))
	require.NoError(t, n1.DB().Put(ctx, keyC, oldVal))

	// n1CachedDesc returns the descriptor that n1's range cache has for k.
	n1CachedDesc := func(k roachpb.Key) roachpb.RangeDescriptor {
		ds := n1.DistSenderI().(*kvcoord.DistSender)
		ri, err := ds.RangeDescriptorCache().TestingGetCached(
			ctx, keys.MustAddr(k), false /* inverted */, roachpb.LAG_BY_CLUSTER_SETTING)
		require.NoError(t, err)
		return ri.Desc
	}
	require.Equal(t, dataDesc.RangeID, n1CachedDesc(keyA).RangeID)
	require.Equal(t, dataDesc.RangeID, n1CachedDesc(keyC).RangeID)

	db := n3.DB()
	txn := db.NewTxn(ctx, "test")
	require.NoError(t, txn.Put(ctx, anchor1, "anchor"))
	require.NoError(t, txn.Put(ctx, keyA, newVal))
	require.NoError(t, txn.Put(ctx, keyC, newVal))
	intentTS := txn.ProvisionalCommitTimestamp()

	// Push the transaction's write timestamp above its intents, so that
	// committing them rewrites the values: a read of anchor2 followed by the
	// transaction's write to it.
	_, err := db.Get(ctx, anchor2)
	require.NoError(t, err)
	require.NoError(t, txn.Put(ctx, anchor2, "anchor"))
	require.True(t, intentTS.Less(txn.ProvisionalCommitTimestamp()))

	if split {
		// Split the data range between a and c. The request goes through n2, so
		// n1 isn't told about the split.
		require.NoError(t, n2.DB().AdminSplit(ctx, keyB, hlc.MaxTimestamp))
		require.Equal(t, dataDesc.RangeID, n1CachedDesc(keyC).RangeID,
			"expected n1's cached descriptor for c to still be the pre-split one")
		require.NotEqual(t, dataDesc.RangeID, tc.LookupRangeOrFatal(t, keyC).RangeID)
	}

	require.NoError(t, txn.Commit(ctx))

	// Wait for the asynchronous intent resolution to finish, which ends with the
	// garbage collection of the transaction record.
	txnKey := keys.TransactionKey(anchor1, txn.ID())
	testutils.SucceedsSoon(t, func() error {
		var rec roachpb.Transaction
		ok, err := storage.MVCCGetProto(
			ctx, n1.Engines()[0], txnKey, hlc.Timestamp{}, &rec, storage.MVCCGetOptions{})
		if err != nil {
			return err
		}
		if ok {
			return errors.Newf("txn record still present: %s", rec)
		}
		return nil
	})

	// Look at the raw state of the two keys on the leaseholder without pushing
	// anything: no intent may outlive the transaction record.
	for _, k := range []roachpb.Key{keyA, keyC} {
		res, err := storage.MVCCGet(ctx, n2.Engines()[0], k, hlc.MaxTimestamp,
			storage.MVCCGetOptions{Inconsistent: true})
		require.NoError(t, err)
		require.Nil(t, res.Intent, "intent on %s left behind after the txn record was GC'ed", k)
	}

	// Read both keys the way any later reader would.
	for _, k := range []roachpb.Key{keyA, keyC} {
		res, err := db.Get(ctx, k)
		require.NoError(t, err)
		b, err := res.Value.GetBytes()
		require.NoError(t, err)
		require.True(t, bytes.Equal(b, newVal), "the committed write to %s was lost", k)
	}
}
