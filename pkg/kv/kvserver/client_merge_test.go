// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func adminMergeArgs(key roachpb.Key) *roachpb.AdminMergeRequest {
	return &roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

// createSplitRanges issues an AdminSplit command for the key "b". It returns
// the descriptors for the ranges to the left and right of the split.
func createSplitRanges(
	ctx context.Context, store *kvserver.Store,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	args := adminSplitArgs(roachpb.Key("b"))
	if _, err := kv.SendWrapped(ctx, store.TestSender(), args); err != nil {
		return nil, nil, err.GoError()
	}

	lhsDesc := store.LookupReplica(roachpb.RKey("a")).Desc()
	rhsDesc := store.LookupReplica(roachpb.RKey("c")).Desc()

	if bytes.Equal(lhsDesc.StartKey, rhsDesc.StartKey) {
		return nil, nil, fmt.Errorf("split ranges have the same start key: %q = %q",
			lhsDesc.StartKey, rhsDesc.StartKey)
	}

	// NB: return copies of the descriptors as a purely precautionary measure.
	// Tests have been observed to mutate the returned memory, for example in #67346.
	return protoutil.Clone(lhsDesc).(*roachpb.RangeDescriptor), protoutil.Clone(rhsDesc).(*roachpb.RangeDescriptor), nil
}

// TestStoreRangeMergeTwoEmptyRanges tries to merge two empty ranges together.
func TestStoreRangeMergeTwoEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())

	store := tc.GetFirstStoreFromServer(t, 0)

	lhsDesc, _, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Merge the RHS back into the LHS.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Verify the merge by looking up keys from both ranges.
	lhsRepl := store.LookupReplica(roachpb.RKey("a"))
	rhsRepl := store.LookupReplica(roachpb.RKey("c"))

	if !reflect.DeepEqual(lhsRepl, rhsRepl) {
		t.Fatalf("ranges were not merged: %s != %s", lhsRepl, rhsRepl)
	}

	// The LHS has been split once and merged once, so it should have received
	// two generation bumps.
	if e, a := roachpb.RangeGeneration(2), lhsRepl.Desc().Generation; e != a {
		t.Fatalf("expected LHS to have generation %d, but got %d", e, a)
	}
}

func getEngineKeySet(t *testing.T, e storage.Engine) map[string]struct{} {
	t.Helper()
	// Have to scan local and global keys separately as mentioned in the comment
	// for storage.Scan (because of its use of intentInterleavingIter).
	kvs, err := storage.Scan(e, roachpb.KeyMin, keys.LocalMax, 0 /* max */)
	if err != nil {
		t.Fatal(err)
	}
	globalKVs, err := storage.Scan(e, keys.LocalMax, roachpb.KeyMax, 0 /* max */)
	if err != nil {
		t.Fatal(err)
	}
	kvs = append(kvs, globalKVs...)
	out := map[string]struct{}{}
	for _, kv := range kvs {
		out[string(kv.Key.Key)] = struct{}{}
	}
	return out
}

// TestStoreRangeMergeMetadataCleanup tests that all metadata of a
// subsumed range is cleaned up on merge.
func TestStoreRangeMergeMetadataCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	content := roachpb.Key("testing!")

	// Write some values left of the proposed split key.
	pArgs := putArgs(roachpb.Key("aaa"), content)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Collect all the keys.
	preKeys := getEngineKeySet(t, store.Engine())

	// Split the range.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values right of the split key.
	pArgs = putArgs(roachpb.Key("ccc"), content)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Collect all the keys again.
	postKeys := getEngineKeySet(t, store.Engine())

	// Compute the new keys.
	for k := range preKeys {
		delete(postKeys, k)
	}

	tombstoneKey := string(keys.RangeTombstoneKey(rhsDesc.RangeID))
	if _, ok := postKeys[tombstoneKey]; !ok {
		t.Errorf("tombstone key (%s) missing after merge", roachpb.Key(tombstoneKey))
	}
	delete(postKeys, tombstoneKey)

	// Keep only the subsumed range's local range-ID keys.
	localRangeKeyPrefix := string(keys.MakeRangeIDPrefix(rhsDesc.RangeID))
	for k := range postKeys {
		if !strings.HasPrefix(k, localRangeKeyPrefix) {
			delete(postKeys, k)
		}
	}

	if numKeys := len(postKeys); numKeys > 0 {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%d keys were not cleaned up:\n", numKeys)
		for k := range postKeys {
			fmt.Fprintf(&buf, "%s (%q)\n", roachpb.Key(k), k)
		}
		t.Fatal(buf.String())
	}
}

// TestStoreRangeMergeWithData attempts to merge two ranges, each containing
// data.
func TestStoreRangeMergeWithData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, retries := range []int64{0, 3} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			mergeWithData(t, retries)
		})
	}
}

func mergeWithData(t *testing.T, retries int64) {
	ctx := context.Background()

	manualClock := hlc.NewHybridManualClock()
	var store *kvserver.Store
	// Maybe inject some retryable errors when the merge transaction commits.
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				if atomic.AddInt64(&retries, -1) >= 0 {
					return roachpb.NewError(
						roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "filter err"))
				}
			}
			if req.GetSubsume() != nil {
				// Introduce targeted chaos by forcing a lease acquisition before
				// Subsume can execute. This triggers an unusual code path where the
				// lease acquisition, not Subsume, notices the merge and installs a
				// mergeComplete channel on the replica.
				manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
			}
		}
		return nil
	}

	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue:    true,
				DisableSplitQueue:    true,
				TestingRequestFilter: testingRequestFilter,
			},
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	scratchKey, err := s.ScratchRangeWithExpirationLease()
	repl := store.LookupReplica(roachpb.RKey(scratchKey))
	require.NoError(t, err)

	lhsDesc, rhsDesc, pErr := s.SplitRange(scratchKey.Next().Next())
	require.NoError(t, pErr)

	content := []byte("testing!")

	// Write some values left and right of the proposed split key.

	put := func(key roachpb.Key, rangeID roachpb.RangeID, value []byte) {
		pArgs := putArgs(key, value)
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rangeID,
		}, pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	verify := func(key roachpb.Key, rangeID roachpb.RangeID, value []byte) {
		// Confirm the values are there.
		gArgs := getArgs(key)
		if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rangeID,
		}, gArgs); pErr != nil {
		} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(replyBytes, value) {
			t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
		}
	}

	put(lhsDesc.StartKey.Next().AsRawKey(), lhsDesc.RangeID, content)
	put(rhsDesc.StartKey.Next().AsRawKey(), rhsDesc.RangeID, content)

	verify(lhsDesc.StartKey.Next().AsRawKey(), lhsDesc.RangeID, content)
	verify(rhsDesc.StartKey.Next().AsRawKey(), rhsDesc.RangeID, content)

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(lhsDesc.StartKey), keys.RangeDescriptorKey(rhsDesc.StartKey)} {
		if _, _, err := storage.MVCCGet(
			ctx, store.Engine(), key, store.Clock().Now(), storage.MVCCGetOptions{},
		); err != nil {
			t.Fatal(err)
		}
	}

	// Verify the merge by looking up keys from both ranges.
	lhsRepl := store.LookupReplica(lhsDesc.StartKey.Next())
	rhsRepl := store.LookupReplica(rhsDesc.StartKey.Next())

	if lhsRepl != rhsRepl {
		t.Fatalf("ranges were not merged %+v=%+v", lhsRepl.Desc(), rhsRepl.Desc())
	}
	if startKey := lhsRepl.Desc().StartKey; !bytes.Equal(startKey, repl.Desc().StartKey) {
		t.Fatalf("The start key is not equal to KeyMin %q=%q", startKey, roachpb.RKeyMin)
	}
	if endKey := rhsRepl.Desc().EndKey; !bytes.Equal(endKey, repl.Desc().EndKey) {
		t.Fatalf("The end key is not equal to KeyMax %q=%q", endKey, roachpb.RKeyMax)
	}

	verify(lhsDesc.StartKey.Next().AsRawKey(), lhsRepl.RangeID, content)
	verify(rhsDesc.StartKey.Next().AsRawKey(), rhsRepl.RangeID, content)

	newContent := []byte("testing!better!")
	// Put new values after the merge on both sides.
	put(lhsDesc.StartKey.Next().AsRawKey(), lhsRepl.RangeID, newContent)
	put(rhsDesc.StartKey.Next().AsRawKey(), rhsRepl.RangeID, newContent)

	// Try to get the newly placed values.
	verify(lhsDesc.StartKey.Next().AsRawKey(), lhsRepl.RangeID, newContent)
	verify(rhsDesc.StartKey.Next().AsRawKey(), rhsRepl.RangeID, newContent)

	gArgs := getArgs(lhsDesc.StartKey.Next().AsRawKey())
	if _, pErr := kv.SendWrappedWith(ctx, store, roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, gArgs); !testutils.IsPError(
		pErr, `was not found on s`,
	) {
		t.Fatalf("expected get on rhs to fail after merge, but got err=%v", pErr)
	}

	if atomic.LoadInt64(&retries) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retries)
	}
}

// TestStoreRangeMergeTimestampCache verifies that the timestamp cache on the
// LHS is properly updated after a merge. The test contains a subtest for each
// of the combinations of the following boolean options:
//
// - disjointLeaseholders: configures whether or not the leaseholder of the
//     LHS range is disjoint from the leaseholder of the RHS range. If false,
//     the leaseholders are collocated before the merge is initiated.
//
// - throughSnapshot: configures whether or not the leaseholder of the LHS of
//     the merge hears about and applies the merge through a Raft snapshot, as
//     opposed to through normal Raft log application.
//
// - futureRead: configures whether or not the reads performed on the RHS range
//     before the merge is initiated are performed in the future of present
//     time using synthetic timestamps.
//
func TestStoreRangeMergeTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderShort(t)

	testutils.RunTrueAndFalse(t, "disjoint-leaseholders", func(t *testing.T, disjointLeaseholders bool) {
		testutils.RunTrueAndFalse(t, "through-snapshot", func(t *testing.T, throughSnapshot bool) {
			testutils.RunTrueAndFalse(t, "future-read", func(t *testing.T, futureRead bool) {
				mergeCheckingTimestampCaches(t, disjointLeaseholders, throughSnapshot, futureRead)
			})
		})
	})
}

func mergeCheckingTimestampCaches(
	t *testing.T, disjointLeaseholders, throughSnapshot, futureRead bool,
) {

	var filterMu struct {
		syncutil.Mutex
		// mergeCommitFilter is used to issue a sequence of operations on the LHS of
		// a range merge immediately before it.
		mergeCommitFilter func()
		// blockHBAndGCs is used to black hole Heartbeat and GC requests for the
		// duration of the merge on the throughSnapshot path. Neither request type
		// is needed and both can create issues by holding latches during the split
		// leader-leaseholder state.
		blockHBAndGCs chan struct{}
	}
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		filterMu.Lock()
		mergeCommitFilterCopy := filterMu.mergeCommitFilter
		blockHBAndGCsCopy := filterMu.blockHBAndGCs
		filterMu.Unlock()
		for _, req := range ba.Requests {
			switch v := req.GetInner().(type) {
			case *roachpb.EndTxnRequest:
				if v.InternalCommitTrigger.GetMergeTrigger() != nil {
					if mergeCommitFilterCopy != nil {
						mergeCommitFilterCopy()
					}
				}
			case *roachpb.HeartbeatTxnRequest, *roachpb.GCRequest:
				if blockHBAndGCsCopy != nil {
					<-blockHBAndGCsCopy
				}
			}
		}
		return nil
	}

	// snapshotFilter is used to listen for the completion of a Raft snapshot.
	var snapshotFilter func(kvserver.IncomingSnapshot)
	beforeSnapshotSSTIngestion := func(
		inSnap kvserver.IncomingSnapshot,
		snapType kvserver.SnapshotRequest_Type,
		_ []string,
	) error {
		filterMu.Lock()
		snapshotFilterCopy := snapshotFilter
		filterMu.Unlock()
		if snapshotFilterCopy != nil {
			snapshotFilterCopy(inSnap)
		}
		return nil
	}

	manualClock := hlc.NewHybridManualClock()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter:       testingRequestFilter,
						BeforeSnapshotSSTIngestion: beforeSnapshotSSTIngestion,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	defer func() {
		filterMu.Lock()
		defer filterMu.Unlock()
		if filterMu.blockHBAndGCs != nil {
			// If test failed before closing the channel, do so now
			// to (maybe) avoid hangs. Note that this must execute
			// before the Stopper().Stop() call above.
			close(filterMu.blockHBAndGCs)
		}
	}()

	lhsStore := tc.GetFirstStoreFromServer(t, 0)
	var rhsStore *kvserver.Store
	if disjointLeaseholders {
		rhsStore = tc.GetFirstStoreFromServer(t, 1)
	} else {
		rhsStore = tc.GetFirstStoreFromServer(t, 0)
	}

	// Disable closed timestamps to ensure that any writes that are bumped to
	// higher timestamps are bumped by the timestamp cache, as expected.
	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '24h'`)
	require.NoError(t, err)

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, lhsStore)
	require.NoError(t, err)

	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))

	if disjointLeaseholders {
		tc.TransferRangeLeaseOrFatal(t, *rhsDesc, tc.Target(1))
		testutils.SucceedsSoon(t, func() error {
			rhsRepl, err := rhsStore.GetReplica(rhsDesc.RangeID)
			if err != nil {
				return err
			}
			if !rhsRepl.OwnsValidLease(ctx, tc.Servers[1].Clock().NowAsClockTimestamp()) {
				return errors.New("rhs store does not own valid lease for rhs range")
			}
			return nil
		})
	}

	// Write a key to the RHS.
	rhsKey := roachpb.Key("c")
	if _, pErr := kv.SendWrappedWith(ctx, rhsStore, roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, incrementArgs(rhsKey, 1)); pErr != nil {
		t.Fatal(pErr)
	}

	readTS := tc.Servers[0].Clock().Now()
	if futureRead {
		readTS = readTS.Add(500*time.Millisecond.Nanoseconds(), 0).WithSynthetic(true)
	}

	// Simulate a read on the RHS from a node with a newer clock.
	var ba roachpb.BatchRequest
	ba.Timestamp = readTS
	ba.RangeID = rhsDesc.RangeID
	ba.Add(getArgs(rhsKey))
	if br, pErr := rhsStore.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	} else if v, err := br.Responses[0].GetGet().Value.GetInt(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatalf("expected 1, but got %d", v)
	} else if br.Timestamp != readTS {
		t.Fatalf("expected read to execute at %v, but executed at %v", readTS, br.Timestamp)
	}

	// Simulate a txn abort on the RHS from a node with a newer clock. Because
	// the transaction record for the pushee was not yet written, this will bump
	// the timestamp cache to record the abort.
	pushee := roachpb.MakeTransaction("pushee", rhsKey, roachpb.MinUserPriority, readTS, 0)
	pusher := roachpb.MakeTransaction("pusher", rhsKey, roachpb.MaxUserPriority, readTS, 0)
	ba = roachpb.BatchRequest{}
	ba.Timestamp = readTS.Next()
	ba.RangeID = rhsDesc.RangeID
	ba.Add(pushTxnArgs(&pusher, &pushee, roachpb.PUSH_ABORT))
	if br, pErr := rhsStore.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	} else if txn := br.Responses[0].GetPushTxn().PusheeTxn; txn.Status != roachpb.ABORTED {
		t.Fatalf("expected aborted pushee, but got %v", txn)
	}

	// Pause the cluster's clock. This accomplishes two things:
	// 1. It ensures that if we force the LHS leaseholder to learn about the
	//    merge through a snapshot (throughSnapshot), the merge transaction is not
	//    allowed to expire and be aborted due to delayed txn heartbeats.
	// 2. it ensures that if we performed a read at a future timestamp, the read
	//    time remains in the future, regardless of the passage of real time.
	manualClock.Pause()

	if !throughSnapshot {
		// The easy case: merge the RHS back into the LHS normally.
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, lhsStore.TestSender(), args)
		require.Nil(t, pErr)
	} else {
		// The hard case: merge the RHS back into the LHS, but make sure the LHS
		// leaseholder finds out about the merge through a Raft snapshot.
		//
		// To do this, we partition the leaseholder from the rest of its range.
		// Once partitioned, we perform another write and truncate the Raft log
		// on the two connected nodes. We then complete the range merge before
		// removing the partition. This ensures that that when the leaseholder
		// reconnects it will require a snapshot from Raft.
		//
		// But there's a wrinkle here that makes things more difficult: the
		// leaseholder needs to play a role in coordinating the range merge and
		// the log truncation, as it is the only replica that can propose such
		// changes. To accommodate this, we put the range into a split
		// leader-leaseholder state and lock down all communication between the
		// two _except_ for forwarded proposal from the leaseholder to the
		// leader. This allows the leaseholder to make proposals, even though it
		// won't be able to hear their result. Because this is such a fragile
		// state, we enter it as late as possible - after the merge begins and
		// only upon receiving the merge's EndTxn request.

		lhsKey := roachpb.Key("a")
		var lhsStores []*kvserver.Store
		var lhsRepls []*kvserver.Replica
		for i := range tc.Servers {
			s := tc.GetFirstStoreFromServer(t, i)
			r := s.LookupReplica(roachpb.RKey(lhsKey))
			lhsStores = append(lhsStores, s)
			lhsRepls = append(lhsRepls, r)
		}

		// Applied to the leaseholder's raft transport during the partition.
		partitionedLeaseholderFuncs := noopRaftHandlerFuncs()
		partitionedLeaseholderFuncs.dropReq = func(*kvserver.RaftMessageRequest) bool {
			// Ignore everything from new leader.
			return true
		}

		// Applied to the leader and other follower's raft transport during the
		// partition.
		partitionedLeaderFuncs := noopRaftHandlerFuncs()
		partitionedLeaderFuncs.dropReq = func(req *kvserver.RaftMessageRequest) bool {
			// Ignore everything from leaseholder, except forwarded proposals.
			return req.FromReplica.StoreID == lhsStore.StoreID() &&
				req.Message.Type != raftpb.MsgProp
		}
		partitionedLeaderFuncs.dropHB = func(hb *kvserver.RaftHeartbeat) bool {
			// Ignore heartbeats from leaseholder, results in campaign.
			return hb.FromReplicaID == roachpb.ReplicaID(lhsRepls[0].RaftStatus().ID)
		}

		// Applied to leaseholder after the partition heals.
		var truncIndex uint64
		restoredLeaseholderFuncs := noopRaftHandlerFuncs()
		restoredLeaseholderFuncs.dropReq = func(req *kvserver.RaftMessageRequest) bool {
			// Make sure that even going forward no MsgApp for what we just
			// truncated can make it through. The Raft transport is asynchronous
			// so this is necessary to make the test pass reliably - otherwise
			// the leaseholder may catch up without needing a snapshot, tripping
			// up the test.
			//
			// NB: the Index on the message is the log index that _precedes_ any of the
			// entries in the MsgApp, so filter where msg.Index < index, not <= index.
			return req.Message.Type == raftpb.MsgApp && req.Message.Index < truncIndex
		}

		// Because we enter a split leader-leaseholder state, none of the
		// operations we perform on the leaseholder will return. Instead, they
		// will block for the duration of the partition, even after they have
		// succeeded on the majority quorum. So we launch async goroutines to
		// perform the write and the log truncation and only wait for them to
		// complete after the partition heals.
		incChan := make(chan *roachpb.Error, 1)
		truncChan := make(chan *roachpb.Error, 1)
		snapChan := make(chan kvserver.IncomingSnapshot, 1)

		filterMu.Lock()
		filterMu.mergeCommitFilter = func() {
			// Install leader-leaseholder partition.
			for i, s := range lhsStores {
				var funcs unreliableRaftHandlerFuncs
				if i == 0 {
					funcs = partitionedLeaseholderFuncs
				} else {
					funcs = partitionedLeaderFuncs
				}
				tc.Servers[i].RaftTransport().Listen(s.StoreID(), &unreliableRaftHandler{
					rangeID:                    lhsDesc.GetRangeID(),
					RaftMessageHandler:         s,
					unreliableRaftHandlerFuncs: funcs,
				})
			}

			// Make sure the LHS range in uniquiesced so that it elects a new
			// Raft leader after the partition is established.
			for _, r := range lhsRepls {
				r.UnquiesceAndWakeLeader()
			}

			// Issue an increment on the range. The leaseholder should evaluate
			// the request and forward a proposal to the leader, but it should
			// be the only replica that does not apply the proposal.
			go func() {
				incArgs := incrementArgs(lhsKey, 4)
				_, pErr := kv.SendWrappedWith(ctx, lhsStore, roachpb.Header{RangeID: lhsDesc.RangeID}, incArgs)
				incChan <- pErr
			}()
			// NB: the operation won't complete, so peek below Raft and wait for
			// the result to apply on the majority quorum.
			tc.WaitForValues(t, lhsKey, []int64{0, 4, 4})

			// Truncate the log to eventually force a snapshot. Determining
			// which log index to truncate is tricky. We need to make sure it is
			// <= to the largest log index on the leaseholder or it will reject
			// the request. But we also need to make sure it is <= to the
			// largest log index on the leader, or it will panic. So we choose
			// the minimum of these two and just pick the smallest "last index"
			// in the range, which does the trick.
			min := func(a, b uint64) uint64 {
				if a < b {
					return a
				}
				return b
			}
			minLastIndex := uint64(math.MaxUint64)
			for _, r := range lhsRepls {
				lastIndex, err := r.GetLastIndex()
				require.NoError(t, err)
				minLastIndex = min(minLastIndex, lastIndex)
			}
			// Truncate the log at index+1 (log entries < N are removed).
			truncIndex = minLastIndex + 1
			go func() {
				truncArgs := truncateLogArgs(truncIndex, lhsDesc.RangeID)
				truncArgs.Key = lhsKey
				_, pErr := kv.SendWrappedWith(ctx, lhsStore, roachpb.Header{RangeID: lhsDesc.RangeID}, truncArgs)
				truncChan <- pErr
			}()
			// NB: the operation won't complete, so peek below Raft and wait for
			// the result to apply on the majority quorum.
			testutils.SucceedsSoon(t, func() error {
				for _, r := range lhsRepls[1:] {
					firstIndex, err := r.GetFirstIndex()
					require.NoError(t, err)
					if firstIndex < truncIndex {
						return errors.Errorf("truncate not applied, %d < %d", firstIndex, truncIndex)
					}
				}
				return nil
			})
		}

		// Begin blocking txn heartbeats and GC requests. They cause issues
		// because they can grab latches and then get stuck once in the split
		// leader-leaseholder state.
		filterMu.blockHBAndGCs = make(chan struct{})

		// Install a filter to capture the Raft snapshot.
		snapshotFilter = func(inSnap kvserver.IncomingSnapshot) {
			if inSnap.State.Desc.RangeID == lhsDesc.RangeID {
				snapChan <- inSnap
			}
		}
		filterMu.Unlock()

		// Merge the RHS back into the LHS.
		mergeChan := make(chan *roachpb.Error, 1)
		go func() {
			args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
			_, pErr := kv.SendWrapped(ctx, lhsStore.TestSender(), args)
			mergeChan <- pErr
		}()
		// NB: the operation won't complete, so peek below Raft and wait for
		// the result to apply on the majority quorum.
		testutils.SucceedsSoon(t, func() error {
			for _, r := range lhsRepls[1:] {
				desc := r.Desc()
				if !desc.EndKey.Equal(rhsDesc.EndKey) {
					return errors.Errorf("merge not applied")
				}
			}
			return nil
		})

		// Remove the partition. A snapshot to the leaseholder should follow.
		// This snapshot will inform the leaseholder about the range merge.
		for i, s := range lhsStores {
			var h kvserver.RaftMessageHandler
			if i == 0 {
				h = &unreliableRaftHandler{
					rangeID:                    lhsDesc.GetRangeID(),
					RaftMessageHandler:         s,
					unreliableRaftHandlerFuncs: restoredLeaseholderFuncs,
				}
			} else {
				h = s
			}
			tc.Servers[i].RaftTransport().Listen(s.StoreID(), h)
		}
		close(filterMu.blockHBAndGCs)
		filterMu.Lock()
		filterMu.blockHBAndGCs = nil
		filterMu.Unlock()

		t.Logf("waiting for snapshot to LHS leaseholder")
		var inSnap kvserver.IncomingSnapshot
		select {
		case inSnap = <-snapChan:
		case <-time.After(45 * time.Second):
			t.Fatal("timed out waiting for snapChan")
		}
		inSnapDesc := inSnap.State.Desc
		require.Equal(t, lhsDesc.StartKey, inSnapDesc.StartKey)
		require.Equal(t, rhsDesc.EndKey, inSnapDesc.EndKey)

		// Wait for all async ops to complete.
		after45s := time.After(45 * time.Second)
		for _, asyncRes := range []struct {
			name string
			ch   chan *roachpb.Error
		}{
			{"increment", incChan},
			{"truncate", truncChan},
			{"merge", mergeChan},
		} {
			t.Logf("waiting for result of %s", asyncRes.name)
			var err *roachpb.Error
			select {
			case err = <-asyncRes.ch:
			case <-after45s:
				t.Fatalf("timed out on %s", asyncRes.name)
			}
			require.NotNil(t, err, "%s should fail", asyncRes.name)
			require.Regexp(t, "result is ambiguous", err, "%s's result should be ambiguous", asyncRes.name)
		}
	}

	// After the merge, attempt to write under the read. The batch should get
	// forwarded to a timestamp after the read.
	ba = roachpb.BatchRequest{}
	ba.Timestamp = readTS
	ba.RangeID = lhsDesc.RangeID
	ba.Add(incrementArgs(rhsKey, 1))
	if br, pErr := lhsStore.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	} else if br.Timestamp.LessEq(readTS) {
		t.Fatalf("expected write to execute after %v, but executed at %v", readTS, br.Timestamp)
	}

	// Attempt to create a transaction record for the pushee transaction, which
	// was aborted before the merge. This should be rejected with a transaction
	// aborted error. The reason will depend on whether the leaseholders were
	// disjoint or not because disjoint leaseholders will lead to a loss of
	// resolution in the timestamp cache. Similarly, the reason will depend on
	// whether the LHS leaseholder learned about the merge through Raft log
	// application or a Raft snapshot. Either way though, the transaction should
	// not be allowed to create its record.
	hb, hbH := heartbeatArgs(&pushee, tc.Servers[0].Clock().Now())
	ba = roachpb.BatchRequest{}
	ba.Header = hbH
	ba.RangeID = lhsDesc.RangeID
	ba.Add(hb)
	var expReason roachpb.TransactionAbortedReason
	if disjointLeaseholders || throughSnapshot {
		expReason = roachpb.ABORT_REASON_TIMESTAMP_CACHE_REJECTED
	} else {
		expReason = roachpb.ABORT_REASON_ABORTED_RECORD_FOUND
	}
	if _, pErr := lhsStore.Send(ctx, ba); pErr == nil {
		t.Fatalf("expected TransactionAbortedError(%s) but got %v", expReason, pErr)
	} else if abortErr, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Fatalf("expected TransactionAbortedError(%s) but got %v", expReason, pErr)
	} else if abortErr.Reason != expReason {
		t.Fatalf("expected TransactionAbortedError(%s) but got %v", expReason, pErr)
	}
}

// TestStoreRangeMergeTimestampCacheCausality verifies that range merges update
// the clock on the subsuming store as necessary to preserve causality.
//
// The test simulates a particularly diabolical sequence of events in which
// causality information is not communicated through the normal channels.
// Suppose two adjacent ranges, A and B, are collocated on S2, S3, and S4. (S1
// is omitted for consistency with the store numbering in the test itself.) S3
// holds the lease on A, while S4 holds the lease on B. Every store's clock
// starts at time T1.
//
// To merge A and B, S3 will launch a merge transaction that sends several RPCs
// to S4. Suppose that, just before S4 begins executing the Subsume request, a
// read sneaks in for some key K at a large timestamp T3. S4 will bump its clock
// from T1 to T3, so when the Subsume goes to determine the current time to use
// for the FreezeStart field in the Subsume response, it will use T3. When S3
// completes the merge, it will thus use T3 as the timestamp cache's low water
// mark for the keys that previously belonged to B.
//
// Importantly, S3 must also update its clock from T1 to T3. Otherwise, as this
// test demonstrates, it is possible for S3 to send a lease to another store, in
// this case S2, that begins at T2. S2 will then assume it is free to accept a
// write at T2, when in fact we already served a read at T3. This would be a
// serializability violation!
//
// Note that there are several mechanisms that *almost* prevent this problem. If
// the read of K at T3 occurs slightly earlier, the batch response for Subsume
// will set the Now field to T3, which S3 will use to bump its clock.
// (BatchResponse.Now is computed when the batch is received, not when it
// finishes executing.) If S3 receives a write for K at T2, it will a) properly
// bump the write to T4, because its timestamp cache is up to date, and then b)
// bump its clock to T4. Or if S4 were to send a single RPC to S3, S3 would bump
// its clock based on the BatchRequest.Timestamp.
//
// In short, this sequence of events is likely to be exceedingly unlikely in
// practice, but is subtle enough to warrant a test.
func TestStoreRangeMergeTimestampCacheCausality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var readTS hlc.Timestamp
	rhsKey := roachpb.Key("c")
	var tc *testcluster.TestCluster
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if ba.IsSingleSubsumeRequest() {
			// Before we execute a Subsume request, execute a read on the same store
			// at a much higher timestamp.
			gba := roachpb.BatchRequest{}
			gba.RangeID = ba.RangeID
			gba.Timestamp = ba.Timestamp.Add(42 /* wallTime */, 0 /* logical */)
			gba.Add(getArgs(rhsKey))
			store := tc.GetFirstStoreFromServer(t, int(ba.Header.Replica.StoreID-1))
			gbr, pErr := store.Send(ctx, gba)
			if pErr != nil {
				t.Error(pErr) // different goroutine, so can't use t.Fatal
			}
			readTS = gbr.Timestamp
		}
		return nil
	}

	tc = testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	distSender := tc.Servers[0].DistSender()

	for _, key := range []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")} {
		if _, pErr := kv.SendWrapped(ctx, distSender, adminSplitArgs(key)); pErr != nil {
			t.Fatal(pErr)
		}
	}

	lhsRangeDesc := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey("a")).Desc()
	rhsRangeDesc := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey("b")).Desc()

	// Replicate [a, b) to s2, s3, and s4, and put the lease on s3.
	tc.AddVotersOrFatal(t, lhsRangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2, 3)...)
	tc.TransferRangeLeaseOrFatal(t, *lhsRangeDesc, tc.Target(2))
	tc.RemoveVotersOrFatal(t, lhsRangeDesc.StartKey.AsRawKey(), tc.Target(0))

	// Replicate [b, Max) to s2, s3, and s4, and put the lease on s4.
	tc.AddVotersOrFatal(t, rhsRangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2, 3)...)
	tc.TransferRangeLeaseOrFatal(t, *rhsRangeDesc, tc.Target(3))
	tc.RemoveVotersOrFatal(t, rhsRangeDesc.StartKey.AsRawKey(), tc.Target(0))

	// N.B. We isolate r1 on s1 so that node liveness heartbeats do not interfere
	// with our precise clock management on s2, s3, and s4.

	// Write a key to [b, Max).
	if _, pErr := kv.SendWrapped(ctx, distSender, incrementArgs(rhsKey, 1)); pErr != nil {
		t.Fatal(pErr)
	}

	// Wait for all relevant stores to have the same value. This indirectly
	// ensures the lease transfers have applied on all relevant stores.
	tc.WaitForValues(t, rhsKey, []int64{0, 1, 1, 1})

	// Merge [a, b) and [b, Max). Our request filter above will intercept the
	// merge and execute a read with a large timestamp immediately before the
	// Subsume request executes.
	if _, pErr := kv.SendWrappedWith(ctx, tc.GetFirstStoreFromServer(t, 2), roachpb.Header{
		RangeID: lhsRangeDesc.RangeID,
	}, adminMergeArgs(roachpb.Key("a"))); pErr != nil {
		t.Fatal(pErr)
	}

	// Immediately transfer the lease on the merged range [a, Max) from s3 to s2.
	// To test that it is, in fact, the merge trigger that properly bumps s3's
	// clock, s3 must not send or receive any requests before it transfers the
	// lease, as those requests could bump s3's clock through other code paths.
	tc.TransferRangeLeaseOrFatal(t, *lhsRangeDesc, tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		lhsRepl1, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(lhsRangeDesc.RangeID)
		if err != nil {
			return err
		}
		if !lhsRepl1.OwnsValidLease(ctx, tc.Servers[1].Clock().NowAsClockTimestamp()) {
			return errors.New("s2 does not own valid lease for lhs range")
		}
		return nil
	})

	// Attempt to write at the same time as the read. The write's timestamp
	// should be forwarded to after the read.
	ba := roachpb.BatchRequest{}
	ba.Timestamp = readTS
	ba.RangeID = lhsRangeDesc.RangeID
	ba.Add(incrementArgs(rhsKey, 1))
	if br, pErr := tc.GetFirstStoreFromServer(t, 1).Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	} else if br.Timestamp.LessEq(readTS) {
		t.Fatalf("expected write to execute after %v, but executed at %v", readTS, br.Timestamp)
	}
}

// TestStoreRangeMergeLastRange verifies that merging the last range fails.
func TestStoreRangeMergeLastRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)
	// Find the range with the max key
	lastKey := roachpb.RKeyMin
	store.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
		if lastKey.Less(replica.Desc().StartKey) {
			lastKey = replica.Desc().StartKey
		}
		return true
	})

	// Merge last range.
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(lastKey.AsRawKey()))
	if !testutils.IsPError(pErr, "cannot merge final range") {
		t.Fatalf("expected 'cannot merge final range' error; got %s", pErr)
	}
}

func TestStoreRangeMergeTxnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Install a store filter that maybe injects retryable errors into a merge
	// transaction before ultimately aborting the merge.
	var retriesBeforeFailure int64
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				if atomic.AddInt64(&retriesBeforeFailure, -1) >= 0 {
					return roachpb.NewError(
						roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "filter err"))
				}
				return roachpb.NewError(errors.New("injected permafail"))
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)
	kvDB := store.DB()

	if err := kvDB.Put(ctx, "aa", "val"); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(ctx, "cc", "val"); err != nil {
		t.Fatal(err)
	}
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	verifyLHSAndRHSLive := func() {
		t.Helper()
		for _, tc := range []struct {
			rangeID roachpb.RangeID
			key     roachpb.Key
		}{
			{lhsDesc.RangeID, roachpb.Key("aa")},
			{rhsDesc.RangeID, roachpb.Key("cc")},
		} {
			if reply, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
				RangeID: tc.rangeID,
			}, getArgs(tc.key)); pErr != nil {
				t.Fatal(pErr)
			} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(replyBytes, []byte("val")) {
				t.Fatalf("actual value %q did not match expected value %q", replyBytes, []byte("val"))
			}
		}
	}

	attemptMerge := func() {
		t.Helper()
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
		if exp := "injected permafail"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %q", exp, pErr)
		}
	}

	verifyLHSAndRHSLive()

	atomic.StoreInt64(&retriesBeforeFailure, 0)
	attemptMerge()
	verifyLHSAndRHSLive()
	if atomic.LoadInt64(&retriesBeforeFailure) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retriesBeforeFailure)
	}

	atomic.StoreInt64(&retriesBeforeFailure, 3)
	attemptMerge()
	verifyLHSAndRHSLive()
	if atomic.LoadInt64(&retriesBeforeFailure) >= 0 {
		t.Fatalf("%d retries remaining (expected less than zero)", retriesBeforeFailure)
	}
}

// TestStoreRangeMergeTxnRefresh verifies that in cases where the range merge
// transaction's timestamp is bumped, it is able to refresh even after it has
// entered the critical phase of the merge and subsumed the RHS.
func TestStoreRangeMergeTxnRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var sawMergeRefresh int32
	testingResponseFilter := func(
		ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
	) *roachpb.Error {
		switch v := ba.Requests[0].GetInner().(type) {
		case *roachpb.ConditionalPutRequest:
			// Detect the range merge's deletion of the local range descriptor
			// and use it as an opportunity to bump the merge transaction's
			// write timestamp. This will necessitate a refresh.
			//
			// Also mark as synthetic, while we're here, to simulate the
			// behavior of a range merge across two ranges with the
			// LEAD_FOR_GLOBAL_READS closed timestamp policy.
			if !v.Value.IsPresent() && bytes.HasSuffix(v.Key, keys.LocalRangeDescriptorSuffix) {
				br.Txn.WriteTimestamp = br.Txn.WriteTimestamp.
					Add(100*time.Millisecond.Nanoseconds(), 0).
					WithSynthetic(true)
			}
		case *roachpb.RefreshRequest:
			if bytes.HasSuffix(v.Key, keys.LocalRangeDescriptorSuffix) {
				atomic.AddInt32(&sawMergeRefresh, 1)
			}
		}
		return nil
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingResponseFilter: testingResponseFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Create the ranges to be merged.
	lhsDesc, _, err := tc.Servers[0].ScratchRangeEx()
	require.NoError(t, err)

	// Launch the merge.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
	require.Nil(t, pErr)

	// Verify that the range merge refreshed.
	require.Greater(t, atomic.LoadInt32(&sawMergeRefresh), int32(1))
}

// TestStoreRangeSplitMergeGeneration verifies that splits and merges both
// update the range descriptor generations of the involved ranges according to
// the comment on the RangeDescriptor.Generation field.
func TestStoreRangeSplitMergeGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "rhsHasHigherGen", func(t *testing.T, rhsHasHigherGen bool) {
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable both splits and merges so that we're in full
					// control over them.
					DisableMergeQueue: true,
					DisableSplitQueue: true,
				},
			},
		})
		defer s.Stopper().Stop(context.Background())

		leftKey := roachpb.Key("z")
		rightKey := leftKey.Next().Next()

		// First, split at the left key for convenience, so that we can check
		// leftDesc.StartKey == leftKey later.
		_, _, err := s.SplitRange(leftKey)
		assert.NoError(t, err)

		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		assert.NoError(t, err)
		leftRepl := store.LookupReplica(keys.MustAddr(leftKey))
		assert.NotNil(t, leftRepl)
		preSplitGen := leftRepl.Desc().Generation
		leftDesc, rightDesc, err := s.SplitRange(rightKey)
		assert.NoError(t, err)

		// Split should increment the LHS' generation and also propagate the result
		// to the RHS.
		assert.Equal(t, preSplitGen+1, leftDesc.Generation)
		assert.Equal(t, preSplitGen+1, rightDesc.Generation)

		if rhsHasHigherGen {
			// Split the RHS again to increment its generation once more, so that
			// we get (assuming preSplitGen=1):
			//
			// |--left@2---||---right@3---||--don't care--|
			//
			rightDesc, _, err = s.SplitRange(rightKey.Next())
			assert.NoError(t, err)
			assert.Equal(t, preSplitGen+2, rightDesc.Generation)
		} else {
			// Split and merge the LHS to increment the generation (it ends up
			// being incremented by two). Note that leftKey.Next() is still in
			// the left range. Assuming preSplitGen=1, we'll end up in the
			// situation:
			//
			// |--left@4---||---right@2---|
			var tmpRightDesc roachpb.RangeDescriptor
			leftDesc, tmpRightDesc, err = s.SplitRange(leftKey.Next())
			assert.Equal(t, preSplitGen+2, leftDesc.Generation)
			assert.Equal(t, preSplitGen+2, tmpRightDesc.Generation)
			assert.NoError(t, err)
			leftDesc, err = s.MergeRanges(leftKey)
			assert.NoError(t, err)
			assert.Equal(t, preSplitGen+3, leftDesc.Generation)
		}

		// Make sure the split/merge shenanigans above didn't get the range
		// descriptors confused.
		assert.Equal(t, leftKey, leftDesc.StartKey.AsRawKey())
		assert.Equal(t, rightKey, rightDesc.StartKey.AsRawKey())

		// Merge the two ranges back to verify that the resulting descriptor
		// has the correct generation.
		mergedDesc, err := s.MergeRanges(leftKey)
		assert.NoError(t, err)

		maxPreMergeGen := leftDesc.Generation
		if rhsGen := rightDesc.Generation; rhsGen > maxPreMergeGen {
			maxPreMergeGen = rhsGen
		}

		assert.Equal(t, maxPreMergeGen+1, mergedDesc.Generation)
		assert.Equal(t, leftDesc.RangeID, mergedDesc.RangeID)
	})
}

// TestStoreRangeMergeStats starts by splitting a range, then writing random
// data to both sides of the split. It then merges the ranges and verifies the
// merged range has stats consistent with recomputations.
func TestStoreRangeMergeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	// Split the range.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	kvserver.WriteRandomDataToRange(t, store, lhsDesc.RangeID, []byte("aaa"))
	kvserver.WriteRandomDataToRange(t, store, rhsDesc.RangeID, []byte("ccc"))

	// Litter some abort span records. txn1 will leave a record on the LHS, txn2
	// will leave a record on the RHS, and txn3 will leave a record on both. This
	// tests whether the merge code properly accounts for merging abort span
	// records for the same transaction.
	txn1 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
	if err := txn1.Put(ctx, "a-txn1", "val"); err != nil {
		t.Fatal(err)
	}
	txn2 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
	if err := txn2.Put(ctx, "c-txn2", "val"); err != nil {
		t.Fatal(err)
	}
	txn3 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
	if err := txn3.Put(ctx, "a-txn3", "val"); err != nil {
		t.Fatal(err)
	}
	if err := txn3.Put(ctx, "c-txn3", "val"); err != nil {
		t.Fatal(err)
	}
	hiPriTxn := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
	hiPriTxn.TestingSetPriority(enginepb.MaxTxnPriority)
	for _, key := range []string{"a-txn1", "c-txn2", "a-txn3", "c-txn3"} {
		if err := hiPriTxn.Put(ctx, key, "val"); err != nil {
			t.Fatal(err)
		}
	}
	if err := hiPriTxn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Leave txn1-txn3 open so that their abort span records exist during the
	// merge below.

	// Get the range stats for both ranges now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	msA, err := stateloader.Make(lhsDesc.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	msB, err := stateloader.Make(rhsDesc.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, lhsDesc, msA, tc.Servers[0].Clock().Now().WallTime); err != nil {
		t.Fatalf("failed to verify range A's stats before split: %+v", err)
	}
	if err := verifyRecomputedStats(snap, rhsDesc, msB, tc.Servers[0].Clock().Now().WallTime); err != nil {
		t.Fatalf("failed to verify range B's stats before split: %+v", err)
	}

	// Merge the b range back into the a range.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, err := kv.SendWrapped(ctx, store.TestSender(), args); err != nil {
		t.Fatal(err)
	}
	replMerged := store.LookupReplica(lhsDesc.StartKey)

	// Get the range stats for the merged range and verify.
	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msMerged, err := stateloader.Make(replMerged.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Merged stats should agree with recomputation.
	nowNanos := tc.Servers[0].Clock().Now().WallTime
	msMerged.AgeTo(nowNanos)
	if err := verifyRecomputedStats(snap, replMerged.Desc(), msMerged, nowNanos); err != nil {
		t.Errorf("failed to verify range's stats after merge: %+v", err)
	}
}

func TestStoreRangeMergeInFlightTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	// Create two adjacent ranges.
	setupReplicas := func() (lhsDesc, rhsDesc *roachpb.RangeDescriptor, err error) {
		lhsDesc, rhsDesc, err = createSplitRanges(ctx, store)
		if err != nil {
			return nil, nil, err
		}
		return lhsDesc, rhsDesc, nil
	}

	// Verify that a transaction can span a merge.
	t.Run("valid", func(t *testing.T) {
		lhsDesc, _, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		lhsKey, rhsKey := roachpb.Key("aa"), roachpb.Key("cc")

		txn := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
		// Put the key on the RHS side first so ownership of the transaction record
		// will need to transfer to the LHS range during the merge.
		if err := txn.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}
		if err := txn.Put(ctx, lhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
		}
		if err := txn.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		for _, key := range []roachpb.Key{lhsKey, rhsKey} {
			kv, err := store.DB().Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			} else if string(kv.ValueBytes()) != t.Name() {
				t.Fatalf("actual value %q did not match expected value %q", kv.ValueBytes(), t.Name())
			}
		}
	})

	// Verify that a transaction's abort span records are preserved when the
	// transaction spans a merge.
	t.Run("abort-span", func(t *testing.T) {
		lhsDesc, _, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		rhsKey := roachpb.Key("cc")

		// Create a transaction that will be aborted before the merge but won't
		// realize until after the merge.
		txn1 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
		// Put the key on the RHS side so ownership of the transaction record and
		// abort span records will need to transfer to the LHS during the merge.
		if err := txn1.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}

		// Create and commit a txn that aborts txn1.
		txn2 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
		txn2.TestingSetPriority(enginepb.MaxTxnPriority)
		if err := txn2.Put(ctx, rhsKey, "muhahahah"); err != nil {
			t.Fatal(err)
		}
		if err := txn2.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// Complete the merge.
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
		}
		expErr := "TransactionAbortedError(ABORT_REASON_ABORT_SPAN)"
		if _, err := txn1.Get(ctx, rhsKey); !testutils.IsError(err, regexp.QuoteMeta(expErr)) {
			t.Fatalf("expected %s but got %v", expErr, err)
		}
	})

	// Verify that the transaction wait queue on the right-hand range in a merge
	// is cleared if the merge commits.
	t.Run("wait-queue", func(t *testing.T) {
		lhsDesc, rhsDesc, err := setupReplicas()
		if err != nil {
			t.Fatal(err)
		}
		rhsKey := roachpb.Key("cc")

		// Set a timeout, and set the transaction liveness threshold to
		// something much larger than our timeout. We want transactions to get stuck
		// in the transaction wait queue and trigger the timeout if we forget to
		// clear it.
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
		defer cancel()
		defer txnwait.TestingOverrideTxnLivenessThreshold(2 * testutils.DefaultSucceedsSoonDuration)

		// Create a transaction that won't complete until after the merge.
		txn1 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
		// Put the key on the RHS side so ownership of the transaction record and
		// abort span records will need to transfer to the LHS during the merge.
		if err := txn1.Put(ctx, rhsKey, t.Name()); err != nil {
			t.Fatal(err)
		}

		// Create a txn that will conflict with txn1.
		txn2 := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
		txn2ErrCh := make(chan error)
		go func() {
			// Get should block on txn1's intent until txn1 commits.
			kv, err := txn2.Get(ctx, rhsKey)
			if err != nil {
				txn2ErrCh <- err
			} else if string(kv.ValueBytes()) != t.Name() {
				txn2ErrCh <- errors.Errorf("actual value %q did not match expected value %q", kv.ValueBytes(), t.Name())
			}
			txn2ErrCh <- nil
		}()

		// Wait for txn2 to realize it conflicts with txn1 and enter its wait queue.
		{
			repl, err := store.GetReplica(rhsDesc.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			for {
				if _, ok := repl.GetConcurrencyManager().TxnWaitQueue().TrackedTxns()[txn1.ID()]; ok {
					break
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("timed out waiting for txn2 to enter wait queue")
				}
			}
		}

		// Complete the merge.
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
			t.Fatal(pErr)
		}

		if err := txn1.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		// Now that txn1 has committed, txn2's get operation should complete.
		select {
		case err := <-txn2ErrCh:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for txn2 to complete get")
		}

		if err := txn2.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestStoreRangeMergeSplitRace_MergeWins (occasionally) reproduces a race where
// a concurrent merge and split could deadlock. It exercises the case where the
// merge commits and the split aborts. See the SplitWins variant of this test
// for the inverse case.
//
// The bug works like this. A merge of adjacent ranges P and Q and a split of Q
// execute concurrently, though the merge executes with an earlier timestamp.
// The merge updates Q's meta2 range descriptor. The split updates Q's local
// range descriptor, then tries to update Q's meta2 range descriptor, but runs
// into the merge's intent and attempts to push the merge. Under our current
// concurrency control strategy, this results in the split waiting for the merge
// to complete. The merge then tries to update Q's local range descriptor but
// runs into the split's intent. While pushing the split, the merge realizes
// that waiting for the split to complete would cause deadlock, so it aborts the
// split instead.
//
// But before the split can clean up its transaction record and intents, the
// merge locks Q and launches a goroutine to unlock Q when the merge commits.
// Then the merge completes, which has a weird side effect: the split's push of
// the merge will succeed! How is this possible? The split's push request is not
// guaranteed to notice that the split has been aborted before it notices that
// the merge has completed. So the aborted split winds up resolving the merge's
// intent on Q's meta2 range descriptor and leaving its own intent in its place.
//
// In the past, the merge watcher goroutine would perform a range lookup for Q;
// this would indirectly wait for the merge to complete by waiting for its
// intent in meta2 to be resolved. In this case, however, its the *split*'s
// intent that the watcher goroutine sees. This intent can't be resolved because
// the split's transaction record is located on the locked range Q! And so Q can
// never be unlocked.
//
// This bug was fixed by teaching the watcher goroutine to push the merge
// transaction directly instead of doing so indirectly by querying meta2.
//
// Attempting a foolproof reproduction of the bug proved challenging and would
// have required a mess of store filters. This test takes a simpler approach of
// running the necessary split and a merge concurrently and allowing the race
// scheduler to occasionally strike the right interleaving. At the time of
// writing, the test would reliably reproduce the bug in about 50 runs (about
// ten seconds of stress on an eight core laptop).
func TestStoreRangeMergeSplitRace_MergeWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	distSender := tc.Servers[0].DistSender()

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	splitErrCh := make(chan error)
	go func() {
		time.Sleep(10 * time.Millisecond)
		splitArgs := adminSplitArgs(rhsDesc.StartKey.AsRawKey().Next())
		_, pErr := kv.SendWrapped(ctx, distSender, splitArgs)
		splitErrCh <- pErr.GoError()
	}()

	mergeArgs := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	if _, pErr := kv.SendWrapped(ctx, distSender, mergeArgs); pErr != nil {
		t.Fatal(pErr)
	}

	if err := <-splitErrCh; err != nil {
		t.Fatal(err)
	}
}

// TestStoreRangeMergeSplitRace_SplitWins reproduces a race where a concurrent
// merge and split could deadlock. It exercises the case where the split commits
// and the merge aborts. See the MergeWins variant of this test for the inverse
// case.
//
// The bug works like this. A merge of adjacent ranges P and Q and a split of Q
// execute concurrently, though the merge executes with an earlier timestamp.
// First, the merge transaction reads Q's local range descriptor to determine
// the combined range's range descriptor. Then it writes an intent to update P's
// local range descriptor.
//
// Next, the split transaction runs from start to finish, updating Q's local
// descriptor and its associated meta2 record. Notably, the split transaction
// does not encounter any intents from the merge transaction, since the merge
// transaction's only intent so far is on P's local range descriptor, and so the
// split transaction can happily commit.
//
// The merge transaction then continues, reading and writing an intent on Q's
// local descriptor. The locking nature of the read request to Q's local
// descriptor ensures that the merge transaction will observe the post-split
// value for Q.
func TestStoreRangeMergeSplitRace_SplitWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var distSender *kvcoord.DistSender
	var lhsDescKey atomic.Value
	var lhsStartKey atomic.Value
	var launchSplit int64
	var mergePreSplit atomic.Value
	var splitCommit atomic.Value
	var mergeEndTxnTimestamp atomic.Value
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if get := req.GetGet(); get != nil && get.KeyLocking != lock.None {
				if v := lhsDescKey.Load(); v != nil && v.(roachpb.Key).Equal(get.Key) {
					// If this is the first merge attempt, launch the split
					// before the merge's first locking read succeeds.
					if atomic.CompareAndSwapInt64(&launchSplit, 1, 0) {
						mergePreSplit.Store(ba.Txn.ReadTimestamp)
						_, pErr := kv.SendWrapped(ctx, distSender, adminSplitArgs(roachpb.Key("c")))
						return pErr
					}
					// Otherwise, proceed.
				}
			}
			if split := req.GetAdminSplit(); split != nil && split.Key.Equal(roachpb.Key("c")) {
				splitCommit.Store(ba.Timestamp)
			}
			if endTxn := req.GetEndTxn(); endTxn != nil {
				ct := endTxn.InternalCommitTrigger
				startKey, _ := lhsStartKey.Load().(roachpb.RKey)
				if ct != nil && ct.MergeTrigger != nil && startKey != nil &&
					startKey.Equal(ct.MergeTrigger.LeftDesc.StartKey) {
					mergeEndTxnTimestamp.Store(ba.Txn.ReadTimestamp)
				}
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)
	distSender = tc.Servers[0].DistSender()

	lhsDesc, _, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	lhsDescKey.Store(keys.RangeDescriptorKey(lhsDesc.StartKey))
	atomic.StoreInt64(&launchSplit, 1)
	lhsStartKey.Store(lhsDesc.StartKey)

	mergeArgs := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, distSender, mergeArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mergePreSplitTS := mergePreSplit.Load().(hlc.Timestamp)
	splitTS := splitCommit.Load().(hlc.Timestamp)
	mergePostSplitTS := mergeEndTxnTimestamp.Load().(hlc.Timestamp)
	if splitTS.LessEq(mergePreSplitTS) {
		t.Fatalf("expected merge to start before concurrent split, %v <= %v", splitTS, mergePreSplitTS)
	}
	if mergePostSplitTS.LessEq(splitTS) {
		t.Fatalf("expected merge to finish after concurrent split, %v <= %v", mergePostSplitTS, splitTS)
	}
}

func checkConsistencyArgs(desc *roachpb.RangeDescriptor) *roachpb.CheckConsistencyRequest {
	return &roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    desc.StartKey.AsRawKey(),
			EndKey: desc.EndKey.AsRawKey(),
		},
		WithDiff:   false,
		Mode:       1,
		Checkpoint: false,
		Terminate:  nil,
	}
}

// TestStoreRangeMergeRHSLeaseExpiration verifies that, if the right-hand range
// in a merge loses its lease while a merge is in progress, the new leaseholder
// does not incorrectly serve traffic before the merge completes.
func TestStoreRangeMergeRHSLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The synchronization in this test is tricky. The merge transaction is
	// controlled by the AdminMerge function and normally commits quite quickly,
	// but we need to ensure an expiration of the RHS's lease occurs while the
	// merge transaction is open. To do so we install various hooks to observe
	// and control requests. It's easiest to understand these hooks after you've
	// read the meat of the test.

	// Install a hook to control when the merge transaction commits.
	mergeEndTxnReceived := make(chan *roachpb.Transaction, 10) // headroom in case the merge transaction retries
	finishMerge := make(chan struct{})
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, r := range ba.Requests {
			if et := r.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				mergeEndTxnReceived <- ba.Txn
				<-finishMerge
			}
		}
		return nil
	}

	// Install a hook to observe when a get or a put request for a special key,
	// rhsSentinel, hits a MergeInProgressError and begins waiting on the merge.
	const reqConcurrency = 10
	var rhsSentinel roachpb.Key
	reqWaitingOnMerge := make(chan struct{}, reqConcurrency)
	testingConcurrencyRetryFilter := func(_ context.Context, ba roachpb.BatchRequest, pErr *roachpb.Error) {
		if _, ok := pErr.GetDetail().(*roachpb.MergeInProgressError); ok {
			for _, r := range ba.Requests {
				req := r.GetInner()
				switch req.Method() {
				case roachpb.Get, roachpb.Put:
					if req.Header().Key.Equal(rhsSentinel) {
						reqWaitingOnMerge <- struct{}{}
					}
				}
			}
		}
	}

	manualClock := hlc.NewHybridManualClock()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter:                    testingRequestFilter,
						TestingConcurrencyRetryFilter:           testingConcurrencyRetryFilter,
						AllowLeaseRequestProposalsWhenNotLeader: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Create the ranges to be merged. Put both ranges on both stores, but give
	// the second store the lease on the RHS. The LHS is largely irrelevant. What
	// matters is that the RHS exists on two stores so we can transfer its lease
	// during the merge.
	lhsDesc, rhsDesc, err := tc.Servers[0].ScratchRangeWithExpirationLeaseEx()
	require.NoError(t, err)
	rhsSentinel = rhsDesc.StartKey.AsRawKey()

	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(1))

	// Launch the merge.
	mergeErr := make(chan error)
	go func() {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
		mergeErr <- pErr.GoError()
	}()

	// Wait for the merge transaction to send its EndTxn request. It won't
	// be able to complete just yet, thanks to the hook we installed above.
	mergeTxn := <-mergeEndTxnReceived

	// Now's our chance to move the lease on the RHS from the second store to the
	// first. This isn't entirely straightforward. The replica on the second store
	// is aware of the merge and is refusing all traffic, so we can't just send a
	// TransferLease request. Instead, we need to expire the second store's lease,
	// then acquire the lease on the first store.
	toAdvance := store.GetStoreConfig().LeaseExpiration()

	// Before doing so, however, ensure that the merge transaction has written
	// its transaction record so that it doesn't run into trouble with the low
	// water mark of the new leaseholder's timestamp cache. This could result in
	// the transaction being inadvertently aborted during its first attempt,
	// which this test is not designed to handle. If the merge transaction did
	// abort then the get requests could complete on r2 before the merge retried.
	//
	// We heartbeat the merge's transaction record with a timestamp forwarded by
	// the duration we plan to advance the clock by so that the transaction does
	// not look expired even after the manual clock update.
	afterAdvance := tc.Servers[0].Clock().Now().Add(toAdvance, 0)
	hb, hbH := heartbeatArgs(mergeTxn, afterAdvance)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), hbH, hb); pErr != nil {
		t.Fatal(pErr)
	}

	// Then increment the clock to expire all leases.
	manualClock.Increment(toAdvance)

	// Send several get and put requests to the RHS. The first of these to
	// arrive will acquire the lease; the remaining requests will wait for that
	// lease acquisition to complete. Then all requests should block waiting for
	// the Subsume request to complete. By sending several of these requests in
	// parallel, we attempt to trigger a race where a request could slip through
	// on the replica between when the new lease is installed and when the
	// mergeComplete channel is installed.
	//
	// Note that the first request would never hit this race on its own. Nor would
	// any request that arrived early enough to see an outdated lease in
	// Replica.mu.state.Lease. All of these requests joined the in-progress lease
	// acquisition and blocked until the lease command acquires its latches,
	// at which point the mergeComplete channel was updated. To hit the race, the
	// request needed to arrive exactly between the update to
	// Replica.mu.state.Lease and the update to Replica.mu.mergeComplete.
	//
	// This race has since been fixed by installing the mergeComplete channel
	// before the new lease.
	reqErrs := make(chan *roachpb.Error) // closed when all reqs done
	var wg sync.WaitGroup
	wg.Add(reqConcurrency)
	go func() {
		wg.Wait()
		close(reqErrs)
	}()

	for i := 0; i < reqConcurrency; i++ {
		go func(i int) {
			defer wg.Done()
			// For this test to have a shot at triggering a race, this log message
			// must be interleaved with the "new range lease" message, like so:
			//
			//     I180821 21:57:53.799207 388 storage/client_merge_test.go:1079  starting get 5
			//     I180821 21:57:53.800122 72 storage/replica_proposal.go:214  [s1,r2/1:{b-/Max}] new range lease ...
			//     I180821 21:57:53.800447 318 storage/client_merge_test.go:1079  starting get 6
			//
			// When this test was written, it would always produce the above
			// interleaving, and successfully trigger the race when run with the race
			// detector enabled about 50% of the time.
			log.Infof(ctx, "starting req %d", i)
			var req roachpb.Request
			if i%2 == 0 {
				req = getArgs(rhsSentinel)
			} else {
				req = putArgs(rhsSentinel, []byte(fmt.Sprintf("val%d", i)))
			}
			_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
				RangeID: rhsDesc.RangeID,
			}, req)
			reqErrs <- pErr
		}(i)
		time.Sleep(time.Millisecond)
	}

	// Wait for the get and put requests to begin waiting on the merge to
	// complete. Then wait a little bit longer. This tests that the requests
	// really do get stuck waiting for the merge to complete without depending
	// too heavily on implementation details.
	for i := 0; i < reqConcurrency; i++ {
		select {
		case <-reqWaitingOnMerge:
			// Waiting on merge.
		case pErr := <-reqErrs:
			// Requests may never wait on the merge if s1 has not yet learned
			// s2's lease is expired. Instead, we'll see a NotLeaseholderError.
			require.IsType(t, &roachpb.NotLeaseHolderError{}, pErr.GetDetail())
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Finally, allow the merge to complete. It should complete successfully.
	close(finishMerge)
	require.NoError(t, <-mergeErr)

	// Because the merge completed successfully, r2 has ceased to exist. We
	// therefore *must* see only RangeNotFoundErrors here from every pending get
	// and put request. Anything else is a consistency error (or a bug in the
	// test).
	for pErr := range reqErrs {
		require.IsType(t, &roachpb.RangeNotFoundError{}, pErr.GetDetail())
	}
}

// TestStoreRangeMergeRHSLeaseTransfers verifies that in cases where a lease
// transfer is triggered while a range merge is in progress, it is rejected
// immediately and does not prevent the merge itself from completing by creating
// a deadlock.
func TestStoreRangeMergeRHSLeaseTransfers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Install a hook to control when the merge transaction subsumes the RHS.
	// Put this in a sync.Once to ignore retries.
	var once sync.Once
	subsumeReceived := make(chan struct{})
	finishSubsume := make(chan struct{})
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if ba.IsSingleSubsumeRequest() {
			once.Do(func() {
				subsumeReceived <- struct{}{}
				<-finishSubsume
			})
		}
		return nil
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter:                    testingRequestFilter,
						AllowLeaseRequestProposalsWhenNotLeader: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Create the ranges to be merged. Put both ranges on both stores, but give
	// the second store the lease on the RHS. The LHS is largely irrelevant. What
	// matters is that the RHS exists on two stores so we can transfer its lease
	// during the merge.
	lhsDesc, rhsDesc, err := tc.Servers[0].ScratchRangeWithExpirationLeaseEx()
	require.NoError(t, err)

	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(1))

	// Launch the merge.
	mergeErr := make(chan error)
	_ = tc.Stopper().RunAsyncTask(ctx, "merge", func(context.Context) {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
		mergeErr <- pErr.GoError()
	})

	// Wait for the merge transaction to send its Subsume request. It won't
	// be able to complete just yet, thanks to the hook we installed above.
	<-subsumeReceived

	// Transfer the lease to store 0. Even though the Subsume request has not
	// yet evaluated, the new leaseholder will notice the deletion intent on its
	// local range descriptor (see maybeWatchForMergeLocked) and will begin
	// blocking most operations.
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(0))

	// Attempt to transfer the lease back to store 1. This will cause the
	// current leaseholder to revoke its lease (see minLeaseProposedTS), which
	// will cause the Subsume request to need to acquire a new range lease.
	//
	// In the past, this lease transfer would get blocked on the mergeComplete
	// channel. While in this state, it would block the lease acquisition
	// triggerred by the Subsume request because a replica only performs a
	// single lease operation at a time. As a result, this would deadlock and
	// neither the lease transfer nor the merge would ever complete.
	err = tc.TransferRangeLease(rhsDesc, tc.Target(1))
	require.Regexp(t, "cannot transfer lease while merge in progress", err)

	// Finally, allow the merge to complete. It should complete successfully.
	close(finishSubsume)
	require.NoError(t, <-mergeErr)
}

// TestStoreRangeMergeLHSLeaseTransfersAfterFreezeTime verifies that in cases
// where the lease start time on a LHS range is moved above the freeze time of a
// range merge, the combined range after the merge does not broadcast a closed
// timestamp that it then allows to be violated.
//
// This is a regression test for #60929. In that issue, which was discovered by
// kvnemesis, we found that a range merge and a lease transfer could race in
// such a way that the closed timestamp could later be violated by a write to
// the subsumed portion of the joint range. The root cause of this was an
// opportunistic optimization made in 7037b54 to consider a range's lease start
// time as an input to its closed timestamp computation. This optimization did
// not account for the possibility of serving writes to a newly subsumed
// keyspace below a range's lease start time if that keyspace was merged into a
// range under its current lease and with a freeze time below the current lease
// start time. This bug was fixed by removing the optimization, which was on its
// way out to allow for #61986 anyway.
func TestStoreRangeMergeLHSLeaseTransfersAfterFreezeTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Install a hook to control when the merge transaction subsumes the RHS.
	// Put this in a sync.Once to ignore retries.
	var once sync.Once
	subsumeReceived := make(chan struct{})
	finishSubsume := make(chan struct{})
	testingResponseFilter := func(_ context.Context, ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
		if ba.IsSingleSubsumeRequest() {
			once.Do(func() {
				subsumeReceived <- struct{}{}
				<-finishSubsume
			})
		}
		return nil
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingResponseFilter: testingResponseFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Create the ranges to be merged. Put both ranges on both stores. Give the
	// first store the lease on the LHS and the second store the lease on the
	// RHS. Before the merge completes, we'll transfer the LHS's lease to the
	// second store so that the two leaseholders are collocated.
	lhsDesc, rhsDesc, err := tc.Servers[0].ScratchRangeEx()
	require.NoError(t, err)

	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, lhsDesc, tc.Target(0))
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(1))

	// Launch the merge.
	mergeErr := make(chan error, 1)
	_ = tc.Stopper().RunAsyncTask(ctx, "merge", func(context.Context) {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), args)
		mergeErr <- pErr.GoError()
	})

	// Wait for the merge transaction to send its Subsume request. It won't
	// be able to complete just yet, thanks to the hook we installed above.
	<-subsumeReceived

	// Transfer the lease on the LHS to the second store. Doing this will
	// increase the lease start time on the LHS past the freeze time of the
	// range merge.
	if err := tc.TransferRangeLease(lhsDesc, tc.Target(1)); err != nil {
		close(finishSubsume) // don't abandon merge
		t.Fatalf(`could transfer lease for range %s error is %+v`, lhsDesc, err)
	}

	store1 := tc.GetFirstStoreFromServer(t, 1)
	lhsLeaseholder := store1.LookupReplica(lhsDesc.StartKey)
	testutils.SucceedsSoon(t, func() error {
		// Wait for the new leaseholder to notice that it received the lease.
		now := tc.Servers[1].Clock().NowAsClockTimestamp()
		if !lhsLeaseholder.OwnsValidLease(ctx, now) {
			return errors.New("not leaseholder")
		}
		return nil
	})
	lhsClosedTS, ok := lhsLeaseholder.MaxClosed(ctx)
	require.True(t, ok)

	// Finally, allow the merge to complete. It should complete successfully.
	close(finishSubsume)
	require.NoError(t, <-mergeErr)

	// Attempt to write below the closed timestamp, to the subsumed keyspace.
	// The write's timestamp should be forwarded to after the closed timestamp.
	// If it is not, we have violated the closed timestamp's promise!
	var ba roachpb.BatchRequest
	ba.Timestamp = lhsClosedTS.Prev()
	ba.RangeID = lhsDesc.RangeID
	ba.Add(incrementArgs(rhsDesc.StartKey.AsRawKey().Next(), 1))
	br, pErr := tc.Servers[1].DistSender().Send(ctx, ba)
	require.Nil(t, pErr)
	require.NotEqual(t, ba.Timestamp, br.Timestamp, "write timestamp not bumped")
	require.True(t, lhsClosedTS.Less(br.Timestamp), "write timestamp not bumped above closed timestamp")
}

// TestStoreRangeMergeCheckConsistencyAfterSubsumption verifies the following:
// 1. While a range is subsumed, ComputeChecksum requests wait until the merge
// is complete before proceeding.
// 2. Once a merge is aborted, pending (and future) requests will be allowed to
// be proposed. An assertion at the end of Replica.propose() ensures that the
// lease applied index of a range cannot be bumped while it is subsumed. A large
// comment block at the end of Subsume() in cmd_subsume.go explains the hazard
// in detail. This test is meant as a sanity check for this assertion.
func TestStoreRangeMergeCheckConsistencyAfterSubsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Install a hook to control when the merge transaction aborts.
	mergeEndTxnReceived := make(chan *roachpb.Transaction, 10) // headroom in case the merge transaction retries
	abortMergeTxn := make(chan struct{})
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, r := range ba.Requests {
			if et := r.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				mergeEndTxnReceived <- ba.Txn
				<-abortMergeTxn
				return roachpb.NewError(errors.New("abort the merge for test"))
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	// Create the ranges to be merged. Put both ranges on both stores, but give
	// the second store the lease on the RHS.
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, *rhsDesc, tc.Target(1))

	// Launch the merge.
	mergeErr := make(chan *roachpb.Error)
	go func() {
		args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
		mergeErr <- pErr
	}()

	// Wait for the merge transaction to send its EndTxn request. It won't
	// be able to complete just yet, thanks to the hook we installed above.
	<-mergeEndTxnReceived

	checkConsistencyResp := make(chan interface{})
	go func() {
		args := checkConsistencyArgs(rhsDesc)
		_, pErr := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, 1).TestSender(), args)
		checkConsistencyResp <- pErr
	}()

	select {
	case <-checkConsistencyResp:
		t.Fatalf("expected the consistency check to wait until the merge was complete")
	case <-time.After(1 * time.Second):
	}

	// Let the merge abort, and then ensure that the consistency check
	// successfully goes through.
	close(abortMergeTxn)

	pErr := <-mergeErr
	require.IsType(t, &roachpb.Error{}, pErr)
	require.Regexp(t, "abort the merge for test", pErr.String())

	testutils.SucceedsSoon(t, func() error {
		pErr := <-checkConsistencyResp
		require.Nil(t, pErr)
		return nil
	})
}

// TestStoreRangeMergeConcurrentRequests tests merging ranges that are serving
// other traffic concurrently.
func TestStoreRangeMergeConcurrentRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var store *kvserver.Store
	manualClock := hlc.NewHybridManualClock()
	testingResponseFilter := func(
		ctx context.Context, ba roachpb.BatchRequest, _ *roachpb.BatchResponse,
	) *roachpb.Error {
		cput := ba.Requests[0].GetConditionalPut()
		if cput != nil && !cput.Value.IsPresent() && bytes.HasSuffix(cput.Key, keys.LocalRangeDescriptorSuffix) && rand.Int()%4 == 0 {
			// After every few deletions of the local range descriptor, expire all
			// range leases. This makes the following sequence of events quite likely:
			//
			//     1. The merge transaction begins and lays down deletion intents for
			//        the meta2 and local copies of the RHS range descriptor.
			//     2. The RHS replica loses its lease, thanks to the following call to
			//        mtc.advanceClock.
			//     3. A Get request arrives at the RHS replica and triggers a
			//        synchronous lease acquisition. The lease acquisition notices
			//        that a merge is in progress and installs a mergeComplete
			//        channel.
			//     4. The Get request blocks on the newly installed mergeComplete
			//        channel.
			//     5. The Subsume request arrives. (Or, if the merge transaction is
			//        incorrectly pipelined, the QueryIntent request for the RHS range
			//        descriptor key that precedes the Subsume request arrives.)
			//
			// This scenario previously caused deadlock. The merge was not able to
			// complete until the Subsume request completed, but the Subsume request
			// was unable to acquire latches until the Get request finished, which
			// was itself waiting for the merge to complete. Whoops!
			manualClock.Increment(store.GetStoreConfig().LeaseExpiration())
		}
		return nil
	}

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue:     true,
				DisableSplitQueue:     true,
				DisableReplicateQueue: true,
				TestingResponseFilter: testingResponseFilter,
			},
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key, err := s.ScratchRangeWithExpirationLease()
	require.NoError(t, err)

	// We need to use a range with expiration based leases, so set up some test keys
	// in the expiration based scratch space.
	keys := make([]roachpb.Key, 6)
	keys[0] = key.Next()
	for i := 1; i < 6; i++ {
		keys[i] = keys[i-1].Next().Next()
	}
	// We chose a split key between key[2] and key[3]
	splitKey := keys[2].Next()
	for _, k := range keys {
		if err := store.DB().Put(ctx, k, "val"); err != nil {
			t.Fatal(err)
		}
	}

	// Failures in this test often present as a deadlock. Set a short timeout to
	// limit the damage.
	ctx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	const numGetWorkers = 16
	const numMerges = 16

	var numGets int64
	doneCh := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	for i := 0; i < numGetWorkers; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-doneCh:
					return nil
				default:
				}
				key := keys[rand.Intn(len(keys))]
				if kv, err := store.DB().Get(ctx, key); err != nil {
					return err
				} else if v := string(kv.ValueBytes()); v != "val" {
					return fmt.Errorf(`expected "val", but got %q`, v)
				}
				atomic.AddInt64(&numGets, 1)
			}
		})
	}

	for i := 0; i < numMerges; i++ {
		lhsDesc, _, err := s.SplitRange(splitKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, pErr := s.MergeRanges(lhsDesc.StartKey.AsRawKey()); pErr != nil {
			t.Fatal(pErr)
		}
	}

	close(doneCh)
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// Expect that each worker was able to issue one at least one get request
	// during every split/merge cycle. Empirical evidence suggests that this a
	// very conservative estimate that is unlikely to be flaky.
	if n := atomic.LoadInt64(&numGets); n < numGetWorkers*numMerges {
		t.Fatalf("suspiciously low numGets (expected at least %d): %d", numGetWorkers*numMerges, n)
	}
}

// TestStoreReplicaGCAfterMerge verifies that the replica GC queue writes the
// correct tombstone when it GCs a replica of range that has been merged away.
//
// Consider the following sequence of events observed in a real cluster:
//
//     1. Adjacent ranges Q and R are slated to be merged. Q has replicas on
//        stores S1, S2, and S3, while R has replicas on S1, S2, and S4.
//     2. To collocate Q and R, the merge queue adds a replica of R on S3 and
//        removes the replica on S4. The replica on S4 is queued for garbage
//        collection, but is not yet processed.
//     3. The merge transaction commits, deleting R's range descriptor from the
//        meta2 index.
//     4. The replica GC queue processes the former replica of R on S4. It
//        performs a consistent lookup of R's start key in the meta2 index to
//        determine whether the replica is still a member of R. Since R has been
//        deleted, the lookup returns Q's range descriptor, not R's.
//
// The replica GC queue would previously fail to notice that it had received Q's
// range descriptor, not R's. It would then proceed to call store.RemoveReplica
// with Q's descriptor, which would write a replica tombstone for Q, when in
// fact the replica tombstone needed to be written for R. Without the correct
// replica tombstone, if S4 received a slow Raft message for the now-GC'd
// replica, it would incorrectly construct an uninitialized replica and panic.
//
// This test also ensures that the nodes which processes the Merge writes a
// tombstone which prevents the range from being resurrected by a raft message.
//
// This test's approach to simulating this sequence of events is based on
// TestReplicaGCRace.
func TestStoreReplicaGCAfterMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableEagerReplicaRemoval: true,
						DisableReplicaGCQueue:      true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store0, store1 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 1)

	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Target(1))
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	tc.RemoveVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.RemoveVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	for _, rangeID := range []roachpb.RangeID{lhsDesc.RangeID, rhsDesc.RangeID} {
		repl, err := store1.GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := store1.ManualReplicaGC(repl); err != nil {
			t.Fatal(err)
		}
		if _, err := store1.GetReplica(rangeID); err == nil {
			t.Fatalf("replica of r%d not gc'd from s1", rangeID)
		}
	}

	rhsReplDesc0, ok := rhsDesc.GetReplicaDescriptor(store0.StoreID())
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rhsDesc, store0)
	}
	rhsReplDesc1, ok := rhsDesc.GetReplicaDescriptor(store1.StoreID())
	if !ok {
		t.Fatalf("expected %s to have a replica on %s", rhsDesc, store1)
	}

	transport := kvserver.NewRaftTransport(
		log.AmbientContext{Tracer: tc.Servers[0].RaftTransport().Tracer},
		cluster.MakeTestingClusterSettings(),
		nodedialer.New(tc.Servers[0].RPCContext(), gossip.AddressResolver(tc.Servers[0].Gossip())),
		nil, /* grpcServer */
		tc.Servers[0].Stopper(),
	)
	errChan := errorChannelTestHandler(make(chan *roachpb.Error, 1))
	transport.Listen(store0.StoreID(), errChan)
	transport.Listen(store1.StoreID(), errChan)

	sendHeartbeat := func(
		rangeID roachpb.RangeID,
		fromReplDesc, toReplDesc roachpb.ReplicaDescriptor,
	) {
		// Try several times, as the message may be dropped (see #18355).
		for i := 0; i < 5; i++ {
			if sent := transport.SendAsync(&kvserver.RaftMessageRequest{
				FromReplica: fromReplDesc,
				ToReplica:   toReplDesc,
				Heartbeats: []kvserver.RaftHeartbeat{
					{
						RangeID:       rangeID,
						FromReplicaID: fromReplDesc.ReplicaID,
						ToReplicaID:   toReplDesc.ReplicaID,
						Commit:        42,
					},
				},
			}, rpc.DefaultClass); !sent {
				t.Fatal("failed to send heartbeat")
			}
			select {
			case pErr := <-errChan:
				switch pErr.GetDetail().(type) {
				case *roachpb.RaftGroupDeletedError:
					return
				default:
					t.Fatalf("unexpected error type %T: %s", pErr.GetDetail(), pErr)
				}
			case <-time.After(time.Second):
			}
		}
		t.Fatal("did not get expected RaftGroupDeleted error")
	}

	// Send a heartbeat to the now-GC'd replica on the stores. If the replica
	// tombstone was not written correctly when the replica was GC'd, this will
	// cause a panic.
	sendHeartbeat(rhsDesc.RangeID, rhsReplDesc0, rhsReplDesc1)
	sendHeartbeat(rhsDesc.RangeID, rhsReplDesc1, rhsReplDesc0)

	// Send a heartbeat to a fictional replicas on with a large replica ID.
	// This tests an implementation detail: the replica tombstone that gets
	// written in this case will use the maximum possible replica ID, so the
	// stores should ignore heartbeats for replicas of the range with _any_
	// replica ID.
	sendHeartbeat(rhsDesc.RangeID, rhsReplDesc0, roachpb.ReplicaDescriptor{
		NodeID:    store1.Ident.NodeID,
		StoreID:   store1.Ident.StoreID,
		ReplicaID: 123456,
	})

	sendHeartbeat(rhsDesc.RangeID, rhsReplDesc1, roachpb.ReplicaDescriptor{
		NodeID:    store0.Ident.NodeID,
		StoreID:   store0.Ident.StoreID,
		ReplicaID: 123456,
	})

	// Be extra paranoid and verify the exact value of the replica tombstone.
	checkTombstone := func(eng storage.Engine) {
		var rhsTombstone roachpb.RangeTombstone
		rhsTombstoneKey := keys.RangeTombstoneKey(rhsDesc.RangeID)
		ok, err = storage.MVCCGetProto(ctx, eng, rhsTombstoneKey, hlc.Timestamp{},
			&rhsTombstone, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("missing range tombstone at key %s", rhsTombstoneKey)
		}
		if e, a := roachpb.ReplicaID(math.MaxInt32), rhsTombstone.NextReplicaID; e != a {
			t.Fatalf("expected next replica ID to be %d, but got %d", e, a)
		}
	}
	checkTombstone(store0.Engine())
	checkTombstone(store1.Engine())
}

// TestStoreRangeMergeAddReplicaRace verifies that when an add replica request
// occurs concurrently with a merge, one of them is aborted with a "descriptor
// changed" CPut error.
func TestStoreRangeMergeAddReplicaRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	origDesc := tc.LookupRangeOrFatal(t, scratchStartKey)
	splitKey := scratchStartKey.Next()
	beforeDesc, _ := tc.SplitRangeOrFatal(t, splitKey)

	mergeErrCh, addErrCh := make(chan error, 1), make(chan error, 1)
	go func() {
		mergeErrCh <- tc.Server(0).DB().AdminMerge(ctx, scratchStartKey)
	}()
	go func() {
		_, err := tc.Server(0).DB().AdminChangeReplicas(
			ctx, scratchStartKey, beforeDesc, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)))
		addErrCh <- err
	}()
	mergeErr := <-mergeErrCh
	addErr := <-addErrCh
	afterDesc := tc.LookupRangeOrFatal(t, scratchStartKey)

	const acceptableMergeErr = `unexpected value: raw_bytes|ranges not collocated` +
		`|is in a joint state or has learners`
	if mergeErr == nil && kvserver.IsRetriableReplicationChangeError(addErr) {
		// Merge won the race, no add happened.
		require.Len(t, afterDesc.Replicas().VoterDescriptors(), 1)
		require.Equal(t, origDesc.EndKey, afterDesc.EndKey)
	} else if addErr == nil && testutils.IsError(mergeErr, acceptableMergeErr) {
		// Add won the race, no merge happened.
		require.Len(t, afterDesc.Replicas().VoterDescriptors(), 2)
		require.Equal(t, beforeDesc.EndKey, afterDesc.EndKey)
	} else {
		t.Fatalf(`expected exactly one of merge or add to succeed got: [merge] %v [add] %v`,
			mergeErr, addErr)
	}
}

// TestStoreRangeMergeResplitAddReplicaRace tests a diabolical edge case in the
// merge/add replica race. If two replicas merge and then split at the previous
// boundary, the descriptor will look unchanged and our usual CPut protection
// would fail. For this reason, we introduced RangeDescriptor.Generation.
//
// Note that splits will not increment the generation counter until the cluster
// version includes VersionRangeMerges. That's ok, because a sequence of splits
// alone will always result in a descriptor with a smaller end key. Only a
// sequence of splits AND merges can result in an unchanged end key, and merges
// always increment the generation counter.
func TestStoreRangeMergeResplitAddReplicaRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	splitKey := scratchStartKey.Next()
	origDesc, _ := tc.SplitRangeOrFatal(t, splitKey)
	require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, scratchStartKey))
	resplitDesc, _ := tc.SplitRangeOrFatal(t, splitKey)

	assert.Equal(t, origDesc.RangeID, resplitDesc.RangeID)
	assert.Equal(t, origDesc.StartKey, resplitDesc.StartKey)
	assert.Equal(t, origDesc.EndKey, resplitDesc.EndKey)
	assert.Equal(t, origDesc.Replicas().Descriptors(), resplitDesc.Replicas().Descriptors())
	assert.NotEqual(t, origDesc.Generation, resplitDesc.Generation)

	_, err := tc.Server(0).DB().AdminChangeReplicas(
		ctx, scratchStartKey, origDesc, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1)))
	if !kvserver.IsRetriableReplicationChangeError(err) {
		t.Fatalf(`expected "descriptor changed" error got: %+v`, err)
	}
}

func TestStoreRangeMergeSlowUnabandonedFollower_NoSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)

	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	testutils.SucceedsSoon(t, func() error {
		if rhsRepl2, err := store2.GetReplica(rhsDesc.RangeID); err != nil || !rhsRepl2.IsInitialized() {
			return errors.Errorf("store2 has not yet processed split. err: %v", err)
		}
		return nil
	})

	// Block Raft traffic to the LHS replica on store2, by holding its raftMu, so
	// that its LHS isn't aware there's a merge in progress.
	lhsRepl2, err := store2.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	lhsRepl2.RaftLock()

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Verify that store2 won't inadvertently GC the RHS before it's heard about
	// the merge. This is a tricky case for the replica GC queue, as meta2 will
	// indicate that the range has been merged away.
	rhsRepl2, err := store2.GetReplica(rhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store2.ManualReplicaGC(rhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(rhsDesc.RangeID); err != nil {
		t.Fatalf("non-abandoned rhs replica unexpectedly GC'd before merge")
	}

	// Restore communication with store2. Give it the lease to force all commands
	// to be applied, including the merge trigger.
	lhsRepl2.RaftUnlock()
	tc.TransferRangeLeaseOrFatal(t, *lhsDesc, tc.Target(2))
}

func TestStoreRangeMergeSlowUnabandonedFollower_WithSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)
	repl0 := store0.LookupReplica(roachpb.RKey("a"))

	tc.AddVotersOrFatal(t, repl0.Desc().StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	testutils.SucceedsSoon(t, func() error {
		_, err := store2.GetReplica(rhsDesc.RangeID)
		return err
	})

	// Start dropping all Raft traffic to the LHS on store2 so that it won't be
	// aware that there is a merge in progress.
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, &unreliableRaftHandler{
		rangeID:            lhsDesc.RangeID,
		RaftMessageHandler: store2,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropReq: func(req *kvserver.RaftMessageRequest) bool {
				return true
			},
		},
	})

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Now split the newly merged range splits back out at exactly the same key.
	// When the replica GC queue looks in meta2 it will find the new RHS range, of
	// which store2 is a member. Note that store2 does not yet have an initialized
	// replica for this range, since it would intersect with the old RHS replica.
	_, newRHSDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the LHS replica from store2.
	tc.RemoveVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))

	// Transfer the lease on the new RHS to store2 and wait for it to apply. This
	// will force its replica to of the new RHS to become up to date, which
	// indirectly tests that the replica GC queue cleans up both the LHS replica
	// and the old RHS replica.
	tc.TransferRangeLeaseOrFatal(t, *newRHSDesc, tc.Target(2))
	testutils.SucceedsSoon(t, func() error {
		rhsRepl, err := store2.GetReplica(newRHSDesc.RangeID)
		if err != nil {
			return err
		}
		if !rhsRepl.OwnsValidLease(ctx, tc.Servers[2].Clock().NowAsClockTimestamp()) {
			return errors.New("rhs store does not own valid lease for rhs range")
		}
		return nil
	})
}

func TestStoreRangeMergeSlowAbandonedFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableReplicaGCQueue: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)

	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for store2 to hear about the split.
	var rhsRepl2 *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		if rhsRepl2, err = store2.GetReplica(rhsDesc.RangeID); err != nil || !rhsRepl2.IsInitialized() {
			return errors.New("store2 has not yet processed split")
		}
		return nil
	})

	// Block Raft traffic to the LHS replica on store2, by holding its raftMu, so
	// that its LHS isn't aware there's a merge in progress.
	lhsRepl2, err := store2.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	lhsRepl2.RaftLock()

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Remove store2 from the range after the merge. It won't hear about this yet,
	// but we'll be able to commit the configuration change because we have two
	// other live members.
	tc.RemoveVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))

	// Verify that store2 won't inadvertently GC the RHS before it's heard about
	// the merge. This is a particularly tricky case for the replica GC queue, as
	// meta2 will indicate that the range has been merged away AND that store2 is
	// not a member of the new range.
	if err := store2.ManualReplicaGC(rhsRepl2); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(rhsDesc.RangeID); err != nil {
		t.Fatal("rhs replica on store2 destroyed before lhs applied merge")
	}

	// Flush store2's queued requests.
	lhsRepl2.RaftUnlock()

	// Ensure that the unblocked merge eventually applies and subsumes the RHS.
	// In general this will happen due to receiving a ReplicaTooOldError but
	// it may require the replica GC queue. In rare cases the LHS will never
	// hear about the merge and may need to be GC'd on its own.
	testutils.SucceedsSoon(t, func() error {
		// Make the LHS gets destroyed.
		if lhsRepl, err := store2.GetReplica(lhsDesc.RangeID); err == nil {
			if err := store2.ManualReplicaGC(lhsRepl); err != nil {
				t.Fatal(err)
			}
		}
		if rhsRepl, err := store2.GetReplica(rhsDesc.RangeID); err == nil {
			if err := store2.ManualReplicaGC(rhsRepl); err != nil {
				t.Fatal(err)
			}
			return errors.New("rhs not yet destroyed")
		}
		return nil
	})
}

func TestStoreRangeMergeAbandonedFollowers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableReplicaGCQueue:      true,
						DisableEagerReplicaRemoval: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store2 := tc.GetFirstStoreFromServer(t, 2)

	rangeDesc := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Split off three ranges.
	keys := []roachpb.RKey{roachpb.RKey("a"), roachpb.RKey("b"), roachpb.RKey("c")}
	for _, key := range keys {
		splitArgs := adminSplitArgs(key.AsRawKey())
		if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), splitArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Wait for store2 to hear about all three splits.
	var repls []*kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repls = nil
		for _, key := range keys {
			repl := store2.LookupReplica(key) /* end */
			if repl == nil || !repl.Desc().StartKey.Equal(key) {
				return fmt.Errorf("replica for key %q is missing or has wrong start key: %s", key, repl)
			}
			repls = append(repls, repl)
		}
		return nil
	})

	// Remove all replicas from store2.
	for _, repl := range repls {
		tc.RemoveVotersOrFatal(t, repl.Desc().StartKey.AsRawKey(), tc.Target(2))
	}

	// Merge all three ranges together. store2 won't hear about this merge.
	for i := 0; i < 2; i++ {
		if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), adminMergeArgs(roachpb.Key("a"))); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Verify that the abandoned ranges on store2 can only be GC'd from left to
	// right.
	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err != nil {
		t.Fatal("c replica on store2 destroyed before b")
	}
	if err := store2.ManualReplicaGC(repls[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[1].RangeID); err != nil {
		t.Fatal("b replica on store2 destroyed before a")
	}
	if err := store2.ManualReplicaGC(repls[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[0].RangeID); err == nil {
		t.Fatal("a replica not destroyed")
	}

	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err != nil {
		t.Fatal("c replica on store2 destroyed before b")
	}
	if err := store2.ManualReplicaGC(repls[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[1].RangeID); err == nil {
		t.Fatal("b replica not destroyed")
	}

	if err := store2.ManualReplicaGC(repls[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := store2.GetReplica(repls[2].RangeID); err == nil {
		t.Fatal("c replica not destroyed")
	}
}

// TestStoreRangeMergeAbandonedFollowersAutomaticallyGarbageCollected verifies
// that the replica GC queue will clean up an abandoned RHS replica whose
// destroyStatus is destroyReasonMergePending. The RHS replica ends up in this
// state when its merge watcher goroutine notices that the merge committed, and
// thus marks it as destroyed with reason destroyReasonMergePending, but the
// corresponding LHS is rebalanced off the store before it can apply the merge
// trigger. The replica GC queue would previously refuse to GC the abandoned
// RHS, as it interpreted destroyReasonMergePending to mean that the RHS replica
// had already been garbage collected.
func TestStoreRangeMergeAbandonedFollowersAutomaticallyGarbageCollected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)

	repl0 := store0.LookupReplica(roachpb.RKey("a"))
	tc.AddVotersOrFatal(t, repl0.Desc().StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	// Make store2 the leaseholder for the RHS and wait for the lease transfer to
	// apply.
	tc.TransferRangeLeaseOrFatal(t, *rhsDesc, tc.Target(2))
	testutils.SucceedsSoon(t, func() error {
		rhsRepl, err := store2.GetReplica(rhsDesc.RangeID)
		if err != nil {
			return err
		}
		if !rhsRepl.OwnsValidLease(ctx, tc.Servers[2].Clock().NowAsClockTimestamp()) {
			return errors.New("store2 does not own valid lease for rhs range")
		}
		return nil
	})

	// Start dropping all Raft traffic to the LHS replica on store2 so that it
	// won't be aware that there is a merge in progress.
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, &unreliableRaftHandler{
		rangeID:            lhsDesc.RangeID,
		RaftMessageHandler: store2,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropReq: func(*kvserver.RaftMessageRequest) bool {
				return true
			},
		},
	})

	// Perform the merge. The LHS replica on store2 whon't hear about this merge
	// and thus won't subsume its RHS replica. The RHS replica's merge watcher
	// goroutine will, however, notice the merge and mark the RHS replica as
	// destroyed with reason destroyReasonMergePending.
	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Remove the merged range from store2. Its replicas of both the LHS and RHS
	// are now eligible for GC.
	tc.RemoveVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))

	// Note that we purposely do not call store.ManualReplicaGC here, as that
	// calls replicaGCQueue.process directly, bypassing the logic in
	// baseQueue.MaybeAdd and baseQueue.Add. We specifically want to test that
	// queuing logic, which has been broken in the past.
	testutils.SucceedsSoon(t, func() error {
		if _, err := store2.GetReplica(lhsDesc.RangeID); err == nil {
			return errors.New("lhs not destroyed")
		}
		if _, err := store2.GetReplica(rhsDesc.RangeID); err == nil {
			return errors.New("rhs not destroyed")
		}
		return nil
	})
}

func TestStoreRangeMergeDeadFollowerBeforeTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store0 := tc.GetFirstStoreFromServer(t, 0)

	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, _, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	tc.StopServer(2)

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	expErr := "waiting for all left-hand replicas to initialize"
	if !testutils.IsPError(pErr, expErr) {
		t.Fatalf("expected %q error, but got %v", expErr, pErr)
	}
}

func TestStoreRangeMergeDeadFollowerDuringTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var tc *testcluster.TestCluster
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if ba.IsSingleSubsumeRequest() {
			tc.StopServer(2) // This is safe to call multiple times, it will only stop once
		}
		return nil
	}
	tc = testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store0 := tc.GetFirstStoreFromServer(t, 0)

	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, _, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	expErr := "merge failed: waiting for all right-hand replicas to catch up"
	if !testutils.IsPError(pErr, expErr) {
		t.Fatalf("expected %q error, but got %v", expErr, pErr)
	}
}

func TestStoreRangeReadoptedLHSFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, withMerge bool) {
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						},
					},
				},
			})
		defer tc.Stopper().Stop(context.Background())
		store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)

		// Create two ranges on store0 and store1.
		lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
		if err != nil {
			t.Fatal(err)
		}
		tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(1))
		tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Target(1))

		// Abandon a replica of the LHS on store2.
		tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))
		var lhsRepl2 *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			lhsRepl2, err = store2.GetReplica(lhsDesc.RangeID)
			if err != nil {
				return err
			}
			if !lhsRepl2.IsInitialized() {
				// Make sure the replica is initialized before unreplicating.
				// Uninitialized replicas that have a replicaID are hard to
				// GC (not implemented at the time of writing).
				return errors.Errorf("%s not initialized", lhsRepl2)
			}
			return nil
		})
		tc.RemoveVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))

		if withMerge {
			// Merge the two ranges together.
			args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
			_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
			if pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Attempt to re-add the merged range to store2. This should succeed
		// immediately because there are no overlapping replicas that would interfere
		// with the widening of the existing LHS replica.
		if _, err := store0.DB().AdminChangeReplicas(
			ctx, lhsDesc.StartKey.AsRawKey(),
			*lhsDesc,
			roachpb.MakeReplicationChanges(
				roachpb.ADD_VOTER, tc.Target(2),
			)); !testutils.IsError(err, "descriptor changed") {
			t.Fatal(err)
		}

		if err := store2.ManualReplicaGC(lhsRepl2); err != nil {
			t.Fatal(err)
		}

		tc.AddVotersOrFatal(t, lhsDesc.StartKey.AsRawKey(), tc.Target(2))
		// Give store2 the lease to force all commands to be applied, including the
		// ChangeReplicas.
		tc.TransferRangeLeaseOrFatal(t, *lhsDesc, tc.Target(2))
	}

	testutils.RunTrueAndFalse(t, "withMerge", run)
}

// slowSnapRaftHandler delays any snapshots to rangeID until WaitCh is closed.
type slowSnapRaftHandler struct {
	rangeID roachpb.RangeID
	waitCh  chan struct{}
	kvserver.RaftMessageHandler
	syncutil.Mutex
}

func (h *slowSnapRaftHandler) unblock() {
	h.Lock()
	if h.waitCh != nil {
		close(h.waitCh)
		h.waitCh = nil
	}
	h.Unlock()
}

func (h *slowSnapRaftHandler) HandleSnapshot(
	header *kvserver.SnapshotRequest_Header, respStream kvserver.SnapshotResponseStream,
) error {
	if header.RaftMessageRequest.RangeID == h.rangeID {
		h.Lock()
		waitCh := h.waitCh
		h.Unlock()
		if waitCh != nil {
			<-waitCh
		}
	}
	return h.RaftMessageHandler.HandleSnapshot(header, respStream)
}

// TestStoreRangeMergeUninitializedLHSFollower reproduces a rare bug in which a
// replica of the RHS of a merge could be garbage collected too soon.
//
// Consider two adjacent ranges, A and B. Suppose the replica of
// A on the last store, S3, is uninitialized, e.g. because A was recently
// created by a split and S3 has neither processed the split trigger nor
// received a snapshot. The leaseholder for A will attempt to send a Raft
// snapshot to bring S3's replica up to date, but this Raft snapshot may be
// delayed due to a busy Raft snapshot queue or a slow network.
//
// Now suppose a merge of A and B commits before S3 receives a Raft snapshot for
// A. There is a small window of time in which S3 can garbage collect its
// replica of B! When S3 looks up B's meta2 descriptor, it will find that B has
// been merged away. S3 will then try to prove that B's local left neighbor is
// generationally up-to-date; if it is, it safe to GC B. Usually, S3 would
// determine A to be B's left neighbor, realize that A has not yet processed the
// merge, and correctly refuse to GC its replica of B. In this case, however,
// S3's replica of A is uninitialized and thus doesn't know its start and end
// key, so S3 will instead discover some more-distant left neighbor of B. This
// distant neighbor might very well be up-to-date, and S3 will incorrectly
// conclude that it can GC its replica of B!
//
// So say S3 GCs its replica of B. There are now two paths that A might take.
// The happy case is that A receives a Raft snapshot that postdates the merge.
// The unhappy case is that A receives a Raft snapshot that predates the merge,
// and is then required to apply the merge via a MsgApp. Since there is no
// longer a replica of B on S3, applying the merge trigger will explode.
//
// The solution was to require that all LHS replicas are initialized before
// beginning a merge transaction. This ensures that the replica GC queue will
// always discover the correct left neighbor when considering whether a subsumed
// range can be GC'd.
func TestStoreRangeMergeUninitializedLHSFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)
	distSender := tc.Servers[0].DistSender()

	split := func(key roachpb.RKey) roachpb.RangeID {
		t.Helper()
		if _, pErr := kv.SendWrapped(ctx, distSender, adminSplitArgs(key.AsRawKey())); pErr != nil {
			t.Fatal(pErr)
		}
		return store0.LookupReplica(key).RangeID
	}

	// We'll create two ranges, A and B, as described in the comment on this test
	// function.
	aKey, bKey := roachpb.RKey("a"), roachpb.RKey("b")

	// Put range 1 on all three stores.
	desc := store0.LookupReplica(aKey).Desc()
	tc.AddVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Create range B and wait for store2 to process the split.
	bRangeID := split(bKey)
	var bRepl2 *kvserver.Replica
	testutils.SucceedsSoon(t, func() (err error) {
		if bRepl2, err = store2.GetReplica(bRangeID); err != nil || !bRepl2.IsInitialized() {
			return errors.New("store2 has not yet processed split of c")
		}
		return nil
	})

	// Now we want to create range A, but we need to make sure store2's replica of
	// A is not initialized. This requires dropping all Raft traffic to store2
	// from range 1, which will be the LHS of the split, so that store2's replica
	// of range 1 never processes the split trigger, which would create an
	// initialized replica of A.
	unreliableHandler := &unreliableRaftHandler{
		rangeID:            desc.RangeID,
		RaftMessageHandler: store2,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropReq: func(request *kvserver.RaftMessageRequest) bool {
				return true
			},
		},
	}
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, unreliableHandler)

	// Perform the split of A, now that store2 won't be able to initialize its
	// replica of A.
	aRangeID := split(aKey)

	// Wedge a Raft snapshot that's destined for A. This allows us to capture a
	// pre-merge Raft snapshot, which we'll let loose after the merge commits.
	slowSnapHandler := &slowSnapRaftHandler{
		rangeID:            aRangeID,
		waitCh:             make(chan struct{}),
		RaftMessageHandler: unreliableHandler,
	}
	defer slowSnapHandler.unblock()
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, slowSnapHandler)

	// Remove the replica of range 1 on store2. If we were to leave it in place,
	// store2 would refuse to GC its replica of C after the merge commits, because
	// the left neighbor of C would be this out-of-date replica of range 1.
	// (Remember that we refused to let it process the split of A.) So we need to
	// remove it so that C has no left neighbor and thus will be eligible for GC.
	{
		r1Repl2, err := store2.GetReplica(desc.RangeID)
		if err != nil {
			t.Fatal(err)
		}
		tc.RemoveVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(2))
		testutils.SucceedsSoon(t, func() error {
			if err := store2.ManualReplicaGC(r1Repl2); err != nil {
				return err
			}
			if _, err := store2.GetReplica(desc.RangeID); err == nil {
				return errors.New("r1Repl2 still exists")
			}
			return nil
		})
	}

	// Launch the merge of A and B.
	mergeErr := make(chan error)
	go func() {
		_, pErr := kv.SendWrapped(ctx, distSender, adminMergeArgs(aKey.AsRawKey()))
		mergeErr <- pErr.GoError()
	}()

	// We want to assert that the merge does not complete until we allow store2's
	// replica of B to be initialized (by releasing the blocked Raft snapshot). A
	// happens-before assertion is nearly impossible to express, though, so
	// instead we just wait in the hope that, if the merge is buggy, it will
	// commit while we wait. Before the bug was fixed, this caused the test
	// to fail reliably.
	start := timeutil.Now()
	for timeutil.Since(start) < 50*time.Millisecond {
		if _, err := store2.GetReplica(bRangeID); err == nil {
			// Attempt to reproduce the exact fatal error described in the comment on
			// the test by running range B through the GC queue. If the bug is
			// present, GC will be successful and so the application of the merge
			// trigger on A to fail once we allow the Raft snapshot through. If the
			// bug is not present, we'll be unable to GC range B because it won't get
			// subsumed until after we allow the Raft snapshot through.
			_ = store2.ManualReplicaGC(bRepl2)
		}
		time.Sleep(5 * time.Millisecond) // don't spin too hot to give the merge CPU time to complete
	}

	select {
	case err := <-mergeErr:
		t.Errorf("merge completed early (err: %v)", err)
		close(mergeErr)
	default:
	}

	// Allow store2's replica of A to initialize with a Raft snapshot that
	// predates the merge.
	slowSnapHandler.unblock()

	// Assert that the merge completes successfully.
	if err := <-mergeErr; err != nil {
		t.Fatal(err)
	}

	repl, err := store0.GetReplica(aRangeID)
	require.NoError(t, err)
	// Give store2 the lease on the merged range to force all commands to be
	// applied, including the merge trigger.
	tc.TransferRangeLeaseOrFatal(t, *repl.Desc(), tc.Target(2))
}

// TestStoreRangeMergeWatcher verifies that the watcher goroutine for a merge's
// RHS does not erroneously permit traffic after the merge commits.
func TestStoreRangeMergeWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "inject-failures", testMergeWatcher)
}

func testMergeWatcher(t *testing.T, injectFailures bool) {
	ctx := context.Background()
	var mergeTxnRetries, pushTxnRetries, meta2GetRetries int64
	if injectFailures {
		mergeTxnRetries = 3
		pushTxnRetries = 3
		meta2GetRetries = 3
	}

	// Maybe inject some retryable errors when the merge transaction commits.
	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			if et := req.GetEndTxn(); et != nil && et.InternalCommitTrigger.GetMergeTrigger() != nil {
				if atomic.AddInt64(&mergeTxnRetries, -1) >= 0 {
					return roachpb.NewError(
						roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "filter err"))
				}
			}
			if pt := req.GetPushTxn(); pt != nil {
				if atomic.AddInt64(&pushTxnRetries, -1) >= 0 {
					return roachpb.NewErrorf("injected failure")
				}
			}
			if g := req.GetGet(); g != nil && ba.ReadConsistency == roachpb.READ_UNCOMMITTED {
				if atomic.AddInt64(&meta2GetRetries, -1) >= 0 {
					return roachpb.NewErrorf("injected failure")
				}
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableReplicaGCQueue: true,
						TestingRequestFilter:  testingRequestFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)

	// Make store0 the leaseholder of the LHS and store2 the leaseholder of the
	// RHS. We'll be forcing store2's LHS to fall behind. This creates an
	// interesting scenario in which the leaseholder for the RHS has very
	// out-of-date information about the status of the merge.
	rangeDesc := store0.LookupReplica(roachpb.RKey("a")).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store0)
	if err != nil {
		t.Fatal(err)
	}
	tc.TransferRangeLeaseOrFatal(t, *rhsDesc, tc.Target(2))

	// After the LHS replica on store2 processes the split, block Raft traffic to
	// it by holding its raftMu, so that it isn't aware there's a merge in
	// progress.
	lhsRepl2, err := store2.GetReplica(lhsDesc.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		if !lhsRepl2.Desc().Equal(lhsDesc) {
			return errors.New("store2 has not processed split")
		}
		return nil
	})
	lhsRepl2.RaftLock()

	args := adminMergeArgs(lhsDesc.StartKey.AsRawKey())
	_, pErr := kv.SendWrapped(ctx, store0.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Immediately after the merge completes, send a request to the RHS which will
	// be handled by the leaseholder, on store2. This exercises a tricky scenario.
	// We've forced store2's LHS replica to fall behind, so it can't subsume
	// store2's RHS. store2's RHS is watching for the merge to complete, however,
	// and will notice that the merge has committed before the LHS does.
	getErr := make(chan error)
	go func() {
		_, pErr = kv.SendWrappedWith(ctx, store2.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, getArgs(rhsDesc.StartKey.AsRawKey()))
		getErr <- pErr.GoError()
	}()

	// Restore communication with store2. Give it the lease to force all commands
	// to be applied, including the merge trigger.
	lhsRepl2.RaftUnlock()
	tc.TransferRangeLeaseOrFatal(t, *lhsDesc, tc.Target(2))

	// We *must* see a RangeNotFound error from the get request we sent earlier
	// because we sent it after the merge completed. Anything else is a
	// consistency error (or a bug in the test).
	if err := <-getErr; !testutils.IsError(err, "was not found on s3") {
		t.Fatalf("expected RangeNotFound error from get after merge, but got %v", err)
	}
}

// TestStoreRangeMergeSlowWatcher verifies that the watcher goroutine for the
// RHS of a merge does not erroneously permit traffic after the merge commits,
// even if the watcher goroutine is so slow in noticing the merge that another
// merge occurs.
//
// This test is a more complicated version of TestStoreRangeMergeWatcher that
// exercises a rare but important edge case.
//
// The test creates three ranges, [a, b), [b, c), and [c, /Max). Hereafter these
// ranges will be referred to as A, B, and C, respectively. store0 holds the
// lease on A and C, while store1 holds the lease on B. The test will execute
// two merges such that first A subsumes B, then AB subsumes C. The idea is to
// inform store1 that the A <- B merge is in progress so that it locks B down,
// but then keep it in the dark about the status of the merge for long enough
// that the AB <- C merge commits.
//
// When store1's merge watcher goroutine looks up whether the A <- B merge
// commit occurred in meta2 with a Get(/Meta2/c) request, it won't find the
// descriptor for B, which would indicate that the merge aborted, nor the
// descriptor for AB, which would indicate that the merge committed. Instead it
// will find no descriptor at all, since the AB <- C merge has committed and the
// descriptor for the merged range ABC is stored at /Meta2/Max, not /Meta2/c.
func TestStoreRangeMergeSlowWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	aKey, bKey, cKey := roachpb.RKey("a"), roachpb.RKey("b"), roachpb.RKey("c")
	var store0, store1 *kvserver.Store

	// Force PushTxn requests generated by the watcher goroutine to wait on a
	// channel. This is how we control when store1's merge watcher goroutine hears
	// about the status of the A <- B merge.
	var delayPushCh <-chan struct{}
	var stopDelayingPush func() // idempotent
	{
		ch := make(chan struct{})
		delayPushCh = ch
		once := sync.Once{}
		stopDelayingPush = func() {
			once.Do(func() {
				close(ch)
			})
		}
	}

	testingRequestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		for _, req := range ba.Requests {
			// We can detect PushTxn requests generated by the watcher goroutine
			// because they use the minimum transaction priority. Note that we
			// only block the watcher goroutine on store1 so that we only interfere
			// with the first merge (A <- B) and not the later merge (AB <- C).
			if pt := req.GetPushTxn(); pt != nil && pt.PusherTxn.Priority == enginepb.MinTxnPriority &&
				ba.GatewayNodeID == store1.Ident.NodeID {
				<-delayPushCh
			}
			if et := req.GetEndTxn(); et != nil && !et.Commit && ba.Txn.Name == "merge" {
				// The merge transaction needed to restart for some reason. To avoid
				// deadlocking, we need to allow the watcher goroutine's PushTxn request
				// through so that it allows traffic on the range again. We'll try again
				// with the restarted merge transaction.
				stopDelayingPush()
			}
		}
		return nil
	}

	// Record whether we've seen a request to Get(/Meta2/c) that returned nil.
	// This verifies that we're actually testing what we claim to.
	var sawMeta2Req int64
	meta2CKey := keys.RangeMetaKey(cKey).AsRawKey()
	testingResponseFilter := func(
		ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
	) *roachpb.Error {
		for i, req := range ba.Requests {
			if g := req.GetGet(); g != nil && g.Key.Equal(meta2CKey) && br.Responses[i].GetGet().Value == nil {
				atomic.StoreInt64(&sawMeta2Req, 1)
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter:  testingRequestFilter,
						TestingResponseFilter: testingResponseFilter,
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())
	store0, store1 = tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 1)

	// Create and place the ranges as described in the comment on this test.
	rangeDesc := store0.LookupReplica(aKey).Desc()
	tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)
	testKeys := []roachpb.RKey{aKey, bKey, cKey}
	for _, key := range testKeys {
		splitArgs := adminSplitArgs(key.AsRawKey())
		if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), splitArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
	bRepl := store0.LookupReplica(bKey)
	tc.TransferRangeLeaseOrFatal(t, *bRepl.Desc(), tc.Target(1))

	// Warm the DistSender cache on each node. We'll be blocking requests to B
	// during the test, and we don't want requests headed for A or C to get routed
	// to B while its blocked because of a stale DistSender cache.
	for _, key := range testKeys {
		for _, server := range tc.Servers {
			if _, pErr := kv.SendWrapped(ctx, server.DistSender(), getArgs(key.AsRawKey())); pErr != nil {
				t.Fatal(pErr)
			}
		}
	}

	// Force the replica of A on store1 to fall behind so that it doesn't apply
	// any merge triggers. This makes the watcher goroutine responsible for
	// marking B as destroyed.
	aRepl1 := store1.LookupReplica(aKey)
	aRepl1.RaftLock()
	defer aRepl1.RaftUnlock()

	// Merge A <- B.
	mergeArgs := adminMergeArgs(aKey.AsRawKey())
	if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), mergeArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Immediately after the merge completes, send a request to B.
	getErr := make(chan error)
	go func() {
		_, pErr := kv.SendWrappedWith(ctx, store1.TestSender(), roachpb.Header{
			RangeID: bRepl.GetRangeID(),
		}, getArgs(bKey.AsRawKey()))
		getErr <- pErr.GoError()
	}()

	// Merge AB <- C.
	if _, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), mergeArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Synchronously ensure that the intent on meta2CKey has been cleaned up.
	// The merge committed, but the intent resolution happens asynchronously.
	_, pErr := kv.SendWrapped(ctx, tc.Servers[0].DistSender(), getArgs(meta2CKey))
	if pErr != nil {
		t.Fatal(pErr)
	}

	// With the meta2CKey intent cleaned up, allow store1's merge watcher
	// goroutine to proceed.
	stopDelayingPush()

	// We *must* see a RangeNotFound error from the get request we sent earlier
	// because we sent it after the merge completed. Anything else is a
	// consistency error (or a bug in the test).
	expErr := fmt.Sprintf("r%d was not found", bRepl.GetRangeID())
	if err := <-getErr; !testutils.IsError(err, expErr) {
		t.Fatalf("expected %q error from get after merge, but got %v", expErr, err)
	}

	if atomic.LoadInt64(&sawMeta2Req) != 1 {
		t.Fatalf("test did not generate expected meta2 get request/response")
	}
}

func TestStoreRangeMergeRaftSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We will be testing the SSTs written on store2's engine.
	var receivingEng, sendingEng storage.Engine
	// All of these variables will be populated later, after starting the cluster..
	var keyStart, keyA, keyB, keyC, keyD, keyEnd roachpb.Key
	rangeIds := make(map[string]roachpb.RangeID, 4)
	beforeSnapshotSSTIngestion := func(
		inSnap kvserver.IncomingSnapshot,
		snapType kvserver.SnapshotRequest_Type,
		sstNames []string,
	) error {
		// Only verify snapshots of type VIA_SNAPSHOT_QUEUE and on the range under
		// exercise (range 2). Note that the keys of range 2 aren't verified in this
		// functions. Unreplicated range-id local keys are not verified because
		// there are too many keys and the other replicated keys are verified later
		// on in the test. This function verifies that the subsumed replicas have
		// been handled properly.
		if snapType != kvserver.SnapshotRequest_VIA_SNAPSHOT_QUEUE ||
			inSnap.State.Desc.RangeID != rangeIds[string(keyA)] {
			return nil
		}

		// The seven to nine SSTs we are expecting to ingest are in the following order:
		// - Replicated range-id local keys of the range in the snapshot.
		// - Range-local keys of the range in the snapshot.
		// - Two SSTs for the lock table keys of the range in the snapshot.
		// - User keys of the range in the snapshot.
		// - Unreplicated range-id local keys of the range in the snapshot.
		// - SST to clear range-id local keys of the subsumed replica with
		//   RangeID 3.
		// - SST to clear range-id local keys of the subsumed replica with
		//   RangeID 4.
		// - SST to clear the user keys of the subsumed replicas.
		//
		// NOTE: There are no range-local keys or lock table keys, in [d, /Max) in
		// the store we're sending a snapshot to, so we aren't expecting SSTs to
		// clear those keys.
		expectedSSTCount := 9
		if len(sstNames) != expectedSSTCount {
			return errors.Errorf("expected to ingest %d SSTs, got %d SSTs",
				expectedSSTCount, len(sstNames))
		}

		// Only try to predict SSTs for:
		// - The user keys in the snapshot
		// - Clearing rhe range-id local keys of the subsumed replicas.
		// - Clearing the user keys of the subsumed replicas.
		// The snapshot SSTs that are excluded from this checking are the
		// replicated range-id, range-local keys, lock table keys in the snapshot,
		// and the unreplicated range-id local keys in the snapshot. The latter is
		// excluded since the state of the Raft log can be non-deterministic with
		// extra entries being appended to the sender's log after the snapshot has
		// already been sent.
		var sstNamesSubset []string
		// The SST with the user keys in the snapshot.
		sstNamesSubset = append(sstNamesSubset, sstNames[4])
		// Remaining ones from the predict list above.
		sstNamesSubset = append(sstNamesSubset, sstNames[6:]...)

		// Construct the expected SSTs and ensure that they are byte-by-byte
		// equal. This verification ensures that the SSTs have the same
		// tombstones and range deletion tombstones.
		var expectedSSTs [][]byte

		// Construct SSTs for the the first 4 bullets as numbered above, but only
		// ultimately keep the last one.
		keyRanges := rditer.MakeReplicatedKeyRanges(inSnap.State.Desc)
		it := rditer.NewReplicaEngineDataIterator(inSnap.State.Desc, sendingEng, true /* replicatedOnly */)
		defer it.Close()
		// Write a range deletion tombstone to each of the SSTs then put in the
		// kv entries from the sender of the snapshot.
		for _, r := range keyRanges {
			sstFile := &storage.MemFile{}
			sst := storage.MakeIngestionSSTWriter(sstFile)
			if err := sst.ClearRawRange(r.Start.Key, r.End.Key); err != nil {
				return err
			}

			// Keep adding kv data to the SST until the key exceeds the
			// bounds of the range, then proceed to the next range.
			for ; ; it.Next() {
				valid, err := it.Valid()
				if err != nil {
					return err
				}
				if !valid || r.End.Key.Compare(it.UnsafeKey().Key) <= 0 {
					if err := sst.Finish(); err != nil {
						return err
					}
					sst.Close()
					expectedSSTs = append(expectedSSTs, sstFile.Data())
					break
				}
				if err := sst.PutEngineKey(it.UnsafeKey(), it.Value()); err != nil {
					return err
				}
			}
		}
		if len(expectedSSTs) != 5 {
			return errors.Errorf("len of expectedSSTs should expected to be %d, but got %d",
				5, len(expectedSSTs))
		}
		// Keep the last one which contains the user keys.
		expectedSSTs = expectedSSTs[len(expectedSSTs)-1:]

		// Construct SSTs for the range-id local keys of the subsumed replicas.
		// with RangeIDs 3 and 4.
		for _, k := range []roachpb.Key{keyB, keyC} {
			rangeID := rangeIds[string(k)]
			sstFile := &storage.MemFile{}
			sst := storage.MakeIngestionSSTWriter(sstFile)
			defer sst.Close()
			r := rditer.MakeRangeIDLocalKeyRange(rangeID, false /* replicatedOnly */)
			if err := sst.ClearRawRange(r.Start.Key, r.End.Key); err != nil {
				return err
			}
			tombstoneKey := keys.RangeTombstoneKey(rangeID)
			tombstoneValue := &roachpb.RangeTombstone{NextReplicaID: math.MaxInt32}
			if err := storage.MVCCBlindPutProto(context.Background(), &sst, nil, tombstoneKey, hlc.Timestamp{}, tombstoneValue, nil); err != nil {
				return err
			}
			err := sst.Finish()
			if err != nil {
				return err
			}
			expectedSSTs = append(expectedSSTs, sstFile.Data())
		}

		// Construct an SST for the user key range of the subsumed replicas.
		sstFile := &storage.MemFile{}
		sst := storage.MakeIngestionSSTWriter(sstFile)
		defer sst.Close()
		desc := roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(keyD),
			EndKey:   roachpb.RKey(keyEnd),
		}
		r := rditer.MakeUserKeyRange(&desc)
		if err := storage.ClearRangeWithHeuristic(receivingEng, &sst, r.Start.Key, r.End.Key); err != nil {
			return err
		}
		err := sst.Finish()
		if err != nil {
			return err
		}
		expectedSSTs = append(expectedSSTs, sstFile.Data())

		var mismatchedSstsIdx []int
		// Iterate over all the tested SSTs and check that they're byte-by-byte equal.
		for i := range sstNamesSubset {
			actualSST, err := receivingEng.ReadFile(sstNamesSubset[i])
			if err != nil {
				return err
			}
			if !bytes.Equal(actualSST, expectedSSTs[i]) {
				mismatchedSstsIdx = append(mismatchedSstsIdx, i)
			}
		}
		if len(mismatchedSstsIdx) != 0 {
			return errors.Errorf("SST indices %v don't match", mismatchedSstsIdx)
		}
		return nil
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						BeforeSnapshotSSTIngestion: beforeSnapshotSSTIngestion,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store0, store2 := tc.GetFirstStoreFromServer(t, 0), tc.GetFirstStoreFromServer(t, 2)
	sendingEng = store0.Engine()
	receivingEng = store2.Engine()
	distSender := tc.Servers[0].DistSender()

	// This test works across 5 ranges in total. We start with a scratch range(1)
	// [Start, End). We then split this range as follows:
	// range(1) = [Start, a)
	// range(2) = [a, b)
	// range(3) = [b, c)
	// range(4) = [c, End).
	keyStart = tc.ScratchRange(t)
	repl := store0.LookupReplica(roachpb.RKey(keyStart))
	keyEnd = repl.Desc().EndKey.AsRawKey()
	keyA = keyStart.Next().Next()
	keyB = keyA.Next().Next()
	keyC = keyB.Next().Next()
	keyD = keyC.Next().Next()
	rangeIds[string(keyStart)] = repl.RangeID

	// Create three fully-caught-up, adjacent ranges on all three stores.
	tc.AddVotersOrFatal(t, keyStart, tc.Targets(1, 2)...)
	for _, key := range []roachpb.Key{keyA, keyB, keyC} {
		_, rhsDesc := tc.SplitRangeOrFatal(t, key)
		rangeIds[string(key)] = rhsDesc.RangeID
		if _, pErr := kv.SendWrapped(ctx, distSender, incrementArgs(key, 1)); pErr != nil {
			t.Fatal(pErr)
		}
		tc.WaitForValues(t, key, []int64{1, 1, 1})
	}

	// Put some keys in [d, /Max) so the subsumed replica of [c, /Max) with range
	// ID 4 has tombstones. We will clear uncontained key range of subsumed
	// replicas, so when we are receiving a snapshot for [a, d), we expect to
	// clear the keys in [d, /Max).
	key := keyD
	for i := 0; i < 10; i++ {
		key = key.Next()
		if _, pErr := kv.SendWrapped(ctx, distSender, incrementArgs(key, 1)); pErr != nil {
			t.Fatal(pErr)
		}
		tc.WaitForValues(t, key, []int64{1, 1, 1})
	}

	aRepl0 := store0.LookupReplica(roachpb.RKey(keyA))

	// Start dropping all Raft traffic to the first range on store2.
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, &unreliableRaftHandler{
		rangeID:            aRepl0.RangeID,
		RaftMessageHandler: store2,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropReq: func(request *kvserver.RaftMessageRequest) bool {
				return true
			},
		},
	})

	// Merge [a, b) into [b, c), then [a, c) into [c, /Max).
	for i := 0; i < 2; i++ {
		if _, pErr := kv.SendWrapped(ctx, distSender, adminMergeArgs(keyA)); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Split [a, /Max) into [a, d) and [d, /Max). This means the Raft snapshot
	// will span both a merge and a split.
	if _, pErr := kv.SendWrapped(ctx, distSender, adminSplitArgs(keyD)); pErr != nil {
		t.Fatal(pErr)
	}

	// Truncate the logs of the LHS.
	index := func() uint64 {
		repl := store0.LookupReplica(roachpb.RKey(keyA))
		index, err := repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		truncArgs := &roachpb.TruncateLogRequest{
			RequestHeader: roachpb.RequestHeader{Key: keyA},
			Index:         index,
			RangeID:       repl.RangeID,
		}
		if _, err := kv.SendWrapped(ctx, distSender, truncArgs); err != nil {
			t.Fatal(err)
		}
		return index
	}()

	beforeRaftSnaps := store2.Metrics().RangeSnapshotsAppliedByVoters.Count()

	// Restore Raft traffic to the LHS on store2.
	log.Infof(ctx, "restored traffic to store 2")
	tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, &unreliableRaftHandler{
		rangeID:            aRepl0.RangeID,
		RaftMessageHandler: store2,
		unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
			dropReq: func(req *kvserver.RaftMessageRequest) bool {
				// Make sure that even going forward no MsgApp for what we just
				// truncated can make it through. The Raft transport is asynchronous
				// so this is necessary to make the test pass reliably - otherwise
				// the follower on store2 may catch up without needing a snapshot,
				// tripping up the test.
				//
				// NB: the Index on the message is the log index that _precedes_ any of the
				// entries in the MsgApp, so filter where msg.Index < index, not <= index.
				return req.Message.Type == raftpb.MsgApp && req.Message.Index < index
			},
			// Don't drop heartbeats or responses.
			dropHB:   func(*kvserver.RaftHeartbeat) bool { return false },
			dropResp: func(*kvserver.RaftMessageResponse) bool { return false },
		},
	})

	// Wait for all replicas to catch up to the same point. Because we truncated
	// the log while store2 was unavailable, this will require a Raft snapshot.
	testutils.SucceedsSoon(t, func() error {
		afterRaftSnaps := store2.Metrics().RangeSnapshotsAppliedByVoters.Count()
		if afterRaftSnaps <= beforeRaftSnaps {
			return errors.New("expected store2 to apply at least 1 additional raft snapshot")
		}
		// We only look at the range of keys the test has been manipulating.
		getKeySet := func(engine storage.Engine) map[string]struct{} {
			kvs, err := storage.Scan(engine, keyStart, keyEnd, 0 /* max */)
			if err != nil {
				t.Fatal(err)
			}
			out := map[string]struct{}{}
			for _, kv := range kvs {
				out[string(kv.Key.Key)] = struct{}{}
			}
			return out
		}

		// Verify that the sets of keys in store0 and store2 are identical.
		storeKeys0 := getKeySet(store0.Engine())
		storeKeys2 := getKeySet(store2.Engine())
		for k := range storeKeys0 {
			if _, ok := storeKeys2[k]; !ok {
				return fmt.Errorf("store2 missing key %s", roachpb.Key(k))
			}
		}
		for k := range storeKeys2 {
			if _, ok := storeKeys0[k]; !ok {
				return fmt.Errorf("store2 has extra key %s", roachpb.Key(k))
			}
		}
		return nil
	})
}

// TestStoreRangeMergeDuringShutdown verifies that a shutdown of a store
// containing the RHS of a merge can occur cleanly. This previously triggered
// a fatal error (#27552).
func TestStoreRangeMergeDuringShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Install a filter that triggers a shutdown when stop is non-zero and the
	// rhsDesc requests a new lease.
	var s *server.TestServer
	var state struct {
		syncutil.Mutex
		rhsDesc        *roachpb.RangeDescriptor
		stop, stopping bool
	}
	testingPostApplyFilter := func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		state.Lock()
		if state.stop && !state.stopping && args.RangeID == state.rhsDesc.RangeID && args.IsLeaseRequest {
			// Shut down the store. The lease acquisition will notice that a merge is
			// in progress and attempt to run a task to watch for its completion.
			// Shutting down the store before running leasePostApply will prevent that
			// task from launching. This error path would previously fatal a node
			// incorrectly (#27552).
			state.stopping = true
			state.Unlock()
			go s.Stopper().Stop(ctx)
			// Sleep to give the shutdown time to propagate. The test appeared to work
			// without this sleep, but best to be somewhat robust to different
			// goroutine schedules.
			time.Sleep(10 * time.Millisecond)
		} else {
			state.Unlock()
		}
		return 0, nil
	}

	manualClock := hlc.NewHybridManualClock()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue:      true,
				DisableSplitQueue:      true,
				DisableReplicateQueue:  true,
				TestingPostApplyFilter: testingPostApplyFilter,
			},
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
		},
	})
	s = serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key, err := s.ScratchRangeWithExpirationLease()
	require.NoError(t, err)
	rhsDesc := store.LookupReplica(roachpb.RKey(key)).Desc()
	state.Lock()
	state.rhsDesc = rhsDesc
	state.Unlock()

	// Simulate a merge transaction by launching a transaction that lays down
	// intents on the two copies of the RHS range descriptor.
	txn := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
	if err := txn.Del(ctx, keys.RangeDescriptorKey(rhsDesc.StartKey)); err != nil {
		t.Fatal(err)
	}
	if err := txn.Del(ctx, keys.RangeMetaKey(rhsDesc.StartKey)); err != nil {
		t.Fatal(err)
	}

	// Indicate to the store filter installed above that the next lease
	// acquisition for the RHS should trigger a shutdown.
	state.Lock()
	state.stop = true
	state.Unlock()

	// Expire all leases.
	manualClock.Increment(store.GetStoreConfig().LeaseExpiration())

	// Send a dummy get request on the RHS to force a lease acquisition. We expect
	// this to fail, as quiescing stores cannot acquire leases.
	err = s.Stopper().RunTaskWithErr(ctx, "test-get-rhs-key", func(ctx context.Context) error {
		_, err := store.DB().Get(ctx, key.Next())
		return err
	})
	if exp := "not lease holder"; !testutils.IsError(err, exp) {
		t.Fatalf("expected %q error, but got %v", exp, err)
	}
}

func verifyMerged(t *testing.T, store *kvserver.Store, lhsStartKey, rhsStartKey roachpb.RKey) {
	t.Helper()
	repl := store.LookupReplica(rhsStartKey)
	if repl == nil {
		t.Fatal("replica doesn't exist")
	}
	if !repl.Desc().StartKey.Equal(lhsStartKey) {
		t.Fatalf("ranges unexpectedly unmerged expected startKey %s, but got %s", lhsStartKey, repl.Desc().StartKey)
	}
}

func verifyUnmerged(t *testing.T, store *kvserver.Store, lhsStartKey, rhsStartKey roachpb.RKey) {
	t.Helper()
	repl := store.LookupReplica(rhsStartKey)
	if repl == nil {
		t.Fatal("replica doesn't exist")
	}
	if repl.Desc().StartKey.Equal(lhsStartKey) {
		t.Fatalf("ranges unexpectedly merged")
	}
}

func TestMergeQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	zoneConfig := zonepb.DefaultZoneConfig()
	rangeMinBytes := int64(1 << 10) // 1KB
	zoneConfig.RangeMinBytes = &rangeMinBytes
	settings := cluster.MakeTestingClusterSettings()
	sv := &settings.SV
	kvserver.MergeQueueInterval.Override(ctx, sv, 0) // process greedily

	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: settings,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource:               manualClock.UnixNano,
						DefaultZoneConfigOverride: &zoneConfig,
					},
					Store: &kvserver.StoreTestingKnobs{
						DisableScanner: true,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)
	// The cluster with manual replication disables the merge queue,
	// so we need to re-enable.
	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled = true`)
	require.NoError(t, err)
	store.SetMergeQueueActive(true)

	split := func(t *testing.T, key roachpb.Key, expirationTime hlc.Timestamp) {
		t.Helper()
		args := adminSplitArgs(key)
		args.ExpirationTime = expirationTime
		if _, pErr := kv.SendWrapped(ctx, store.DB().NonTransactionalSender(), args); pErr != nil {
			t.Fatal(pErr)
		}
	}

	clearRange := func(t *testing.T, start, end roachpb.RKey) {
		if _, pErr := kv.SendWrapped(ctx, store.DB().NonTransactionalSender(), &roachpb.ClearRangeRequest{
			RequestHeader: roachpb.RequestHeader{Key: start.AsRawKey(), EndKey: end.AsRawKey()},
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	rng, _ := randutil.NewPseudoRand()
	randBytes := randutil.RandBytes(rng, int(*zoneConfig.RangeMinBytes))

	lhsStartKey := roachpb.RKey(tc.ScratchRange(t))
	rhsStartKey := lhsStartKey.Next().Next()
	rhsEndKey := rhsStartKey.Next().Next()

	for _, k := range []roachpb.RKey{lhsStartKey, rhsStartKey, rhsEndKey} {
		split(t, k.AsRawKey(), hlc.Timestamp{} /* expirationTime */)
	}
	lhs := func() *kvserver.Replica { return store.LookupReplica(lhsStartKey) }
	rhs := func() *kvserver.Replica { return store.LookupReplica(rhsStartKey) }

	// setThresholds simulates a zone config update that updates the ranges'
	// minimum and maximum sizes.
	setZones := func(t *testing.T, zone zonepb.ZoneConfig) {
		t.Helper()
		if l := lhs(); l == nil {
			t.Fatal("left-hand side range not found")
		} else {
			l.SetZoneConfig(&zone)
		}
		if r := rhs(); r == nil {
			t.Fatal("right-hand side range not found")
		} else {
			r.SetZoneConfig(&zone)
		}
	}

	reset := func(t *testing.T) {
		t.Helper()
		clearRange(t, lhsStartKey, rhsEndKey)
		for _, k := range []roachpb.RKey{lhsStartKey, rhsStartKey} {
			if err := store.DB().Put(ctx, k, randBytes); err != nil {
				t.Fatal(err)
			}
		}
		setZones(t, zoneConfig)
		// Disable load-based splitting, so that the absence of sufficient QPS
		// measurements do not prevent ranges from merging. Certain subtests
		// re-enable the functionality.
		kvserver.SplitByLoadEnabled.Override(ctx, sv, false)
		store.MustForceMergeScanAndProcess() // drain any merges that might already be queued
		split(t, rhsStartKey.AsRawKey(), hlc.Timestamp{} /* expirationTime */)
	}

	t.Run("sanity", func(t *testing.T) {
		// Check that ranges are not trivially merged after reset.
		reset(t)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
		reset(t)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("both-empty", func(t *testing.T) {
		reset(t)
		clearRange(t, lhsStartKey, rhsEndKey)
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("lhs-undersize", func(t *testing.T) {
		reset(t)
		zone := protoutil.Clone(&zoneConfig).(*zonepb.ZoneConfig)
		*zone.RangeMinBytes *= 2
		lhs().SetZoneConfig(zone)
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("combined-size-threshold", func(t *testing.T) {
		reset(t)

		// The ranges are individually beneath the minimum size threshold, but
		// together they'll exceed the maximum size threshold.
		zone := protoutil.Clone(&zoneConfig).(*zonepb.ZoneConfig)
		zone.RangeMinBytes = proto.Int64(rhs().GetMVCCStats().Total() + 1)
		zone.RangeMaxBytes = proto.Int64(lhs().GetMVCCStats().Total() + rhs().GetMVCCStats().Total() - 1)
		setZones(t, *zone)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Once the maximum size threshold is increased, the merge can occur.
		zone.RangeMaxBytes = proto.Int64(*zone.RangeMaxBytes + 1)
		setZones(t, *zone)
		l := lhs().RangeID
		r := rhs().RangeID
		log.Infof(ctx, "Left=%s, Right=%s", l, r)
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("non-collocated", func(t *testing.T) {
		reset(t)
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
		tc.AddVotersOrFatal(t, rhs().Desc().StartKey.AsRawKey(), tc.Target(1))
		tc.TransferRangeLeaseOrFatal(t, *rhs().Desc(), tc.Target(1))
		// NB: We're running on a 2 node `TestCluster` so we can count on the
		// replica being removed to learn about the removal synchronously.
		require.Equal(t, tc.NumServers(), 2)
		tc.RemoveVotersOrFatal(t, rhs().Desc().StartKey.AsRawKey(), tc.Target(0))

		clearRange(t, lhsStartKey, rhsEndKey)
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("load-based-merging", func(t *testing.T) {
		const splitByLoadQPS = 10
		const mergeByLoadQPS = splitByLoadQPS / 2 // see conservativeLoadBasedSplitThreshold
		const splitByLoadMergeDelay = 500 * time.Millisecond

		resetForLoadBasedSubtest := func(t *testing.T) {
			reset(t)

			// Enable load-based splitting for these subtests, which also instructs
			// the mergeQueue to consider load when making range merge decisions. When
			// load is a consideration, the mergeQueue is fairly conservative. In an
			// effort to avoid thrashing and to avoid overreacting to temporary
			// fluctuations in load, the mergeQueue will only consider a merge when
			// the combined load across the RHS and LHS ranges is below half the
			// threshold required to split a range due to load. Furthermore, to ensure
			// that transient drops in load do not trigger range merges, the
			// mergeQueue will only consider a merge when it deems the maximum qps
			// measurement from both sides to be sufficiently stable and reliable,
			// meaning that it was a maximum measurement over some extended period of
			// time.
			kvserver.SplitByLoadEnabled.Override(ctx, sv, true)
			kvserver.SplitByLoadQPSThreshold.Override(ctx, sv, splitByLoadQPS)

			// Drop the load-based splitting merge delay setting, which also dictates
			// the duration that a leaseholder must measure QPS before considering its
			// measurements to be reliable enough to base range merging decisions on.
			kvserverbase.SplitByLoadMergeDelay.Override(ctx, sv, splitByLoadMergeDelay)

			// Reset both range's load-based splitters, so that QPS measurements do
			// not leak over between subtests. Then, bump the manual clock so that
			// both range's load-based splitters consider their measurements to be
			// reliable.
			lhs().LoadBasedSplitter().Reset(tc.Servers[0].Clock().PhysicalTime())
			rhs().LoadBasedSplitter().Reset(tc.Servers[1].Clock().PhysicalTime())
			manualClock.Increment(splitByLoadMergeDelay.Nanoseconds())
		}

		t.Run("unreliable-lhs-qps", func(t *testing.T) {
			resetForLoadBasedSubtest(t)

			lhs().LoadBasedSplitter().Reset(tc.Servers[0].Clock().PhysicalTime())

			clearRange(t, lhsStartKey, rhsEndKey)
			store.MustForceMergeScanAndProcess()
			verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
		})

		t.Run("unreliable-rhs-qps", func(t *testing.T) {
			resetForLoadBasedSubtest(t)

			rhs().LoadBasedSplitter().Reset(tc.Servers[1].Clock().PhysicalTime())

			clearRange(t, lhsStartKey, rhsEndKey)
			store.MustForceMergeScanAndProcess()
			verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
		})

		t.Run("combined-qps-above-threshold", func(t *testing.T) {
			resetForLoadBasedSubtest(t)

			moreThanHalfQPS := mergeByLoadQPS/2 + 1
			rhs().LoadBasedSplitter().RecordMax(tc.Servers[0].Clock().PhysicalTime(), float64(moreThanHalfQPS))
			lhs().LoadBasedSplitter().RecordMax(tc.Servers[1].Clock().PhysicalTime(), float64(moreThanHalfQPS))

			clearRange(t, lhsStartKey, rhsEndKey)
			store.MustForceMergeScanAndProcess()
			verifyUnmerged(t, store, lhsStartKey, rhsStartKey)
		})

		t.Run("combined-qps-below-threshold", func(t *testing.T) {
			resetForLoadBasedSubtest(t)

			manualClock.Increment(splitByLoadMergeDelay.Nanoseconds())
			lessThanHalfQPS := mergeByLoadQPS/2 - 1
			rhs().LoadBasedSplitter().RecordMax(tc.Servers[0].Clock().PhysicalTime(), float64(lessThanHalfQPS))
			lhs().LoadBasedSplitter().RecordMax(tc.Servers[1].Clock().PhysicalTime(), float64(lessThanHalfQPS))

			clearRange(t, lhsStartKey, rhsEndKey)
			store.MustForceMergeScanAndProcess()
			verifyMerged(t, store, lhsStartKey, rhsStartKey)
		})
	})

	t.Run("sticky-bit", func(t *testing.T) {
		reset(t)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Perform manual merge and verify that no merge occurred.
		split(t, rhsStartKey.AsRawKey(), hlc.MaxTimestamp /* expirationTime */)
		clearRange(t, lhsStartKey, rhsEndKey)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Delete sticky bit and verify that merge occurs.
		unsplitArgs := &roachpb.AdminUnsplitRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: rhsStartKey.AsRawKey(),
			},
		}
		if _, err := kv.SendWrapped(ctx, store.DB().NonTransactionalSender(), unsplitArgs); err != nil {
			t.Fatal(err)
		}
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})

	t.Run("sticky-bit-expiration", func(t *testing.T) {
		skip.WithIssue(t, 66942, "flakey test")
		manualSplitTTL := time.Millisecond * 200
		reset(t)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Perform manual merge and verify that no merge occurred.
		split(t, rhsStartKey.AsRawKey(), tc.Servers[0].Clock().Now().Add(manualSplitTTL.Nanoseconds(), 0) /* expirationTime */)
		clearRange(t, lhsStartKey, rhsEndKey)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Sticky bit is not expired yet.
		manualClock.Increment(manualSplitTTL.Nanoseconds() / 2)
		store.MustForceMergeScanAndProcess()
		verifyUnmerged(t, store, lhsStartKey, rhsStartKey)

		// Sticky bit is expired.
		manualClock.Increment(manualSplitTTL.Nanoseconds())
		store.MustForceMergeScanAndProcess()
		verifyMerged(t, store, lhsStartKey, rhsStartKey)
	})
}

func TestMergeQueueSeesNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type test struct {
		name                                                   string
		leftVoters, rightVoters, leftNonVoters, rightNonVoters []int
	}

	// NB: The test setup code places a single voter replica on (n1,s1) for both
	// left and right range, which we remove after setting the test up.
	tests := []test{
		{
			name:           "collocated-per-type",
			leftVoters:     []int{2, 3, 4},
			rightVoters:    []int{2, 3, 4},
			leftNonVoters:  []int{1},
			rightNonVoters: []int{1},
		},
		{
			name:           "collocated-overall",
			leftVoters:     []int{3, 4},
			rightVoters:    []int{1, 2},
			leftNonVoters:  []int{1, 2},
			rightNonVoters: []int{3, 4},
		},
		{
			name:           "collocated-voters-only",
			leftVoters:     []int{3, 4},
			rightVoters:    []int{3, 4},
			leftNonVoters:  []int{2},
			rightNonVoters: []int{1},
		},
		{
			name:           "collocated-non-voters-only",
			leftVoters:     []int{3},
			rightVoters:    []int{4},
			leftNonVoters:  []int{1, 2},
			rightNonVoters: []int{1, 2},
		},
		{
			name:           "not-collocated",
			leftVoters:     []int{3},
			rightVoters:    []int{4},
			leftNonVoters:  []int{2},
			rightNonVoters: []int{1},
		},
		{
			name:           "partially-collocated-voters-only",
			leftVoters:     []int{2, 3},
			rightVoters:    []int{1, 4},
			leftNonVoters:  []int{1},
			rightNonVoters: []int{2},
		},
		{
			name:           "partially-collocated-non-voters-only",
			leftVoters:     []int{4},
			rightVoters:    []int{4},
			leftNonVoters:  []int{1, 3},
			rightNonVoters: []int{1, 2},
		},
		{
			name:           "partially-collocated",
			leftVoters:     []int{2},
			rightVoters:    []int{4},
			leftNonVoters:  []int{1, 3},
			rightNonVoters: []int{1, 2},
		},
		{
			name:           "collocated-rhs-being-reconfigured-1",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{1, 2, 3, 4, 5, 6},
			leftNonVoters:  []int{4, 5, 6},
			rightNonVoters: []int{},
		},
		{
			name:           "collocated-rhs-being-reconfigured-2",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{1, 2, 3, 4},
			leftNonVoters:  []int{4, 5, 6},
			rightNonVoters: []int{},
		},
		{
			name:           "collocated-rhs-being-reconfigured-3",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{1},
			leftNonVoters:  []int{4, 5, 6},
			rightNonVoters: []int{2, 3, 4, 5, 6},
		},
		{
			name:           "non-collocated-rhs-being-reconfigured",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{5},
			leftNonVoters:  []int{4, 6},
			rightNonVoters: []int{},
		},
		{
			name:           "partially-collocated-rhs-being-downreplicated",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{1, 2, 3, 4, 5, 6},
			leftNonVoters:  []int{4, 5},
			rightNonVoters: []int{},
		},
		{
			name:           "partially-collocated-rhs-being-upreplicated",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{1},
			leftNonVoters:  []int{4, 5, 6},
			rightNonVoters: []int{},
		},
		{
			// This is a subtest that should trigger at least 3 voter<->non-voter
			// swaps.
			name:           "lhs-voters-collocated-with-rhs-non-voters",
			leftVoters:     []int{1, 2, 3},
			rightVoters:    []int{4},
			leftNonVoters:  []int{},
			rightNonVoters: []int{1, 2, 3},
		},
	}

	var clusterArgs = base.TestClusterArgs{
		// We dont want the replicate queue mucking with our test, so disable it.
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable load-based splitting, so that the absence of sufficient QPS
					// measurements do not prevent ranges from merging.
					DisableLoadBasedSplitting: true,
				},
			},
		},
	}
	ctx := context.Background()

	dbName := "testdb"
	numNodes := 7
	for _, subtest := range tests {
		t.Run(subtest.name, func(t *testing.T) {
			tc, _ := setupTestClusterWithDummyRange(t, clusterArgs, dbName, "kv", numNodes)
			defer tc.Stopper().Stop(ctx)
			// We're controlling merge queue operation via
			// `store.SetMergeQueueActive`, so enable the cluster setting here.
			_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled=true`)
			require.NoError(t, err)

			store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(1)
			require.Nil(t, err)
			// We're going to split the dummy range created above with an empty
			// expiration time. Disable the merge queue before splitting so that the
			// split ranges aren't immediately merged.
			store.SetMergeQueueActive(false)
			leftDesc, rightDesc := splitDummyRangeInTestCluster(
				t, tc, dbName, "kv" /* tableName */, hlc.Timestamp{} /* splitExpirationTime */)

			leftRepls := leftDesc.Replicas().Descriptors()
			rightRepls := rightDesc.Replicas().Descriptors()

			require.Equal(t, 1, len(leftRepls))
			require.Equal(t, 1, len(rightRepls))

			for i, id := range subtest.leftVoters {
				tc.AddVotersOrFatal(t, leftDesc.StartKey.AsRawKey(), tc.Target(id))
				if i == 0 {
					// Transfer range lease away from n1,s1 to the first voting replica we
					// add. Otherwise we will fail when trying to remove the voting
					// replica from n1,s1 below.
					require.NoError(t, tc.TransferRangeLease(leftDesc, tc.Target(id)))
					store, err = tc.Server(id).GetStores().(*kvserver.Stores).GetStore(roachpb.StoreID(id + 1))
					require.NoError(t, err)
				}
			}
			for i, id := range subtest.rightVoters {
				tc.AddVotersOrFatal(t, rightDesc.StartKey.AsRawKey(), tc.Target(id))
				if i == 0 {
					// Transfer range lease away from n1,s1 to the first voting replica we
					// add. Otherwise we will fail when trying to remove the voting
					// replica from n1,s1 below.
					require.NoError(t, tc.TransferRangeLease(rightDesc, tc.Target(id)))
				}
			}
			for _, id := range subtest.leftNonVoters {
				tc.AddNonVotersOrFatal(t, leftDesc.StartKey.AsRawKey(), tc.Target(id))
			}
			for _, id := range subtest.rightNonVoters {
				tc.AddNonVotersOrFatal(t, rightDesc.StartKey.AsRawKey(), tc.Target(id))
			}

			// Remove the voting replicas on n1,s1 that we started off with.
			tc.RemoveVotersOrFatal(t, leftDesc.StartKey.AsRawKey(), tc.Target(0))
			leftDesc = tc.LookupRangeOrFatal(t, leftDesc.StartKey.AsRawKey())
			tc.RemoveVotersOrFatal(t, rightDesc.StartKey.AsRawKey(), tc.Target(0))
			rightDesc = tc.LookupRangeOrFatal(t, rightDesc.StartKey.AsRawKey())

			store.SetMergeQueueActive(true)
			store.MustForceMergeScanAndProcess()
			verifyMerged(t, store, leftDesc.StartKey, rightDesc.StartKey)
		})
	}
}

// TestMergeQueueWithSlowNonVoterSnaps aims to check that non-voting replicas
// are initially upreplicated through a synchronously-sent snapshot inside of
// `AdminChangeReplicas`, like voting replicas are. Otherwise, range merges
// could be allowed to proceed with subsuming the right-hand side range while it
// still has uninitialized non-voters.
//
// Regression test for https://github.com/cockroachdb/cockroach/issues/63199.
func TestMergeQueueWithSlowNonVoterSnaps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t, "this test sleeps for a few seconds")

	ctx := context.Background()
	var delaySnapshotTrap atomic.Value
	var clusterArgs = base.TestClusterArgs{
		// We dont want the replicate queue mucking with our test, so disable it.
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable load-based splitting, so that the absence of sufficient QPS
					// measurements do not prevent ranges from merging.
					DisableLoadBasedSplitting: true,
				},
			},
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			1: {
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						ReceiveSnapshot: func(header *kvserver.SnapshotRequest_Header) error {
							val := delaySnapshotTrap.Load()
							if val != nil {
								fn := val.(func() error)
								return fn()
							}
							return nil
						},
						// See above.
						DisableLoadBasedSplitting: true,
					},
				},
			},
		},
	}

	dbName := "testdb"
	tableName := "kv"
	numNodes := 3
	tc, _ := setupTestClusterWithDummyRange(t, clusterArgs, dbName, tableName, numNodes)
	defer tc.Stopper().Stop(ctx)
	// We're controlling merge queue operation via
	// `store.SetMergeQueueActive`, so enable the cluster setting here.
	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled=true`)
	require.NoError(t, err)

	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(1)
	require.Nil(t, err)
	// We're going to split the dummy range created above with an empty
	// expiration time. Disable the merge queue before splitting so that the
	// split ranges aren't immediately merged.
	store.SetMergeQueueActive(false)
	leftDesc, rightDesc := splitDummyRangeInTestCluster(
		t, tc, dbName, tableName, hlc.Timestamp{}, /* splitExpirationTime */
	)
	require.Equal(t, 1, len(leftDesc.Replicas().Descriptors()))
	require.Equal(t, 1, len(rightDesc.Replicas().Descriptors()))

	// Add non-voters for the LHS and RHS on servers 1 and 2 respectively so that
	// the merge queue logic has to explicitly relocate the RHS non-voter to
	// server 1, in order to align replica sets to proceed with the merge.
	tc.AddNonVotersOrFatal(t, leftDesc.StartKey.AsRawKey(), tc.Target(1))
	tc.AddNonVotersOrFatal(t, rightDesc.StartKey.AsRawKey(), tc.Target(2))

	delaySnapshotTrap.Store(func() error {
		time.Sleep(5 * time.Second)
		return nil
	})
	store.SetMergeQueueActive(true)
	store.MustForceMergeScanAndProcess()
	verifyMerged(t, store, leftDesc.StartKey, rightDesc.StartKey)
}

func TestInvalidSubsumeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	// A Subsume request that succeeds when it shouldn't will wedge a
	// store because it waits for a merge that is not actually in progress. Set a
	// short timeout to limit the damage.
	ctx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
	defer cancel()

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	getSnapArgs := roachpb.SubsumeRequest{
		RequestHeader: roachpb.RequestHeader{Key: rhsDesc.StartKey.AsRawKey()},
		LeftDesc:      *lhsDesc,
		RightDesc:     *rhsDesc,
	}

	// Subsume with an incorrect RightDesc should fail.
	{
		badRHSDesc := *rhsDesc
		badRHSDesc.EndKey = badRHSDesc.EndKey.Next()
		badArgs := getSnapArgs
		badArgs.RightDesc = badRHSDesc
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, &badArgs)
		if exp := "RHS range bounds do not match"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %v", exp, pErr)
		}
	}

	// Subsume from a non-neighboring LHS should fail.
	{
		badArgs := getSnapArgs
		badArgs.LeftDesc.EndKey = badArgs.LeftDesc.EndKey.Next()
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, &badArgs)
		if exp := "ranges are not adjacent"; !testutils.IsPError(pErr, exp) {
			t.Fatalf("expected %q error, but got %v", exp, pErr)
		}
	}

	// Subsume without an intent on the local range descriptor should fail.
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: rhsDesc.RangeID,
	}, &getSnapArgs)
	if exp := "range missing intent on its local descriptor"; !testutils.IsPError(pErr, exp) {
		t.Fatalf("expected %q error, but got %v", exp, pErr)
	}

	// Subsume when a non-deletion intent is present on the
	// local range descriptor should fail.
	err = store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.Put(ctx, keys.RangeDescriptorKey(rhsDesc.StartKey), "garbage"); err != nil {
			return err
		}
		// NB: Subsume intentionally takes place outside of the txn so
		// that it sees an intent rather than the value the txn just wrote.
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
			RangeID: rhsDesc.RangeID,
		}, &getSnapArgs)
		if exp := "non-deletion intent on local range descriptor"; !testutils.IsPError(pErr, exp) {
			return fmt.Errorf("expected %q error, but got %v", exp, pErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func sendWithTxn(
	store *kvserver.Store,
	desc *roachpb.RangeDescriptor,
	ts hlc.Timestamp,
	maxOffset time.Duration,
	args roachpb.Request,
) error {
	txn := roachpb.MakeTransaction("test txn", desc.StartKey.AsRawKey(),
		0, ts, maxOffset.Nanoseconds())
	_, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{Txn: &txn}, args)
	return pErr.GoError()
}

// TODO(nvanbenschoten): fix this test. In b192bba, we allowed historical reads
// up to the freeze start time on subsumed ranges. This turned out not to be
// quite the right idea, because we can now ship the timestamp cache to the LHS
// and be more optimal about the resulting timestamp cache on the joint range.
// However, since we started allowing reads up to the freeze time, we were
// effectively closing this time for all future writes on the joint range, so we
// couldn't take advantage of the new ability to ship the timestamp cache
// around. But the change was very well intentioned and revealed that we should
// have no problem allowing reads below the closed timestamp on subsumed ranges.
// Add support for this and update this test.
//
// // TestHistoricalReadsAfterSubsume tests that a subsumed right hand side range
// // can only serve read-only traffic for timestamps that precede the subsumption
// // time, but don't contain the subsumption time in their uncertainty interval.
// func TestHistoricalReadsAfterSubsume(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	defer log.Scope(t).Close(t)
// 	ctx := context.Background()

// 	maxOffset := 100 * time.Millisecond
// 	preUncertaintyTs := func(ts hlc.Timestamp) hlc.Timestamp {
// 		return hlc.Timestamp{
// 			WallTime: ts.GoTime().Add(-maxOffset).UnixNano() - 1,
// 			Logical:  ts.Logical,
// 		}
// 	}

// 	type testCase struct {
// 		name          string
// 		queryTsFunc   func(freezeStart hlc.Timestamp) hlc.Timestamp
// 		queryArgsFunc func(key roachpb.Key) roachpb.Request
// 		shouldBlock   bool
// 	}

// 	tests := []testCase{
// 		// Ensure that a read query for a timestamp older than freezeStart-MaxOffset
// 		// is let through.
// 		{
// 			name:        "historical read",
// 			queryTsFunc: preUncertaintyTs,
// 			queryArgsFunc: func(key roachpb.Key) roachpb.Request {
// 				return getArgs(key)
// 			},
// 			shouldBlock: false,
// 		},
// 		// Write queries for the same historical timestamp should block (and then
// 		// eventually fail because the range no longer exists).
// 		{
// 			name:        "historical write",
// 			queryTsFunc: preUncertaintyTs,
// 			queryArgsFunc: func(key roachpb.Key) roachpb.Request {
// 				return putArgs(key, []byte(`test value`))
// 			},
// 			shouldBlock: true,
// 		},
// 		// Read queries that contain the subsumption time in its uncertainty interval
// 		// should block and eventually fail.
// 		{
// 			name: "historical read with uncertainty",
// 			queryTsFunc: func(freezeStart hlc.Timestamp) hlc.Timestamp {
// 				return freezeStart.Prev()
// 			},
// 			queryArgsFunc: func(key roachpb.Key) roachpb.Request {
// 				return getArgs(key)
// 			},
// 			shouldBlock: true,
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			tc, store, rhsDesc, freezeStart, waitForBlocked, cleanupFunc :=
// 				setupClusterWithSubsumedRange(ctx, t, 1 /* numNodes */, maxOffset)
// 			defer tc.Stopper().Stop(ctx)
// 			errCh := make(chan error)
// 			go func() {
// 				errCh <- sendWithTxn(store, rhsDesc, test.queryTsFunc(freezeStart), maxOffset,
// 					test.queryArgsFunc(rhsDesc.StartKey.AsRawKey()))
// 			}()
// 			if test.shouldBlock {
// 				waitForBlocked()
// 				cleanupFunc()
// 				// RHS should cease to exist once the merge completes but we cannot
// 				// guarantee that the merge wasn't internally retried before it was able
// 				// to successfully commit. If it did, requests blocked on the previous
// 				// merge attempt might go through successfully. Thus, we cannot make any
// 				// assertions about the result of these blocked requests.
// 				<-errCh
// 			} else {
// 				require.NoError(t, <-errCh)
// 				// We cleanup *after* the non-blocking read request succeeds to prevent
// 				// it from racing with the merge commit trigger.
// 				cleanupFunc()
// 			}
// 		})
// 	}
// }

// TestStoreBlockTransferLeaseRequestAfterSubsumption tests that a
// TransferLeaseRequest checks & waits for an ongoing merge before it can be
// evaluated.
// Regression test for #52517.
func TestStoreBlockTransferLeaseRequestAfterSubsumption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	numNodes := 2
	maxOffset := 0 * time.Second
	tc, store, rhsDesc, freezeStart, waitForBlocked, cleanupFunc :=
		setupClusterWithSubsumedRange(ctx, t, numNodes, maxOffset)
	defer tc.Stopper().Stop(ctx)

	errCh := make(chan error)
	target := tc.Target(1)
	go func() {
		args := adminTransferLeaseArgs(rhsDesc.StartKey.AsRawKey(), target.StoreID)
		errCh <- sendWithTxn(store, rhsDesc, freezeStart.Prev(), maxOffset, args)
	}()
	// Expect the TransferLeaseRequest to block until we allow the merge to commit.
	waitForBlocked()
	// Let the merge commit.
	cleanupFunc()
	// RHS should cease to exist once the merge completes but we cannot guarantee
	// that the merge wasn't internally retried before it was able to successfully
	// commit. If it did, this blocked transfer lease request might go through
	// successfully. Thus, we cannot make any assertions about the result of such
	// blocked requests.
	<-errCh
}

// setupClusterWithSubsumedRange returns a TestCluster during an ongoing merge
// transaction, such that the merge has been suspended right before the merge
// trigger is evaluated (with the RHS of the merge on the first store of the
// first server). This leaves the right hand side range of the merge in its
// subsumed state. It is the responsibility of the caller to call `cleanupFunc`
// to unblock the merge and Stop() the tc's Stopper when done.
func setupClusterWithSubsumedRange(
	ctx context.Context, t *testing.T, numNodes int, testMaxOffset time.Duration,
) (
	tc serverutils.TestClusterInterface,
	store *kvserver.Store,
	rhsDesc *roachpb.RangeDescriptor,
	freezeStart hlc.Timestamp,
	waitForBlocked func(),
	cleanupFunc func(),
) {
	filter := mergeFilter{}
	var blockedRequestCount int32
	clusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableMergeQueue:    true,
					MaxOffset:            testMaxOffset,
					TestingRequestFilter: filter.SuspendMergeTrigger,
					TestingConcurrencyRetryFilter: func(
						ctx context.Context, ba roachpb.BatchRequest, pErr *roachpb.Error,
					) {
						if _, ok := pErr.GetDetail().(*roachpb.MergeInProgressError); ok {
							atomic.AddInt32(&blockedRequestCount, 1)
						}
					},
				},
			},
		},
	}
	tc = serverutils.StartNewTestCluster(t, numNodes, clusterArgs)
	ts := tc.Server(0)
	stores, _ := ts.GetStores().(*kvserver.Stores)
	store, err := stores.GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	require.NoError(t, err)
	add := func(desc *roachpb.RangeDescriptor) *roachpb.RangeDescriptor {
		var newDesc roachpb.RangeDescriptor
		testutils.SucceedsSoon(t, func() error {
			var err error
			newDesc, err = tc.AddVoters(desc.StartKey.AsRawKey(), tc.Target(1))
			if kv.IsExpectedRelocateError(err) {
				// Retry.
				return errors.Newf("ChangeReplicas: received error %s", err)
			}
			return nil
		})
		require.NoError(t, tc.(*testcluster.TestCluster).WaitForFullReplication())
		testutils.SucceedsSoon(t, func() error {
			if count := len(replsForRange(ctx, t, tc, newDesc, numNodes)); count != 2 {
				return errors.Newf("expected %d replicas for range %d; found %d", 2, newDesc.RangeID, count)
			}
			return nil
		})
		require.NoError(t, tc.(*testcluster.TestCluster).WaitForVoters(newDesc.StartKey.AsRawKey(), tc.Target(1)))
		require.NoError(t, tc.(*testcluster.TestCluster).WaitForVoters(newDesc.StartKey.AsRawKey(), tc.Target(0)))
		return &newDesc
	}
	if numNodes > 1 {
		// Replicate the involved ranges to at least one other node in case the
		// TestCluster is a multi-node cluster.
		rhsDesc = add(rhsDesc)
		lhsDesc = add(lhsDesc)
	}
	errCh := make(chan error)
	blocker := filter.BlockNextMerge()
	go func() {
		errCh <- mergeWithRightNeighbor(ctx, store, *lhsDesc)
	}()
	defer func() {
		// Ensure that the request doesn't stay blocked if we fail.
		if t.Failed() {
			blocker.Unblock()
		}
	}()
	select {
	case freezeStart = <-blocker.WaitCh():
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(45 * time.Second):
		t.Fatal("did not receive the merge commit trigger as expected")
	}
	cleanupFunc = func() {
		// Let the merge commit.
		blocker.Unblock()
		require.NoError(t, <-errCh)
	}
	waitForBlocked = func() {
		testutils.SucceedsSoon(t, func() error {
			if actualBlocked := atomic.LoadInt32(&blockedRequestCount); actualBlocked < 1 {
				return errors.Newf("expected at least 1 blocked request but found none")
			}
			return nil
		})
	}
	return tc, store, rhsDesc, freezeStart, waitForBlocked, cleanupFunc
}

func BenchmarkStoreRangeMerge(b *testing.B) {
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(b, 0)

	lhsDesc, rhsDesc, err := createSplitRanges(ctx, store)
	if err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the proposed split key.
	kvserver.WriteRandomDataToRange(b, store, lhsDesc.RangeID, []byte("aaa"))
	kvserver.WriteRandomDataToRange(b, store, rhsDesc.RangeID, []byte("ccc"))

	// Create args to merge the b range back into the a range.
	mArgs := adminMergeArgs(lhsDesc.StartKey.AsRawKey())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Merge the ranges.
		b.StartTimer()
		if _, err := kv.SendWrapped(ctx, store.TestSender(), mArgs); err != nil {
			b.Fatal(err)
		}

		// Split the range.
		b.StopTimer()
		if _, _, err := createSplitRanges(ctx, store); err != nil {
			b.Fatal(err)
		}
	}
}
