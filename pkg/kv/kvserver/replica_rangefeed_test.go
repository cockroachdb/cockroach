// Copyright 2014 The Cockroach Authors.
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
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc/metadata"
)

// testStream is a mock implementation of roachpb.Internal_RangeFeedServer.
type testStream struct {
	ctx    context.Context
	cancel func()
	mu     struct {
		syncutil.Mutex
		events []*roachpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, cancel: cancel}
}

func (s *testStream) SendMsg(m interface{}) error  { panic("unimplemented") }
func (s *testStream) RecvMsg(m interface{}) error  { panic("unimplemented") }
func (s *testStream) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (s *testStream) SendHeader(metadata.MD) error { panic("unimplemented") }
func (s *testStream) SetTrailer(metadata.MD)       { panic("unimplemented") }

func (s *testStream) Context() context.Context {
	return s.ctx
}

func (s *testStream) Cancel() {
	s.cancel()
}

func (s *testStream) Send(e *roachpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, e)
	return nil
}

func (s *testStream) Events() []*roachpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.events
}

func TestReplicaRangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	sc := kvserver.TestStoreConfig(nil)
	sc.Clock = nil // manual clock
	kvserver.RangefeedEnabled.Override(&sc.Settings.SV, true)
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)
	mtc.replicateRange(1, 1, 2)
	db := mtc.dbs[0].NonTransactionalSender()

	// Split the range so that the RHS uses epoch-based leases.
	startKey := []byte("a")
	splitArgs := adminSplitArgs(startKey)
	if _, pErr := kv.SendWrapped(ctx, db, splitArgs); pErr != nil {
		t.Fatalf("split saw unexpected error: %v", pErr)
	}
	rangeID := mtc.Store(0).LookupReplica(startKey).RangeID

	// Insert a key before starting the rangefeeds.
	initTime := mtc.clock().Now()
	mtc.manualClock.Increment(1)
	ts1 := mtc.clock().Now()
	incArgs := incrementArgs(roachpb.Key("b"), 9)
	_, pErr := kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts1}, incArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(roachpb.Key("b"), []int64{9, 9, 9})

	replNum := 3
	streams := make([]*testStream, replNum)
	streamErrC := make(chan *roachpb.Error, replNum)
	rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	for i := 0; i < replNum; i++ {
		stream := newTestStream()
		streams[i] = stream
		go func(i int) {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					Timestamp: initTime,
					RangeID:   rangeID,
				},
				Span:     rangefeedSpan,
				WithDiff: true,
			}

			pErr := mtc.Store(i).RangeFeed(&req, stream)
			streamErrC <- pErr
		}(i)
	}

	checkForExpEvents := func(expEvents []*roachpb.RangeFeedEvent) {
		t.Helper()
		for i, stream := range streams {
			var events []*roachpb.RangeFeedEvent
			testutils.SucceedsSoon(t, func() error {
				if len(streamErrC) > 0 {
					// Break if the error channel is already populated.
					return nil
				}

				events = stream.Events()
				if len(events) < len(expEvents) {
					return errors.Errorf("too few events: %v", events)
				}
				return nil
			})

			if len(streamErrC) > 0 {
				t.Fatalf("unexpected error from stream: %v", <-streamErrC)
			}
			if !reflect.DeepEqual(events, expEvents) {
				t.Fatalf("incorrect events on stream %d, found %v, want %v", i, events, expEvents)
			}
		}
	}

	// Wait for all streams to observe the catch-up related events.
	expVal1 := roachpb.Value{Timestamp: ts1}
	expVal1.SetInt(9)
	expVal1.InitChecksum(roachpb.Key("b"))
	expEvents := []*roachpb.RangeFeedEvent{
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal1,
		}},
		{Checkpoint: &roachpb.RangeFeedCheckpoint{
			Span:       rangefeedSpan,
			ResolvedTS: hlc.Timestamp{},
		}},
	}
	checkForExpEvents(expEvents)

	// Insert a key non-transactionally.
	mtc.manualClock.Increment(1)
	ts2 := mtc.clock().Now()
	pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
	_, pErr = kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts2}, pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Insert a second key transactionally.
	mtc.manualClock.Increment(1)
	ts3 := mtc.clock().Now()
	if err := mtc.dbs[1].Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, ts3)
		return txn.Put(ctx, roachpb.Key("m"), []byte("val3"))
	}); err != nil {
		t.Fatal(err)
	}
	// Read to force intent resolution.
	if _, err := mtc.dbs[1].Get(ctx, roachpb.Key("m")); err != nil {
		t.Fatal(err)
	}

	// Update the originally incremented key non-transactionally.
	mtc.manualClock.Increment(1)
	ts4 := mtc.clock().Now()
	_, pErr = kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts4}, incArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Update the originally incremented key transactionally.
	mtc.manualClock.Increment(1)
	ts5 := mtc.clock().Now()
	if err := mtc.dbs[1].Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, ts5)
		_, err := txn.Inc(ctx, incArgs.Key, 7)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Read to force intent resolution.
	if _, err := mtc.dbs[1].Get(ctx, roachpb.Key("b")); err != nil {
		t.Fatal(err)
	}

	// Wait for all streams to observe the expected events.
	expVal2 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val2"), ts2)
	expVal3 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val3"), ts3)
	expVal3.InitChecksum([]byte("m")) // client.Txn sets value checksum
	expVal4 := roachpb.Value{Timestamp: ts4}
	expVal4.SetInt(18)
	expVal4.InitChecksum(roachpb.Key("b"))
	expVal5 := roachpb.Value{Timestamp: ts5}
	expVal5.SetInt(25)
	expVal5.InitChecksum(roachpb.Key("b"))
	expVal1NoTS, expVal4NoTS := expVal1, expVal4
	expVal1NoTS.Timestamp, expVal4NoTS.Timestamp = hlc.Timestamp{}, hlc.Timestamp{}
	expEvents = append(expEvents, []*roachpb.RangeFeedEvent{
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("c"), Value: expVal2,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("m"), Value: expVal3,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal4, PrevValue: expVal1NoTS,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal5, PrevValue: expVal4NoTS,
		}},
	}...)
	checkForExpEvents(expEvents)

	// Cancel each of the rangefeed streams.
	for _, stream := range streams {
		stream.Cancel()

		pErr := <-streamErrC
		if !testutils.IsPError(pErr, "context canceled") {
			t.Fatalf("got error for RangeFeed: %v", pErr)
		}
	}

	// Bump the GC threshold and assert that RangeFeed below the timestamp will
	// catch an error.
	gcReq := &roachpb.GCRequest{
		Threshold: initTime.Add(0, 1),
	}
	gcReq.Key = roachpb.Key(startKey)
	gcReq.EndKey = mtc.Store(0).LookupReplica(startKey).Desc().EndKey.AsRawKey()
	var ba roachpb.BatchRequest
	ba.RangeID = rangeID
	ba.Add(gcReq)
	if _, pErr := mtc.Store(0).Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	req := roachpb.RangeFeedRequest{
		Header: roachpb.Header{
			Timestamp: initTime,
			RangeID:   rangeID,
		},
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
	}

	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < replNum; i++ {
			repl := mtc.Store(i).LookupReplica(startKey)
			if repl == nil {
				return errors.Errorf("replica not found on node #%d", i+1)
			}
			if cur := repl.GetGCThreshold(); cur.Less(gcReq.Threshold) {
				return errors.Errorf("%s has GCThreshold %s < %s; hasn't applied the bump yet", repl, cur, gcReq.Threshold)
			}
			stream := newTestStream()
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			defer stream.Cancel()

			if pErr := mtc.Store(i).RangeFeed(&req, stream); !testutils.IsPError(
				pErr, `must be after replica GC threshold`,
			) {
				return pErr.GoError()
			}
		}
		return nil
	})
}

func TestReplicaRangefeedExpiringLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := kvserver.TestStoreConfig(nil)
	sc.Clock = nil // manual clock
	kvserver.RangefeedEnabled.Override(&sc.Settings.SV, true)
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 1)

	// Establish a rangefeed on the replica we plan to remove.
	stream := newTestStream()
	req := roachpb.RangeFeedRequest{
		Header: roachpb.Header{
			RangeID: 1,
		},
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
	}

	// Cancel the stream's context so that RangeFeed would return
	// immediately even if it didn't return the correct error.
	stream.Cancel()

	pErr := mtc.Store(0).RangeFeed(&req, stream)
	const exp = "expiration-based leases are incompatible with rangefeeds"
	if !testutils.IsPError(pErr, exp) {
		t.Errorf("expected error %q, found %v", exp, pErr)
	}
}

func TestReplicaRangefeedRetryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startKey := []byte("a")
	setup := func(subT *testing.T) (*multiTestContext, roachpb.RangeID) {
		subT.Helper()
		sc := kvserver.TestStoreConfig(nil)
		sc.Clock = nil // manual clock
		kvserver.RangefeedEnabled.Override(&sc.Settings.SV, true)
		mtc := &multiTestContext{
			storeConfig: &sc,
			// This test was written before the multiTestContext started creating many
			// system ranges at startup, and hasn't been update to take that into
			// account.
			startWithSingleRange: true,
		}
		mtc.Start(subT, 3)
		mtc.replicateRange(1, 1, 2)

		// Split the range so that the RHS uses epoch-based leases.
		splitArgs := adminSplitArgs(startKey)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], splitArgs); pErr != nil {
			subT.Fatalf("split saw unexpected error: %v", pErr)
		}
		rangeID := mtc.Store(0).LookupReplica(startKey).RangeID

		// Write to the RHS of the split and wait for all replicas to process it.
		// This ensures that all replicas have seen the split before we move on.
		incArgs := incrementArgs(roachpb.Key("a"), 9)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(roachpb.Key("a"), []int64{9, 9, 9})

		return mtc, rangeID
	}

	waitForInitialCheckpointAcrossSpan := func(
		subT *testing.T, stream *testStream, streamErrC <-chan *roachpb.Error, span roachpb.Span,
	) {
		subT.Helper()
		expEvents := []*roachpb.RangeFeedEvent{
			{Checkpoint: &roachpb.RangeFeedCheckpoint{
				Span:       span,
				ResolvedTS: hlc.Timestamp{},
			}},
		}
		var events []*roachpb.RangeFeedEvent
		testutils.SucceedsSoon(t, func() error {
			if len(streamErrC) > 0 {
				// Break if the error channel is already populated.
				return nil
			}

			events = stream.Events()
			if len(events) < len(expEvents) {
				return errors.Errorf("too few events: %v", events)
			}
			return nil
		})
		if len(streamErrC) > 0 {
			subT.Fatalf("unexpected error from stream: %v", <-streamErrC)
		}
		if !reflect.DeepEqual(events, expEvents) {
			subT.Fatalf("incorrect events on stream, found %v, want %v", events, expEvents)
		}
	}

	assertRangefeedRetryErr := func(
		subT *testing.T, pErr *roachpb.Error, expReason roachpb.RangeFeedRetryError_Reason,
	) {
		subT.Helper()
		expErr := roachpb.NewRangeFeedRetryError(expReason)
		if pErr == nil {
			subT.Fatalf("got nil error for RangeFeed: expecting %v", expErr)
		}
		rfErr, ok := pErr.GetDetail().(*roachpb.RangeFeedRetryError)
		if !ok {
			subT.Fatalf("got incorrect error for RangeFeed: %v; expecting %v", pErr, expErr)
		}
		if rfErr.Reason != expReason {
			subT.Fatalf("got incorrect RangeFeedRetryError reason for RangeFeed: %v; expecting %v",
				rfErr.Reason, expReason)
		}
	}

	t.Run(roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED.String(), func(t *testing.T) {
		const removeStore = 2
		mtc, rangeID := setup(t)
		defer mtc.Stop()

		// Establish a rangefeed on the replica we plan to remove.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}

			pErr := mtc.Store(removeStore).RangeFeed(&req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Remove the replica from the range.
		mtc.unreplicateRange(rangeID, removeStore)

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT.String(), func(t *testing.T) {
		mtc, rangeID := setup(t)
		defer mtc.Stop()

		// Establish a rangefeed on the replica we plan to split.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}

			pErr := mtc.Store(0).RangeFeed(&req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Split the range.
		args := adminSplitArgs([]byte("m"))
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], args); pErr != nil {
			t.Fatalf("split saw unexpected error: %v", pErr)
		}

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_MERGED.String(), func(t *testing.T) {
		mtc, rangeID := setup(t)
		defer mtc.Stop()

		// Split the range.
		splitKey := []byte("m")
		splitArgs := adminSplitArgs(splitKey)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], splitArgs); pErr != nil {
			t.Fatalf("split saw unexpected error: %v", pErr)
		}
		rightRangeID := mtc.Store(0).LookupReplica(splitKey).RangeID

		// Write to the RHS of the split and wait for all replicas to process it.
		// This ensures that all replicas have seen the split before we move on.
		incArgs := incrementArgs(roachpb.Key("n"), 9)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(roachpb.Key("n"), []int64{9, 9, 9})

		// Establish a rangefeed on the left replica.
		streamLeft := newTestStream()
		streamLeftErrC := make(chan *roachpb.Error, 1)
		rangefeedLeftSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: splitKey}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedLeftSpan,
			}

			pErr := mtc.Store(0).RangeFeed(&req, streamLeft)
			streamLeftErrC <- pErr
		}()

		// Establish a rangefeed on the right replica.
		streamRight := newTestStream()
		streamRightErrC := make(chan *roachpb.Error, 1)
		rangefeedRightSpan := roachpb.Span{Key: splitKey, EndKey: roachpb.Key("z")}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedRightSpan,
			}

			pErr := mtc.Store(0).RangeFeed(&req, streamRight)
			streamRightErrC <- pErr
		}()

		// Wait for the first checkpoint event on each stream.
		waitForInitialCheckpointAcrossSpan(t, streamLeft, streamLeftErrC, rangefeedLeftSpan)
		waitForInitialCheckpointAcrossSpan(t, streamRight, streamRightErrC, rangefeedRightSpan)

		// Merge the ranges back together
		mergeArgs := adminMergeArgs(startKey)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], mergeArgs); pErr != nil {
			t.Fatalf("merge saw unexpected error: %v", pErr)
		}

		// Check the errors.
		pErrLeft, pErrRight := <-streamLeftErrC, <-streamRightErrC
		assertRangefeedRetryErr(t, pErrLeft, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
		assertRangefeedRetryErr(t, pErrRight, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT.String(), func(t *testing.T) {
		mtc, rangeID := setup(t)
		defer mtc.Stop()
		partitionStore := mtc.Store(2)

		mtc.stores[0].SetReplicaGCQueueActive(false)
		mtc.stores[1].SetReplicaGCQueueActive(false)
		mtc.stores[2].SetReplicaGCQueueActive(false)

		// Establish a rangefeed on the replica we plan to partition.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}

			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()

			pErr := partitionStore.RangeFeed(&req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Force the leader off the replica on partitionedStore. If it's the
		// leader, this test will fall over when it cuts the replica off from
		// Raft traffic.
		testutils.SucceedsSoon(t, func() error {
			repl, err := partitionStore.GetReplica(rangeID)
			if err != nil {
				return err
			}
			raftStatus := repl.RaftStatus()
			if raftStatus != nil && raftStatus.RaftState == raft.StateFollower {
				return nil
			}
			err = repl.AdminTransferLease(ctx, roachpb.StoreID(1))
			return errors.Errorf("not raft follower: %+v, transferred lease: %v", raftStatus, err)
		})

		// Partition the replica from the rest of its range.
		mtc.transport.Listen(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:            rangeID,
			RaftMessageHandler: partitionStore,
		})

		// Perform a write on the range.
		pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Get that command's log index.
		repl, err := mtc.Store(0).GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		index, err := repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}

		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the put). This necessitates a snapshot when the partitioned
		// replica rejoins the rest of the range.
		truncArgs := truncateLogArgs(index+1, rangeID)
		truncArgs.Key = startKey
		if _, err := kv.SendWrapped(ctx, mtc.distSenders[0], truncArgs); err != nil {
			t.Fatal(err)
		}

		// Remove the partition. Snapshot should follow.
		mtc.transport.Listen(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:            rangeID,
			RaftMessageHandler: partitionStore,
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserver.RaftMessageRequest) bool {
					// Make sure that even going forward no MsgApp for what we just truncated can
					// make it through. The Raft transport is asynchronous so this is necessary
					// to make the test pass reliably.
					// NB: the Index on the message is the log index that _precedes_ any of the
					// entries in the MsgApp, so filter where msg.Index < index, not <= index.
					return req.Message.Type == raftpb.MsgApp && req.Message.Index < index
				},
				dropHB:   func(*kvserver.RaftHeartbeat) bool { return false },
				dropResp: func(*kvserver.RaftMessageResponse) bool { return false },
			},
		})

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING.String(), func(t *testing.T) {
		mtc, _ := setup(t)
		defer mtc.Stop()

		// Split the range so that the RHS is not a system range and thus will
		// respect the rangefeed_enabled cluster setting.
		startKey := keys.UserTableDataMin
		splitArgs := adminSplitArgs(startKey)
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], splitArgs); pErr != nil {
			t.Fatalf("split saw unexpected error: %v", pErr)
		}
		rightRangeID := mtc.Store(0).LookupReplica(roachpb.RKey(startKey)).RangeID

		// Establish a rangefeed.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)

		endKey := keys.TableDataMax
		rangefeedSpan := roachpb.Span{Key: startKey, EndKey: endKey}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedSpan,
			}

			pErr := mtc.Store(0).RangeFeed(&req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Disable rangefeeds, which stops logical op logs from being provided
		// with Raft commands.
		kvserver.RangefeedEnabled.Override(&mtc.storeConfig.Settings.SV, false)

		// Perform a write on the range.
		writeKey := encoding.EncodeStringAscending(keys.SystemSQLCodec.TablePrefix(55), "c")
		pArgs := putArgs(writeKey, []byte("val2"))
		if _, pErr := kv.SendWrapped(ctx, mtc.distSenders[0], pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING)
	})
}

// TestReplicaRangefeedPushesTransactions tests that rangefeed detects intents
// that are holding up its resolved timestamp and periodically pushes them to
// ensure that its resolved timestamp continues to advance.
func TestReplicaRangefeedPushesTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc, db, _, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// While we're here, drop the target duration. This was set to
	// testingTargetDuration above, but this is higher then it needs to be now
	// that cluster and schema setup is complete.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'`)

	// Make sure all the nodes have gotten the rangefeed enabled setting from
	// gossip, so that they will immediately be able to accept RangeFeeds. The
	// target_duration one is just to speed up the test, we don't care if it has
	// propagated everywhere yet.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			var enabled bool
			if err := tc.ServerConn(i).QueryRow(
				`SHOW CLUSTER SETTING kv.rangefeed.enabled`,
			).Scan(&enabled); err != nil {
				return err
			}
			if !enabled {
				return errors.Errorf(`waiting for rangefeed to be enabled on node %d`, i)
			}
		}
		return nil
	})

	ts1 := tc.Server(0).Clock().Now()
	rangeFeedCtx, rangeFeedCancel := context.WithCancel(ctx)
	defer rangeFeedCancel()
	rangeFeedChs := make([]chan *roachpb.RangeFeedEvent, len(repls))
	rangeFeedErrC := make(chan error, len(repls))
	for i := range repls {
		desc := repls[i].Desc()
		ds := tc.Server(i).DistSenderI().(*kvcoord.DistSender)
		rangeFeedCh := make(chan *roachpb.RangeFeedEvent)
		rangeFeedChs[i] = rangeFeedCh
		go func() {
			span := roachpb.Span{
				Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
			}
			rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, span, ts1, false /* withDiff */, rangeFeedCh)
		}()
	}

	// Wait for a RangeFeed checkpoint on each RangeFeed after the RangeFeed
	// initial scan time (which is the timestamp passed in the request) to make
	// sure everything is set up. We intentionally don't care about the spans in
	// the checkpoints, just verifying that something has made it past the
	// initial scan and is running.
	waitForCheckpoint := func(ts hlc.Timestamp) {
		t.Helper()
		for _, rangeFeedCh := range rangeFeedChs {
			checkpointed := false
			for !checkpointed {
				select {
				case event := <-rangeFeedCh:
					if c := event.Checkpoint; c != nil && ts.Less(c.ResolvedTS) {
						checkpointed = true
					}
				case err := <-rangeFeedErrC:
					t.Fatal(err)
				}
			}
		}
	}
	waitForCheckpoint(ts1)

	// Start a transaction and write an intent on the range. This intent would
	// prevent from the rangefeed's resolved timestamp from advancing. To get
	// around this, the rangefeed periodically pushes all intents on its range
	// to higher timestamps.
	tx1, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx1.ExecContext(ctx, "INSERT INTO cttest.kv VALUES (1, 'test')")
	require.NoError(t, err)

	// Read the current transaction timestamp. This prevents the txn from committing
	// if it ever gets pushed.
	var ts2Str string
	require.NoError(t, tx1.QueryRowContext(ctx, "SELECT cluster_logical_timestamp()").Scan(&ts2Str))
	ts2, err := sql.ParseHLC(ts2Str)
	require.NoError(t, err)

	// Wait for the RangeFeed checkpoint on each RangeFeed to exceed this timestamp.
	// For this to be possible, it must push the transaction's timestamp forward.
	waitForCheckpoint(ts2)

	// The txn should not be able to commit since its commit timestamp was pushed
	// and it has observed its timestamp.
	require.Regexp(t, "TransactionRetryError: retry txn", tx1.Commit())

	// Make sure the RangeFeed hasn't errored yet.
	select {
	case err := <-rangeFeedErrC:
		t.Fatal(err)
	default:
	}
	// Now cancel it and wait for it to shut down.
	rangeFeedCancel()
}

// TestReplicaRangefeedNudgeSlowClosedTimestamp tests that rangefeed detects
// that its closed timestamp updates have stalled and requests new information
// from its Range's leaseholder. This is a regression test for #35142.
func TestReplicaRangefeedNudgeSlowClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc, db, desc, repls := setupTestClusterForClosedTimestampTesting(ctx, t, testingTargetDuration)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// While we're here, drop the target duration. This was set to
	// testingTargetDuration above, but this is higher then it needs to be now
	// that cluster and schema setup is complete.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'`)

	// Make sure all the nodes have gotten the rangefeed enabled setting from
	// gossip, so that they will immediately be able to accept RangeFeeds. The
	// target_duration one is just to speed up the test, we don't care if it has
	// propagated everywhere yet.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			var enabled bool
			if err := tc.ServerConn(i).QueryRow(
				`SHOW CLUSTER SETTING kv.rangefeed.enabled`,
			).Scan(&enabled); err != nil {
				return err
			}
			if !enabled {
				return errors.Errorf(`waiting for rangefeed to be enabled on node %d`, i)
			}
		}
		return nil
	})

	ts1 := tc.Server(0).Clock().Now()
	rangeFeedCtx, rangeFeedCancel := context.WithCancel(ctx)
	defer rangeFeedCancel()
	rangeFeedChs := make([]chan *roachpb.RangeFeedEvent, len(repls))
	rangeFeedErrC := make(chan error, len(repls))
	for i := range repls {
		ds := tc.Server(i).DistSenderI().(*kvcoord.DistSender)
		rangeFeedCh := make(chan *roachpb.RangeFeedEvent)
		rangeFeedChs[i] = rangeFeedCh
		go func() {
			span := roachpb.Span{
				Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
			}
			rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, span, ts1, false /* withDiff */, rangeFeedCh)
		}()
	}

	// Wait for a RangeFeed checkpoint on each RangeFeed after the RangeFeed
	// initial scan time (which is the timestamp passed in the request) to make
	// sure everything is set up. We intentionally don't care about the spans in
	// the checkpoints, just verifying that something has made it past the
	// initial scan and is running.
	waitForCheckpoint := func(ts hlc.Timestamp) {
		t.Helper()
		for _, rangeFeedCh := range rangeFeedChs {
			checkpointed := false
			for !checkpointed {
				select {
				case event := <-rangeFeedCh:
					if c := event.Checkpoint; c != nil && ts.Less(c.ResolvedTS) {
						checkpointed = true
					}
				case err := <-rangeFeedErrC:
					t.Fatal(err)
				}
			}
		}
	}
	waitForCheckpoint(ts1)

	// Clear the closed timestamp storage on each server. This simulates the case
	// where a closed timestamp message is lost or a node restarts. To recover,
	// the servers will need to request an update from the leaseholder.
	for i := 0; i < tc.NumServers(); i++ {
		stores := tc.Server(i).GetStores().(*kvserver.Stores)
		err := stores.VisitStores(func(s *kvserver.Store) error {
			s.ClearClosedTimestampStorage()
			return nil
		})
		require.NoError(t, err)
	}

	// Wait for another RangeFeed checkpoint after the store was cleared. Without
	// RangeFeed nudging closed timestamps, this doesn't happen on its own. Again,
	// we intentionally don't care about the spans in the checkpoints, just
	// verifying that something has made it past the cleared time.
	ts2 := tc.Server(0).Clock().Now()
	waitForCheckpoint(ts2)

	// Make sure the RangeFeed hasn't errored yet.
	select {
	case err := <-rangeFeedErrC:
		t.Fatal(err)
	default:
	}
	// Now cancel it and wait for it to shut down.
	rangeFeedCancel()
}
