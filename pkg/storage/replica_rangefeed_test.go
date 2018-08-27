// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	sc := storage.TestStoreConfig(nil)
	storage.RangefeedEnabled.Override(&sc.Settings.SV, true)
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 3)
	db := mtc.dbs[0].NonTransactionalSender()

	rangeID := roachpb.RangeID(1)
	mtc.replicateRange(rangeID, 1, 2)
	initTime := mtc.clock.Now()

	// Insert a key before starting the rangefeeds.
	mtc.manualClock.Increment(1)
	ts1 := mtc.clock.Now()
	incArgs := incrementArgs(roachpb.Key("b"), 9)
	_, pErr := client.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts1}, incArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(roachpb.Key("b"), []int64{9, 9, 9})

	replNum := 3
	streams := make([]*testStream, replNum)
	streamErrC := make(chan *roachpb.Error, replNum)
	for i := 0; i < replNum; i++ {
		stream := newTestStream()
		streams[i] = stream
		go func(i int) {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					Timestamp: initTime,
					RangeID:   rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(i).RangeFeed(ctx, &req, stream)
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
			Span:       roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax},
			ResolvedTS: hlc.Timestamp{},
		}},
	}
	checkForExpEvents(expEvents)

	// Insert a key non-transactionally.
	mtc.manualClock.Increment(1)
	ts2 := mtc.clock.Now()
	pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
	_, pErr = client.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts2}, pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Insert a second key transactionally.
	mtc.manualClock.Increment(1)
	ts3 := mtc.clock.Now()
	if err := mtc.dbs[1].Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, ts3)
		return txn.Put(ctx, roachpb.Key("m"), []byte("val3"))
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for all streams to observe the expected events.
	val2 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val2"), ts2)
	val3 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val3"), ts3)
	val3.InitChecksum([]byte("m")) // client.Txn sets value checksum
	expEvents = append(expEvents, []*roachpb.RangeFeedEvent{
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("c"), Value: val2,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("m"), Value: val3,
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
}

func TestReplicaRangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rangeID := roachpb.RangeID(1)
	setup := func() *multiTestContext {
		sc := storage.TestStoreConfig(nil)
		storage.RangefeedEnabled.Override(&sc.Settings.SV, true)
		mtc := &multiTestContext{storeConfig: &sc}
		mtc.Start(t, 3)
		mtc.replicateRange(rangeID, 1, 2)
		return mtc
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
	waitForInitialCheckpoint := func(
		subT *testing.T, stream *testStream, streamErrC <-chan *roachpb.Error,
	) {
		span := roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}
		waitForInitialCheckpointAcrossSpan(subT, stream, streamErrC, span)
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
		mtc := setup()
		defer mtc.Stop()

		// Establish a rangefeed on the replica we plan to remove.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(removeStore).RangeFeed(ctx, &req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpoint(t, stream, streamErrC)

		// Remove the replica from the range.
		mtc.unreplicateRange(rangeID, removeStore)

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT.String(), func(t *testing.T) {
		mtc := setup()
		defer mtc.Stop()

		// Establish a rangefeed on the replica we plan to split.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(0).RangeFeed(ctx, &req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpoint(t, stream, streamErrC)

		// Split the range.
		args := adminSplitArgs([]byte("m"))
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], args); pErr != nil {
			t.Fatalf("split saw unexpected error: %v", pErr)
		}

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_MERGED.String(), func(t *testing.T) {
		mtc := setup()
		defer mtc.Stop()

		// Split the range.
		splitKey := []byte("m")
		splitArgs := adminSplitArgs(splitKey)
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], splitArgs); pErr != nil {
			t.Fatalf("split saw unexpected error: %v", pErr)
		}
		rightRangeID := mtc.Store(0).LookupReplica(splitKey).RangeID

		// Write to the RHS of the split and wait for all replicas to process it.
		// This ensures that all replicas have seen the split before we move on.
		incArgs := incrementArgs(roachpb.Key("n"), 9)
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(roachpb.Key("n"), []int64{9, 9, 9})

		// Establish a rangefeed on the left replica.
		streamLeft := newTestStream()
		streamLeftErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: splitKey},
			}

			pErr := mtc.Store(0).RangeFeed(ctx, &req, streamLeft)
			streamLeftErrC <- pErr
		}()

		// Establish a rangefeed on the right replica.
		streamRight := newTestStream()
		streamRightErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rightRangeID,
				},
				Span: roachpb.Span{Key: splitKey, EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(0).RangeFeed(ctx, &req, streamRight)
			streamRightErrC <- pErr
		}()

		// Wait for the first checkpoint event on each stream.
		waitForInitialCheckpointAcrossSpan(t, streamLeft, streamLeftErrC, roachpb.Span{
			Key: roachpb.KeyMin, EndKey: splitKey,
		})
		waitForInitialCheckpointAcrossSpan(t, streamRight, streamRightErrC, roachpb.Span{
			Key: splitKey, EndKey: roachpb.KeyMax,
		})

		// Merge the ranges back together
		mergeArgs := adminMergeArgs(keys.MinKey)
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], mergeArgs); pErr != nil {
			t.Fatalf("merge saw unexpected error: %v", pErr)
		}

		// Check the errors.
		pErrLeft, pErrRight := <-streamLeftErrC, <-streamRightErrC
		assertRangefeedRetryErr(t, pErrLeft, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
		assertRangefeedRetryErr(t, pErrRight, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT.String(), func(t *testing.T) {
		const partitionStore = 2
		mtc := setup()
		defer mtc.Stop()

		mtc.stores[0].SetReplicaGCQueueActive(false)
		mtc.stores[1].SetReplicaGCQueueActive(false)
		mtc.stores[2].SetReplicaGCQueueActive(false)

		// Establish a rangefeed on the replica we plan to partition.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(partitionStore).RangeFeed(ctx, &req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpoint(t, stream, streamErrC)

		// Partition the replica from the rest of its range.
		mtc.transport.GetCircuitBreaker(mtc.idents[partitionStore].NodeID).Break()

		// Perform a write on the range.
		pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Get that command's log index.
		index, err := mtc.Store(0).LookupReplica(roachpb.RKeyMin).GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}

		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the put). This necessitates a snapshot when the partitioned
		// replica rejoins the rest of the range.
		truncArgs := truncateLogArgs(index+1, 1)
		if _, err := client.SendWrapped(ctx, mtc.distSenders[0], truncArgs); err != nil {
			t.Fatal(err)
		}

		// Remove the partition. Snapshot should follow.
		mtc.transport.GetCircuitBreaker(mtc.idents[partitionStore].NodeID).Reset()

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING.String(), func(t *testing.T) {
		mtc := setup()
		defer mtc.Stop()

		// Establish a rangefeed.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			}

			pErr := mtc.Store(0).RangeFeed(ctx, &req, stream)
			streamErrC <- pErr
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpoint(t, stream, streamErrC)

		// Disable rangefeeds, which stops logical op logs from being provided
		// with Raft commands.
		storage.RangefeedEnabled.Override(&mtc.storeConfig.Settings.SV, false)

		// Perform a write on the range.
		pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
		if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING)
	})
}
