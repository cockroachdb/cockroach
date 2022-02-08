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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numNodes = 5
	args := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: make(map[int]base.TestServerArgs, numNodes),
	}
	for i := 0; i < numNodes; i++ {
		// Disable closed timestamps as this test was designed assuming no closed
		// timestamps would get propagated.
		settings := cluster.MakeTestingClusterSettings()
		closedts.TargetDuration.Override(ctx, &settings.SV, 24*time.Hour)
		kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)
		args.ServerArgsPerNode[i] = base.TestServerArgs{Settings: settings}
	}
	tc := testcluster.StartTestCluster(t, numNodes, args)
	defer tc.Stopper().Stop(ctx)

	ts := tc.Servers[0]
	firstStore, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}

	db := firstStore.DB().NonTransactionalSender()

	// Split the range so that the RHS uses epoch-based leases.
	startKey := []byte("a")
	tc.SplitRangeOrFatal(t, startKey)
	tc.AddVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
	tc.AddNonVotersOrFatal(t, startKey, tc.Target(3), tc.Target(4))
	if pErr := tc.WaitForVoters(startKey, tc.Target(1), tc.Target(2)); pErr != nil {
		t.Fatalf("Unexpected error waiting for replication: %v", pErr)
	}
	rangeID := firstStore.LookupReplica(startKey).RangeID

	// Insert a key before starting the rangefeeds.
	initTime := ts.Clock().Now()
	ts1 := initTime.Add(0, 1)
	incArgs := incrementArgs(roachpb.Key("b"), 9)
	if _, pErr := kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts1}, incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	tc.WaitForValues(t, roachpb.Key("b"), []int64{9, 9, 9, 9, 9})

	streams := make([]*testStream, numNodes)
	streamErrC := make(chan *roachpb.Error, numNodes)
	rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	for i := 0; i < numNodes; i++ {
		stream := newTestStream()
		streams[i] = stream
		srv := tc.Servers[i]
		store, err := srv.Stores().GetStore(srv.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func(i int) {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					Timestamp: initTime,
					RangeID:   rangeID,
				},
				Span:     rangefeedSpan,
				WithDiff: true,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- store.RangeFeed(&req, stream)
		}(i)
	}

	checkForExpEvents := func(expEvents []*roachpb.RangeFeedEvent) {
		t.Helper()

		// SSTs may not be equal byte-for-byte due to AddSSTable rewrites. We nil
		// out the expected data here for require.Equal comparison, and compare
		// the actual contents separately.
		type sstTest struct {
			expect     []byte
			expectSpan roachpb.Span
			expectTS   hlc.Timestamp
			actual     []byte
		}
		var ssts []sstTest
		for _, e := range expEvents {
			if e.SST != nil {
				ssts = append(ssts, sstTest{
					expect:     e.SST.Data,
					expectSpan: e.SST.Span,
					expectTS:   e.SST.WriteTS,
				})
				e.SST.Data = nil
			}
		}

		for _, stream := range streams {
			var events []*roachpb.RangeFeedEvent
			testutils.SucceedsSoon(t, func() error {
				if len(streamErrC) > 0 {
					// Break if the error channel is already populated.
					return nil
				}

				events = stream.Events()
				// Filter out checkpoints. Those are not deterministic; they can come at any time.
				var filteredEvents []*roachpb.RangeFeedEvent
				for _, e := range events {
					if e.Checkpoint != nil {
						continue
					}
					filteredEvents = append(filteredEvents, e)
				}
				events = filteredEvents
				if len(events) < len(expEvents) {
					return errors.Errorf("too few events: %v", events)
				}
				return nil
			})

			if len(streamErrC) > 0 {
				t.Fatalf("unexpected error from stream: %v", <-streamErrC)
			}

			i := 0
			for _, e := range events {
				if e.SST != nil && i < len(ssts) {
					ssts[i].actual = e.SST.Data
					e.SST.Data = nil
					i++
				}
			}

			require.Equal(t, expEvents, events)

			for _, sst := range ssts {
				expIter, err := storage.NewMemSSTIterator(sst.expect, false)
				require.NoError(t, err)
				defer expIter.Close()

				sstIter, err := storage.NewMemSSTIterator(sst.actual, false)
				require.NoError(t, err)
				defer sstIter.Close()

				expIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
				sstIter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
				for {
					expOK, expErr := expIter.Valid()
					require.NoError(t, expErr)
					sstOK, sstErr := sstIter.Valid()
					require.NoError(t, sstErr)
					if !expOK {
						require.False(t, sstOK)
						break
					}

					expKey, expValue := expIter.UnsafeKey(), expIter.UnsafeValue()
					sstKey, sstValue := sstIter.UnsafeKey(), sstIter.UnsafeValue()
					require.Equal(t, expKey.Key, sstKey.Key)
					require.Equal(t, expValue, sstValue)
					// We don't compare expKey.Timestamp and sstKey.Timestamp, because the
					// SST timestamp may have been rewritten to the request timestamp. We
					// assert on the write timestamp instead.
					require.Equal(t, sst.expectTS, sstKey.Timestamp)

					expIter.Next()
					sstIter.Next()
				}
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
	}
	checkForExpEvents(expEvents)

	// Insert a key non-transactionally.
	ts2 := initTime.Add(0, 2)
	pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
	_, err := kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts2}, pArgs)
	if err != nil {
		t.Fatal(err)
	}

	server1 := tc.Servers[1]
	store1, pErr := server1.Stores().GetStore(server1.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Insert a second key transactionally.
	ts3 := initTime.Add(0, 3)
	if err := store1.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, ts3); err != nil {
			return err
		}
		return txn.Put(ctx, roachpb.Key("m"), []byte("val3"))
	}); err != nil {
		t.Fatal(err)
	}
	// Read to force intent resolution.
	if _, err := store1.DB().Get(ctx, roachpb.Key("m")); err != nil {
		t.Fatal(err)
	}

	// Update the originally incremented key non-transactionally.
	ts4 := initTime.Add(0, 4)
	_, err = kv.SendWrappedWith(ctx, db, roachpb.Header{Timestamp: ts4}, incArgs)
	if err != nil {
		t.Fatal(err)
	}

	// Update the originally incremented key transactionally.
	ts5 := initTime.Add(0, 5)
	if err := store1.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, ts5); err != nil {
			return err
		}
		_, err := txn.Inc(ctx, incArgs.Key, 7)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Read to force intent resolution.
	if _, err := store1.DB().Get(ctx, roachpb.Key("b")); err != nil {
		t.Fatal(err)
	}

	// Ingest an SSTable. We use a new timestamp to avoid getting pushed by the
	// timestamp cache due to the read above.
	ts6 := ts.Clock().Now().Add(0, 6)

	expVal6b := roachpb.Value{}
	expVal6b.SetInt(6)
	expVal6b.InitChecksum(roachpb.Key("b"))

	expVal6q := roachpb.Value{}
	expVal6q.SetInt(7)
	expVal6q.InitChecksum(roachpb.Key("q"))

	st := cluster.MakeTestingClusterSettings()
	sstFile := &storage.MemFile{}
	sstWriter := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstWriter.Close()
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("b"), Timestamp: ts6},
		expVal6b.RawBytes))
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("q"), Timestamp: ts6},
		expVal6q.RawBytes))
	require.NoError(t, sstWriter.Finish())
	expSST := sstFile.Data()
	expSSTSpan := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("r")}

	_, pErr = store1.DB().AddSSTableAtBatchTimestamp(ctx, roachpb.Key("b"), roachpb.Key("r"), sstFile.Data(),
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, ts6)
	require.Nil(t, pErr)

	// Ingest an SSTable as writes.
	ts7 := ts.Clock().Now().Add(0, 7)

	expVal7b := roachpb.Value{Timestamp: ts7}
	expVal7b.SetInt(7)
	expVal7b.InitChecksum(roachpb.Key("b"))

	expVal7q := roachpb.Value{Timestamp: ts7}
	expVal7q.SetInt(7)
	expVal7q.InitChecksum(roachpb.Key("q"))

	sstFile = &storage.MemFile{}
	sstWriter = storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstWriter.Close()
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("b"), Timestamp: ts7},
		expVal7b.RawBytes))
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("q"), Timestamp: ts7},
		expVal7q.RawBytes))
	require.NoError(t, sstWriter.Finish())

	_, pErr = store1.DB().AddSSTableAtBatchTimestamp(ctx, roachpb.Key("b"), roachpb.Key("r"), sstFile.Data(),
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		true /* ingestAsWrites */, ts7)
	require.Nil(t, pErr)

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
		{SST: &roachpb.RangeFeedSSTable{
			// Binary representation of Data may be modified by SST rewrite, see checkForExpEvents.
			Data: expSST, Span: expSSTSpan, WriteTS: ts6,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal7b, PrevValue: expVal6b,
		}},
		{Val: &roachpb.RangeFeedValue{
			Key: roachpb.Key("q"), Value: expVal7q, PrevValue: expVal6q,
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
	gcReq.Key = startKey
	gcReq.EndKey = firstStore.LookupReplica(startKey).Desc().EndKey.AsRawKey()
	var ba roachpb.BatchRequest
	ba.RangeID = rangeID
	ba.Add(gcReq)
	if _, pErr := firstStore.Send(ctx, ba); pErr != nil {
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
		for i := 0; i < numNodes; i++ {
			ts := tc.Servers[i]
			store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
			if pErr != nil {
				t.Fatal(pErr)
			}
			repl := store.LookupReplica(startKey)
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

			if pErr := store.RangeFeed(&req, stream); !testutils.IsPError(
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	_, rdesc, err := s.ScratchRangeWithExpirationLeaseEx()
	require.NoError(t, err)

	// Establish a rangefeed on the replica we plan to remove.
	stream := newTestStream()
	req := roachpb.RangeFeedRequest{
		Header: roachpb.Header{
			RangeID: store.LookupReplica(rdesc.StartKey).RangeID,
		},
		Span: roachpb.Span{Key: rdesc.StartKey.AsRawKey(), EndKey: rdesc.EndKey.AsRawKey()},
	}

	// Cancel the stream's context so that RangeFeed would return
	// immediately even if it didn't return the correct error.
	stream.Cancel()

	kvserver.RangefeedEnabled.Override(ctx, &store.ClusterSettings().SV, true)
	pErr := store.RangeFeed(&req, stream)
	const exp = "expiration-based leases are incompatible with rangefeeds"
	if !testutils.IsPError(pErr, exp) {
		t.Errorf("expected error %q, found %v", exp, pErr)
	}
}

func TestReplicaRangefeedRetryErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	mkRKey := func(s string) roachpb.RKey {
		return append(append([]byte(nil), keys.ScratchRangeMin...), s...)
	}
	mkKey := func(s string) roachpb.Key {
		return mkRKey(s).AsRawKey()
	}
	mkSpan := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: mkKey(start), EndKey: mkKey(end)}
	}
	startRKey := mkRKey("a")
	startKey := mkKey("a")

	setup := func(t *testing.T, knobs base.TestingKnobs) (*testcluster.TestCluster, roachpb.RangeID) {
		t.Helper()

		tc := testcluster.StartTestCluster(t, 3,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs:      base.TestServerArgs{Knobs: knobs},
			},
		)

		ts := tc.Servers[0]
		store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}
		tc.SplitRangeOrFatal(t, startKey)
		tc.AddVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
		rangeID := store.LookupReplica(startRKey).RangeID

		// Write to the RHS of the split and wait for all replicas to process it.
		// This ensures that all replicas have seen the split before we move on.
		incArgs := incrementArgs(mkKey("a"), 9)
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		tc.WaitForValues(t, mkKey("a"), []int64{9, 9, 9})
		return tc, rangeID
	}

	waitForInitialCheckpointAcrossSpan := func(
		t *testing.T, stream *testStream, streamErrC <-chan *roachpb.Error, span roachpb.Span,
	) {
		t.Helper()
		noResolveTimestampEvent := roachpb.RangeFeedEvent{
			Checkpoint: &roachpb.RangeFeedCheckpoint{
				Span:       span,
				ResolvedTS: hlc.Timestamp{},
			},
		}
		resolveTimestampEvent := roachpb.RangeFeedEvent{
			Checkpoint: &roachpb.RangeFeedCheckpoint{
				Span: span,
			},
		}
		var events []*roachpb.RangeFeedEvent
		testutils.SucceedsSoon(t, func() error {
			if len(streamErrC) > 0 {
				// Break if the error channel is already populated.
				return nil
			}
			events = stream.Events()
			if len(events) < 1 {
				return errors.Errorf("too few events: %v", events)
			}
			return nil
		})
		if len(streamErrC) > 0 {
			t.Fatalf("unexpected error from stream: %v", <-streamErrC)
		}
		expEvents := []*roachpb.RangeFeedEvent{&noResolveTimestampEvent}
		if len(events) > 1 {
			// Unfortunately there is a timing issue here and the range feed may
			// publish two checkpoints, one with a resolvedTs and one without, so we
			// check for either case.
			resolveTimestampEvent.Checkpoint.ResolvedTS = events[1].Checkpoint.ResolvedTS
			expEvents = []*roachpb.RangeFeedEvent{
				&noResolveTimestampEvent,
				&resolveTimestampEvent,
			}
		}
		if !reflect.DeepEqual(events, expEvents) {
			t.Fatalf("incorrect events on stream, found %v, want %v", events, expEvents)
		}

	}

	assertRangefeedRetryErr := func(
		t *testing.T, pErr *roachpb.Error, expReason roachpb.RangeFeedRetryError_Reason,
	) {
		t.Helper()
		expErr := roachpb.NewRangeFeedRetryError(expReason)
		if pErr == nil {
			t.Fatalf("got nil error for RangeFeed: expecting %v", expErr)
		}
		rfErr, ok := pErr.GetDetail().(*roachpb.RangeFeedRetryError)
		if !ok {
			t.Fatalf("got incorrect error for RangeFeed: %v; expecting %v", pErr, expErr)
		}
		if rfErr.Reason != expReason {
			t.Fatalf("got incorrect RangeFeedRetryError reason for RangeFeed: %v; expecting %v",
				rfErr.Reason, expReason)
		}
	}

	t.Run(roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED.String(), func(t *testing.T) {
		const removeStore = 2
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		// Establish a rangefeed on the replica we plan to remove.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := mkSpan("a", "z")
		ts := tc.Servers[removeStore]
		store, err := ts.Stores().GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- store.RangeFeed(&req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Remove the replica from the range.
		tc.RemoveVotersOrFatal(t, startKey, tc.Target(removeStore))

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		// Establish a rangefeed on the replica we plan to split.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := mkSpan("a", "z")
		ts := tc.Servers[0]
		store, err := ts.Stores().GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- store.RangeFeed(&req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Split the range.
		tc.SplitRangeOrFatal(t, mkKey("m"))

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RANGE_SPLIT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RANGE_MERGED.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		ts := tc.Servers[0]
		store, err := ts.Stores().GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		// Split the range.e
		splitRKey := mkRKey("m")
		splitKey := splitRKey.AsRawKey()
		tc.SplitRangeOrFatal(t, splitKey)
		if pErr := tc.WaitForSplitAndInitialization(splitKey); pErr != nil {
			t.Fatalf("Unexpected error waiting for range split: %v", pErr)
		}

		rightRangeID := store.LookupReplica(splitRKey).RangeID

		// Establish a rangefeed on the left replica.
		streamLeft := newTestStream()
		streamLeftErrC := make(chan *roachpb.Error, 1)
		rangefeedLeftSpan := roachpb.Span{Key: mkKey("a"), EndKey: splitKey}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedLeftSpan,
			}
			timer := time.AfterFunc(10*time.Second, streamLeft.Cancel)
			defer timer.Stop()
			streamLeftErrC <- store.RangeFeed(&req, streamLeft)
		}()

		// Establish a rangefeed on the right replica.
		streamRight := newTestStream()
		streamRightErrC := make(chan *roachpb.Error, 1)
		rangefeedRightSpan := roachpb.Span{Key: splitKey, EndKey: mkKey("z")}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedRightSpan,
			}
			timer := time.AfterFunc(10*time.Second, streamRight.Cancel)
			defer timer.Stop()
			streamRightErrC <- store.RangeFeed(&req, streamRight)
		}()

		// Wait for the first checkpoint event on each stream.
		waitForInitialCheckpointAcrossSpan(t, streamLeft, streamLeftErrC, rangefeedLeftSpan)
		waitForInitialCheckpointAcrossSpan(t, streamRight, streamRightErrC, rangefeedRightSpan)

		// Merge the ranges back together
		mergeArgs := adminMergeArgs(startKey)
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), mergeArgs); pErr != nil {
			t.Fatalf("merge saw unexpected error: %v", pErr)
		}

		// Check the errors.
		pErrLeft, pErrRight := <-streamLeftErrC, <-streamRightErrC
		assertRangefeedRetryErr(t, pErrLeft, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
		assertRangefeedRetryErr(t, pErrRight, roachpb.RangeFeedRetryError_REASON_RANGE_MERGED)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		ts2 := tc.Servers[2]
		partitionStore, err := ts2.Stores().GetStore(ts2.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		ts := tc.Servers[0]
		firstStore, err := ts.Stores().GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		secondStore, err := tc.Servers[1].Stores().GetStore(tc.Servers[1].GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}

		for _, server := range tc.Servers {
			store, err := server.Stores().GetStore(server.GetFirstStoreID())
			if err != nil {
				t.Fatal(err)
			}
			store.SetReplicaGCQueueActive(false)
		}

		// Establish a rangefeed on the replica we plan to partition.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)
		rangefeedSpan := mkSpan("a", "z")
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- partitionStore.RangeFeed(&req, stream)
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
			// NB: errors.Wrapf(nil, ...) returns nil.
			// nolint:errwrap
			return errors.Errorf("not raft follower: %+v, transferred lease: %v", raftStatus, err)
		})

		// Partition the replica from the rest of its range.
		partitionStore.Transport().Listen(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:            rangeID,
			RaftMessageHandler: partitionStore,
		})

		// Perform a write on the range.
		pArgs := putArgs(mkKey("c"), []byte("val2"))
		if _, pErr := kv.SendWrapped(ctx, firstStore.TestSender(), pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Get that command's log index.
		repl, err := firstStore.GetReplica(rangeID)
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
		if _, err := kv.SendWrapped(ctx, firstStore.TestSender(), truncArgs); err != nil {
			t.Fatal(err)
		}
		for _, store := range []*kvserver.Store{firstStore, secondStore} {
			_, err := store.GetReplica(rangeID)
			if err != nil {
				t.Fatal(err)
			}
			waitForTruncationForTesting(t, repl, index+1)
		}

		// Remove the partition. Snapshot should follow.
		partitionStore.Transport().Listen(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:            rangeID,
			RaftMessageHandler: partitionStore,
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
					// Make sure that even going forward no MsgApp for what we just truncated can
					// make it through. The Raft transport is asynchronous so this is necessary
					// to make the test pass reliably.
					// NB: the Index on the message is the log index that _precedes_ any of the
					// entries in the MsgApp, so filter where msg.Index < index, not <= index.
					return req.Message.Type == raftpb.MsgApp && req.Message.Index < index
				},
				dropHB:   func(*kvserverpb.RaftHeartbeat) bool { return false },
				dropResp: func(*kvserverpb.RaftMessageResponse) bool { return false },
			},
		})

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT)
	})
	t.Run(roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING.String(), func(t *testing.T) {
		knobs := base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// This test splits off a range manually from system table ranges.
				// Because this happens "underneath" the span configs infra, when
				// applying a config over the replicas, we fallback to one that
				// enables rangefeeds by default. The sub-test doesn't want that, so
				// we add an intercept and disable it for the test range.
				SetSpanConfigInterceptor: func(desc *roachpb.RangeDescriptor, conf roachpb.SpanConfig) roachpb.SpanConfig {
					if !desc.ContainsKey(roachpb.RKey(startKey)) {
						return conf
					}
					conf.RangefeedEnabled = false
					return conf
				},
			},
		}
		tc, _ := setup(t, knobs)
		defer tc.Stopper().Stop(ctx)

		ts := tc.Servers[0]
		store, err := ts.Stores().GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		// Split the range so that the RHS is not a system range and thus will
		// respect the rangefeed_enabled cluster setting.
		tc.SplitRangeOrFatal(t, startKey)

		rightRangeID := store.LookupReplica(roachpb.RKey(startKey)).RangeID

		// Establish a rangefeed.
		stream := newTestStream()
		streamErrC := make(chan *roachpb.Error, 1)

		endKey := keys.ScratchRangeMax
		rangefeedSpan := roachpb.Span{Key: startKey, EndKey: endKey}
		go func() {
			req := roachpb.RangeFeedRequest{
				Header: roachpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedSpan,
			}
			kvserver.RangefeedEnabled.Override(ctx, &store.ClusterSettings().SV, true)
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- store.RangeFeed(&req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Disable rangefeeds, which stops logical op logs from being provided
		// with Raft commands.
		kvserver.RangefeedEnabled.Override(ctx, &store.ClusterSettings().SV, false)

		// Perform a write on the range.
		writeKey := mkKey("c")
		pArgs := putArgs(writeKey, []byte("val2"))
		if _, pErr := kv.SendWrapped(ctx, store.TestSender(), pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, roachpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING)
	})
}

// TestReplicaRangefeedMVCCHistoryMutationError tests that rangefeeds are
// disconnected when an MVCC history mutation is applied.
func TestReplicaRangefeedMVCCHistoryMutationError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	splitKey := roachpb.Key("a")

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	ts := tc.Servers[0]
	store, err := ts.Stores().GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	tc.SplitRangeOrFatal(t, splitKey)
	tc.AddVotersOrFatal(t, splitKey, tc.Target(1), tc.Target(2))
	rangeID := store.LookupReplica(roachpb.RKey(splitKey)).RangeID

	// Write to the RHS of the split and wait for all replicas to process it.
	// This ensures that all replicas have seen the split before we move on.
	incArgs := incrementArgs(splitKey, 9)
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), incArgs)
	require.Nil(t, pErr)
	tc.WaitForValues(t, splitKey, []int64{9, 9, 9})

	// Set up a rangefeed across a-c.
	stream := newTestStream()
	streamErrC := make(chan *roachpb.Error, 1)
	go func() {
		req := roachpb.RangeFeedRequest{
			Header: roachpb.Header{RangeID: rangeID},
			Span:   roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		}
		timer := time.AfterFunc(10*time.Second, stream.Cancel)
		defer timer.Stop()
		streamErrC <- store.RangeFeed(&req, stream)
	}()

	// Wait for a checkpoint.
	require.Eventually(t, func() bool {
		if len(streamErrC) > 0 {
			require.Fail(t, "unexpected rangefeed error", "%v", <-streamErrC)
		}
		events := stream.Events()
		for _, event := range events {
			require.NotNil(t, event.Checkpoint, "received non-checkpoint event: %v", event)
		}
		return len(events) > 0
	}, 5*time.Second, 100*time.Millisecond)

	// Apply a ClearRange command that mutates MVCC history across c-e.
	// This does not overlap with the rangefeed registration, and should
	// not disconnect it.
	_, pErr = kv.SendWrapped(ctx, store.TestSender(), &roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    roachpb.Key("c"),
			EndKey: roachpb.Key("e"),
		},
	})
	require.Nil(t, pErr)
	if len(streamErrC) > 0 {
		require.Fail(t, "unexpected rangefeed error", "%v", <-streamErrC)
	}

	// Apply a ClearRange command that mutates MVCC history across b-e.
	// This overlaps with the rangefeed, and should disconnect it.
	_, pErr = kv.SendWrapped(ctx, store.TestSender(), &roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("e"),
		},
	})
	require.Nil(t, pErr)
	select {
	case pErr = <-streamErrC:
		require.NotNil(t, pErr)
		var mvccErr *roachpb.MVCCHistoryMutationError
		require.ErrorAs(t, pErr.GoError(), &mvccErr)
		require.Equal(t, &roachpb.MVCCHistoryMutationError{
			Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("e")},
		}, mvccErr)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for rangefeed disconnection")
	}
}

// TestReplicaRangefeedPushesTransactions tests that rangefeed detects intents
// that are holding up its resolved timestamp and periodically pushes them to
// ensure that its resolved timestamp continues to advance.
func TestReplicaRangefeedPushesTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, db, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, aggressiveResolvedTimestampClusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc, numNodes)

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
			rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []roachpb.Span{span}, ts1, false /* withDiff */, rangeFeedCh)
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
	ts2, err := tree.ParseHLC(ts2Str)
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

// Test that a rangefeed registration receives checkpoints even if the lease
// expires at some point. In other words, test that the rangefeed forces the
// range to maintain a lease (i.e. renew the lease when the old one expires). In
// particular, this test orchestrates a particularly tricky scenario - the
// current lease expiring and the replica with the rangefeed registration not
// being the Raft leader. In this case, the leader replica needs to acquire the
// lease, and it does so because of a read performed by
// r.ensureClosedTimestampStarted().
func TestRangefeedCheckpointsRecoverFromLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var scratchRangeID int64 // accessed atomically
	// nudgeSeen will be set if a request filter sees the signature of the
	// rangefeed nudger, as proof that the expected mechanism kicked in.
	var nudgeSeen int64 // accessed atomically
	// At some point, the test will require full control over the requests
	// evaluating on the scratch range.
	var rejectExtraneousRequests int64 // accessed atomically

	cargs := aggressiveResolvedTimestampClusterArgs
	cargs.ReplicationMode = base.ReplicationManual
	manualClock := hlc.NewHybridManualClock()
	cargs.ServerArgs = base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					// Once reject is set, the test wants full control over the requests
					// evaluating on the scratch range. On that range, we'll reject
					// everything that's not triggered by the test because we want to only
					// allow the rangefeed nudging mechanism to trigger a lease
					// acquisition (which it does through a LeaseInfoRequests). Allowing
					// other randos to trigger a lease acquisition would make the test
					// inconclusive.
					reject := atomic.LoadInt64(&rejectExtraneousRequests)
					if reject == 0 {
						return nil
					}
					scratch := atomic.LoadInt64(&scratchRangeID)
					if roachpb.RangeID(scratch) != ba.RangeID {
						return nil
					}
					if ba.IsSingleLeaseInfoRequest() {
						atomic.StoreInt64(&nudgeSeen, 1)
						return nil
					}
					nudged := atomic.LoadInt64(&nudgeSeen)
					if ba.IsLeaseRequest() && (nudged == 1) {
						return nil
					}
					log.Infof(ctx, "test rejecting request: %s", ba)
					return roachpb.NewErrorf("test injected error")
				},
			},
		},
	}
	tci := serverutils.StartNewTestCluster(t, 2, cargs)
	tc := tci.(*testcluster.TestCluster)
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	// Add a replica; we're going to move the lease to it below.
	desc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(1))
	atomic.StoreInt64(&scratchRangeID, int64(desc.RangeID))

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// Drop the target closedts duration in order to speed up the rangefeed
	// "nudging" once the range loses its lease.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'`)

	n1 := tc.Server(0)
	n2 := tc.Server(1)
	n2Target := tc.Target(1)
	ts1 := n1.Clock().Now()
	rangeFeedCtx, rangeFeedCancel := context.WithCancel(ctx)
	defer rangeFeedCancel()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	rangeFeedCh := make(chan *roachpb.RangeFeedEvent)
	rangeFeedErrC := make(chan error, 1)
	go func() {
		span := roachpb.Span{
			Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
		}
		rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []roachpb.Span{span}, ts1, false /* withDiff */, rangeFeedCh)
	}()

	// Wait for a checkpoint above ts.
	waitForCheckpoint := func(ts hlc.Timestamp) {
		t.Helper()
		checkpointed := false
		timeout := time.After(60 * time.Second)
		for !checkpointed {
			select {
			case event := <-rangeFeedCh:
				if c := event.Checkpoint; c != nil && ts.Less(c.ResolvedTS) {
					checkpointed = true
				}
			case err := <-rangeFeedErrC:
				t.Fatal(err)
			case <-timeout:
				t.Fatal("timed out waiting for checkpoint")
			}
		}
	}
	// Wait for a RangeFeed checkpoint after the RangeFeed initial scan time (ts1)
	// to make sure everything is set up. We intentionally don't care about the
	// spans in the checkpoints, just verifying that something has made it past
	// the initial scan and is running.
	waitForCheckpoint(ts1)

	// Move the lease from n1 to n2 in order for the Raft leadership to move with
	// it.
	tc.TransferRangeLeaseOrFatal(t, desc, n2Target)
	testutils.SucceedsSoon(t, func() error {
		leader := tc.GetRaftLeader(t, desc.StartKey).NodeID()
		if tc.Target(1).NodeID != leader {
			return errors.Errorf("leader still on n%d", leader)
		}
		return nil
	})

	// Expire the lease. Given that the Raft leadership is on n2, only n2 will be
	// eligible to acquire a new lease.
	log.Infof(ctx, "test expiring lease")
	nl := n2.NodeLiveness().(*liveness.NodeLiveness)
	resumeHeartbeats := nl.PauseAllHeartbeatsForTest()
	n2Liveness, ok := nl.Self()
	require.True(t, ok)
	manualClock.Increment(n2Liveness.Expiration.ToTimestamp().Add(1, 0).WallTime - manualClock.UnixNano())
	atomic.StoreInt64(&rejectExtraneousRequests, 1)
	// Ask another node to increment n2's liveness record.
	require.NoError(t, n1.NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(ctx, n2Liveness))
	resumeHeartbeats()

	// Wait for another RangeFeed checkpoint after the lease expired.
	log.Infof(ctx, "test waiting for another checkpoint")
	ts2 := n1.Clock().Now()
	waitForCheckpoint(ts2)
	nudged := atomic.LoadInt64(&nudgeSeen)
	require.Equal(t, int64(1), nudged)

	// Check that n2 renewed its lease, like the test intended.
	li, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
	require.NoError(t, err)
	require.True(t, li.Current().OwnedBy(n2.GetFirstStoreID()))
	require.Equal(t, int64(2), li.Current().Epoch)

	// Make sure the RangeFeed hasn't errored.
	select {
	case err := <-rangeFeedErrC:
		t.Fatal(err)
	default:
	}
	// Now cancel it and wait for it to shut down.
	rangeFeedCancel()
}

// Test that a new rangefeed that initially times out trying to ensure
// a lease is created on its targeted range will retry.
func TestNewRangefeedForceLeaseRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var scratchRangeID int64 // accessed atomically
	// nudgeSeen will be set if a request filter sees the signature of the
	// rangefeed nudger, as proof that the expected mechanism kicked in.
	var nudgeSeen int64 // accessed atomically
	// At some point, the test will require full control over the requests
	// evaluating on the scratch range.
	var rejectExtraneousRequests int64 // accessed atomically

	var timeoutSimulated bool

	cargs := aggressiveResolvedTimestampClusterArgs
	cargs.ReplicationMode = base.ReplicationManual
	manualClock := hlc.NewHybridManualClock()
	cargs.ServerArgs = base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manualClock.UnixNano,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {

					// Once reject is set, the test wants full control over the requests
					// evaluating on the scratch range. On that range, we'll reject
					// everything that's not triggered by the test because we want to only
					// allow the rangefeed nudging mechanism to trigger a lease
					// acquisition (which it does through a LeaseInfoRequests). Allowing
					// other randos to trigger a lease acquisition would make the test
					// inconclusive.
					reject := atomic.LoadInt64(&rejectExtraneousRequests)
					if reject == 0 {
						return nil
					}
					scratch := atomic.LoadInt64(&scratchRangeID)
					if roachpb.RangeID(scratch) != ba.RangeID {
						return nil
					}
					if ba.IsSingleLeaseInfoRequest() {
						atomic.StoreInt64(&nudgeSeen, 1)
						if !timeoutSimulated {
							mockTimeout := contextutil.RunWithTimeout(ctx, "test", 0,
								func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
							timeoutSimulated = true
							return roachpb.NewError(mockTimeout)
						}
						log.Infof(ctx, "lease succeeds this time")
						return nil
					}
					nudged := atomic.LoadInt64(&nudgeSeen)
					if ba.IsLeaseRequest() && (nudged == 1) {
						return nil
					}
					log.Infof(ctx, "test rejecting request: %s", ba)
					return roachpb.NewErrorf("test injected error")
				},
			},
		},
	}
	tci := serverutils.StartNewTestCluster(t, 2, cargs)
	tc := tci.(*testcluster.TestCluster)
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	// Add a replica; we're going to move the lease to it below.
	desc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(1))
	atomic.StoreInt64(&scratchRangeID, int64(desc.RangeID))

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	n1 := tc.Server(0)
	n2 := tc.Server(1)
	n2Target := tc.Target(1)
	ts1 := n1.Clock().Now()
	rangeFeedCtx, rangeFeedCancel := context.WithCancel(ctx)
	defer rangeFeedCancel()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	rangeFeedCh := make(chan *roachpb.RangeFeedEvent)
	rangeFeedErrC := make(chan error, 1)
	startRangefeed := func() {
		span := roachpb.Span{
			Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
		}
		rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []roachpb.Span{span}, ts1, false /* withDiff */, rangeFeedCh)
	}

	// Wait for a checkpoint above ts.
	waitForCheckpoint := func(ts hlc.Timestamp) {
		t.Helper()
		checkpointed := false
		timeout := time.After(60 * time.Second)
		for !checkpointed {
			select {
			case event := <-rangeFeedCh:
				if c := event.Checkpoint; c != nil && ts.Less(c.ResolvedTS) {
					checkpointed = true
				}
			case err := <-rangeFeedErrC:
				t.Fatal(err)
			case <-timeout:
				t.Fatal("timed out waiting for checkpoint")
			}
		}
	}

	// Move the lease from n1 to n2 in order for the Raft leadership to move with
	// it.
	tc.TransferRangeLeaseOrFatal(t, desc, n2Target)
	testutils.SucceedsSoon(t, func() error {
		leader := tc.GetRaftLeader(t, desc.StartKey).NodeID()
		if tc.Target(1).NodeID != leader {
			return errors.Errorf("leader still on n%d", leader)
		}
		return nil
	})

	// Expire the lease. Given that the Raft leadership is on n2, only n2 will be
	// eligible to acquire a new lease.
	log.Infof(ctx, "test expiring lease")
	nl := n2.NodeLiveness().(*liveness.NodeLiveness)
	resumeHeartbeats := nl.PauseAllHeartbeatsForTest()
	n2Liveness, ok := nl.Self()
	require.True(t, ok)
	manualClock.Increment(n2Liveness.Expiration.ToTimestamp().Add(1, 0).WallTime - manualClock.UnixNano())
	atomic.StoreInt64(&rejectExtraneousRequests, 1)
	// Ask another node to increment n2's liveness record.
	require.NoError(t, n1.NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(ctx, n2Liveness))

	resumeHeartbeats()

	go startRangefeed()

	// Wait for a RangeFeed checkpoint after the lease expired.
	log.Infof(ctx, "test waiting for another checkpoint")
	ts2 := n1.Clock().Now()
	waitForCheckpoint(ts2)
	nudged := atomic.LoadInt64(&nudgeSeen)
	require.Equal(t, int64(1), nudged)

	// Check that a lease now exists
	_, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
	require.NoError(t, err)

	// Make sure the RangeFeed hasn't errored.
	select {
	case err := <-rangeFeedErrC:
		t.Fatal(err)
	default:
	}
	// Now cancel it and wait for it to shut down.
	rangeFeedCancel()

}
