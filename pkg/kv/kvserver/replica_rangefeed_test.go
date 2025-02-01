// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// testStream is a mock implementation of kvpb.RangeFeedEventSink.
type testStream struct {
	ctx    context.Context
	cancel func()
	done   chan *kvpb.Error
	mu     struct {
		syncutil.Mutex
		events []*kvpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, cancel: cancel, done: make(chan *kvpb.Error, 1)}
}

func (s *testStream) SendMsg(m interface{}) error  { panic("unimplemented") }
func (s *testStream) RecvMsg(m interface{}) error  { panic("unimplemented") }
func (s *testStream) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (s *testStream) SendHeader(metadata.MD) error { panic("unimplemented") }
func (s *testStream) SetTrailer(metadata.MD)       { panic("unimplemented") }

func (s *testStream) Cancel() {
	s.cancel()
}

func (s *testStream) SendUnbufferedIsThreadSafe() {}

func (s *testStream) SendUnbuffered(e *kvpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, e)
	return nil
}

func (s *testStream) Events() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.events
}

// SendError implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (s *testStream) SendError(error *kvpb.Error) {
	s.done <- error
}

// WaitForError waits for the rangefeed to complete and returns the error sent
// to the done channel. It fails the test if rangefeed cannot complete within 30
// seconds.
func (s *testStream) WaitForError(t *testing.T) error {
	select {
	case err := <-s.done:
		return err.GoError()
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatalf("time out waiting for rangefeed completion")
		return nil
	}
}

func waitRangeFeed(
	t *testing.T, store *kvserver.Store, req *kvpb.RangeFeedRequest, stream *testStream,
) error {
	if _, err := store.RangeFeed(stream.ctx, req, stream, nil /* perConsumerCatchupLimiter */); err != nil {
		return err
	}
	return stream.WaitForError(t)
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
	firstStore, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
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
	if _, pErr := kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts1}, incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	tc.WaitForValues(t, roachpb.Key("b"), []int64{9, 9, 9, 9, 9})

	streams := make([]*testStream, numNodes)
	streamErrC := make(chan error, numNodes)
	rangefeedSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	for i := 0; i < numNodes; i++ {
		stream := newTestStream()
		streams[i] = stream
		srv := tc.Servers[i]
		store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func(i int) {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					Timestamp: initTime,
					RangeID:   rangeID,
				},
				Span:          rangefeedSpan,
				WithDiff:      true,
				WithFiltering: true,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}(i)
	}

	checkForExpEvents := func(expEvents []*kvpb.RangeFeedEvent) {
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
			var events []*kvpb.RangeFeedEvent
			testutils.SucceedsSoon(t, func() error {
				if len(streamErrC) > 0 {
					// Break if the error channel is already populated.
					return nil
				}

				events = stream.Events()
				// Filter out checkpoints. Those are not deterministic; they can come at any time.
				var filteredEvents []*kvpb.RangeFeedEvent
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
				expIter, err := storage.NewMemSSTIterator(sst.expect, false, storage.IterOptions{
					LowerBound: keys.MinKey,
					UpperBound: keys.MaxKey,
				})
				require.NoError(t, err)
				defer expIter.Close()

				sstIter, err := storage.NewMemSSTIterator(sst.actual, false, storage.IterOptions{
					LowerBound: keys.MinKey,
					UpperBound: keys.MaxKey,
				})
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

					checkValErr := func(v []byte, err error) []byte {
						require.NoError(t, err)
						return v
					}
					expKey, expValue := expIter.UnsafeKey(), checkValErr(expIter.UnsafeValue())
					sstKey, sstValue := sstIter.UnsafeKey(), checkValErr(sstIter.UnsafeValue())
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
	expEvents := []*kvpb.RangeFeedEvent{
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal1,
		}},
	}
	checkForExpEvents(expEvents)

	// Insert a key non-transactionally.
	ts2 := initTime.Add(0, 2)
	pArgs := putArgs(roachpb.Key("c"), []byte("val2"))
	_, err := kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts2}, pArgs)
	if err != nil {
		t.Fatal(err)
	}

	server1 := tc.Servers[1]
	store1, pErr := server1.GetStores().(*kvserver.Stores).GetStore(server1.GetFirstStoreID())
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
	_, err = kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts4}, incArgs)
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
	sstFile := &storage.MemObject{}
	sstWriter := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstWriter.Close()
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("b"), Timestamp: ts6},
		storage.MVCCValue{Value: expVal6b}))
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("q"), Timestamp: ts6},
		storage.MVCCValue{Value: expVal6q}))
	require.NoError(t, sstWriter.Finish())
	expSST := sstFile.Data()
	expSSTSpan := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("r")}

	_, _, _, pErr = store1.DB().AddSSTableAtBatchTimestamp(ctx, roachpb.Key("b"), roachpb.Key("r"), sstFile.Data(),
		false /* disallowConflicts */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, ts6)
	require.Nil(t, pErr)

	// Ingest an SSTable as writes.
	ts7 := ts.Clock().Now().Add(0, 7)

	expVal7b := roachpb.Value{}
	expVal7b.SetInt(7)
	expVal7b.InitChecksum(roachpb.Key("b"))

	expVal7q := roachpb.Value{}
	expVal7q.SetInt(7)
	expVal7q.InitChecksum(roachpb.Key("q"))

	sstFile = &storage.MemObject{}
	sstWriter = storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstWriter.Close()
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("b"), Timestamp: ts7},
		storage.MVCCValue{Value: expVal7b}))
	require.NoError(t, sstWriter.PutMVCC(
		storage.MVCCKey{Key: roachpb.Key("q"), Timestamp: ts7},
		storage.MVCCValue{Value: expVal7q}))
	require.NoError(t, sstWriter.Finish())

	_, _, _, pErr = store1.DB().AddSSTableAtBatchTimestamp(ctx, roachpb.Key("b"), roachpb.Key("r"), sstFile.Data(),
		false /* disallowConflicts */, hlc.Timestamp{}, nil, /* stats */
		true /* ingestAsWrites */, ts7)
	require.Nil(t, pErr)

	// Delete range non-transactionally.
	ts8 := initTime.Add(0, 8)
	dArgs := delRangeArgs(roachpb.Key("c"), roachpb.Key("d"), true /* useRangeTombstone */)
	_, err = kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts8}, dArgs)
	require.Nil(t, err)

	// Insert a key transactionally with omitInRangefeeds = true.
	ts9 := initTime.Add(0, 9)
	pErr = store1.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		pErr = txn.SetFixedTimestamp(ctx, ts9)
		require.Nil(t, pErr)
		txn.SetOmitInRangefeeds()
		return txn.Put(ctx, roachpb.Key("n"), []byte("val"))
	})
	require.Nil(t, err)
	// Read to force intent resolution.
	_, pErr = store1.DB().Get(ctx, roachpb.Key("n"))
	require.Nil(t, err)

	// Delete range transactionally with omitInRangefeeds = true.
	ts10 := initTime.Add(0, 10)
	pErr = store1.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		pErr = txn.SetFixedTimestamp(ctx, ts10)
		require.Nil(t, pErr)
		txn.SetOmitInRangefeeds()
		_, err := txn.DelRange(ctx, "f", "g", false)
		return err
	})
	require.Nil(t, pErr)
	// Read to force intent resolution.
	_, pErr = store1.DB().Get(ctx, roachpb.Key("f"))
	require.Nil(t, pErr)

	// Insert a key non-transactionally.
	ts11 := initTime.Add(0, 11)
	pArgs = putArgs(roachpb.Key("o"), []byte("val11"))
	_, err = kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts11}, pArgs)
	require.Nil(t, err)

	// Insert a key transactionally with omitInRangefeeds = true and 1PC.
	ts12 := initTime.Add(0, 12)
	pErr = store1.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		pErr = txn.SetFixedTimestamp(ctx, ts12)
		require.Nil(t, pErr)
		txn.SetOmitInRangefeeds()
		b := txn.NewBatch()
		b.Put(roachpb.Key("o"), []byte("val12"))
		return txn.CommitInBatch(ctx, b)
	})
	require.Nil(t, err)

	// Insert a key non-transactionally.
	ts13 := initTime.Add(0, 13)
	pArgs = putArgs(roachpb.Key("o"), []byte("val13"))
	_, err = kv.SendWrappedWith(ctx, db, kvpb.Header{Timestamp: ts13}, pArgs)
	require.Nil(t, err)

	// Wait for all streams to observe the expected events.
	expVal2 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val2"), ts2)
	expVal3 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val3"), ts3)
	expVal3.InitChecksum([]byte("m")) // kv.Txn sets value checksum
	expVal4 := roachpb.Value{Timestamp: ts4}
	expVal4.SetInt(18)
	expVal4.InitChecksum(roachpb.Key("b"))
	expVal5 := roachpb.Value{Timestamp: ts5}
	expVal5.SetInt(25)
	expVal5.InitChecksum(roachpb.Key("b"))
	expVal7b.Timestamp = ts7
	expVal7q.Timestamp = ts7
	expVal1NoTS, expVal4NoTS := expVal1, expVal4
	expVal1NoTS.Timestamp, expVal4NoTS.Timestamp = hlc.Timestamp{}, hlc.Timestamp{}
	expVal11 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val11"), ts11)
	expVal12 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val12"), ts12)
	expVal12.InitChecksum([]byte("o")) // kv.Txn sets value checksum
	expVal12NoTS := expVal12
	expVal12NoTS.Timestamp = hlc.Timestamp{}
	expVal13 := roachpb.MakeValueFromBytesAndTimestamp([]byte("val13"), ts13)
	expEvents = append(expEvents, []*kvpb.RangeFeedEvent{
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("c"), Value: expVal2,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("m"), Value: expVal3,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal4, PrevValue: expVal1NoTS,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal5, PrevValue: expVal4NoTS,
		}},
		{SST: &kvpb.RangeFeedSSTable{
			// Binary representation of Data may be modified by SST rewrite, see checkForExpEvents.
			Data: expSST, Span: expSSTSpan, WriteTS: ts6,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("b"), Value: expVal7b, PrevValue: expVal6b,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("q"), Value: expVal7q, PrevValue: expVal6q,
		}},
		{DeleteRange: &kvpb.RangeFeedDeleteRange{
			Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, Timestamp: ts8,
		}},
		{Val: &kvpb.RangeFeedValue{
			Key: roachpb.Key("o"), Value: expVal11,
		}},
		{Val: &kvpb.RangeFeedValue{
			// Even though the event that wrote val12 is filtered out, we want to keep
			// val2 as a previous value of the next event.
			Key: roachpb.Key("o"), Value: expVal13, PrevValue: expVal12NoTS,
		}},
	}...)
	// here
	checkForExpEvents(expEvents)

	// Cancel each of the rangefeed streams.
	for _, stream := range streams {
		stream.Cancel()
		require.True(t, errors.Is(<-streamErrC, context.Canceled))
	}

	// Bump the GC threshold and assert that RangeFeed below the timestamp will
	// catch an error.
	gcReq := &kvpb.GCRequest{
		Threshold: initTime.Add(0, 1),
	}
	gcReq.Key = startKey
	gcReq.EndKey = firstStore.LookupReplica(startKey).Desc().EndKey.AsRawKey()
	ba := &kvpb.BatchRequest{}
	ba.RangeID = rangeID
	ba.Add(gcReq)
	if _, pErr := firstStore.Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	req := kvpb.RangeFeedRequest{
		Header: kvpb.Header{
			Timestamp: initTime,
			RangeID:   rangeID,
		},
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
	}

	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < numNodes; i++ {
			ts := tc.Servers[i]
			store, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
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

			if pErr := waitRangeFeed(t, store, &req, stream); !testutils.IsError(
				pErr, `must be after replica GC threshold`,
			) {
				return pErr
			}
		}
		return nil
	})
}

// setupSimpleRangefeed creates a range on a 3 node cluster starting at key "a",
// and begins a test rangefeed from [a,d).
func setupSimpleRangefeed(
	ctx context.Context, t *testing.T, tc *testcluster.TestCluster,
) (stream *testStream, streamErrC chan error) {
	splitKey := roachpb.Key("a")
	ts := tc.Servers[0]
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
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

	// Set up a rangefeed across a-d.
	stream = newTestStream()
	streamErrC = make(chan error, 1)
	go func() {
		req := kvpb.RangeFeedRequest{
			Header:                kvpb.Header{RangeID: rangeID},
			Span:                  roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")},
			WithMatchingOriginIDs: []uint32{0},
			WithDiff:              true,
		}
		timer := time.AfterFunc(10*time.Second, stream.Cancel)
		defer timer.Stop()
		streamErrC <- waitRangeFeed(t, store, &req, stream)
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
	return stream, streamErrC
}

// TestReplicaRangefeedOriginIDFiltering tests that a rangefeed configured to
// emit events from OriginID 0 will filter remote events but include them as a
// previous event.
func TestReplicaRangefeedOriginIDFiltering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	stream, streamErrC := setupSimpleRangefeed(ctx, t, tc)

	store := tc.GetFirstStoreFromServer(t, 0)
	b1Args := putArgs(roachpb.Key("b"), []byte("b1"))
	_, err := kv.SendWrapped(ctx, store.TestSender(), b1Args)
	require.Nil(t, err)

	b2Args := putArgs(roachpb.Key("b"), []byte("b2"))
	_, err = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{WriteOptions: &kvpb.WriteOptions{OriginID: 1}}, b2Args)
	require.Nil(t, err)

	// Send with Txn to exercise MVCCCommitIntent logical op path.
	if err := store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		batch := txn.NewBatch()
		batch.Put(roachpb.Key("c"), []byte("c3"))
		batch.Header.WriteOptions = &kvpb.WriteOptions{OriginID: 1}
		return txn.Run(ctx, batch)
	}); err != nil {
		t.Fatal(err)
	}

	// Read to force intent resolution.
	_, getErr := store.DB().Get(ctx, roachpb.Key("c"))
	require.Nil(t, getErr)

	c4Args := putArgs(roachpb.Key("c"), []byte("c4"))
	_, err = kv.SendWrapped(ctx, store.TestSender(), c4Args)
	require.Nil(t, err)

	expCValue := roachpb.MakeValueFromBytes([]byte("c4"))

	expPrevCValue := roachpb.MakeValueFromBytes([]byte("c3"))
	expPrevCValue.InitChecksum([]byte("c"))

	expEvents := []*kvpb.RangeFeedEvent{
		{Val: &kvpb.RangeFeedValue{
			Key:   roachpb.Key("b"),
			Value: roachpb.MakeValueFromBytes([]byte("b1")),
		}},
		{Val: &kvpb.RangeFeedValue{
			Key:       roachpb.Key("c"),
			Value:     expCValue,
			PrevValue: expPrevCValue,
		}},
	}

	testutils.SucceedsSoon(t, func() error {
		if len(streamErrC) > 0 {
			// Break if the error channel is already populated.
			return nil
		}

		events := stream.Events()
		// Filter out checkpoints.
		var filteredEvents []*kvpb.RangeFeedEvent
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
		// Ensure the key and value are the same (dont worry about the timestamp).
		for i, e := range events {
			require.Equal(t, expEvents[i].Val.Key, e.Val.Key, "key mismatch")
			require.Equal(t, expEvents[i].Val.Value.RawBytes, e.Val.Value.RawBytes, "value mismatch")
			if e.Val.PrevValue.RawBytes != nil {
				require.Equal(t, expEvents[i].Val.PrevValue.RawBytes, e.Val.PrevValue.RawBytes, "prev value mismatch")
			}
		}
		return nil
	})
}

func TestReplicaRangefeedErrors(t *testing.T) {
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
		store, pErr := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
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
		t *testing.T, stream *testStream, streamErrC <-chan error, span roachpb.Span,
	) {
		t.Helper()
		var events []*kvpb.RangeFeedEvent
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

		var lastTS hlc.Timestamp
		for _, evt := range events {
			require.NotNil(t, evt.Checkpoint, "expected only checkpoint events")
			require.Equal(t, span, evt.Checkpoint.Span)
			require.True(t, evt.Checkpoint.ResolvedTS.After(lastTS) || evt.Checkpoint.ResolvedTS.Equal(lastTS), "unexpected resolved timestamp regression")
			lastTS = evt.Checkpoint.ResolvedTS
		}
	}

	assertRangefeedRetryErr := func(
		t *testing.T, pErr error, expReason kvpb.RangeFeedRetryError_Reason,
	) {
		t.Helper()
		expErr := kvpb.NewRangeFeedRetryError(expReason)
		if pErr == nil {
			t.Fatalf("got nil error for RangeFeed: expecting %v", expErr)
		}
		rfErr, ok := kvpb.NewError(pErr).GetDetail().(*kvpb.RangeFeedRetryError)
		if !ok {
			t.Fatalf("got incorrect error for RangeFeed: %v; expecting %v", pErr, expErr)
		}
		if rfErr.Reason != expReason {
			t.Fatalf("got incorrect RangeFeedRetryError reason for RangeFeed: %v; expecting %v",
				rfErr.Reason, expReason)
		}
	}

	t.Run(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED.String(), func(t *testing.T) {
		const removeStore = 2
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		// Establish a rangefeed on the replica we plan to remove.
		stream := newTestStream()
		streamErrC := make(chan error, 1)
		rangefeedSpan := mkSpan("a", "z")
		ts := tc.Servers[removeStore]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Remove the replica from the range.
		tc.RemoveVotersOrFatal(t, startKey, tc.Target(removeStore))

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)
	})
	t.Run(kvpb.RangeFeedRetryError_REASON_MANUAL_RANGE_SPLIT.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		// Establish a rangefeed on the replica we plan to split.
		stream := newTestStream()
		streamErrC := make(chan error, 1)
		rangefeedSpan := mkSpan("a", "z")
		ts := tc.Servers[0]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)

		// Split the range.
		tc.SplitRangeOrFatal(t, mkKey("m"))

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, kvpb.RangeFeedRetryError_REASON_MANUAL_RANGE_SPLIT)
	})
	t.Run(kvpb.RangeFeedRetryError_REASON_RANGE_MERGED.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		ts := tc.Servers[0]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
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
		streamLeftErrC := make(chan error, 1)
		rangefeedLeftSpan := roachpb.Span{Key: mkKey("a"), EndKey: splitKey}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedLeftSpan,
			}
			timer := time.AfterFunc(10*time.Second, streamLeft.Cancel)
			defer timer.Stop()
			streamLeftErrC <- waitRangeFeed(t, store, &req, streamLeft)
		}()

		// Establish a rangefeed on the right replica.
		streamRight := newTestStream()
		streamRightErrC := make(chan error, 1)
		rangefeedRightSpan := roachpb.Span{Key: splitKey, EndKey: mkKey("z")}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedRightSpan,
			}
			timer := time.AfterFunc(10*time.Second, streamRight.Cancel)
			defer timer.Stop()
			streamRightErrC <- waitRangeFeed(t, store, &req, streamRight)
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
		assertRangefeedRetryErr(t, pErrLeft, kvpb.RangeFeedRetryError_REASON_RANGE_MERGED)
		assertRangefeedRetryErr(t, pErrRight, kvpb.RangeFeedRetryError_REASON_RANGE_MERGED)
	})
	t.Run(kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT.String(), func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		ts2 := tc.Servers[2]
		partitionStore, err := ts2.GetStores().(*kvserver.Stores).GetStore(ts2.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		ts := tc.Servers[0]
		firstStore, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		secondStore, err := tc.Servers[1].GetStores().(*kvserver.Stores).GetStore(tc.Servers[1].GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}

		for _, server := range tc.Servers {
			store, err := server.GetStores().(*kvserver.Stores).GetStore(server.GetFirstStoreID())
			if err != nil {
				t.Fatal(err)
			}
			store.SetReplicaGCQueueActive(false)
		}

		// Establish a rangefeed on the replica we plan to partition.
		stream := newTestStream()
		streamErrC := make(chan error, 1)
		rangefeedSpan := mkSpan("a", "z")
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, partitionStore, &req, stream)
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
			if raftStatus != nil && raftStatus.RaftState == raftpb.StateFollower {
				return nil
			}
			err = repl.AdminTransferLease(ctx, roachpb.StoreID(1), false /* bypassSafetyChecks */)
			// NB: errors.Wrapf(nil, ...) returns nil.
			// nolint:errwrap
			return errors.Errorf("not raft follower: %+v, transferred lease: %v", raftStatus, err)
		})

		// Partition the replica from the rest of its range.
		partitionStore.Transport().ListenIncomingRaftMessages(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:                    rangeID,
			IncomingRaftMessageHandler: partitionStore,
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
		index := repl.GetLastIndex()

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
		partitionStore.Transport().ListenIncomingRaftMessages(partitionStore.Ident.StoreID, &unreliableRaftHandler{
			rangeID:                    rangeID,
			IncomingRaftMessageHandler: partitionStore,
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropReq: func(req *kvserverpb.RaftMessageRequest) bool {
					// Make sure that even going forward no MsgApp for what we just truncated can
					// make it through. The Raft transport is asynchronous so this is necessary
					// to make the test pass reliably.
					// NB: the Index on the message is the log index that _precedes_ any of the
					// entries in the MsgApp, so filter where msg.Index < index, not <= index.
					return req.Message.Type == raftpb.MsgApp && kvpb.RaftIndex(req.Message.Index) < index
				},
				dropHB:   func(*kvserverpb.RaftHeartbeat) bool { return false },
				dropResp: func(*kvserverpb.RaftMessageResponse) bool { return false },
			},
		})

		// Check the error.
		pErr := <-streamErrC
		assertRangefeedRetryErr(t, pErr, kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT)
	})
	t.Run(kvpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING.String(), func(t *testing.T) {
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
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		// Split the range so that the RHS is not a system range and thus will
		// respect the rangefeed_enabled cluster setting.
		tc.SplitRangeOrFatal(t, startKey)

		rightRangeID := store.LookupReplica(roachpb.RKey(startKey)).RangeID

		// Establish a rangefeed.
		stream := newTestStream()
		streamErrC := make(chan error, 1)

		endKey := keys.ScratchRangeMax
		rangefeedSpan := roachpb.Span{Key: startKey, EndKey: endKey}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedSpan,
			}
			kvserver.RangefeedEnabled.Override(ctx, &store.ClusterSettings().SV, true)
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
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
		assertRangefeedRetryErr(t, pErr, kvpb.RangeFeedRetryError_REASON_LOGICAL_OPS_MISSING)
	})
	t.Run("range key mismatch", func(t *testing.T) {
		knobs := base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Use a span config override to check that we get a key mismatch error
				// despite the span config's setting whenever the key is outside the
				// bounds of the range.
				SetSpanConfigInterceptor: func(desc *roachpb.RangeDescriptor, conf roachpb.SpanConfig) roachpb.SpanConfig {
					if desc.ContainsKey(roachpb.RKey(keys.ScratchRangeMin)) {
						conf.RangefeedEnabled = false
						return conf
					} else if desc.ContainsKey(startRKey) {
						conf.RangefeedEnabled = true
						return conf
					}
					return conf
				},
			},
		}
		tc, _ := setup(t, knobs)
		defer tc.Stopper().Stop(ctx)

		ts := tc.Servers[0]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		// Split the range so that the RHS should have a span config with
		// rangefeeds enabled (like a range on a system table would), while the
		// LHS does not. A rangefeed request on the LHS should still return a
		// RangeKeyMismatchError given the span is outside the range, even though
		// rangefeeds are not enabled.
		tc.SplitRangeOrFatal(t, startKey)

		leftReplica := store.LookupReplica(roachpb.RKey(keys.ScratchRangeMin))
		leftRangeID := leftReplica.RangeID
		rightReplica := store.LookupReplica(startRKey)
		rightRangeID := rightReplica.RangeID

		// Attempt to establish a rangefeed, sending the request to the LHS.
		stream := newTestStream()
		streamErrC := make(chan error, 1)

		endKey := keys.ScratchRangeMax
		rangefeedSpan := roachpb.Span{Key: startKey, EndKey: endKey}

		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: leftRangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}()

		// Check the error.
		pErr := <-streamErrC
		require.True(t, errors.HasType(pErr, &kvpb.RangeKeyMismatchError{}),
			"got incorrect error for RangeFeed: %v; expecting RangeKeyMismatchError", pErr)

		// Now send the range feed request to the correct replica, which should not
		// encounter errors.
		stream = newTestStream()
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rightRangeID,
				},
				Span: rangefeedSpan,
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}()

		// Wait for the first checkpoint event.
		waitForInitialCheckpointAcrossSpan(t, stream, streamErrC, rangefeedSpan)
	})
	t.Run("multiple-origin-ids", func(t *testing.T) {
		tc, rangeID := setup(t, base.TestingKnobs{})
		defer tc.Stopper().Stop(ctx)

		stream := newTestStream()
		streamErrC := make(chan error, 1)
		rangefeedSpan := mkSpan("a", "z")
		ts := tc.Servers[0]
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			req := kvpb.RangeFeedRequest{
				Header: kvpb.Header{
					RangeID: rangeID,
				},
				Span:                  rangefeedSpan,
				WithMatchingOriginIDs: []uint32{0, 1},
			}
			timer := time.AfterFunc(10*time.Second, stream.Cancel)
			defer timer.Stop()
			streamErrC <- waitRangeFeed(t, store, &req, stream)
		}()

		// Check the error.
		pErr := <-streamErrC
		require.Equal(t, pErr.Error(), "multiple origin IDs and OriginID != 0 not supported yet")
	})
}

// TestReplicaRangefeedMVCCHistoryMutationError tests that rangefeeds are
// disconnected when an MVCC history mutation is applied.
func TestReplicaRangefeedMVCCHistoryMutationError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	_, streamErrC := setupSimpleRangefeed(ctx, t, tc)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Apply a ClearRange command that mutates MVCC history across c-e.
	// This does not overlap with the rangefeed registration, and should
	// not disconnect it.
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), &kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("d"),

			EndKey: roachpb.Key("e"),
		},
	})
	require.Nil(t, pErr)
	if len(streamErrC) > 0 {
		require.Fail(t, "unexpected rangefeed error", "%v", <-streamErrC)
	}

	// Apply a ClearRange command that mutates MVCC history across b-e.
	// This overlaps with the rangefeed, and should disconnect it.
	_, pErr = kv.SendWrapped(ctx, store.TestSender(), &kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("e"),
		},
	})
	require.Nil(t, pErr)
	select {
	case streamErr := <-streamErrC:
		require.NotNil(t, streamErr)
		var mvccErr *kvpb.MVCCHistoryMutationError
		require.ErrorAs(t, streamErr, &mvccErr)
		require.Equal(t, &kvpb.MVCCHistoryMutationError{
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
	cArgs := aggressiveResolvedTimestampManuallyReplicatedClusterArgs
	tc, db, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration, 0, cArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	repls := replsForRange(ctx, t, tc, desc)

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
	rangeFeedChs := make([]chan kvcoord.RangeFeedMessage, len(repls))
	rangeFeedErrC := make(chan error, len(repls))
	for i := range repls {
		desc := repls[i].Desc()
		ds := tc.Server(i).DistSenderI().(*kvcoord.DistSender)
		rangeFeedCh := make(chan kvcoord.RangeFeedMessage)
		rangeFeedChs[i] = rangeFeedCh
		go func() {
			span := roachpb.Span{
				Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
			}
			rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []kvcoord.SpanTimePair{{Span: span, StartAfter: ts1}}, rangeFeedCh)
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
	ts2, err := hlc.ParseHLC(ts2Str)
	require.NoError(t, err)

	// Wait for the RangeFeed checkpoint on each RangeFeed to exceed this timestamp.
	// For this to be possible, it must push the transaction's timestamp forward.
	waitForCheckpoint(ts2)

	// The txn should not be able to commit since its commit timestamp was pushed
	// and it has observed its timestamp.
	require.Regexp(t, "TransactionRetryWithProtoRefreshError", tx1.Commit())

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

	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	var storeLivenessHeartbeatsOff atomic.Value
	cargs := aggressiveResolvedTimestampManuallyReplicatedClusterArgs
	manualClock := hlc.NewHybridManualClock()
	cargs.ServerArgs = base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manualClock,
			},
			Store: &kvserver.StoreTestingKnobs{
				DisableMaxOffsetCheck: true, // has been seen to cause flakes
				TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
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
					if ba.IsSingleRequestLeaseRequest() && (nudged == 1) {
						return nil
					}
					log.Infof(ctx, "test rejecting request: %s", ba)
					return kvpb.NewErrorf("test injected error")
				},
				StoreLivenessKnobs: &storeliveness.TestingKnobs{
					SupportManagerKnobs: storeliveness.SupportManagerKnobs{
						DisableHeartbeats: &storeLivenessHeartbeatsOff,
					},
				},
			},
		},
	}
	tci := serverutils.StartCluster(t, 2, cargs)
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
	rangeFeedCh := make(chan kvcoord.RangeFeedMessage)
	rangeFeedErrC := make(chan error, 1)
	go func() {
		span := roachpb.Span{
			Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
		}
		rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []kvcoord.SpanTimePair{{Span: span, StartAfter: ts1}}, rangeFeedCh)
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

	// Wait for the expiration based lease to upgrade to either an epoch or a
	// leader lease. If we run up the clock here, the test will be faster, but
	// it will disrupt store liveness (support will be withdrawn).
	var firstLease roachpb.Lease
	testutils.SucceedsSoon(t, func() error {
		repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(scratchKey))
		leaseStatus := repl.CurrentLeaseStatus(ctx)
		if leaseStatus.Lease.Type() == roachpb.LeaseExpiration {
			return errors.Errorf("lease still an expiration based lease")
		}
		firstLease = leaseStatus.Lease
		return nil
	})

	atomic.StoreInt64(&rejectExtraneousRequests, 1)
	if firstLease.Type() == roachpb.LeaseEpoch {
		// Expire the lease. Given that the Raft leadership is on n2, only n2 will be
		// eligible to acquire a new lease.
		log.Infof(ctx, "test expiring lease")
		nl2 := n2.NodeLiveness().(*liveness.NodeLiveness)
		resumeHeartbeats := nl2.PauseAllHeartbeatsForTest()
		n2Liveness, ok := nl2.Self()
		require.True(t, ok)
		manualClock.Increment(max(firstLease.MinExpiration.WallTime, n2Liveness.Expiration.ToTimestamp().
			Add(1, 0).WallTime) - manualClock.UnixNano())

		// Ask another node to increment n2's liveness record, but first, wait until
		// n1's liveness state is the same as n2's. Otherwise, the epoch below might
		// get rejected because of mismatching liveness records.
		testutils.SucceedsSoon(t, func() error {
			nl1 := n1.NodeLiveness().(*liveness.NodeLiveness)
			n2LivenessFromN1, _ := nl1.GetLiveness(n2.NodeID())
			if n2Liveness != n2LivenessFromN1.Liveness {
				return errors.Errorf("waiting for node 2 liveness to converge on both nodes 1 and 2")
			}
			return nil
		})
		require.NoError(t, n1.NodeLiveness().(*liveness.NodeLiveness).IncrementEpoch(ctx, n2Liveness))
		resumeHeartbeats()
	}

	if firstLease.Type() == roachpb.LeaseLeader {
		// Stop heartbeats on store 2.
		localStoreID := slpb.StoreIdent{NodeID: n2.NodeID(), StoreID: n2.GetFirstStoreID()}
		storeLivenessHeartbeatsOff.Store(localStoreID)
		// Ensure store liveness support is withdrawn.
		remoteStoreSM := tc.GetFirstStoreFromServer(t, 0).TestingStoreLivenessSupportManager()
		testutils.SucceedsSoon(t, func() error {
			_, supported := remoteStoreSM.SupportFor(localStoreID)
			if supported {
				return errors.Errorf("support from the leader not withdrawn yet")
			}
			return nil
		})
		// Ensure replica 2 loses the leadership.
		testutils.SucceedsSoon(t, func() error {
			repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(scratchKey))
			if repl.RaftStatus().RaftState == raftpb.StateLeader {
				return errors.Errorf("leader for r%v still on n2", desc)
			}
			return nil
		})
		storeLivenessHeartbeatsOff.Store(slpb.StoreIdent{})
	}

	// Wait for another RangeFeed checkpoint after the lease expired.
	log.Infof(ctx, "test waiting for another checkpoint")
	ts2 := n1.Clock().Now()
	waitForCheckpoint(ts2)
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&nudgeSeen) != int64(1) {
			return errors.Errorf("nudge not seen yet")
		}
		return nil
	})

	// Check that n2 renewed its lease, like the test intended.
	// Unfortunately this is flaky and it's not so clear how to fix it.
	// See: https://github.com/cockroachdb/cockroach/issues/102169
	// But even if the lease is on n1 the rangefeed shouldn't have errored.
	li, _, err := tc.FindRangeLeaseEx(ctx, desc, nil)
	require.NoError(t, err)
	curLease := li.Current()
	t.Logf("lease before expiration: %s", firstLease)
	t.Logf("lease after expiration: %s", curLease)
	// If the lease is an epoch lease, make sure the epoch is incremented.
	if curLease.OwnedBy(n2.GetFirstStoreID()) && curLease.Type() == roachpb.LeaseEpoch {
		require.Equal(t, int64(2), curLease.Epoch)
	}
	// If the lease is a leader lease, make sure the term is incremented.
	if curLease.Type() == roachpb.LeaseLeader {
		testutils.SucceedsSoon(t, func() error {
			li, _, err = tc.FindRangeLeaseEx(ctx, desc, nil)
			require.NoError(t, err)
			curLease = li.Current()
			if curLease.Term <= firstLease.Term {
				return errors.Errorf("lease term is still: %v", curLease.Term)
			}
			return nil
		})
	}

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

	cargs := aggressiveResolvedTimestampManuallyReplicatedClusterArgs
	manualClock := hlc.NewHybridManualClock()
	cargs.ServerArgs = base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manualClock,
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {

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
							mockTimeout := timeutil.RunWithTimeout(ctx, "test", 0,
								func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
							timeoutSimulated = true
							return kvpb.NewError(mockTimeout)
						}
						log.Infof(ctx, "lease succeeds this time")
						return nil
					}
					nudged := atomic.LoadInt64(&nudgeSeen)
					if ba.IsSingleRequestLeaseRequest() && (nudged == 1) {
						return nil
					}
					log.Infof(ctx, "test rejecting request: %s", ba)
					return kvpb.NewErrorf("test injected error")
				},
			},
		},
	}
	tci := serverutils.StartCluster(t, 2, cargs)
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
	rangeFeedCh := make(chan kvcoord.RangeFeedMessage)
	rangeFeedErrC := make(chan error, 1)
	rangefeedSpan := roachpb.Span{
		Key: desc.StartKey.AsRawKey(), EndKey: desc.EndKey.AsRawKey(),
	}
	startRangefeed := func() {
		span := rangefeedSpan
		rangeFeedErrC <- ds.RangeFeed(rangeFeedCtx, []kvcoord.SpanTimePair{{Span: span, StartAfter: ts1}}, rangeFeedCh)
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
				if !event.RegisteredSpan.Equal(rangefeedSpan) {
					t.Fatal("registered span in the message should be equal to " +
						"the span used to create the rangefeed")
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
	nl2 := n2.NodeLiveness().(*liveness.NodeLiveness)
	resumeHeartbeats := nl2.PauseAllHeartbeatsForTest()
	n2Liveness, ok := nl2.Self()
	require.True(t, ok)
	manualClock.Increment(n2Liveness.Expiration.ToTimestamp().Add(1, 0).WallTime - manualClock.UnixNano())
	atomic.StoreInt64(&rejectExtraneousRequests, 1)

	// Ask another node to increment n2's liveness record, but first, wait until
	// n1's liveness state is the same as n2's. Otherwise, the epoch below might
	// get rejected because of mismatching liveness records.
	testutils.SucceedsSoon(t, func() error {
		nl1 := n1.NodeLiveness().(*liveness.NodeLiveness)
		n2LivenessFromN1, _ := nl1.GetLiveness(n2.NodeID())
		if n2Liveness != n2LivenessFromN1.Liveness {
			return errors.Errorf("waiting for node 2 liveness to converge on both nodes 1 and 2")
		}
		return nil
	})
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

// TestRangefeedTxnPusherBarrierRangeKeyMismatch is a regression test for
// https://github.com/cockroachdb/cockroach/issues/119333
//
// Specifically, it tests that a Barrier call that encounters a
// RangeKeyMismatchError will eagerly attempt to refresh the DistSender range
// cache. The DistSender does not do this itself for unsplittable requests, so
// it might otherwise continually fail.
func TestRangefeedTxnPusherBarrierRangeKeyMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // too slow, times out
	skip.UnderDeadlock(t)

	// Use a timeout, to prevent a hung test.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a cluster with 3 nodes.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	defer cancel()

	n1 := tc.Server(0)
	n3 := tc.Server(2)
	db1 := n1.ApplicationLayer().DB()
	db3 := n3.ApplicationLayer().DB()

	// Split off a range and upreplicate it, with leaseholder on n1. This is the
	// range we'll run the barrier across.
	prefix := append(n1.ApplicationLayer().Codec().TenantPrefix(), keys.ScratchRangeMin...)
	_, _, err := n1.StorageLayer().SplitRange(prefix)
	require.NoError(t, err)
	desc := tc.AddVotersOrFatal(t, prefix, tc.Targets(1, 2)...)
	t.Logf("split off range %s", desc)

	rspan := desc.RSpan()
	span := rspan.AsRawSpanWithNoLocals()

	// Split off three other ranges.
	splitKeys := []roachpb.Key{
		append(prefix.Clone(), roachpb.Key("/a")...),
		append(prefix.Clone(), roachpb.Key("/b")...),
		append(prefix.Clone(), roachpb.Key("/c")...),
	}
	for _, key := range splitKeys {
		_, desc, err = n1.StorageLayer().SplitRange(key)
		require.NoError(t, err)
		t.Logf("split off range %s", desc)
	}

	// Scan the ranges on n3 to update the range caches, then run a barrier
	// request which should fail with RangeKeyMismatchError.
	_, err = db3.Scan(ctx, span.Key, span.EndKey, 0)
	require.NoError(t, err)

	_, _, err = db3.BarrierWithLAI(ctx, span.Key, span.EndKey)
	require.Error(t, err)
	require.IsType(t, &kvpb.RangeKeyMismatchError{}, err)
	t.Logf("n3 barrier returned %s", err)

	// Merge the ranges on n1.
	for range splitKeys {
		desc, err = n1.StorageLayer().MergeRanges(span.Key)
		require.NoError(t, err)
		t.Logf("merged range %s", desc)
	}

	// Barriers should now succeed on n1, which have an updated range cache, but
	// fail on n3 which doesn't.
	lai, _, err := db1.BarrierWithLAI(ctx, span.Key, span.EndKey)
	require.NoError(t, err)
	t.Logf("n1 barrier returned LAI %d", lai)

	// NB: this could potentially flake if n3 somehow updates its range cache. If
	// it does, we can remove this assertion, but it's nice to make sure we're
	// actually testing what we think we're testing.
	_, _, err = db3.BarrierWithLAI(ctx, span.Key, span.EndKey)
	require.Error(t, err)
	require.IsType(t, &kvpb.RangeKeyMismatchError{}, err)
	t.Logf("n3 barrier returned %s", err)

	// However, rangefeedTxnPusher.Barrier() will refresh the cache and
	// successfully apply the barrier.
	s3 := tc.GetFirstStoreFromServer(t, 2)
	repl3 := s3.LookupReplica(roachpb.RKey(span.Key))
	t.Logf("repl3 desc: %s", repl3.Desc())
	txnPusher := kvserver.NewRangefeedTxnPusher(nil, repl3, rspan)
	require.NoError(t, txnPusher.Barrier(ctx))
	t.Logf("n3 txnPusher barrier succeeded")
}
