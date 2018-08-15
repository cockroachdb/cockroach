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

	"google.golang.org/grpc/metadata"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
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

	// Send's contract does not promise that its provided events are safe for
	// use after it has returned. To work around this, make a clone.
	var eClone roachpb.RangeFeedEvent
	{
		b, err := protoutil.Marshal(e)
		if err != nil {
			panic(err)
		}
		err = protoutil.Unmarshal(b, &eClone)
		if err != nil {
			panic(err)
		}
	}

	s.mu.events = append(s.mu.events, &eClone)
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
	mtc.replicateRange(1, 1, 2)
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
			Span:       roachpb.Span{Key: nil, EndKey: roachpb.KeyMax},
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
