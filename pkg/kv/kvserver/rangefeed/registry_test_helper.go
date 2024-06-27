// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var (
	keyA, keyB = roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD = roachpb.Key("c"), roachpb.Key("d")
	keyX, keyY = roachpb.Key("x"), roachpb.Key("y")

	spAB = roachpb.Span{Key: keyA, EndKey: keyB}
	spBC = roachpb.Span{Key: keyB, EndKey: keyC}
	spCD = roachpb.Span{Key: keyC, EndKey: keyD}
	spAC = roachpb.Span{Key: keyA, EndKey: keyC}
	spXY = roachpb.Span{Key: keyX, EndKey: keyY}
)

type testStream struct {
	ctx     context.Context
	ctxDone func()
	done    chan *kvpb.Error
	cleanUp func()
	mu      struct {
		syncutil.Mutex
		sendErr error
		events  []*kvpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, done := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, ctxDone: done, done: make(chan *kvpb.Error, 1), cleanUp: func() {}}
}

func (s *testStream) Context() context.Context {
	return s.ctx
}

func (s *testStream) Send(e *kvpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.sendErr != nil {
		return s.mu.sendErr
	}
	s.mu.events = append(s.mu.events, e)
	return nil
}

func (s *testStream) Disconnect(err *kvpb.Error) {
	s.ctxDone()
	s.done <- err
	s.cleanUp()
}

func (s *testStream) RegisterRangefeedCleanUp(f func()) {
	s.cleanUp = f
}

func (s *testStream) Cancel() {
	s.ctxDone()
}

func (s *testStream) Err(t *testing.T) error {
	select {
	case err := <-s.done:
		return err.GoError()
	case <-time.After(30 * time.Second):
		t.Fatalf("time out waiting for rangefeed completion")
		return nil
	}
}

func (s *testStream) SetSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sendErr = err
}

func (s *testStream) TryErr() error {
	select {
	case err := <-s.done:
		return err.GoError()
	default:
		return nil
	}
}

func (s *testStream) Events() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

type testRegistration struct {
	registration
	*testStream
}

func makeCatchUpIterator(
	iter storage.SimpleMVCCIterator, span roachpb.Span, startTime hlc.Timestamp,
) *CatchUpIterator {
	if iter == nil {
		return nil
	}
	return &CatchUpIterator{
		simpleCatchupIter: simpleCatchupIterAdapter{iter},
		span:              span,
		startTime:         startTime,
	}
}

func newTestRegistration(
	span roachpb.Span,
	ts hlc.Timestamp,
	catchup storage.SimpleMVCCIterator,
	withDiff bool,
	withFiltering bool,
) *testRegistration {
	s := newTestStream()
	r := newRegistration(
		span,
		ts,
		makeCatchUpIterator(catchup, span, ts),
		withDiff,
		withFiltering,
		5,
		false, /* blockWhenFull */
		NewMetrics(),
		s,
		func() {},
	)
	return &testRegistration{
		registration: r,
		testStream:   s,
	}
}

//func (r *testRegistration) Events() []*kvpb.RangeFeedEvent {
//	return r.stream.Events()
//}
//
//func (r *testRegistration) Err(t *testing.T) error {
//	return r.stream.Err(t)
//}
//
//func (r *testRegistration) TryErr() error {
//	return r.stream.TryErr()
//}
