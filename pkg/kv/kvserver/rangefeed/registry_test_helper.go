// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	mu      struct {
		syncutil.Mutex
		sendErr error
		events  []*kvpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, done := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, ctxDone: done, done: make(chan *kvpb.Error, 1)}
}

func (s *testStream) Cancel() {
	s.ctxDone()
}

func (s *testStream) SendUnbufferedIsThreadSafe() {}

func (s *testStream) SendUnbuffered(e *kvpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.sendErr != nil {
		return s.mu.sendErr
	}
	s.mu.events = append(s.mu.events, e)
	return nil
}

func (s *testStream) SetSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sendErr = err
}

func (s *testStream) Events() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

func (s *testStream) BlockSend() func() {
	s.mu.Lock()
	var once sync.Once
	return func() {
		// safe to call multiple times, e.g. defer and explicit
		once.Do(s.mu.Unlock) // nolint:deferunlockcheck
	}
}

// Disconnect implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (s *testStream) Disconnect(err *kvpb.Error) {
	s.done <- err
}

// Error returns the error that was sent to the done channel. It returns nil if
// no error was sent yet.
func (s *testStream) Error() error {
	select {
	case err := <-s.done:
		return err.GoError()
	default:
		return nil
	}
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

type testRegistration struct {
	*bufferedRegistration
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
	withOmitRemote bool,
) *testRegistration {
	s := newTestStream()
	r := newBufferedRegistration(
		s.ctx,
		span,
		ts,
		makeCatchUpIterator(catchup, span, ts),
		withDiff,
		withFiltering,
		withOmitRemote,
		5,
		false, /* blockWhenFull */
		NewMetrics(),
		s,
		func() {},
	)
	return &testRegistration{
		bufferedRegistration: r,
		testStream:           s,
	}
}
