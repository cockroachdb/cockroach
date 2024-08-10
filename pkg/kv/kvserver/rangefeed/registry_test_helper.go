// Copyright 2024 The Cockroach Authors.
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
	cleanup func()
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

func (s *testStream) Context() context.Context {
	return s.ctx
}

func (s *testStream) Cancel() {
	s.ctxDone()
}

func (s *testStream) SendIsThreadSafe() {}

func (s *testStream) Send(e *kvpb.RangeFeedEvent) error {
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
		once.Do(s.mu.Unlock) //nolint:deferunlockcheck
	}
}

// Disconnect implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (s *testStream) Disconnect(err *kvpb.Error) {
	s.done <- err
	if s.cleanup != nil {
		go s.cleanup()
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *testStream) RegisterRangefeedCleanUp(cleanup func()) {
	s.cleanup = cleanup
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

type registrationOption func(*testRegistrationConfig)

func withCatchUpIter(iter storage.SimpleMVCCIterator) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.catchup = iter
	}
}

func withDiff(opt bool) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.withDiff = opt
	}
}

func withFiltering(opt bool) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.withFiltering = opt
	}
}

func withRMetrics(metrics *Metrics) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.metrics = metrics
	}
}

func withOmitRemote(opt bool) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.withOmitRemote = opt
	}
}

func withRegistrationType(regType registrationType) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.useUnbufferedRegistration = bool(regType)
	}
}

func withRSpan(span roachpb.Span) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.span = span
	}
}

func withStartTs(ts hlc.Timestamp) registrationOption {
	return func(cfg *testRegistrationConfig) {
		cfg.ts = ts
	}
}

type registrationType bool

const (
	buffered   registrationType = false
	unbuffered                  = true
)

var registrationTestTypes = []registrationType{buffered, unbuffered}

type testRegistrationConfig struct {
	span                      roachpb.Span
	ts                        hlc.Timestamp
	catchup                   storage.SimpleMVCCIterator
	withDiff                  bool
	withFiltering             bool
	withOmitRemote            bool
	useUnbufferedRegistration bool
	metrics                   *Metrics
}

func newTestRegistration(s *testStream, opts ...registrationOption) registration {
	cfg := testRegistrationConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.metrics == nil {
		cfg.metrics = NewMetrics()
	}
	if cfg.useUnbufferedRegistration {
		return newUnbufferedRegistration(
			cfg.span,
			cfg.ts,
			makeCatchUpIterator(cfg.catchup, cfg.span, cfg.ts),
			cfg.withDiff,
			cfg.withFiltering,
			cfg.withOmitRemote,
			5,
			cfg.metrics,
			s,
			func() {},
		)
	}

	return newBufferedRegistration(
		cfg.span,
		cfg.ts,
		makeCatchUpIterator(cfg.catchup, cfg.span, cfg.ts),
		cfg.withDiff,
		cfg.withFiltering,
		cfg.withOmitRemote,
		5,
		false, /* blockWhenFull */
		cfg.metrics,
		s,
		func() {},
	)
}
