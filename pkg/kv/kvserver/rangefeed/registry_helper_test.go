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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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

var txn1, txn2 = uuid.MakeV4(), uuid.MakeV4()

var keyValues = []storage.MVCCKeyValue{
	makeKV("a", "valA1", 10),
	makeIntent("c", txn1, "txnKeyC", 15),
	makeProvisionalKV("c", "txnKeyC", 15),
	makeKV("c", "valC2", 11),
	makeKV("c", "valC1", 9),
	makeIntent("d", txn2, "txnKeyD", 21),
	makeProvisionalKV("d", "txnKeyD", 21),
	makeKV("d", "valD5", 20),
	makeKV("d", "valD4", 19),
	makeKV("d", "valD3", 16),
	makeKV("d", "valD2", 3),
	makeKV("d", "valD1", 1),
	makeKV("e", "valE3", 6),
	makeKV("e", "valE2", 5),
	makeKV("e", "valE1", 4),
	makeKV("f", "valF3", 7),
	makeKV("f", "valF2", 6),
	makeKV("f", "valF1", 5),
	makeKV("h", "valH1", 15),
	makeKV("m", "valM1", 1),
	makeIntent("n", txn1, "txnKeyN", 12),
	makeProvisionalKV("n", "txnKeyN", 12),
	makeIntent("r", txn1, "txnKeyR", 19),
	makeProvisionalKV("r", "txnKeyR", 19),
	makeKV("r", "valR1", 4),
	makeKV("s", "valS3", 21),
	makeKVWithHeader("s", "valS2", 20, enginepb.MVCCValueHeader{OmitInRangefeeds: true}),
	makeKV("s", "valS1", 19),
	makeIntent("w", txn1, "txnKeyW", 3),
	makeProvisionalKV("w", "txnKeyW", 3),
	makeIntent("z", txn2, "txnKeyZ", 21),
	makeProvisionalKV("z", "txnKeyZ", 21),
	makeKV("z", "valZ1", 4),
}

func expEvents(filtering bool) []*kvpb.RangeFeedEvent {
	expEvents := []*kvpb.RangeFeedEvent{
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD3", 16),
			makeVal("valD2"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD4", 19),
			makeVal("valD3"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			makeValWithTs("valD5", 20),
			makeVal("valD4"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			makeValWithTs("valE2", 5),
			makeVal("valE1"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			makeValWithTs("valE3", 6),
			makeVal("valE2"),
		),
		rangeFeedValue(
			roachpb.Key("f"),
			makeValWithTs("valF1", 5),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			makeValWithTs("valF2", 6),
			makeVal("valF1"),
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			makeValWithTs("valF3", 7),
			makeVal("valF2"),
		),
		rangeFeedValue(
			roachpb.Key("h"),
			makeValWithTs("valH1", 15),
		),
		rangeFeedValue(
			roachpb.Key("s"),
			makeValWithTs("valS1", 19),
		),
	}
	if !filtering {
		expEvents = append(expEvents,
			rangeFeedValueWithPrev(
				roachpb.Key("s"),
				makeValWithTs("valS2", 20),
				makeVal("valS1"),
			))
	}
	expEvents = append(expEvents, rangeFeedValueWithPrev(
		roachpb.Key("s"),
		makeValWithTs("valS3", 21),
		// Even though the event that wrote val2 is filtered out, we want to keep
		// val2 as a previous value of the next event.
		makeVal("valS2"),
	))
	return expEvents
}

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

func (s *testStream) Context() context.Context {
	return s.ctx
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

func (s *testStream) GetAndClearEvents() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

func (s *testStream) GetEvents() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	return es
}

func (s *testStream) BlockSend() func() {
	s.mu.Lock()
	var once sync.Once
	return func() {
		// nolint:deferunlockcheck
		once.Do(s.mu.Unlock) // safe to call multiple times, e.g. defer and explicit
	}
}

// SendError implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (s *testStream) SendError(err *kvpb.Error) {
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

func (s *testStream) waitForEventCount(t *testing.T, expected int) {
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		actual := len(s.mu.events)
		if actual != expected {
			return errors.Errorf("expected %d events, got %d", expected, actual)
		}
		return nil
	})
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
		cfg.withRegistrationTestTypes = regType
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

func (t registrationType) String() string {
	switch t {
	case buffered:
		return "buffered registration"
	case unbuffered:
		return "unbuffered registration"
	}
	panic("unknown processor type")
}

var registrationTestTypes = []registrationType{buffered, unbuffered}

type testRegistrationConfig struct {
	span                      roachpb.Span
	ts                        hlc.Timestamp
	catchup                   storage.SimpleMVCCIterator
	withDiff                  bool
	withFiltering             bool
	withOmitRemote            bool
	withRegistrationTestTypes registrationType
	metrics                   *Metrics
}

func newTestRegistration(s *testStream, opts ...registrationOption) testRegistration {
	cfg := testRegistrationConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.metrics == nil {
		cfg.metrics = NewMetrics()
	}

	switch cfg.withRegistrationTestTypes {
	case buffered:
		return newBufferedRegistration(
			s.ctx,
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
			func(registration) {},
		)
	case unbuffered:
		return newUnbufferedRegistration(
			s.ctx,
			cfg.span,
			cfg.ts,
			makeCatchUpIterator(cfg.catchup, cfg.span, cfg.ts),
			cfg.withDiff,
			cfg.withFiltering,
			cfg.withOmitRemote,
			5,
			cfg.metrics,
			&testBufferedStream{Stream: s},
			func(registration) {},
		)
	default:
		panic("unknown registration type")
	}
}

type testRegistration interface {
	registration
	getBuf() chan *sharedEvent
	getOverflowed() bool
	maybeRunCatchUpScan(ctx context.Context) error
}
