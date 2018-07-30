// Copyright 2018 The Cockroach Authors.
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

package rangefeed

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type testStream struct {
	ctx     context.Context
	ctxDone func()
	mu      struct {
		syncutil.Mutex
		sendErr error
		events  []*roachpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, done := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, ctxDone: done}
}

func (s *testStream) Context() context.Context {
	return s.ctx
}

func (s *testStream) Cancel() {
	s.ctxDone()
}

func (s *testStream) Send(e *roachpb.RangeFeedEvent) error {
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

func (s *testStream) Events() []*roachpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

type testRegistration struct {
	registration
	stream *testStream
	errC   <-chan *roachpb.Error
}

func newTestRegistration(span roachpb.Span) *testRegistration {
	s := newTestStream()
	errC := make(chan *roachpb.Error, 1)
	return &testRegistration{
		registration: registration{
			span:   span,
			stream: s,
			errC:   errC,
		},
		stream: s,
		errC:   errC,
	}
}

func (r *testRegistration) Events() []*roachpb.RangeFeedEvent {
	return r.stream.Events()
}

func (r *testRegistration) Err() *roachpb.Error {
	select {
	case pErr := <-r.errC:
		return pErr
	default:
		return nil
	}
}

func TestRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD := roachpb.Key("c"), roachpb.Key("d")

	spAB := roachpb.Span{Key: keyA, EndKey: keyB}
	spBC := roachpb.Span{Key: keyB, EndKey: keyC}
	spCD := roachpb.Span{Key: keyC, EndKey: keyD}
	spAC := roachpb.Span{Key: keyA, EndKey: keyC}

	ev1, ev2 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev3, ev4 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	err1 := roachpb.NewErrorf("error1")

	reg := makeRegistry()
	require.Equal(t, 0, reg.Len())
	require.NotPanics(t, func() { reg.PublishToOverlapping(spAB, ev1) })
	require.NotPanics(t, func() { reg.Disconnect(spAB) })
	require.NotPanics(t, func() { reg.DisconnectWithErr(spAB, err1) })
	require.NotPanics(t, func() { reg.CheckStreams() })

	r1 := newTestRegistration(spAB)
	r2 := newTestRegistration(spBC)
	r3 := newTestRegistration(spCD)
	r4 := newTestRegistration(spAC)

	// Register 3 registrations.
	reg.Register(r1.registration)
	require.Equal(t, 1, reg.Len())
	reg.Register(r2.registration)
	require.Equal(t, 2, reg.Len())
	reg.Register(r3.registration)
	require.Equal(t, 3, reg.Len())
	reg.Register(r4.registration)
	require.Equal(t, 4, reg.Len())

	// Publish to different spans.
	reg.PublishToOverlapping(spAB, ev1)
	reg.PublishToOverlapping(spBC, ev2)
	reg.PublishToOverlapping(spCD, ev3)
	reg.PublishToOverlapping(spAC, ev4)
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev4}, r1.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2, ev4}, r2.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev3}, r3.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2, ev4}, r4.Events())
	require.Nil(t, r1.Err())
	require.Nil(t, r2.Err())
	require.Nil(t, r3.Err())
	require.Nil(t, r4.Err())

	// Check streams, all still alive.
	reg.CheckStreams()
	require.Equal(t, 4, reg.Len())
	require.Nil(t, r1.Err())
	require.Nil(t, r2.Err())
	require.Nil(t, r3.Err())
	require.Nil(t, r4.Err())

	// Cancel r2 and check streams again. r2 should disconnect.
	r2.stream.Cancel()
	require.Equal(t, 4, reg.Len())
	reg.CheckStreams()
	require.Equal(t, 3, reg.Len())
	require.Nil(t, r1.Err())
	require.NotNil(t, r2.Err())
	require.Nil(t, r3.Err())
	require.Nil(t, r4.Err())

	// Set a stream error on r4 and publish. Once a publication
	// notices the error it disconnects.
	r4.stream.SetSendErr(errors.New("can't send"))
	reg.PublishToOverlapping(spCD, ev1)
	require.Equal(t, 3, reg.Len())
	require.Nil(t, r1.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1}, r3.Events())
	require.Nil(t, r4.Events())
	require.Nil(t, r1.Err())
	require.Nil(t, r3.Err())
	require.Nil(t, r4.Err())
	reg.PublishToOverlapping(spAB, ev2)
	require.Equal(t, 2, reg.Len())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2}, r1.Events())
	require.Nil(t, r3.Events())
	require.Nil(t, r4.Events())
	require.Nil(t, r1.Err())
	require.Nil(t, r3.Err())
	require.NotNil(t, r4.Err())

	// Disconnect span that overlaps with r3.
	reg.DisconnectWithErr(spCD, err1)
	require.Equal(t, 1, reg.Len())
	require.Nil(t, r1.Err())
	require.Equal(t, err1, r3.Err())

	// Can still publish to r1.
	reg.PublishToOverlapping(spAB, ev4)
	reg.PublishToOverlapping(spBC, ev3)
	reg.PublishToOverlapping(spCD, ev2)
	reg.PublishToOverlapping(spAC, ev1)
	require.Equal(t, []*roachpb.RangeFeedEvent{ev4, ev1}, r1.Events())
	require.Nil(t, r1.Err())

	// Disconnect from r1 without error.
	reg.Disconnect(spBC)
	require.Equal(t, 1, reg.Len())
	reg.Disconnect(spAC)
	require.Equal(t, 0, reg.Len())
	require.Nil(t, r1.Err())
}
