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

	rAB := newTestRegistration(spAB)
	rBC := newTestRegistration(spBC)
	rCD := newTestRegistration(spCD)
	rAC := newTestRegistration(spAC)

	// Register 4 registrations.
	reg.Register(rAB.registration)
	require.Equal(t, 1, reg.Len())
	reg.Register(rBC.registration)
	require.Equal(t, 2, reg.Len())
	reg.Register(rCD.registration)
	require.Equal(t, 3, reg.Len())
	reg.Register(rAC.registration)
	require.Equal(t, 4, reg.Len())

	// Publish to different spans.
	reg.PublishToOverlapping(spAB, ev1)
	reg.PublishToOverlapping(spBC, ev2)
	reg.PublishToOverlapping(spCD, ev3)
	reg.PublishToOverlapping(spAC, ev4)
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev4}, rAB.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2, ev4}, rBC.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev3}, rCD.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2, ev4}, rAC.Events())
	require.Nil(t, rAB.Err())
	require.Nil(t, rBC.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())

	// Check streams, all still alive.
	reg.CheckStreams()
	require.Equal(t, 4, reg.Len())
	require.Nil(t, rAB.Err())
	require.Nil(t, rBC.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())

	// Cancel rBC and check streams again. rBC should disconnect.
	rBC.stream.Cancel()
	require.Equal(t, 4, reg.Len())
	reg.CheckStreams()
	require.Equal(t, 3, reg.Len())
	require.Nil(t, rAB.Err())
	require.NotNil(t, rBC.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())

	// Set a stream error on rAC and publish. Once a publication
	// notices the error it disconnects.
	rAC.stream.SetSendErr(errors.New("can't send"))
	reg.PublishToOverlapping(spCD, ev1)
	require.Equal(t, 3, reg.Len())
	require.Nil(t, rAB.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1}, rCD.Events())
	require.Nil(t, rAC.Events())
	require.Nil(t, rAB.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())
	reg.PublishToOverlapping(spAB, ev2)
	require.Equal(t, 2, reg.Len())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2}, rAB.Events())
	require.Nil(t, rCD.Events())
	require.Nil(t, rAC.Events())
	require.Nil(t, rAB.Err())
	require.Nil(t, rCD.Err())
	require.NotNil(t, rAC.Err())

	// Disconnect span that overlaps with rCD.
	reg.DisconnectWithErr(spCD, err1)
	require.Equal(t, 1, reg.Len())
	require.Nil(t, rAB.Err())
	require.Equal(t, err1, rCD.Err())

	// Can still publish to rAB.
	reg.PublishToOverlapping(spAB, ev4)
	reg.PublishToOverlapping(spBC, ev3)
	reg.PublishToOverlapping(spCD, ev2)
	reg.PublishToOverlapping(spAC, ev1)
	require.Equal(t, []*roachpb.RangeFeedEvent{ev4, ev1}, rAB.Events())
	require.Nil(t, rAB.Err())

	// Disconnect from rAB without error.
	reg.Disconnect(spBC)
	require.Equal(t, 1, reg.Len())
	reg.Disconnect(spAC)
	require.Equal(t, 0, reg.Len())
	require.Nil(t, rAB.Err())
}
