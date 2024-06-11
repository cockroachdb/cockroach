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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Responsible to coordinate rangefeed level shutdown.
type producer struct {
	syncutil.RWMutex
	streamId int64
	rangeID  roachpb.RangeID
	// TODO(wenyihu6): check if only disconnected need to be protected
	disconnected bool
	// clean up callback is needed later after removing registration goroutines
	rangefeedCleanUp func()
}

type StreamMuxer struct {
	wrapped    kvpb.MuxRangeFeedEventSink
	notify     chan struct{}
	cleanup    chan int64
	muxErrorsC chan *kvpb.MuxRangeFeedEvent

	nodeLevelCleanUp func(streamID int64) bool

	prodsMu struct {
		syncutil.RWMutex
		prods map[int64]*producer
	}
}

// Responsible for sending errors, node and rangefeed level cleanups.
func NewStreamMuxer(
	wrapped kvpb.MuxRangeFeedEventSink, nodeLevelCleanUp func(int64) bool,
) *StreamMuxer {
	muxer := &StreamMuxer{
		wrapped: wrapped,
		notify:  make(chan struct{}, 1),
		cleanup: make(chan int64, 10),
		// TODO(wenyihu6): check if 10 is a large enough number
		muxErrorsC:       make(chan *kvpb.MuxRangeFeedEvent, 10),
		nodeLevelCleanUp: nodeLevelCleanUp,
	}
	//TODO(wenyihu6): check if int32 is enough
	muxer.prodsMu.prods = make(map[int64]*producer)
	return muxer
}

type Muxer interface {
	Register(streamID int64, rangeID roachpb.RangeID, rangefeedCleanUp func())
	SendUnbuffered(*kvpb.MuxRangeFeedEvent) error
	HandleRangefeedDisconnectError(streamId int64, rangeId roachpb.RangeID, err *kvpb.Error)
}

func (m *StreamMuxer) SendUnbuffered(event *kvpb.MuxRangeFeedEvent) error {
	return m.wrapped.Send(event)
}

func (m *StreamMuxer) OutputLoop(ctx context.Context, stopper *stop.Stopper) {
	// TODO(wenyihu6): do we need to watch stopper.ShouldQuiesce
	for {
		select {
		case muxErr := <-m.muxErrorsC:
			// nothing we could do if stream is broken TODO(wenyihu6): future send
			// would also fail and just clean up -> check if we need to do any
			// additional cleanup
			if err := m.wrapped.Send(muxErr); err != nil {
				// think about recursion here maybe you should just do nothing
				// pass in nil so that we send nithing back to the client again
				m.cleanupProducers(nil)
				return
			}
		case streamId := <-m.cleanup:
			m.cleanupProducerIfExists(streamId)
		case <-ctx.Done():
			m.cleanupProducers(kvpb.NewError(ctx.Err()))
			return
		// case <-m.output.Context().Done(): check if this is needed
		case <-stopper.ShouldQuiesce():
			// m.cleanupProducers(ctx, nil)
			return
		}
	}
}

func (m *StreamMuxer) HandleRangefeedDisconnectError(
	streamId int64, rangeId roachpb.RangeID, err *kvpb.Error,
) {
	p, _ := m.getProducerIfExists(streamId)
	// even if producer is not found we should still clean up node level
	// if producer has been removed, then it shouldn't exist in map in node level
	// and we will ignore
	// if producer found but disconnected, we should do nothing
	// repeatedly clean up producer should have no side effects
	needRangefeedCleanUp := p.setDisconnected()

	// node level clean up first
	loaded := m.nodeLevelCleanUp(streamId)
	if loaded {
		clientErrorEvent := transformSingleFeedErrorToMuxEvent(streamId, rangeId, err)
		// should we send an error here or wait and push back
		m.sendErrorToClient(clientErrorEvent)
	}

	// producer level clean up if needed
	// err can be nil
	if needRangefeedCleanUp {
		m.cleanup <- streamId
	}
}

func (m *StreamMuxer) cleanupProducerIfExists(streamId int64) {
	p := m.popProducerByStreamId(streamId)
	if p == nil {
		return
	}
	// r.cleanup() should be already called, but call again just in case plus some
	// p clean up
	p.rangefeedLevelCleanUp()
}
func (m *StreamMuxer) cleanupProducers(streamErr *kvpb.Error) {
	prods := m.popAllConnectedProducers()
	for _, p := range prods {
		loaded := m.nodeLevelCleanUp(p.streamId)
		// streamErr is nil means stream is broken now when we should just shut down
		// without sending error back
		if streamErr != nil && loaded {
			clientErrorEvent := transformSingleFeedErrorToMuxEvent(p.streamId, p.rangeID, streamErr)
			m.sendErrorToClient(clientErrorEvent)
		}
		p.rangefeedLevelCleanUp()
	}
}
func transformSingleFeedErrorToMuxEvent(
	streamId int64, rangeID roachpb.RangeID, singleFeedErr *kvpb.Error,
) *kvpb.MuxRangeFeedEvent {
	// we should instead just make p to return an actual rangefeedclosed error
	// rather than allowing nil error here
	if singleFeedErr == nil {
		// If the stream was explicitly closed by the client, we expect to see
		// context.Canceled error.  In this case, return
		// kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED to the client.
		singleFeedErr = kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}

	ev := &kvpb.MuxRangeFeedEvent{
		RangeID:  rangeID,
		StreamID: streamId,
	}
	ev.SetValue(&kvpb.RangeFeedError{
		Error: *singleFeedErr,
	})
	return ev
}

func (m *StreamMuxer) sendErrorToClient(event *kvpb.MuxRangeFeedEvent) {
	if event == nil {
		log.Infof(context.Background(), "unexpected event is nil")
		return
	}
	// terminate a stream here
	m.muxErrorsC <- event
}

func (m *StreamMuxer) Register(streamID int64, rangeID roachpb.RangeID, cleanup func()) {
	m.addProducer(streamID, rangeID, cleanup)
}

func (m *StreamMuxer) addProducer(
	streamId int64, rangeID roachpb.RangeID, rangefeedCleanUp func(),
) {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()
	if _, ok := m.prodsMu.prods[streamId]; ok {
		log.Error(context.Background(), "stream already exists")
	}
	m.prodsMu.prods[streamId] = &producer{streamId: streamId, rangeID: rangeID, rangefeedCleanUp: rangefeedCleanUp}
}

func (m *StreamMuxer) popAllConnectedProducers() (prods []*producer) {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()

	for streamId, p := range m.prodsMu.prods {
		delete(m.prodsMu.prods, streamId)
		if !p.isDisconnected() {
			prods = append(prods, p)
		}
	}
	return prods
}

func (p *producer) rangefeedLevelCleanUp() {
	p.Lock()
	f := p.rangefeedCleanUp
	p.rangefeedCleanUp = nil
	p.Unlock()
	if f != nil {
		f()
	}
}

func (m *StreamMuxer) popProducerByStreamId(streamId int64) *producer {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()

	p, ok := m.prodsMu.prods[streamId]
	delete(m.prodsMu.prods, streamId)
	if !ok {
		log.Error(context.Background(), "attempt to remove non-existent stream")
	}
	return p
}

func (m *StreamMuxer) getProducerIfExists(streamId int64) (*producer, bool) {
	m.prodsMu.RLock()
	defer m.prodsMu.RUnlock()
	if _, ok := m.prodsMu.prods[streamId]; !ok {
		return nil, false
	}
	return m.prodsMu.prods[streamId], true
}

func (p *producer) setDisconnected() (needCleanUp bool) {
	if p == nil {
		return false
	}
	p.RLock()
	defer p.RUnlock()
	if p.disconnected {
		return false
	}
	p.disconnected = true
	return true
}

func (p *producer) isDisconnected() bool {
	p.RLock()
	defer p.RUnlock()
	return !p.disconnected
}

type BufferedStream interface {
	SendUnbuffered(event *kvpb.RangeFeedEvent) error
	SendError(err *kvpb.Error)
	Register(rangefeedCleanUp func())
}

type StreamSink struct {
	RangeID     roachpb.RangeID
	StreamID    int64
	StreamMuxer Muxer
}

func (s *StreamSink) SendError(err *kvpb.Error) {
	s.StreamMuxer.HandleRangefeedDisconnectError(s.StreamID, s.RangeID, err)
}

func (s *StreamSink) Register(rangefeedCleanUp func()) {
	s.StreamMuxer.Register(s.StreamID, s.RangeID, rangefeedCleanUp)
}

func (s *StreamSink) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	return s.StreamMuxer.SendUnbuffered(&kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.RangeID,
		StreamID:       s.StreamID,
	})
}

var _ BufferedStream = (*StreamSink)(nil)
