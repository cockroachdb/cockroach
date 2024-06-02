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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type BufferedStream interface {
	SendBuffered(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	SendUnbuffered(event *kvpb.RangeFeedEvent) error
	SendError(err *kvpb.Error)
}

type LockedRangefeedStreamAdapter struct {
	Wrapped kvpb.RangeFeedEventSink
	sendMu  syncutil.Mutex
}

var _ BufferedStream = (*LockedRangefeedStreamAdapter)(nil)

func (s *LockedRangefeedStreamAdapter) Context() context.Context {
	return s.Wrapped.Context()
}

func (s *LockedRangefeedStreamAdapter) SendUnbuffered(e *kvpb.RangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.Wrapped.Send(e)
}

func (s *LockedRangefeedStreamAdapter) SendBuffered(
	event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	alloc.Release(context.Background())
	_ = s.SendUnbuffered(event)
}

func (s *LockedRangefeedStreamAdapter) SendError(_ *kvpb.Error) {
}

type StreamSink struct {
	RangeID     roachpb.RangeID
	StreamID    int64
	StreamMuxer Muxer
}

type MuxFeedStream struct {
	rangeID  roachpb.RangeID
	streamID int64
	muxer    *StreamMuxer
}

type Muxer interface {
	Register(streamID int64, rangeID roachpb.RangeID) BufferedStream
	// There should be a node level clean up and a rangefeed level clean up.
	AddRangefeedCleanUpCallback(streamID int64, cleanup func())
}

func (m *StreamMuxer) AddRangefeedCleanUpCallback(streamID int64, cleanup func()) {
	p, ok := m.getProducerIfExists(streamID)
	if !ok {
		return
	}
	p.rangefeedCleanUp = cleanup
}

func (m *StreamMuxer) Register(streamID int64, rangeID roachpb.RangeID) BufferedStream {
	s := &MuxFeedStream{
		streamID: streamID,
		rangeID:  rangeID,
		muxer:    m,
	}
	m.addProducer(streamID, rangeID)
	return s
}

func (s *MuxFeedStream) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	return s.muxer.wrapped.Send(&kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	})
}

func (s *MuxFeedStream) SendBuffered(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation) {
	s.muxer.publishEvent(s.streamID, s.rangeID, event, alloc)
}

// different help-er function  for err from rangefeed level and from node level
func (s *MuxFeedStream) SendError(err *kvpb.Error) {
	s.muxer.HandleRangefeedDisconnectError(s.streamID, s.rangeID, err)
}

var _ BufferedStream = (*MuxFeedStream)(nil)

type sharedMuxEvent struct {
	streamID int64
	rangeID  roachpb.RangeID
	event    *kvpb.RangeFeedEvent
	alloc    *SharedBudgetAllocation
}

type producer struct {
	syncutil.RWMutex
	streamId int64
	rangeID  roachpb.RangeID
	// TODO(wenyihu6): check if only disconnected need to be protected
	disconnected bool
	// clean up callback is needed later after removing registration goroutines
	rangefeedCleanUp func()
}

// todo(wenyihu6): check for deadlock
type StreamMuxer struct {
	wrapped    kvpb.MuxRangeFeedEventSink
	capacity   atomic.Int32
	notify     chan struct{}
	cleanup    chan int64
	muxErrorsC chan *kvpb.MuxRangeFeedEvent

	nodeLevelCleanUp func(streamID int64) bool

	// queue
	queueMu struct {
		syncutil.Mutex
		buffer   muxEventQueue
		overflow bool
	}

	prodsMu struct {
		syncutil.RWMutex
		prods map[int64]*producer
	}
}

const defaultEventBufferCapacity = 4096

func NewStreamMuxer(wrapped kvpb.MuxRangeFeedEventSink) *StreamMuxer {
	muxer := &StreamMuxer{
		wrapped:  wrapped,
		capacity: atomic.Int32{},
		notify:   make(chan struct{}, 1),
		cleanup:  make(chan int64, 10),
		// TODO(wenyihu6): check if 10 is a large enough number
		muxErrorsC:       make(chan *kvpb.MuxRangeFeedEvent, 10),
		nodeLevelCleanUp: func(streamID int64) bool { return false },
	}
	//TODO(wenyihu6): check if int32 is enough
	muxer.capacity.Store(defaultEventBufferCapacity)
	muxer.queueMu.buffer = newMuxEventQueue()
	muxer.prodsMu.prods = make(map[int64]*producer)
	return muxer
}

func (m *StreamMuxer) sendEventToStream(ctx context.Context, e sharedMuxEvent) (err error) {
	// alloc could be nil but release is safe to call on nil
	defer e.alloc.Release(ctx)
	switch {
	case e.event != nil:
		return m.wrapped.Send(&kvpb.MuxRangeFeedEvent{
			RangeFeedEvent: *e.event,
			RangeID:        e.rangeID,
			StreamID:       e.streamID,
		})
	default:
		// producers may be removed and empty event
	}
	return nil
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
		case <-m.notify:
			e, empty, overflowedAndDone := m.popFront()
			if empty {
				// no more events to send
				break
			}
			if overflowedAndDone {
				// TODO(wenyihu6): rationalize overflow and done should happen before empty
				m.cleanupProducers(newErrBufferCapacityExceeded())
				return
			}

			e.alloc.Release(ctx)
			if e.event == nil {
				continue
			}

			if err := m.wrapped.Send(&kvpb.MuxRangeFeedEvent{
				RangeFeedEvent: *e.event,
				RangeID:        e.rangeID,
				StreamID:       e.streamID,
			}); err != nil {
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

// TODO(wenyihu6): we can see if we can just forward this to forward mux rangefeed completion
func (m *StreamMuxer) SendErrorToClient(event *kvpb.MuxRangeFeedEvent) {
	if event == nil {
		log.Infof(context.Background(), "unexpected event is nil")
		return
	}
	// terminate a stream here
	m.muxErrorsC <- event

	// check if we should use select and check whether context is done muxer should manage that fine
	//select {
	//case <-m.wrapped.Context().Done():
	//	// If underlying transport was cancelled then don't try to send error as
	//	// it could block as downstream infrastructure might be winding down
	//	// already.
	//	// Note that this is different from this mux feed context which can be
	//	// cancelled by mux range feed upon client request.
	//}
}

func (m *StreamMuxer) HandleRangefeedDisconnectError(
	streamId int64, rangeId roachpb.RangeID, err *kvpb.Error,
) {
	p, ok := m.getProducerIfExists(streamId)
	needRangefeedLevelCleanUp := ok && p.isDisconnected()
	if needRangefeedLevelCleanUp {
		// check if accessing lock multiple times here is good practise
		// dont delete producer here
		p.setDisconnected()
	}
	m.removeProducerEvent(streamId)
	// node level clean up first
	loaded := m.nodeLevelCleanUp(streamId)
	if loaded {
		clientErrorEvent := TransformSingleFeedErrorToMuxEvent(streamId, rangeId, err)
		// should we send an error here or wait and push back
		m.SendErrorToClient(clientErrorEvent)
	}
	// producer level clean up if needed
	// err can be nil
	if needRangefeedLevelCleanUp {
		m.cleanup <- streamId
	}
}

func (m *StreamMuxer) RegisterNodeLevelCleanUp(nodeLevelCleanUp func(streamID int64) bool) {
	m.nodeLevelCleanUp = nodeLevelCleanUp
}

func (m *StreamMuxer) publishEvent(
	streamId int64,
	rangeID roachpb.RangeID,
	event *kvpb.RangeFeedEvent,
	alloc *SharedBudgetAllocation,
) {
	p, ok := m.getProducerIfExists(streamId)
	if !ok || p.isDisconnected() {
		return
	}

	alloc.Use(context.Background())
	//TODO(wenyihu6): check if alloc.Use() is needed
	//TODO(wenyihu6): put sharedMuxEvent in pool
	if !m.pushBack(sharedMuxEvent{
		streamID: streamId,
		rangeID:  rangeID,
		event:    event,
		alloc:    alloc,
	}) {
		alloc.Release(context.Background())
		return
	}

	// notify data incoming unless the signal is already there
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *StreamMuxer) addProducer(streamId int64, rangeID roachpb.RangeID) {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()
	if _, ok := m.prodsMu.prods[streamId]; ok {
		log.Error(context.Background(), "stream already exists")
	}
	m.capacity.Add(defaultEventBufferCapacity)
	m.prodsMu.prods[streamId] = &producer{streamId: streamId, rangeID: rangeID, rangefeedCleanUp: func() {}}
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

func (p *producer) isDisconnected() bool {
	p.RLock()
	defer p.RUnlock()
	return !p.disconnected
}

func (p *producer) setDisconnected() {
	p.Lock()
	defer p.Unlock()
	p.disconnected = true
}

// shouldn't happen in the same goroutine as r.disconnect
func (p *producer) rangefeedLevelCleanUp() {
	p.RLock()
	f := p.rangefeedCleanUp
	p.RUnlock()
	f()
}

// event queue has been cleaned up already
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

func TransformSingleFeedErrorToMuxEvent(
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

func (m *StreamMuxer) cleanupProducerIfExists(streamId int64) {
	p := m.popProducerByStreamId(streamId)
	if p == nil {
		return
	}
	// p.callback() should be already called call again just in case plus some p clean up
	p.rangefeedLevelCleanUp()
}

func (m *StreamMuxer) cleanupProducers(streamErr *kvpb.Error) {
	m.removeAll()
	prods := m.popAllConnectedProducers()
	for _, p := range prods {
		loaded := m.nodeLevelCleanUp(p.streamId)
		// streamErr is nil means stream is broken now when we should just shut down
		// without sending error back
		if streamErr != nil && loaded {
			clientErrorEvent := TransformSingleFeedErrorToMuxEvent(p.streamId, p.rangeID, streamErr)
			m.SendErrorToClient(clientErrorEvent)
		}
		p.rangefeedLevelCleanUp()
	}
}

func (m *StreamMuxer) removeAll() {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.removeAll(context.Background())
}

func (m *StreamMuxer) removeProducerEvent(streamId int64) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.remove(context.Background(), streamId)
}

func (m *StreamMuxer) pushBack(event sharedMuxEvent) bool {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	if m.queueMu.overflow {
		return false
	}
	if int32(m.queueMu.buffer.len()) >= m.capacity.Load() {
		m.queueMu.overflow = true
		return false
	}
	m.queueMu.buffer.pushBack(event)
	return true
}

func (m *StreamMuxer) popFront() (event sharedMuxEvent, empty bool, overflowedAndDone bool) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	event, empty = m.queueMu.buffer.popFront()
	overflow, remains := m.queueMu.overflow, m.queueMu.buffer.len()
	return event, empty, overflow && remains == 0
}

// TODO(wenyihu6): check if adding back to err overflow should trigger
//func (m *StreamMuxer) addError(event sharedMuxEvent) {
//	m.queueMu.Lock()
//	defer m.queueMu.Unlock()
//	m.queueMu.buffer.pushBack(event)
//}
