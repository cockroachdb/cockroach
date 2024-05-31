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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type BufferedStream interface {
	Context() context.Context
	SendBuffered(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation) error
	SendUnbuffered(event *kvpb.RangeFeedEvent) error
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
) error {
	alloc.Release(context.Background())
	return s.SendUnbuffered(event)
}

type MuxFeedStream struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	muxer    *StreamMuxer
}

func NewMuxFeedStream(
	ctx context.Context, streamID int64, rangeID roachpb.RangeID, streamMuxer *StreamMuxer,
) *MuxFeedStream {
	m := &MuxFeedStream{
		ctx:      ctx,
		streamID: streamID,
		rangeID:  rangeID,
		muxer:    streamMuxer,
	}
	m.muxer.addProducer(ctx, streamID, rangeID)
	return m
}

func (s *MuxFeedStream) Context() context.Context {
	return s.ctx
}

func (s *MuxFeedStream) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	return s.muxer.wrapped.Send(&kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	})
}

func (s *MuxFeedStream) SendBuffered(
	event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	s.muxer.publishEvent(s.streamID, s.rangeID, event, alloc)
	return nil
}

var _ BufferedStream = (*MuxFeedStream)(nil)

type sharedMuxEvent struct {
	streamID int64
	rangeID  roachpb.RangeID
	event    *kvpb.RangeFeedEvent
	alloc    *SharedBudgetAllocation
	err      *kvpb.Error
}

type producer struct {
	syncutil.RWMutex
	streamId int64
	rangeID  roachpb.RangeID
	// TODO(wenyihu6): check if only disconnected need to be protected
	disconnected bool
	// clean up callback is needed later after removing registration goroutines
	cleanup func()
}

// todo(wenyihu6): check for deadlock
type StreamMuxer struct {
	wrapped  kvpb.MuxRangeFeedEventSink
	capacity int
	notify   chan struct{}
	cleanup  chan int64

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

const defaultEventBufferCapacity = 4096 * 2

func NewStreamMuxer(wrapped kvpb.MuxRangeFeedEventSink) *StreamMuxer {
	muxer := &StreamMuxer{
		wrapped:  wrapped,
		capacity: defaultEventBufferCapacity,
		notify:   make(chan struct{}, 1),
		cleanup:  make(chan int64, 10),
	}
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
	case e.err != nil:
		ev := &kvpb.MuxRangeFeedEvent{
			RangeID:  e.rangeID,
			StreamID: e.streamID,
		}
		ev.SetValue(&kvpb.RangeFeedError{
			Error: *e.err,
		})
		return m.wrapped.Send(ev)
	default:
		// producers may be removed and empty event
	}
	return nil
}
func (m *StreamMuxer) OutputLoop(ctx context.Context) {
	for {
		select {
		case <-m.notify:
			event, empty, overflowedAndDone := m.popFront()
			if empty {
				// no more events to send
				break
			}
			if overflowedAndDone {
				// TODO(wenyihu6): rationalize overflow and done should happen before empty
				m.cleanupProducers(ctx, newErrBufferCapacityExceeded())
				return
			}
			err := m.sendEventToStream(ctx, event)
			if err != nil {
				m.cleanupProducers(ctx, nil)
				return
			}
		case streamId := <-m.cleanup:
			m.cleanupProducerIfExists(streamId)
		case <-ctx.Done():
			m.cleanupProducers(ctx, kvpb.NewError(ctx.Err()))
			return
			// case <-m.output.Context().Done(): check if this is needed
		}
	}
}

// TODO(wenyihu6): we can see if we can just forward this to forward mux rangefeed completion
//func (m *StreamMuxer) HandleError(e *kvpb.MuxRangeFeedEvent) error {
//	if e.Error == nil {
//		return nil
//	}
//	m.PublishErrorAndDisconnectId(e.StreamID, e.RangeID, kvpb.NewError(e.Error))
//}

func (m *StreamMuxer) PublishErrorAndDisconnectId(
	streamId int64, rangeId roachpb.RangeID, err *kvpb.Error,
) {
	p, ok := m.getProducerIfExists(streamId)
	if !ok || p.isDisconnected() {
		return
	}
	// check if accessing lock multiple times here is good practise
	p.setDisconnected()
	m.addError(sharedMuxEvent{
		streamID: streamId,
		rangeID:  rangeId,
		err:      err,
	})
	m.cleanup <- streamId
	// notify error enqueued
	select {
	case m.notify <- struct{}{}:
	default:
	}
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

	//TODO(wenyihu6): check if alloc.Use() is needed
	//TODO(wenyihu6): put sharedMuxEvent in pool
	success := m.pushBack(sharedMuxEvent{
		streamID: streamId,
		rangeID:  rangeID,
		event:    event,
		alloc:    alloc,
	})

	if !success {
		alloc.Release(context.Background())
		return
	}

	// notify data incoming unless the signal is already there
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *StreamMuxer) addProducer(ctx context.Context, streamId int64, rangeID roachpb.RangeID) {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()
	if _, ok := m.prodsMu.prods[streamId]; ok {
		log.Error(ctx, "stream already exists")
	}
	m.prodsMu.prods[streamId] = &producer{streamId: streamId, rangeID: rangeID, cleanup: func() {}}
}

func (m *StreamMuxer) popAllConnectedProducers() (prods []*producer) {
	m.prodsMu.Lock()
	defer m.prodsMu.Unlock()

	for streamId, producer := range m.prodsMu.prods {
		delete(m.prodsMu.prods, streamId)
		if !producer.isDisconnected() {
			prods = append(prods, producer)
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

func (p *producer) callback() func() {
	p.RLock()
	defer p.RUnlock()
	return p.cleanup
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

func (m *StreamMuxer) cleanupProducerIfExists(streamId int64) {
	p := m.popProducerByStreamId(streamId)
	if p == nil {
		return
	}
	p.callback()()
}

func (m *StreamMuxer) cleanupProducers(ctx context.Context, streamErr *kvpb.Error) {
	m.removeAll()
	prods := m.popAllConnectedProducers()
	var err error
	for _, p := range prods {
		if err != nil && streamErr != nil {
			errEvent := &kvpb.MuxRangeFeedEvent{
				RangeID:  p.rangeID,
				StreamID: p.streamId,
			}
			errEvent.SetValue(&kvpb.RangeFeedError{
				Error: *streamErr,
			})
			err = m.wrapped.Send(errEvent)
		}
		p.callback()()
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
	if m.queueMu.buffer.len() >= m.capacity {
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
func (m *StreamMuxer) addError(event sharedMuxEvent) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.pushBack(event)
}
