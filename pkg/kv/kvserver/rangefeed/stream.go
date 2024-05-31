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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type BufferedStream interface {
	Context() context.Context
	SendBuffered(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	SendUnbuffered(event *kvpb.RangeFeedEvent) error
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
	return &MuxFeedStream{
		ctx:      ctx,
		streamID: streamID,
		rangeID:  rangeID,
		muxer:    streamMuxer,
	}
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

func (s *MuxFeedStream) SendBuffered(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation) {
	s.muxer.publish(s.streamID, s.rangeID, event, alloc)
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
	cleanup      func()
}

type StreamMuxer struct {
	wrapped  kvpb.MuxRangeFeedEventSink
	capacity int
	notify   chan struct{}
	cleanup  chan int

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
		cleanup:  make(chan int, 10),
	}
	muxer.queueMu.buffer = newMuxEventQueue()
	muxer.prodsMu.prods = make(map[int64]*producer)
	return muxer
}

func (m *StreamMuxer) publish(
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
	m.prodsMu.prods[streamId] = &producer{streamId: streamId, rangeID: rangeID}
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

func (m *StreamMuxer) cleanupProducers(streamErr *kvpb.Error) {
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

// TODO(wenyihu6): check if adding back to err overflow should trigger
func (m *StreamMuxer) addError(event sharedMuxEvent) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.pushBack(event)
}
