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

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// Send blocks until it sends m, the stream is done, or the stream breaks.
	// Send must be safe to call on the same stream in different goroutines.
	Send(*kvpb.RangeFeedEvent) error
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

func (s *MuxFeedStream) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.muxer.publish(response)
}

var _ kvpb.RangeFeedEventSink = (*MuxFeedStream)(nil)
var _ Stream = (*MuxFeedStream)(nil)

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
	wrapped    kvpb.MuxRangeFeedEventSink
	capacity   int
	notifyData chan struct{}
	cleanup    chan int

	// queue
	queueMu struct {
		syncutil.Mutex
		buffer muxEventQueue
	}

	prodsMu struct {
		syncutil.RWMutex
		prods map[int64]*producer
	}
}

const defaultEventBufferCapacity = 4096 * 2

func NewStreamMuxer(wrapped kvpb.MuxRangeFeedEventSink) *StreamMuxer {
	muxer := &StreamMuxer{
		wrapped:    wrapped,
		capacity:   defaultEventBufferCapacity,
		notifyData: make(chan struct{}, 1),
		cleanup:    make(chan int, 10),
	}
	muxer.queueMu.buffer = newMuxEventQueue()
	muxer.prodsMu.prods = make(map[int64]*producer)
	return muxer
}

func (m *StreamMuxer) publish(e *kvpb.MuxRangeFeedEvent) error {

	return m.wrapped.Send(e)
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
		if producer.isConnected() {
			prods = append(prods, producer)
		}
	}
	return prods
}

func (p *producer) isConnected() bool {
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

func (m *StreamMuxer) getProducerIfExists(streamId int64) (bool, *producer) {
	m.prodsMu.RLock()
	defer m.prodsMu.RUnlock()
	if _, ok := m.prodsMu.prods[streamId]; !ok {
		return false, nil
	}
	return true, m.prodsMu.prods[streamId]
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

func (m *StreamMuxer) pushBack(event sharedMuxEvent) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.pushBack(event)
}
