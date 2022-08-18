// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package obs

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otel_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	otel_logs_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	otel_res_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/peer"
)

// EventsExporter abstracts exporting events to the Observability Service. It is
// implemented by EventsServer.
type EventsExporter interface {
	// SendEvent buffers an event to be sent to subscribers.
	SendEvent(ctx context.Context, typ obspb.EventType, event otel_logs_pb.LogRecord)
}

// EventsServer implements the obspb.ObsServer gRPC service. It responds to
// requests from the Observability Service to subscribe to the events stream.
// Once a subscription is established, new events (published through
// SendEvent()) are sent to the subscriber.
//
// The EventsServer supports a single subscriber at a time. If a new
// subscription request arrives while a subscriber is active (i.e. while a
// SubscribeToEvents gRPC call is running), the previous subscriber is
// disconnected (i.e. the RPC returns), and future events are sent to the new
// subscriber.
//
// When a subscriber is active, the EventsServer buffers events and flushes them
// out to the subscriber periodically (according to flushInterval) and when a
// buffer size threshold is met (triggerSizeBytes).
// The EventsServer does not buffer events when no subscriber is active, for
// better or worse.
type EventsServer struct {
	ambientCtx log.AmbientContext
	stop       *stop.Stopper
	clock      timeutil.TimeSource
	resource   otel_res_pb.Resource
	// resourceSet is set by SetResourceInfo(). The server is ready to serve RPCs
	// once this is set.
	resourceSet syncutil.AtomicBool

	// flushInterval is the duration after which a flush is triggered.
	// 0 disables this trigger.
	flushInterval time.Duration
	// triggerSizeBytes is the size in bytes of accumulated messages which trigger a flush.
	// 0 disables this trigger.
	triggerSizeBytes   uint64
	maxBufferSizeBytes uint64

	// buf accumulates events to be sent to a subscriber.
	buf eventsBuffers

	mu struct {
		syncutil.Mutex
		// sub is the current subscriber. nil if there is no subscriber.
		sub *subscriber
	}
	TestingKnobs EventServerTestingKnobs
}

// EventServerTestingKnobs represents the testing knobs on EventsServer.
type EventServerTestingKnobs struct {
	// OnSubscriber, if set, is called when an event subscriber is registered.
	OnConnect func(ctx context.Context)
}

var _ base.ModuleTestingKnobs = EventServerTestingKnobs{}

// ModuleTestingKnobs implements base.ModuleTestingKnobs.
func (e EventServerTestingKnobs) ModuleTestingKnobs() {}

var _ EventsExporter = &EventsServer{}

var _ obspb.ObsServer = &EventsServer{}

// NewEventServer creates an EventServer.
//
// SetResourceInfo needs to be called before the EventServer is registered with
// a gRPC server.
//
// flushInterval and triggerSize control the circumstances under which the sink
// automatically flushes its contents to the child sink. Zero values disable
// these flush triggers. If all triggers are disabled, the buffer is only ever
// flushed when a flush is explicitly requested through the extraFlush or
// forceSync options passed to output().
//
// maxBufferSize, if not zero, limits the size of the buffer. When a new message
// is causing the buffer to overflow, old messages are dropped. The caller must
// ensure that maxBufferSize makes sense in relation to triggerSize: triggerSize
// should be lower (otherwise the buffer will never flush based on the size
// threshold), and there should be enough of a gap between the two to generally
// fit at least one message (otherwise the buffer might again never flush, since
// incoming messages would cause old messages to be dropped and the buffer's
// size might never fall in between triggerSize and maxSize). See the diagram
// below.
//
// |msg|msg|msg|msg|msg|msg|msg|msg|msg|
// └----------------------^--------------┘
//                      triggerSize    maxBufferSize
//                        └--------------┘
//                           sized-based flush is triggered when size falls in this range
//
// maxBufferSize should also be set such that it makes sense in relationship
// with the flush latency: only one flush is ever in flight at a time, so the
// buffer should be sized to generally hold at least the amount of data that is
// expected to be produced during the time it takes one flush to complete.
func NewEventServer(
	ambient log.AmbientContext,
	clock timeutil.TimeSource,
	stop *stop.Stopper,
	maxStaleness time.Duration,
	triggerSizeBytes uint64,
	maxBufferSizeBytes uint64,
	memMonitor *mon.BytesMonitor,
) *EventsServer {
	s := &EventsServer{
		ambientCtx:         ambient,
		stop:               stop,
		clock:              clock,
		flushInterval:      maxStaleness,
		triggerSizeBytes:   triggerSizeBytes,
		maxBufferSizeBytes: maxBufferSizeBytes,
	}
	s.buf.mu.events = map[obspb.EventType]*eventsBuffer{
		obspb.EventlogEvent: {
			instrumentationScope: otel_pb.InstrumentationScope{
				Name:    string(obspb.EventlogEvent),
				Version: "1.0",
			},
		},
	}
	s.buf.mu.memAccount = memMonitor.MakeBoundAccount()
	return s
}

// SetResourceInfo sets identifying information that will be attached to all the
// exported data.
//
// nodeID can be either a roachpb.NodeID (for KV nodes) or a base.SQLInstanceID
// (for SQL tenants).
func (s *EventsServer) SetResourceInfo(clusterID uuid.UUID, nodeID int32, version string) {
	s.resource = otel_res_pb.Resource{
		Attributes: []*otel_pb.KeyValue{
			{
				Key:   obspb.ClusterID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: clusterID.String()}},
			},
			{
				Key:   obspb.NodeID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_IntValue{IntValue: int64(nodeID)}},
			},
			{
				Key:   obspb.NodeBinaryVersion,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: version}},
			},
		},
	}
	s.resourceSet.Set(true)
}

// eventsBuffers groups together a buffer for each EventType.
//
// Ordered delivery of events (with possible dropped events) to subscribers is
// ensured for individual EventTypes, not across them.
type eventsBuffers struct {
	mu struct {
		syncutil.Mutex
		// events stores all the buffered data.
		events map[obspb.EventType]*eventsBuffer
		// sizeBytes is the sum of sizes for the eventsBuffer's.
		sizeBytes uint64
		// memAccount tracks the memory usage of events.
		memAccount mon.BoundAccount
	}
}

var errEventTooLarge = errors.New("event is too large")

// maybeDropEventsForSizeLocked makes sure there's room in the buffer for
// a new event with size newEventBytes.
//
// If the new event would cause the buffer to overflow (according to maxSize),
// then events are dropped from the buffer until its size drops below maxSize/2.
func (bufs *eventsBuffers) maybeDropEventsForSizeLocked(
	ctx context.Context, newEventSize uint64, maxSize uint64,
) error {
	if newEventSize > maxSize/2 {
		return errEventTooLarge
	}
	size := bufs.mu.sizeBytes
	if (size + newEventSize) < maxSize {
		// The new message fits. There's nothing to do.
		return nil
	}

	// Drop the oldest events from the event types that take up the most space.
	targetSize := maxSize / 2
	needToClearBytes := size - targetSize
	for {
		if bufs.mu.sizeBytes <= targetSize {
			break
		}

		// Find the largest event type.
		var maxEventType obspb.EventType
		maxSize := uint64(0)
		for typ, buf := range bufs.mu.events {
			if buf.sizeBytes > maxSize {
				maxSize = buf.sizeBytes
				maxEventType = typ
			}
		}
		if maxEventType == "" {
			panic("failed to find non-empty EventType")
		}

		// Drop events from the largest event type.
		buf := bufs.mu.events[maxEventType]
		droppedBytes := buf.dropEvents(needToClearBytes)
		buf.sizeBytes -= droppedBytes
		bufs.mu.sizeBytes -= droppedBytes
		bufs.mu.memAccount.Shrink(ctx, int64(droppedBytes))
	}
	return nil
}

// clear clears all the buffers, dropping all messages.
func (bufs *eventsBuffers) clear(ctx context.Context) {
	bufs.mu.Lock()
	defer bufs.mu.Unlock()
	bufs.mu.sizeBytes = 0
	for _, buf := range bufs.mu.events {
		_, _ = buf.moveContents()
	}
	bufs.mu.memAccount.Empty(ctx)
}

// eventsBuffer represents a queue of events of a particular type (identified by
// instrumentationScope).
type eventsBuffer struct {
	instrumentationScope otel_pb.InstrumentationScope
	events               []otel_logs_pb.LogRecord
	sizeBytes            uint64
	// droppedEvents maintains the count of events that have been dropped from the
	// buffer because of memory limits.
	droppedEvents uint64
}

// moveContents empties the buffer, returning all the events in it, and their
// total byte size.
func (b *eventsBuffer) moveContents() ([]otel_logs_pb.LogRecord, uint64) {
	events := b.events
	sizeBytes := b.sizeBytes
	b.events = nil
	b.sizeBytes = 0
	return events, sizeBytes
}

// dropEvents drops events from b until either b is empty, or needToClearBytes
// worth of events have been dropped. Returns the bytes dropped.
func (b *eventsBuffer) dropEvents(needToClearBytes uint64) uint64 {
	cleared := uint64(0)
	for len(b.events) != 0 && cleared < needToClearBytes {
		evSize, err := sizeOfEvent(b.events[0])
		if err != nil {
			// If an event made it in the buffer, its size is supposed to be
			// computable.
			panic(err)
		}
		cleared += evSize
		b.sizeBytes -= evSize
		b.droppedEvents++
		b.events = b.events[1:]
	}
	return cleared
}

var unrecognizedEventEveryN = log.Every(time.Minute)
var unrecognizedEventPayloadEveryN = log.Every(time.Minute)

// SendEvent buffers an event to be sent to subscribers.
func (s *EventsServer) SendEvent(
	ctx context.Context, typ obspb.EventType, event otel_logs_pb.LogRecord,
) {
	// If there's no subscriber, short-circuit.
	//
	// TODO(andrei): We should buffer at least a little bit, so that we don't miss
	// events close to the node start, before the Obs Service (if any) has had a
	// chance to subscribe.
	s.mu.Lock()
	sub := s.mu.sub
	s.mu.Unlock()
	if sub == nil {
		return
	}

	// Make sure there's room for the new event. If there isn't, we'll drop
	// events from the front of the buffer (the oldest), until there is room.
	newEventSize, err := sizeOfEvent(event)
	if err != nil {
		if unrecognizedEventPayloadEveryN.ShouldLog() {
			evCpy := event // copy escapes to the heap
			log.Infof(ctx, "unrecognized event payload for event type: %s (%v)", typ, &evCpy)
		}
	}
	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()
	if err := s.buf.maybeDropEventsForSizeLocked(ctx, newEventSize, s.maxBufferSizeBytes); err != nil {
		log.Warningf(ctx, "%s", err.Error())
		return
	}

	buf, ok := s.buf.mu.events[typ]
	if !ok {
		if unrecognizedEventEveryN.ShouldLog() {
			evCpy := event // copy escapes to the heap
			log.Infof(ctx, "unrecognized event of type: %s (%s)", typ, &evCpy)
		}
	}
	if err := s.buf.mu.memAccount.Grow(ctx, int64(newEventSize)); err != nil {
		// No memory available.
		buf.droppedEvents++
		return
	}

	buf.events = append(buf.events, event)
	buf.sizeBytes += newEventSize
	s.buf.mu.sizeBytes += newEventSize

	// If we've hit the flush threshold, trigger a flush.
	if s.triggerSizeBytes > 0 && s.buf.mu.sizeBytes > s.triggerSizeBytes {
		select {
		case sub.flushC <- struct{}{}:
		default:
		}
	}
}

// sizeOfEvent computes the size, in bytes, of event. This size will be used for
// memory accounting.
//
// Returns an error if the event has a payload for which we haven't implemented
// a measurement.
func sizeOfEvent(event otel_logs_pb.LogRecord) (uint64, error) {
	switch {
	case event.Body.GetBytesValue() != nil:
		return uint64(len(event.Body.GetBytesValue())), nil
	case event.Body.GetStringValue() != "":
		return uint64(len(event.Body.GetStringValue())), nil
	default:
		return 0, errors.Newf("unsupported event: %s", event.Body)
	}
}

// subscriber represents data about an events subscrriber - a caller to the
// SubscribeToEvents RPC.
type subscriber struct {
	// identity represents an identifier for the subscriber. It's used for logging
	// purposes.
	identity string
	// res represents metadata attached to all events, identifying this CRDB node.
	res otel_res_pb.Resource
	// stopC is signaled on close().
	stopC chan error
	// flushAndStopC is closed to signal to the flusher that it should attempt to
	// flush everything and then terminate.
	flushAndStopC <-chan struct{}
	// flusherDoneC is signaled by the flusher goroutine, informing the RPC
	// handler that it finished.
	flusherDoneC chan struct{}
	// flushC is used to signal the flusher goroutine to flush.
	flushC chan struct{}

	mu struct {
		syncutil.Mutex
		conn obspb.Obs_SubscribeToEventsServer
	}
}

// close closes the subscriber. Further calls to send() will return an error.
// The call blocks until sub's flusher goroutine terminates.
//
// close can be called multiple times.
func (sub *subscriber) close(err error) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if sub.mu.conn == nil {
		return
	}

	// Mark ourselves as closed.
	sub.mu.conn = nil
	// Tell the flusher goroutine to terminate.
	sub.stopC <- err
	<-sub.flusherDoneC
}

var errSubscriberClosed = errors.New("subscriber closed")

// send sends events to the remote subscriber. It might block if the network
// connection buffers are full.
//
// If an error is returned, sub is closed and sub.send() should not be called
// anymore.
func (sub *subscriber) send(ctx context.Context, events []otel_logs_pb.ScopeLogs) error {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if sub.mu.conn == nil {
		return errSubscriberClosed
	}

	msg := &obspb.Events{
		ResourceLogs: []*otel_logs_pb.ResourceLogs{
			{
				Resource:  &sub.res,
				ScopeLogs: events,
			},
		},
	}
	err := sub.mu.conn.Send(msg)
	if err != nil {
		// If we failed to send, we can't use this subscriber anymore.
		//
		// TODO(andrei): Figure out how to tolerate errors; we should put the events
		// back in the buffer (or not take them out of the buffer in the first
		// place) in hope that a new subscriber comes along.
		log.Infof(ctx, "error sending to Observability Service %s: %s", sub.identity, err)
		sub.close(err)
		return err
	}
	return nil
}

// newSubscriber creates a subscriber. Events will be sent on conn. The
// subscriber's flusher goroutine listens to flushAndStopC for a signal to flush
// and close.
//
// identity represents an identifier for the subscriber. It's used for logging
// purposes.
func (s *EventsServer) newSubscriber(
	conn obspb.Obs_SubscribeToEventsServer, identity string, flushAndStopC <-chan struct{},
) *subscriber {
	if len(s.resource.Attributes) == 0 {
		panic("resource not set")
	}
	sub := &subscriber{
		identity:      identity,
		res:           s.resource,
		stopC:         make(chan error, 1),
		flushAndStopC: flushAndStopC,
		flusherDoneC:  make(chan struct{}, 1),
		flushC:        make(chan struct{}, 1),
	}
	sub.mu.conn = conn
	return sub
}

// errNewSubscriber is passed to an existing subscriber when a new subscriber
// comes along.
var errNewSubscriber = errors.New("new subscriber")

// errServerNotReady is returned by SubscribeToEvents if the server is not ready
// to process requests.
var errServerNotReady = errors.New("server starting up; not ready to serve RPC")

// SubscribeToEvents is the EventsServer's RPC interface. Events will be pushed
// to subscriber.
func (s *EventsServer) SubscribeToEvents(
	req *obspb.SubscribeToEventsRequest, subscriber obspb.Obs_SubscribeToEventsServer,
) error {
	ctx := s.ambientCtx.AnnotateCtx(subscriber.Context())

	if !s.resourceSet.Get() {
		return errServerNotReady
	}

	var clientAddr redact.SafeString
	client, ok := peer.FromContext(ctx)
	if ok {
		clientAddr = redact.SafeString(client.Addr.String())
	}
	log.Infof(ctx, "received events subscription request from Observability Service; "+
		"subscriber identifying as: %s (%s)", redact.SafeString(req.Identity), clientAddr)

	// Register the new subscriber, replacing any existing one.
	sub := s.newSubscriber(subscriber, req.Identity, s.stop.ShouldQuiesce())
	{
		s.mu.Lock()
		if s.mu.sub != nil {
			s.mu.sub.close(errNewSubscriber)
		}
		s.mu.sub = sub
		s.mu.Unlock()
	}
	if fn := s.TestingKnobs.OnConnect; fn != nil {
		fn(ctx)
	}

	// Run the flusher. This call blocks until this subscriber is signaled to
	// terminate in one of a couple of ways:
	// 1. Through the remote RPC client terminating	the call by canceling ctx.
	// 2. Through a new subscriber coming and calling close() on the old one.
	// 3. Through the stopper quiescing.
	err := sub.runFlusher(ctx, &s.buf, s.flushInterval)

	// Close the subscription, if it hasn't been closed already by the remote
	// subscriber.
	sub.close(nil /* err */)
	s.reset(ctx, sub)
	// Return the reason why we stopped to the caller. If the caller has triggered
	// this stop, then it's probably no longer listening for this return value.
	return err
}

// runFlusher runs the flusher goroutine for the subscriber. The flusher will
// consume eventsBuffer.
//
// flushInterval, if not zero, controls the flush's timer. Flushes are also
// triggered by events size.
//
// runFlusher returns when stopC, flushAndStopC or ctx.Done() are signaled.
func (sub *subscriber) runFlusher(
	ctx context.Context, bufs *eventsBuffers, flushInterval time.Duration,
) error {
	defer close(sub.flusherDoneC)
	timer := timeutil.NewTimer()
	defer timer.Stop()
	if flushInterval != 0 {
		timer.Reset(flushInterval)
	}
	for {
		done := false
		select {
		case err := <-sub.stopC:
			// The sink has gone away; we need to stop consuming the buffers
			// and terminate the flusher goroutine.
			return err
		case <-ctx.Done():
			// The RPC context was canceled. This also signifies that the subscriber
			// has gone away.
			return ctx.Err()
		case <-sub.flushAndStopC:
			// We'll return after flushing everything.
			done = true
		case <-timer.C:
			timer.Read = true
			timer.Reset(flushInterval)
		case <-sub.flushC:
		}

		// Flush the buffers for all event types.
		var msg []otel_logs_pb.ScopeLogs
		msgSize := uint64(0)
		bufs.mu.Lock()
		for _, buf := range bufs.mu.events {
			events, sizeBytes := buf.moveContents()
			if len(events) == 0 {
				continue
			}
			bufs.mu.sizeBytes -= sizeBytes
			msgSize += sizeBytes
			msg = append(msg, otel_logs_pb.ScopeLogs{Scope: &buf.instrumentationScope, LogRecords: events})
		}
		bufs.mu.Unlock()

		if len(msg) > 0 {
			err := sub.send(ctx, msg)
			bufs.mu.Lock()
			bufs.mu.memAccount.Shrink(ctx, int64(msgSize))
			bufs.mu.Unlock()
			// If we failed to send, the subscriber has been closed and cannot be used anymore.
			if err != nil {
				return err
			}
		}

		if done {
			return errors.New("node shutting down")
		}
	}
}

// reset resets the server to an empty state - no subscriber and an empty events
// buffer.
//
// The reset is conditional on the server's subscriber still being sub.
func (s *EventsServer) reset(ctx context.Context, sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.sub != sub {
		// We have already switched to another subscriber.
		return
	}

	s.mu.sub = nil
	s.buf.clear(ctx)
}
