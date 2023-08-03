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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otel_collector_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	otel_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	otel_logs_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	otel_res_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EventsExporterInterface abstracts exporting events to the Observability
// Service. It is implemented by EventsExporter.
type EventsExporterInterface interface {
	// SetNodeInfo initializes the node information that will be included in every
	// batch of events that gets exported. This information is not passed to
	// NewEventsExporter() to allow the EventsExporter to be constructed before
	// the node ID is known.
	SetNodeInfo(NodeInfo)

	// SetDialer configures the dialer to be used when opening network connections.
	SetDialer(dialer func(ctx context.Context, _ string) (net.Conn, error))

	// Start starts the goroutine that will periodically flush the events to the
	// configured sink.
	//
	// Flushes are triggered by the configured flush interval and by the buffer size
	// threshold.
	Start(context.Context, *stop.Stopper) error

	// SendEvent buffers an event to be sent.
	//
	// SendEvent does not block. If the buffer is full, old events are dropped.
	//
	// SendEvent can be called before Start(). Such events will be buffered
	// (within the buffering limits) and sent after Start() is eventually called.
	SendEvent(ctx context.Context, typ obspb.EventType, event otel_logs_pb.LogRecord)
}

// NoopEventsExporter is an EventsExporter that ignores events.
type NoopEventsExporter struct{}

var _ EventsExporterInterface = &NoopEventsExporter{}

// SetNodeInfo is part of the EventsExporterInterface.
func (nop NoopEventsExporter) SetNodeInfo(NodeInfo) {}

// Start is part of the EventsExporterInterface.
func (d NoopEventsExporter) Start(ctx context.Context, stop *stop.Stopper) error {
	return nil
}

// SetDialer is part of the EventsExporterInterface.
func (nop NoopEventsExporter) SetDialer(
	dialer func(ctx context.Context, _ string) (net.Conn, error),
) {
}

// SendEvent is part of the EventsExporterInterface.
func (nop NoopEventsExporter) SendEvent(context.Context, obspb.EventType, otel_logs_pb.LogRecord) {}

// EventExporterTestingKnobs can be passed to Server to adjust flushing for the
// EventExporter.
type EventExporterTestingKnobs struct {
	// FlushInterval, if set, overrides the default trigger interval for the
	// EventExporter.
	FlushInterval time.Duration
	// FlushTriggerByteSize, if set, overrides the default trigger value for the
	// EventExporter.
	FlushTriggerByteSize uint64
	// TestConsumer, if set, sets the consumer to be used by the embedded ingest
	// component used. This allows us to capture consumed events when running
	// in embedded mode, so we can make assertions against them in tests.
	TestConsumer obslib.EventConsumer
}

var _ base.ModuleTestingKnobs = &EventExporterTestingKnobs{}

// ModuleTestingKnobs implements the ModuleTestingKnobs interface.
func (e *EventExporterTestingKnobs) ModuleTestingKnobs() {}

// EventsExporter is a buffered client for the OTLP logs gRPC service. It is
// used to export events to the Observability Service (possibly through an
// OpenTelemetry Collector).
//
// The EventsExporter buffers events and flushes them out periodically
// (according to flushInterval) and when a buffer size threshold is met
// (triggerSizeBytes).
//
// NOTE: In the future, the EventsExporter might be replaced by direct use of
// the otel Go SDK. As of this writing, though, the SDK does not support logs.
type EventsExporter struct {
	clock      timeutil.TimeSource
	tr         *tracing.Tracer
	targetAddr string

	dialer func(ctx context.Context, _ string) (net.Conn, error)

	// flushInterval is the duration after which a flush is triggered.
	// 0 disables this trigger.
	flushInterval time.Duration
	// triggerSizeBytes is the size in bytes of accumulated messages which trigger a flush.
	// 0 disables this trigger.
	triggerSizeBytes   uint64
	maxBufferSizeBytes uint64

	resource otel_res_pb.Resource

	// buf accumulates events to be sent.
	buf eventsBuffers

	// flushC is used to signal the flusher goroutine to flush.
	flushC chan struct{}

	// otelClient is the client for the OpenTelemetry Logs Service. It is used
	// to push events to the Obs Service (directly or through the Otel
	// Collector).
	//
	// Nil if the EventsExporter is not configured to send out the events.
	otelClient otel_collector_pb.LogsServiceClient
	conn       *grpc.ClientConn
}

// ValidateOTLPTargetAddr validates the target address filling the possible
// missing port with the default.
func ValidateOTLPTargetAddr(targetAddr string) (string, error) {
	otlpHost, otlpPort, err := addr.SplitHostPort(targetAddr, "4317" /* defaultPort */)
	if err != nil {
		return "", errors.Newf("invalid OTLP host in --obsservice-addr=%s", targetAddr)
	}
	if otlpHost == "" {
		return "", errors.Newf("missing OTLP host in --obsservice-addr=%s", targetAddr)
	}
	return net.JoinHostPort(otlpHost, otlpPort), nil
}

// NewEventsExporter creates an EventsExporter.
//
// Start() needs to be called before the EventsExporter actually exports any
// events.
//
// An error is returned if targetAddr is invalid.
//
// flushInterval and triggerSize control the circumstances under which the
// exporter flushes its contents to the network sink. Zero values disable these
// flush triggers.
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
//
//	triggerSize    maxBufferSize
//	  └--------------┘
//	     sized-based flush is triggered when size falls in this range
//
// maxBufferSize should also be set such that it makes sense in relationship
// with the flush latency: only one flush is ever in flight at a time, so the
// buffer should be sized to generally hold at least the amount of data that is
// expected to be produced during the time it takes one flush to complete.
func NewEventsExporter(
	targetAddr string,
	clock timeutil.TimeSource,
	tr *tracing.Tracer,
	maxStaleness time.Duration,
	triggerSizeBytes uint64,
	maxBufferSizeBytes uint64,
	memMonitor *mon.BytesMonitor,
) *EventsExporter {
	s := &EventsExporter{
		clock:              clock,
		tr:                 tr,
		targetAddr:         targetAddr,
		flushInterval:      maxStaleness,
		triggerSizeBytes:   triggerSizeBytes,
		maxBufferSizeBytes: maxBufferSizeBytes,
		flushC:             make(chan struct{}, 1),
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

// NodeInfo groups the information identifying a node that will be included in
// all exported events.
type NodeInfo struct {
	ClusterID uuid.UUID
	// NodeID can be either a roachpb.NodeID (for KV nodes) or a
	// base.SQLInstanceID (for SQL tenants).
	NodeID int32
	// BinaryVersion is the executable's version.
	BinaryVersion string
}

// SetDialer configures the dialer to be used when opening network connections.
func (s *EventsExporter) SetDialer(dialer func(ctx context.Context, _ string) (net.Conn, error)) {
	s.dialer = dialer
}

// SetNodeInfo initializes the node information that will be included in every
// batch of events that gets exported. This information is not passed to
// NewEventsExporter() to allow the EventsExporter to be constructed before
// the node ID is known.
func (s *EventsExporter) SetNodeInfo(nodeInfo NodeInfo) {
	s.resource = otel_res_pb.Resource{
		Attributes: []*otel_pb.KeyValue{
			{
				Key:   obspb.ClusterID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: nodeInfo.ClusterID.String()}},
			},
			{
				Key:   obspb.NodeID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_IntValue{IntValue: int64(nodeInfo.NodeID)}},
			},
			{
				Key:   obspb.NodeBinaryVersion,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: nodeInfo.BinaryVersion}},
			},
		},
	}
}

// Start starts the goroutine that will periodically flush the events to the
// configured sink.
//
// Flushes are triggered by the configured flush interval and by the buffer size
// threshold.
func (s *EventsExporter) Start(ctx context.Context, stopper *stop.Stopper) error {
	// TODO(andrei): Add support for TLS / mutual TLS.
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if s.dialer != nil {
		opts = append(opts, grpc.WithContextDialer(s.dialer))
	}
	// Note that Dial is non-blocking.
	conn, err := grpc.Dial(s.targetAddr, opts...)
	if err != nil {
		return err
	}
	s.otelClient = otel_collector_pb.NewLogsServiceClient(conn)
	s.conn = conn

	ctx = logtags.WithTags(context.Background(), logtags.FromContext(ctx))
	ctx, cancel := context.WithCancel(ctx)
	stopper.AddCloser(stop.CloserFn(func() {
		cancel()
	}))
	ctx, sp := s.tr.StartSpanCtx(ctx, "obsservice flusher", tracing.WithSterile())
	go func() {
		defer sp.Finish()
		defer func() {
			_ = s.conn.Close() // nolint:grpcconnclose
		}()
		timer := timeutil.NewTimer()
		defer timer.Stop()
		if s.flushInterval != 0 {
			timer.Reset(s.flushInterval)
		}
		for {
			done := false
			select {
			case <-ctx.Done():
				// We'll return after flushing everything.
				done = true
			case <-timer.C:
				timer.Read = true
				timer.Reset(s.flushInterval)
			case <-s.flushC:
			}

			// Flush the buffers for all event types.
			msgSize := uint64(0)
			totalEvents := 0
			req := &otel_collector_pb.ExportLogsServiceRequest{
				ResourceLogs: []*otel_logs_pb.ResourceLogs{
					{Resource: &s.resource},
				},
			}
			func() {
				s.buf.mu.Lock()
				defer s.buf.mu.Unlock()
				// Iterate through the different types of events.
				req.ResourceLogs[0].ScopeLogs = make([]otel_logs_pb.ScopeLogs, 0, len(s.buf.mu.events))
				for _, buf := range s.buf.mu.events {
					events, sizeBytes := buf.moveContents()
					if len(events) == 0 {
						continue
					}
					totalEvents += len(events)
					s.buf.mu.sizeBytes -= sizeBytes
					msgSize += sizeBytes
					req.ResourceLogs[0].ScopeLogs = append(req.ResourceLogs[0].ScopeLogs,
						otel_logs_pb.ScopeLogs{Scope: &buf.instrumentationScope, LogRecords: events})
				}
			}()

			if len(req.ResourceLogs[0].ScopeLogs) > 0 {
				_, err := s.otelClient.Export(ctx, req, grpc.WaitForReady(true))
				func() {
					s.buf.mu.Lock()
					defer s.buf.mu.Unlock()
					s.buf.mu.memAccount.Shrink(ctx, int64(msgSize))
				}()
				if err != nil {
					log.Warningf(ctx, "failed to export events: %s", err)
				} else {
					log.VInfof(ctx, 2, "exported %d events totalling %d bytes", totalEvents, msgSize)
				}
			}

			if done {
				return
			}
		}
	}()
	return nil
}

// eventsBuffers groups together a buffer for each EventType.
//
// Ordered exporting of events (with possible dropped events) is ensured for
// individual EventTypes, not across them.
type eventsBuffers struct {
	mu struct {
		syncutil.Mutex
		// events stores all the buffered data, grouped by the type of event.
		events map[obspb.EventType]*eventsBuffer
		// sizeBytes is the sum of sizes for the eventsBuffers.
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

// SendEvent buffers an event to be sent.
//
// SendEvent does not block. If the buffer is full, old events are dropped.
//
// SendEvent can be called before Start(). Such events will be buffered
// (within the buffering limits) and sent after Start() is eventually called.
func (s *EventsExporter) SendEvent(
	ctx context.Context, typ obspb.EventType, event otel_logs_pb.LogRecord,
) {
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
		log.Warningf(ctx, "%v", err)
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
		case s.flushC <- struct{}{}:
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
