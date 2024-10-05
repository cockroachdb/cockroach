// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
)

// OutboxBufRows is the maximum number of rows that are buffered by the Outbox
// before flushing.
const OutboxBufRows = 16
const outboxFlushPeriod = 100 * time.Microsecond

type flowStream interface {
	Send(*execinfrapb.ProducerMessage) error
	Recv() (*execinfrapb.ConsumerSignal, error)
}

// Outbox implements an outgoing mailbox as a RowReceiver that receives rows and
// sends them to a gRPC stream. Its core logic runs in a goroutine. We send rows
// when we accumulate OutboxBufRows or every outboxFlushPeriod (whichever comes
// first).
type Outbox struct {
	// RowChannel implements the RowReceiver interface.
	execinfra.RowChannel

	flowCtx       *execinfra.FlowCtx
	processorID   int32
	streamID      execinfrapb.StreamID
	sqlInstanceID base.SQLInstanceID
	// The rows received from the RowChannel will be forwarded on this stream once
	// it is established.
	stream flowStream

	encoder StreamEncoder
	// numRows is the number of rows that have been accumulated in the encoder.
	numRows int

	// flowCtxCancel is the cancellation function for this flow's ctx; context
	// cancellation is used to stop processors on this flow. It is invoked
	// whenever the consumer returns an error on the stream above. Set
	// to a non-null value in start().
	flowCtxCancel context.CancelFunc
	// outboxCtxCancel is the cancellation function for this Outbox's ctx. It is
	// used when gracefully shutting down the tree rooted in this Outbox. (Note
	// that it is possible for a single flow to have multiple Outboxes, and
	// outboxCtxCancel shuts down only one of them whereas flowCtxCancel shuts
	// down all of them.)
	outboxCtxCancel context.CancelFunc

	mu struct {
		syncutil.Mutex
		// err is only used for testing
		err error
	}

	statsCollectionEnabled bool
	streamStats, flowStats execinfrapb.ComponentStats

	// numOutboxes is an atomic that keeps track of how many outboxes are left.
	// When there is one outbox left, the flow-level stats are added to the last
	// outbox's span stats unless isGatewayNode is true, in which case, the flow
	// will do so in its Cleanup method.
	numOutboxes *int32

	// isGatewayNode specifies whether this outbox is running on the gateway node.
	isGatewayNode bool
}

var _ execinfra.RowReceiver = &Outbox{}
var _ Startable = &Outbox{}

// NewOutbox creates a new Outbox.
func NewOutbox(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	sqlInstanceID base.SQLInstanceID,
	streamID execinfrapb.StreamID,
	numOutboxes *int32,
	isGatewayNode bool,
) *Outbox {
	m := &Outbox{flowCtx: flowCtx, processorID: processorID, sqlInstanceID: sqlInstanceID}
	m.encoder.SetHeaderFields(flowCtx.ID, streamID)
	m.streamID = streamID
	m.numOutboxes = numOutboxes
	m.isGatewayNode = isGatewayNode
	m.streamStats.Component = flowCtx.StreamComponentID(streamID)
	m.flowStats.Component = flowCtx.FlowComponentID()
	return m
}

// Init initializes the Outbox.
func (m *Outbox) Init(typs []*types.T) {
	if typs == nil {
		// We check for nil to detect uninitialized cases; but we support 0-length
		// rows.
		typs = make([]*types.T, 0)
	}
	m.RowChannel.InitWithNumSenders(typs, 1)
	m.encoder.Init(typs)
}

// AddRow encodes a row into rowBuf. If enough rows were accumulated, flush() is
// called.
//
// If an error is returned, the outbox's stream might or might not be usable; if
// it's not usable, it will have been set to nil. The error might be a
// communication error, in which case the other side of the stream should get it
// too, or it might be an encoding error, in which case we've forwarded it on
// the stream.
func (m *Outbox) AddRow(
	ctx context.Context, row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) error {
	mustFlush := false
	var encodingErr error
	if meta != nil {
		m.encoder.AddMetadata(ctx, *meta)
		// If we hit an error, let's forward it ASAP. The consumer will probably
		// close.
		mustFlush = meta.Err != nil
	} else {
		encodingErr = m.encoder.AddRow(row)
		if encodingErr != nil {
			m.encoder.AddMetadata(ctx, execinfrapb.ProducerMetadata{Err: encodingErr})
			mustFlush = true
		}
		if m.statsCollectionEnabled {
			m.streamStats.NetTx.TuplesSent.Add(1)
		}
	}
	m.numRows++
	var flushErr error
	if m.numRows >= OutboxBufRows || mustFlush {
		flushErr = m.flush(ctx)
	}
	if encodingErr != nil {
		return encodingErr
	}
	return flushErr
}

// flush sends the rows accumulated so far in a ProducerMessage. Any error
// returned indicates that sending a message on the outbox's stream failed, and
// thus the stream can't be used any more. The stream is also set to nil if
// an error is returned.
func (m *Outbox) flush(ctx context.Context) error {
	if m.numRows == 0 && m.encoder.HasHeaderBeenSent() {
		return nil
	}
	msg := m.encoder.FormMessage(ctx)

	log.VEvent(ctx, 2, "Outbox flushing")
	sendErr := m.stream.Send(msg)
	if m.statsCollectionEnabled {
		m.streamStats.NetTx.BytesSent.Add(int64(msg.Size()))
		m.streamStats.NetTx.MessagesSent.Add(1)
	}
	for _, rpm := range msg.Data.Metadata {
		if metricsMeta, ok := rpm.Value.(*execinfrapb.RemoteProducerMetadata_Metrics_); ok {
			metricsMeta.Metrics.Release()
		}
	}
	if sendErr != nil {
		HandleStreamErr(ctx, "flushing", sendErr, m.flowCtxCancel, m.outboxCtxCancel)
		// Make sure the stream is not used any more.
		m.stream = nil
		log.VWarningf(ctx, 1, "Outbox flush error: %s", sendErr)
	} else {
		log.VEvent(ctx, 2, "Outbox flushed")
	}
	if sendErr != nil {
		return sendErr
	}

	m.numRows = 0
	return nil
}

// mainLoop reads from m.RowChannel and writes to the output stream through
// AddRow()/flush() until the producer doesn't have any more data to send or an
// error happened.
//
// If the consumer asks the producer to drain, mainLoop() will relay this
// information and, again, wait until the producer doesn't have any more data to
// send (the producer is supposed to only send trailing metadata once it
// receives this signal).
//
// If an error is returned, it's either a communication error from the outbox's
// stream, or otherwise the error has already been forwarded on the stream.
// Depending on the specific error, the stream might or might not need to be
// closed. In case it doesn't, m.stream has been set to nil.
func (m *Outbox) mainLoop(ctx context.Context, wg *sync.WaitGroup) (retErr error) {
	// No matter what happens, we need to make sure we close our RowChannel, since
	// writers could be writing to it as soon as we are started.
	defer m.RowChannel.ConsumerClosed()

	// Derive a child context so that we can cancel all components rooted in
	// this outbox. This cancellation function will be called when shutting down
	// the outbox gracefully (in case of the ungraceful shutdown flowCtxCancel
	// is called). See comment on startWatchdogGoroutine for more details.
	ctx, m.outboxCtxCancel = context.WithCancel(ctx)

	var span *tracing.Span
	ctx, span = execinfra.ProcessorSpan(ctx, m.flowCtx, "outbox", m.processorID)
	defer span.Finish()
	if span != nil {
		m.statsCollectionEnabled = span.RecordingType() != tracingpb.RecordingOff
		if span.IsVerbose() {
			span.SetTag(execinfrapb.StreamIDTagKey, attribute.IntValue(int(m.streamID)))
		}
	}

	if err := func() error {
		conn, err := execinfra.GetConnForOutbox(
			ctx, m.flowCtx.Cfg.SQLInstanceDialer, m.sqlInstanceID, SettingFlowStreamTimeout.Get(&m.flowCtx.Cfg.Settings.SV),
		)
		if err != nil {
			log.VWarningf(ctx, 1, "Outbox Dial connection error, distributed query will fail: %+v", err)
			return err
		}
		client := execinfrapb.NewDistSQLClient(conn)
		if log.V(2) {
			log.Infof(ctx, "outbox: calling FlowStream")
		}
		m.stream, err = client.FlowStream(ctx)
		if err != nil {
			log.VWarningf(ctx, 1, "Outbox FlowStream connection error, distributed query will fail: %+v", err)
			return err
		}
		return nil
	}(); err != nil {
		// An error during stream setup - the whole query will fail, so we might
		// as well proactively cancel the flow on this node.
		m.flowCtxCancel()
		return err
	}
	if log.V(2) {
		log.Infof(ctx, "outbox: FlowStream returned")
	}

	// Make sure to always close the stream if it is still usable (if not, then
	// the field is set to nil).
	defer func() {
		if stream, ok := m.stream.(execinfrapb.DistSQL_FlowStreamClient); ok {
			closeErr := stream.CloseSend()
			if retErr == nil {
				retErr = closeErr
			}
		}
	}()

	var flushTimer timeutil.Timer
	defer flushTimer.Stop()

	draining := false

	watchdogCh, err := m.startWatchdogGoroutine(ctx, wg)
	if err != nil {
		return err
	}

	// Send a first message that will contain the header (i.e. the StreamID), so
	// that the stream is properly initialized on the consumer. The consumer has
	// a timeout in which inbound streams must be established.
	if err := m.flush(ctx); err != nil {
		return err
	}

	for {
		select {
		case msg, ok := <-m.RowChannel.C:
			if !ok {
				// No more data.
				if m.statsCollectionEnabled {
					err := m.flush(ctx)
					if err != nil {
						return err
					}
					if !m.isGatewayNode && m.numOutboxes != nil && atomic.AddInt32(m.numOutboxes, -1) == 0 {
						m.flowStats.FlowStats.MaxMemUsage.Set(uint64(m.flowCtx.Mon.MaximumBytes()))
						m.flowStats.FlowStats.MaxDiskUsage.Set(uint64(m.flowCtx.DiskMonitor.MaximumBytes()))
						m.flowStats.FlowStats.ConsumedRU.Set(uint64(m.flowCtx.TenantCPUMonitor.EndCollection(ctx)))
					}
					span.RecordStructured(&m.streamStats)
					span.RecordStructured(&m.flowStats)
					if !m.flowCtx.Gateway {
						if trace := tracing.SpanFromContext(ctx).GetConfiguredRecording(); trace != nil {
							err := m.AddRow(ctx, nil, &execinfrapb.ProducerMetadata{TraceData: trace})
							if err != nil {
								return err
							}
						}
					}
				}
				return m.flush(ctx)
			}
			if !draining || msg.Meta != nil {
				// If we're draining, we ignore all the rows and just send metadata.
				err := m.AddRow(ctx, msg.Row, msg.Meta)
				if err != nil {
					return err
				}
				if msg.Meta != nil {
					// Now that we have added metadata, it is safe to release it to the
					// pool.
					msg.Meta.Release()
				}
				// If the message to add was metadata, a flush was already forced. If
				// this is our first row, restart the flushTimer.
				if m.numRows == 1 {
					flushTimer.Reset(outboxFlushPeriod)
				}
			}
		case <-flushTimer.C:
			flushTimer.Read = true
			err := m.flush(ctx)
			if err != nil {
				return err
			}
		case _, ok := <-watchdogCh:
			if !ok {
				// The watchdog goroutine exited indicating that we should too.
				return nil
			}
			// The consumer requested draining.
			draining = true
			m.RowChannel.ConsumerDone()
		}
	}
}

// startWatchdogGoroutine spins up a new goroutine whose job is to listen
// continually on the stream from the consumer for errors or drain requests.
//
// This goroutine will tear down the flow (by calling flowCtxCancel) if
// non-io.EOF error is received - without it, a producer goroutine might spin
// doing work for a long time after a connection is closed, since it wouldn't
// notice a closed connection until it tried to Send a message over that
// connection.
//
// Similarly, if an io.EOF error is received, it indicates that the server side
// of FlowStream RPC (the inbox) has exited gracefully, so the inbox doesn't
// need anything else from this outbox, and this goroutine will shut down the
// tree of operators rooted in this outbox (by calling outboxCtxCancel).
//
// It returns a channel that serves two purposes:
// - it will be signaled when draining is requested by the consumer;
// - it will be closed once the new goroutine exits. When that happens, the main
// goroutine of the outbox can exit too since the consumer no longer needs
// anything from the outbox.
func (m *Outbox) startWatchdogGoroutine(
	ctx context.Context, wg *sync.WaitGroup,
) (<-chan struct{}, error) {
	// The channel is buffered in order to not block the watchdog goroutine when
	// it is signaling about the drain request to the main goroutine.
	ch := make(chan struct{}, 1)

	stream := m.stream
	wg.Add(1)
	if err := m.flowCtx.Cfg.Stopper.RunAsyncTask(ctx, "watchdog", func(ctx context.Context) {
		defer wg.Done()
		for {
			signal, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					// io.EOF is considered a graceful termination of the gRPC
					// stream, so it is ignored.
					m.setErr(err)
				}
				HandleStreamErr(ctx, "watchdog Recv", err, m.flowCtxCancel, m.outboxCtxCancel)
				break
			}
			switch {
			case signal.Handshake != nil:
				log.VEventf(ctx, 2, "Outbox received handshake: %s", signal.Handshake)
			case signal.DrainRequest != nil:
				log.VEventf(ctx, 2, "Outbox received drain request")
				ch <- struct{}{}
			}
		}
		close(ch)
	}); err != nil {
		wg.Done()
		return nil, err
	}
	return ch, nil
}

// Start starts the outbox.
func (m *Outbox) Start(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc) {
	if m.OutputTypes() == nil {
		panic("outbox not initialized")
	}
	m.flowCtxCancel = flowCtxCancel
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.setErr(m.mainLoop(ctx, wg))
	}()
}

// setErr sets the error stored in the Outbox if it hasn't been set previously.
func (m *Outbox) setErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.err == nil {
		m.mu.err = err
	}
}

// Err returns the error (if any occurred) while Outbox was running.
func (m *Outbox) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.err
}

// HandleStreamErr is a utility method used to handle an error when calling
// a method on a flowStreamClient. If err is an io.EOF, outboxCtxCancel is
// called, for all other errors flowCtxCancel is. The given error is logged with
// the associated opName.
func HandleStreamErr(
	ctx context.Context,
	opName redact.SafeString,
	err error,
	flowCtxCancel, outboxCtxCancel context.CancelFunc,
) {
	if err == io.EOF {
		log.VEventf(ctx, 2, "Outbox calling outboxCtxCancel after %s EOF", opName)
		outboxCtxCancel()
	} else {
		log.VEventf(ctx, 1, "Outbox calling flowCtxCancel after %s connection error: %+v", opName, err)
		flowCtxCancel()
	}
}
