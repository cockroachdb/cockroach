// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	err error

	statsCollectionEnabled bool
	stats                  execinfrapb.ComponentStats

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
	sqlInstanceID base.SQLInstanceID,
	streamID execinfrapb.StreamID,
	numOutboxes *int32,
	isGatewayNode bool,
) *Outbox {
	m := &Outbox{flowCtx: flowCtx, sqlInstanceID: sqlInstanceID}
	m.encoder.SetHeaderFields(flowCtx.ID, streamID)
	m.streamID = streamID
	m.numOutboxes = numOutboxes
	m.isGatewayNode = isGatewayNode
	m.stats.Component = flowCtx.StreamComponentID(streamID)
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
			m.stats.NetTx.TuplesSent.Add(1)
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

	if log.V(3) {
		log.Infof(ctx, "flushing outbox")
	}
	sendErr := m.stream.Send(msg)
	if m.statsCollectionEnabled {
		m.stats.NetTx.BytesSent.Add(int64(msg.Size()))
		m.stats.NetTx.MessagesSent.Add(1)
	}
	for _, rpm := range msg.Data.Metadata {
		if metricsMeta, ok := rpm.Value.(*execinfrapb.RemoteProducerMetadata_Metrics_); ok {
			metricsMeta.Metrics.Release()
		}
	}
	if sendErr != nil {
		// Make sure the stream is not used any more.
		m.stream = nil
		if log.V(1) {
			log.Errorf(ctx, "outbox flush error: %s", sendErr)
		}
	} else if log.V(3) {
		log.Infof(ctx, "outbox flushed")
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
func (m *Outbox) mainLoop(ctx context.Context) error {
	// No matter what happens, we need to make sure we close our RowChannel, since
	// writers could be writing to it as soon as we are started.
	defer m.RowChannel.ConsumerClosed()

	var span *tracing.Span
	ctx, span = execinfra.ProcessorSpan(ctx, "outbox")
	defer span.Finish()
	if span != nil && span.IsVerbose() {
		m.statsCollectionEnabled = true
		span.SetTag(execinfrapb.FlowIDTagKey, attribute.StringValue(m.flowCtx.ID.String()))
		span.SetTag(execinfrapb.StreamIDTagKey, attribute.IntValue(int(m.streamID)))
	}

	if m.stream == nil {
		conn, err := execinfra.GetConnForOutbox(
			ctx, m.flowCtx.Cfg.PodNodeDialer, m.sqlInstanceID, SettingFlowStreamTimeout.Get(&m.flowCtx.Cfg.Settings.SV),
		)
		if err != nil {
			// Log any Dial errors. This does not have a verbosity check due to being
			// a critical part of query execution: if this step doesn't work, the
			// receiving side might end up hanging or timing out.
			log.Infof(ctx, "outbox: connection dial error: %+v", err)
			return err
		}
		client := execinfrapb.NewDistSQLClient(conn)
		if log.V(2) {
			log.Infof(ctx, "outbox: calling FlowStream")
		}
		// The context used here escapes, so it has to be a background context.
		// TODO(yuzefovich): the usage of the TODO context here is suspicious.
		// Investigate this.
		m.stream, err = client.FlowStream(context.TODO())
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "FlowStream error: %s", err)
			}
			return err
		}
		if log.V(2) {
			log.Infof(ctx, "outbox: FlowStream returned")
		}
	}

	var flushTimer timeutil.Timer
	defer flushTimer.Stop()

	draining := false

	// TODO(andrei): It's unfortunate that we're spawning a goroutine for every
	// outgoing stream, but I'm not sure what to do instead. The streams don't
	// have a non-blocking API. We could start this goroutine only after a
	// timeout, but that timeout would affect queries that use flows with
	// LimitHint's (so, queries where the consumer is expected to quickly ask the
	// producer to drain). Perhaps what we want is a way to tell when all the rows
	// corresponding to the first KV batch have been sent and only start the
	// goroutine if more batches are needed to satisfy the query.
	listenToConsumerCtx, cancel := contextutil.WithCancel(ctx)
	drainCh, err := m.listenForDrainSignalFromConsumer(listenToConsumerCtx)
	defer cancel()
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
						// TODO(cathymw): maxMemUsage shouldn't be attached to span stats that are associated with streams,
						// since it's a flow level stat. However, due to the row exec engine infrastructure, it is too
						// complicated to attach this to a flow level span. If the row exec engine gets removed, getting
						// maxMemUsage from streamStats should be removed as well.
						m.stats.FlowStats.MaxMemUsage.Set(uint64(m.flowCtx.EvalCtx.Mon.MaximumBytes()))
						m.stats.FlowStats.MaxDiskUsage.Set(uint64(m.flowCtx.DiskMonitor.MaximumBytes()))
					}
					span.RecordStructured(&m.stats)
					if trace := execinfra.GetTraceData(ctx); trace != nil {
						err := m.AddRow(ctx, nil, &execinfrapb.ProducerMetadata{TraceData: trace})
						if err != nil {
							return err
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
		case drainSignal := <-drainCh:
			if drainSignal.err != nil {
				// Stop work from proceeding in this flow. This also causes FlowStream
				// RPCs that have this node as consumer to return errors.
				m.flowCtxCancel()
				// The consumer either doesn't care any more (it returned from the
				// FlowStream RPC with an error), or there was a communication error
				// and the stream is dead. In any case, the stream has been closed and
				// the consumer will not consume more rows from this outbox. Make sure
				// the stream is not used any more.
				m.stream = nil
				return drainSignal.err
			}
			drainCh = nil
			if drainSignal.drainRequested {
				// Enter draining mode.
				draining = true
				m.RowChannel.ConsumerDone()
			} else {
				// No draining required. We're done; no need to consume any more.
				// m.RowChannel.ConsumerClosed() is called in a defer above.
				return nil
			}
		}
	}
}

// drainSignal is a signal received from the consumer telling the producer that
// it doesn't need any more rows and optionally asking the producer to drain.
type drainSignal struct {
	// drainRequested, if set, means that the consumer is interested in the
	// trailing metadata that the producer might have. If not set, the producer
	// should close immediately (the consumer is probably gone by now).
	drainRequested bool
	// err, if set, is either the error that the consumer returned when closing
	// the FlowStream RPC or a communication error.
	err error
}

// listenForDrainSignalFromConsumer returns a channel that will be pinged once the
// consumer has closed its send-side of the stream, or has sent a drain signal.
//
// This method runs a task that will run until either the consumer closes the
// stream or until the caller cancels the context. The caller has to cancel the
// context once it no longer reads from the channel, otherwise this method might
// deadlock when attempting to write to the channel.
func (m *Outbox) listenForDrainSignalFromConsumer(ctx context.Context) (<-chan drainSignal, error) {
	ch := make(chan drainSignal, 1)

	stream := m.stream
	if err := m.flowCtx.Cfg.Stopper.RunAsyncTask(ctx, "drain", func(ctx context.Context) {
		sendDrainSignal := func(drainRequested bool, err error) (shouldExit bool) {
			select {
			case ch <- drainSignal{drainRequested: drainRequested, err: err}:
				return false
			case <-ctx.Done():
				// Listening for consumer signals has been canceled indicating
				// that the main outbox routine is no longer listening to these
				// signals.
				return true
			}
		}

		for {
			signal, err := stream.Recv()
			if err == io.EOF {
				// io.EOF indicates graceful completion of the stream, so we
				// don't use io.EOF as an error.
				sendDrainSignal(false, nil)
				return
			}
			if err != nil {
				sendDrainSignal(false, err)
				return
			}
			switch {
			case signal.DrainRequest != nil:
				if shouldExit := sendDrainSignal(true, nil); shouldExit {
					return
				}
			case signal.Handshake != nil:
				log.Eventf(ctx, "consumer sent handshake.\nConsuming flow scheduled: %t",
					signal.Handshake.ConsumerScheduled)
			}
		}
	}); err != nil {
		return nil, err
	}
	return ch, nil
}

func (m *Outbox) run(ctx context.Context, wg *sync.WaitGroup) {
	err := m.mainLoop(ctx)
	if stream, ok := m.stream.(execinfrapb.DistSQL_FlowStreamClient); ok {
		closeErr := stream.CloseSend()
		if err == nil {
			err = closeErr
		}
	}
	m.err = err
	if wg != nil {
		wg.Done()
	}
}

// Start starts the outbox.
func (m *Outbox) Start(ctx context.Context, wg *sync.WaitGroup, flowCtxCancel context.CancelFunc) {
	if m.OutputTypes() == nil {
		panic("outbox not initialized")
	}
	if wg != nil {
		wg.Add(1)
	}
	m.flowCtxCancel = flowCtxCancel
	go m.run(ctx, wg)
}

// Err returns the error (if any occurred) while Outbox was running.
func (m *Outbox) Err() error {
	return m.err
}
