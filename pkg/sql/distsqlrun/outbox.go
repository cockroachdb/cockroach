// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsqlrun

import (
	"io"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const outboxBufRows = 16
const outboxFlushPeriod = 100 * time.Microsecond

// preferredEncoding is the encoding used for EncDatums that don't already have
// an encoding available.
const preferredEncoding = sqlbase.DatumEncoding_ASCENDING_KEY

// outbox implements an outgoing mailbox as a RowReceiver that receives rows and
// sends them to a gRPC stream. Its core logic runs in a goroutine. We send rows
// when we accumulate outboxBufRows or every outboxFlushPeriod (whichever comes
// first).
type outbox struct {
	// RowChannel implements the RowReceiver interface.
	RowChannel

	flowCtx *FlowCtx
	addr    string
	// The rows received from the RowChannel will be forwarded on this stream once
	// it is established.
	stream DistSQL_FlowStreamClient

	// syncFlowStream is set if we are outputting to a sync flow stream; in
	// that case addr and stream will not be set.
	syncFlowStream DistSQL_RunSyncFlowServer

	encoder StreamEncoder
	// numRows is the number of rows that have been accumulated in the encoder.
	numRows int

	err error
	wg  *sync.WaitGroup
}

var _ RowReceiver = &outbox{}

func newOutbox(flowCtx *FlowCtx, addr string, flowID FlowID, streamID StreamID) *outbox {
	m := &outbox{flowCtx: flowCtx, addr: addr}
	m.encoder.setHeaderFields(flowID, streamID)
	return m
}

// newOutboxSyncFlowStream sets up an outbox for the special "sync flow"
// stream. The flow context should be provided via setFlowCtx when it is
// available.
func newOutboxSyncFlowStream(stream DistSQL_RunSyncFlowServer) *outbox {
	return &outbox{syncFlowStream: stream}
}

func (m *outbox) setFlowCtx(flowCtx *FlowCtx) {
	m.flowCtx = flowCtx
}

// addRow encodes a row into rowBuf. If enough rows were accumulated, flush() is
// called.
//
// If an error is returned, the outbox's stream might or might not be usable; if
// it's not usable, it will have been set to nil. The error might be a
// communication error, in which case the other side of the stream should get it
// too, or it might be an encoding error, in which case we've forwarded it on
// the stream.
func (m *outbox) addRow(ctx context.Context, row sqlbase.EncDatumRow, meta ProducerMetadata) error {
	mustFlush := false
	var encodingErr error
	if !meta.Empty() {
		m.encoder.AddMetadata(meta)
		// If we hit an error, let's forward it ASAP. The consumer will probably
		// close.
		mustFlush = meta.Err != nil
	} else {
		encodingErr = m.encoder.AddRow(row)
		if encodingErr != nil {
			m.encoder.AddMetadata(ProducerMetadata{Err: encodingErr})
			mustFlush = true
		}
	}
	m.numRows++
	var flushErr error
	if m.numRows >= outboxBufRows || mustFlush {
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
func (m *outbox) flush(ctx context.Context) error {
	if m.numRows == 0 && m.encoder.headerSent {
		return nil
	}
	msg := m.encoder.FormMessage(ctx)

	if log.V(3) {
		log.Infof(ctx, "flushing outbox")
	}
	var sendErr error
	if m.stream != nil {
		sendErr = m.stream.Send(msg)
	} else {
		sendErr = m.syncFlowStream.Send(msg)
	}
	if sendErr != nil {
		// Make sure the stream is not used any more.
		m.stream = nil
		m.syncFlowStream = nil
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
// addRow()/flush() until the producer doesn't have any more data to send or an
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
// closed. In case it doesn't, m.stream (and also m.syncFlowStream) have been
// set to nil.
func (m *outbox) mainLoop(ctx context.Context) error {
	if m.syncFlowStream == nil {
		conn, err := m.flowCtx.rpcCtx.GRPCDial(m.addr)
		if err != nil {
			return err
		}
		client := NewDistSQLClient(conn)
		if log.V(2) {
			log.Infof(ctx, "outbox: calling FlowStream")
		}
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

	// TODO(andrei): It's unfortunate that we're spawning a goroutine for every
	// outgoing stream, but I'm not sure what to do instead. The streams don't
	// have a non-blocking API. We could start this goroutine only after a
	// timeout, but that timeout would affect queries that use flows with
	// LimitHint's (so, queries where the consumer is expected to quickly ask the
	// producer to drain). Perhaps what we want is a way to tell when all the rows
	// corresponding to the first KV batch have been sent and only start the
	// goroutine if more batches are needed to satisfy the query.
	drainCh := m.waitForDrainSignalFromConsumer()

	flushTicker := time.NewTicker(outboxFlushPeriod)
	defer flushTicker.Stop()

	draining := false
	defer func() {
		m.RowChannel.ConsumerClosed()
	}()

	if m.stream != nil {
		// Send a first message that will contain the header (i.e. the StreamID), so
		// that the stream is properly initialized on the consumer. The consumer has
		// a timeout in which inbound streams must be established.
		if err := m.flush(ctx); err != nil {
			return err
		}
	}

	for {
		select {
		case msg, ok := <-m.RowChannel.C:
			if !ok {
				// No more data.
				return m.flush(ctx)
			}
			if !draining || !msg.Meta.Empty() {
				// If we're draining, we ignore all the rows and just send metadata.
				err := m.addRow(ctx, msg.Row, msg.Meta)
				if err != nil {
					return err
				}
			}
		case <-flushTicker.C:
			err := m.flush(ctx)
			if err != nil {
				return err
			}
		case drainSignal := <-drainCh:
			if drainSignal.err != nil {
				// The consumer either doesn't care any more (it returned from the
				// FlowStream RPC with an error if the outbox established the stream or
				// it cancelled the client context if the consumer established the
				// stream through a RunSyncFlow RPC), or there was a communication error
				// and the stream is dead. In any case, the stream has been closed and
				// the consumer will not consume more rows from this outbox. Make sure
				// the stream is not used any more.
				m.stream = nil
				m.syncFlowStream = nil
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

// waitForDrainSignalFromConsumer returns a channel that will be pinged once the
// consumer has closed its send-side of the stream, or has sent a drain signal.
func (m *outbox) waitForDrainSignalFromConsumer() <-chan drainSignal {
	ch := make(chan drainSignal, 1)

	go func(
		stream DistSQL_FlowStreamClient,
		syncFlowStream DistSQL_RunSyncFlowServer,
	) {
		var signal *ConsumerSignal
		var err error
		if stream != nil {
			signal, err = stream.Recv()
		} else {
			signal, err = syncFlowStream.Recv()
		}
		if err == io.EOF {
			ch <- drainSignal{drainRequested: false, err: nil}
			return
		}
		if err != nil {
			ch <- drainSignal{drainRequested: false, err: err}
			return
		}
		ch <- drainSignal{drainRequested: signal.DrainRequest != nil, err: nil}
	}(m.stream, m.syncFlowStream)
	return ch
}

func (m *outbox) run(ctx context.Context) {
	err := m.mainLoop(ctx)
	if m.stream != nil {
		closeErr := m.stream.CloseSend()
		if err == nil {
			err = closeErr
		}
	}
	m.err = err
	if m.wg != nil {
		m.wg.Done()
	}
}

func (m *outbox) start(ctx context.Context, wg *sync.WaitGroup) {
	m.wg = wg
	m.RowChannel.Init(nil)
	go m.run(ctx)
}
