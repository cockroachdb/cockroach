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

package distsql

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
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
	stream  DistSQL_FlowStreamClient

	// simpleFlowStream is set if we are outputting to a simple flow stream; in
	// that case addr and stream will not be set.
	simpleFlowStream DistSQL_RunSimpleFlowServer

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

// newOutboxSimpleFlowStream sets up an outbox for the special "sync flow"
// stream. The flow context should be provided via setFlowCtx when it is
// available.
func newOutboxSimpleFlowStream(stream DistSQL_RunSimpleFlowServer) *outbox {
	return &outbox{simpleFlowStream: stream}
}

func (m *outbox) setFlowCtx(flowCtx *FlowCtx) {
	m.flowCtx = flowCtx
}

// addRow encodes a row into rowBuf. If enough rows were accumulated
// calls flush().
func (m *outbox) addRow(row sqlbase.EncDatumRow) error {
	err := m.encoder.AddRow(row)
	if err != nil {
		return err
	}
	m.numRows++
	if m.numRows >= outboxBufRows {
		return m.flush(false, nil)
	}
	return nil
}

// flush sends the rows accumulated so far in a StreamMessage.
func (m *outbox) flush(last bool, err error) error {
	if !last && m.numRows == 0 {
		return nil
	}
	msg := m.encoder.FormMessage(last, err)

	if log.V(3) {
		log.Infof(m.flowCtx.Context, "flushing outbox")
	}
	var sendErr error
	if m.stream != nil {
		sendErr = m.stream.Send(msg)
	} else {
		sendErr = m.simpleFlowStream.Send(msg)
	}
	if sendErr != nil {
		if log.V(1) {
			log.Errorf(m.flowCtx.Context, "outbox flush error: %s", sendErr)
		}
	} else if log.V(3) {
		log.Infof(m.flowCtx.Context, "outbox flushed")
	}
	if sendErr != nil {
		return sendErr
	}

	m.numRows = 0
	return nil
}

func (m *outbox) mainLoop() error {
	if m.simpleFlowStream == nil {
		conn, err := m.flowCtx.rpcCtx.GRPCDial(m.addr)
		if err != nil {
			return err
		}
		client := NewDistSQLClient(conn)
		if log.V(2) {
			log.Infof(m.flowCtx.Context, "outbox: calling FlowStream")
		}
		m.stream, err = client.FlowStream(context.TODO())
		if err != nil {
			if log.V(1) {
				log.Infof(m.flowCtx.Context, "FlowStream error: %s", err)
			}
			return err
		}
		if log.V(2) {
			log.Infof(m.flowCtx.Context, "outbox: FlowStream returned")
		}
	}

	flushTicker := time.NewTicker(outboxFlushPeriod)
	defer flushTicker.Stop()

	for {
		select {
		case d, ok := <-m.RowChannel.C:
			if !ok {
				// No more data.
				return m.flush(true, nil)
			}
			err := d.Err
			if err == nil {
				err = m.addRow(d.Row)
			}
			if err != nil {
				// Try to flush to send out the error, but ignore any
				// send error.
				_ = m.flush(true, err)
				return err
			}
		case <-flushTicker.C:
			err := m.flush(false, nil)
			if err != nil {
				return err
			}
		}
	}
}

func (m *outbox) run() {
	err := m.mainLoop()

	m.RowChannel.NoMoreRows()
	if err != nil {
		// Drain to allow senders to finish.
		for range m.dataChan {
		}
	}
	if m.stream != nil {
		resp, recvErr := m.stream.CloseAndRecv()
		if err == nil {
			if recvErr != nil {
				err = recvErr
			} else if resp.Error != nil {
				err = resp.Error.GoError()
			}
		}
	}
	m.err = err
	if m.wg != nil {
		m.wg.Done()
	}
}

func (m *outbox) start(wg *sync.WaitGroup) {
	wg.Add(1)
	m.wg = wg
	m.RowChannel.Init()
	go m.run()
}
