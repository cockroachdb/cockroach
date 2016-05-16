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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

const outboxBufRows = 16
const outboxChanRows = 16
const outboxFlushPeriod = 100 * time.Microsecond

// preferredEncoding is the encoding used for EncDatums that don't already have
// an encoding available.
const preferredEncoding = sqlbase.AscendingKeyEncoding

type outboxStream interface {
	Send(*StreamMessage) error
}

// outbox implements an outgoing mailbox as a rowReceiver that receives rows and
// sends them to a gRPC stream. Its core logic runs in a goroutine.
type outbox struct {
	stream outboxStream

	dataChan    chan streamMsg
	flushTicker *time.Ticker

	// infos is initialized when the first row is received
	infos []DatumInfo

	rowBuf []byte
	// numRows is the number of rows that have been accumulated in rowBuf.
	numRows          int
	sentFirstMessage bool

	err error
	wg  *sync.WaitGroup

	alloc sqlbase.DatumAlloc
	// Placeholders to avoid allocations
	msg    StreamMessage
	msgHdr StreamHeader
	msgTrl StreamTrailer
}

var _ rowReceiver = &outbox{}

// pushRow is part of the rowReceiver interface.
func (m *outbox) pushRow(row row) {
	m.dataChan <- streamMsg{row: row, err: nil}
}

// close is part of the rowReceiver interface.
func (m *outbox) close(err error) {
	if err != nil {
		m.dataChan <- streamMsg{row: nil, err: err}
	}
	close(m.dataChan)
}

func newOutbox(stream outboxStream) *outbox {
	return &outbox{stream: stream}
}

// addRow encodes a row into rowBuf.
func (m *outbox) addRow(row []sqlbase.EncDatum) error {
	if m.infos == nil {
		// First row. Initialize encodings.
		m.infos = make([]DatumInfo, len(row))
		for i := range row {
			enc := row[i].Encoding()
			if enc == sqlbase.NoEncoding {
				enc = preferredEncoding
			}
			m.infos[i].Encoding = datumEncodingToDatumInfoEncoding(enc)
		}
	}
	if len(m.infos) != len(row) {
		return util.Errorf("inconsistent row length: had %d, now %d",
			len(m.infos), len(row))
	}
	for i := range row {
		enc := m.infos[i].Encoding.toDatumEncoding()
		var err error
		m.rowBuf, err = row[i].Encode(&m.alloc, enc, m.rowBuf)
		if err != nil {
			return err
		}
	}
	m.numRows++
	if m.numRows >= outboxBufRows {
		return m.flush(false, nil)
	}
	return nil
}

// flush sends the rows accumulated so far in a StreamMessage.
func (m *outbox) flush(last bool, err error) error {
	msg := &m.msg
	msg.Header = nil
	msg.Data.RawBytes = m.rowBuf
	msg.Trailer = nil
	if !m.sentFirstMessage {
		msg.Header = &m.msgHdr
		msg.Header.Info = m.infos
	}
	if last {
		msg.Trailer = &m.msgTrl
		msg.Trailer.Error = roachpb.NewError(err)
	}

	sendErr := m.stream.Send(msg)
	if sendErr != nil {
		return sendErr
	}

	m.numRows = 0
	m.rowBuf = m.rowBuf[:0]
	m.sentFirstMessage = true
	return nil
}

func (m *outbox) mainLoop() {
	var err error
loop:
	for {
		select {
		case d, ok := <-m.dataChan:
			if !ok {
				// No more data.
				err = m.flush(true, nil)
				break loop
			}
			err = d.err
			if err == nil {
				err = m.addRow(d.row)
			}
			if err != nil {
				// Try to flush to send out the error, but ignore any
				// send error.
				_ = m.flush(true, err)
				break loop
			}
		case <-m.flushTicker.C:
			err = m.flush(false, nil)
			if err != nil {
				break loop
			}
		}
	}
	m.flushTicker.Stop()
	if err != nil {
		// Drain to allow senders to finish.
		for range m.dataChan {
		}
	}
	m.err = err
	if m.wg != nil {
		m.wg.Done()
	}
}

func (m *outbox) start(wg *sync.WaitGroup) {
	m.wg = wg
	m.dataChan = make(chan streamMsg, outboxChanRows)
	m.flushTicker = time.NewTicker(outboxFlushPeriod)
	go m.mainLoop()
}
