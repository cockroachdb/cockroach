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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

const rowChannelBuf = 16

// rowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type rowReceiver interface {
	// PushRow sends a row to this receiver. May block.
	// Returns true if the row was sent, or false if the receiver does not need
	// any more rows. In all cases, Close() still needs to be called.
	PushRow(row sqlbase.EncDatumRow) bool
	// Close is called when we have no more rows; it causes the rowReceiver to
	// process all rows and clean up. If err is not null, the error is sent to
	// the receiver (and the function may block).
	Close(err error)
}

// processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type processor interface {
	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(wg *sync.WaitGroup)
}

// StreamMsg is the message used in the channels that implement
// local physical streams.
type StreamMsg struct {
	// Only one of these fields will be set.
	row sqlbase.EncDatumRow
	err error
}

// RowChannel is a thin layer over a StreamMsg channel
type RowChannel struct {
	// The channel on which rows are delivered.
	C <-chan StreamMsg

	// dataChan is the same channel as C.
	dataChan chan StreamMsg

	// noMoreRows is an atomic that signals we no longer accept rows via
	// PushRow.
	noMoreRows uint32
}

var _ rowReceiver = &RowChannel{}

func (rc *RowChannel) init() {
	rc.dataChan = make(chan StreamMsg, rowChannelBuf)
	rc.C = rc.dataChan
	atomic.StoreUint32(&rc.noMoreRows, 0)
}

// PushRow is part of the rowReceiver interface.
func (rc *RowChannel) PushRow(row sqlbase.EncDatumRow) bool {
	if atomic.LoadUint32(&rc.noMoreRows) == 1 {
		return false
	}

	rc.dataChan <- StreamMsg{row: row, err: nil}
	return true
}

// Close is part of the rowReceiver interface.
func (rc *RowChannel) Close(err error) {
	if err != nil {
		rc.dataChan <- StreamMsg{row: nil, err: err}
	}
	close(rc.dataChan)
}

// NoMoreRows causes future PushRow calls to return false. The caller should
// still drain the channel to make sure the sender is not blocked.
func (rc *RowChannel) NoMoreRows() {
	atomic.StoreUint32(&rc.noMoreRows, 1)
}
