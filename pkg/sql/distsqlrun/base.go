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

package distsqlrun

import (
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type joinType int

const (
	innerJoin joinType = iota
	leftOuter
	rightOuter
	fullOuter
)

const rowChannelBufSize = 16

type columns []uint32

// RowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type RowReceiver interface {
	// PushRow sends a row to this receiver. May block.
	// Returns true if the row was sent, or false if the receiver does not need
	// any more rows. In all cases, ProducerDone() still needs to be called.
	// The sender must not modify the row after calling this function.
	PushRow(row sqlbase.EncDatumRow) bool
	// ProducerDone is called when the producer has pushed all the rows; it causes
	// the RowReceiver to process all rows and clean up. If err is not null, the
	// error is sent to the receiver (and the function may block).
	ProducerDone(err error)
}

// RowSource is any component of a flow that produces rows that cam be consumed
// by another component.
type RowSource interface {
	// Types returns the schema for the rows in this source.
	Types() []sqlbase.ColumnType

	// NextRow retrieves the next row. Returns a nil row if there are no more
	// rows. Depending on the implementation, it may block.
	// The caller must not modify the received row.
	NextRow() (sqlbase.EncDatumRow, error)

	// ConsumerDone lets the source know that we will not need any more rows. May
	// block. If the consumer of the source stops consuming rows before NextRow
	// indicates that there are no more rows, this method must be called. It is ok
	// to call the method even if all the rows were consumed.
	ConsumerDone()
}

// StreamMsg is the message used in the channels that implement
// local physical streams.
type StreamMsg struct {
	// Only one of these fields will be set.
	Row sqlbase.EncDatumRow
	Err error
}

// RowChannel is a thin layer over a StreamMsg channel, which can be used to
// transfer rows between goroutines.
type RowChannel struct {
	types []sqlbase.ColumnType

	// The channel on which rows are delivered.
	C <-chan StreamMsg

	// dataChan is the same channel as C.
	dataChan chan StreamMsg

	// noMoreRows is an atomic that signals we no longer accept rows via
	// PushRow.
	noMoreRows uint32
}

var _ RowReceiver = &RowChannel{}
var _ RowSource = &RowChannel{}

// InitWithBufSize initializes the RowChannel with a given buffer size.
func (rc *RowChannel) InitWithBufSize(types []sqlbase.ColumnType, chanBufSize int) {
	rc.types = types
	rc.dataChan = make(chan StreamMsg, chanBufSize)
	rc.C = rc.dataChan
	atomic.StoreUint32(&rc.noMoreRows, 0)
}

// Init initializes the RowChannel with the default buffer size.
func (rc *RowChannel) Init(types []sqlbase.ColumnType) {
	rc.InitWithBufSize(types, rowChannelBufSize)
}

// PushRow is part of the RowReceiver interface.
func (rc *RowChannel) PushRow(row sqlbase.EncDatumRow) bool {
	if atomic.LoadUint32(&rc.noMoreRows) == 1 {
		return false
	}

	rc.dataChan <- StreamMsg{Row: row, Err: nil}
	return true
}

// ProducerDone is part of the interface.
func (rc *RowChannel) ProducerDone(err error) {
	if err != nil {
		rc.dataChan <- StreamMsg{Row: nil, Err: err}
	}
	close(rc.dataChan)
}

// Types is part of the RowSource interface.
func (rc *RowChannel) Types() []sqlbase.ColumnType {
	return rc.types
}

// NextRow is part of the RowSource interface.
func (rc *RowChannel) NextRow() (sqlbase.EncDatumRow, error) {
	d, ok := <-rc.C
	if !ok {
		// No more rows.
		return nil, nil
	}
	if d.Err != nil {
		return nil, d.Err
	}
	return d.Row, nil
}

// ConsumerDone is part of the RowSource interface.
func (rc *RowChannel) ConsumerDone() {
	atomic.StoreUint32(&rc.noMoreRows, 1)
	// Drain (at most) one message in case the sender is blocked trying to emit a
	// row.
	<-rc.dataChan
}

// MultiplexedRowChannel is a RowChannel wrapper which allows multiple row
// producers to push rows on the same channel.
type MultiplexedRowChannel struct {
	rowChan RowChannel
	// numSenders is an atomic counter that keeps track of how many senders have
	// yet to call ProducerDone().
	numSenders int32
	firstErr   chan error
}

var _ RowReceiver = &MultiplexedRowChannel{}
var _ RowSource = &MultiplexedRowChannel{}

// Init initializes the MultiplexedRowChannel with the default buffer size.
func (mrc *MultiplexedRowChannel) Init(numSenders int, types []sqlbase.ColumnType) {
	mrc.rowChan.Init(types)
	atomic.StoreInt32(&mrc.numSenders, int32(numSenders))
	mrc.firstErr = make(chan error, 1)
}

// PushRow is part of the RowReceiver interface.
func (mrc *MultiplexedRowChannel) PushRow(row sqlbase.EncDatumRow) bool {
	return mrc.rowChan.PushRow(row)
}

// ProducerDone is part of the interface.
func (mrc *MultiplexedRowChannel) ProducerDone(err error) {
	if err != nil {
		select {
		case mrc.firstErr <- err:

		default:
		}
	}
	newVal := atomic.AddInt32(&mrc.numSenders, -1)
	if newVal < 0 {
		panic("too many ProducerDone() calls")
	}
	if newVal == 0 {
		var err error
		if len(mrc.firstErr) > 0 {
			err = <-mrc.firstErr
		}
		mrc.rowChan.ProducerDone(err)
	}
}

// Types is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) Types() []sqlbase.ColumnType {
	return mrc.rowChan.types
}

// NextRow is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) NextRow() (sqlbase.EncDatumRow, error) {
	return mrc.rowChan.NextRow()
}

// ConsumerDone is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) ConsumerDone() {
	atomic.StoreUint32(&mrc.rowChan.noMoreRows, 1)
	numSenders := atomic.LoadInt32(&mrc.numSenders)
	// Drain (at most) numSenders messages in case senders are blocked trying to
	// emit a row.
	for i := int32(0); i < numSenders; i++ {
		if _, ok := <-mrc.rowChan.dataChan; !ok {
			break
		}
	}
}

// RowBuffer is an implementation of RowReceiver that buffers (accumulates)
// results in memory, as well as an implementation of RowSource that returns
// rows from a row buffer.
type RowBuffer struct {
	// Rows in this buffer. PushRow appends a row to the back, NextRow removes a
	// row from the front.
	Rows sqlbase.EncDatumRows

	// Err is used when the RowBuffer is used as a RowReceiver; it is the error
	// passed via ProducerDone().
	Err error

	// ProducerClosed is used when the RowBuffer is used as a RowReceiver; it is
	// set to true when the sender calls ProducerDone().
	ProducerClosed bool

	// Done is used when the RowBuffer is used as a RowSource; it is set to true
	// when the receiver read all the rows.
	Done bool

	// Schema of the rows in this buffer.
	types []sqlbase.ColumnType
}

var _ RowReceiver = &RowBuffer{}
var _ RowSource = &RowBuffer{}

// NewRowBuffer creates a RowBuffer with the given schema and initial rows. The
// types are optional if there is at least one row.
func NewRowBuffer(types []sqlbase.ColumnType, rows sqlbase.EncDatumRows) *RowBuffer {
	rb := &RowBuffer{types: types, Rows: rows}

	if len(rb.Rows) > 0 && rb.types == nil {
		rb.types = make([]sqlbase.ColumnType, len(rb.Rows[0]))
		for i, d := range rb.Rows[0] {
			rb.types[i] = d.Type
		}
	}
	return rb
}

// PushRow is part of the RowReceiver interface.
func (rb *RowBuffer) PushRow(row sqlbase.EncDatumRow) bool {
	if rb.ProducerClosed {
		panic("PushRow called after ProducerDone")
	}
	rowCopy := append(sqlbase.EncDatumRow(nil), row...)
	rb.Rows = append(rb.Rows, rowCopy)
	return true
}

// ProducerDone is part of the interface.
func (rb *RowBuffer) ProducerDone(err error) {
	if rb.ProducerClosed {
		panic("RowBuffer already closed")
	}
	rb.Err = err
	rb.ProducerClosed = true
}

// Types is part of the RowSource interface.
func (rb *RowBuffer) Types() []sqlbase.ColumnType {
	if rb.types == nil {
		panic("not initialized with types")
	}
	return rb.types
}

// NextRow is part of the RowSource interface.
func (rb *RowBuffer) NextRow() (sqlbase.EncDatumRow, error) {
	if rb.Err != nil {
		return nil, rb.Err
	}
	if len(rb.Rows) == 0 {
		rb.Done = true
		return nil, nil
	}
	row := rb.Rows[0]
	rb.Rows = rb.Rows[1:]
	return row, nil
}

// ConsumerDone is part of the RowSource interface.
func (rb *RowBuffer) ConsumerDone() {}

// SetFlowRequestTrace populates req.Trace with the context of the current Span
// in the context (if any).
func SetFlowRequestTrace(ctx context.Context, req *SetupFlowRequest) error {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return nil
	}
	req.TraceContext = &tracing.SpanContextCarrier{}
	tracer := sp.Tracer()
	return tracer.Inject(sp.Context(), basictracer.Delegator, req.TraceContext)
}
