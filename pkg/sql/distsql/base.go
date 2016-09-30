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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

const rowChannelBufSize = 16

// RowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type RowReceiver interface {
	// PushRow sends a row to this receiver. May block.
	// Returns true if the row was sent, or false if the receiver does not need
	// any more rows. In all cases, Close() still needs to be called.
	// The sender must not modify the row after calling this function.
	PushRow(row sqlbase.EncDatumRow) bool
	// Close is called when we have no more rows; it causes the RowReceiver to
	// process all rows and clean up. If err is not null, the error is sent to
	// the receiver (and the function may block).
	Close(err error)
}

// RowSource is any component of a flow that produces rows that cam be consumed
// by another component.
type RowSource interface {
	// NextRow retrieves the next row. Returns a nil row if there are no more
	// rows. Depending on the implementation, it may block.
	// The caller must not modify the received row.
	NextRow() (sqlbase.EncDatumRow, error)
}

// processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type processor interface {
	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(wg *sync.WaitGroup)
}

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the router. It can be useful in the last stage of a
// computation, where we may only need the synchronizer to join streams.
type noopProcessor struct {
	flowCtx *FlowCtx
	input   RowSource
	output  RowReceiver
}

var _ processor = &noopProcessor{}

func newNoopProcessor(flowCtx *FlowCtx, input RowSource, output RowReceiver) *noopProcessor {
	return &noopProcessor{flowCtx: flowCtx, input: input, output: output}
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	for {
		row, err := n.input.NextRow()
		if err != nil || row == nil {
			n.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(n.flowCtx.Context, "noop: pushing row %s\n", row)
		}
		if !n.output.PushRow(row) {
			return
		}
	}
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
func (rc *RowChannel) InitWithBufSize(chanBufSize int) {
	rc.dataChan = make(chan StreamMsg, chanBufSize)
	rc.C = rc.dataChan
	atomic.StoreUint32(&rc.noMoreRows, 0)
}

// Init initializes the RowChannel with the default buffer size.
func (rc *RowChannel) Init() {
	rc.InitWithBufSize(rowChannelBufSize)
}

// PushRow is part of the RowReceiver interface.
func (rc *RowChannel) PushRow(row sqlbase.EncDatumRow) bool {
	if atomic.LoadUint32(&rc.noMoreRows) == 1 {
		return false
	}

	rc.dataChan <- StreamMsg{Row: row, Err: nil}
	return true
}

// Close is part of the RowReceiver interface.
func (rc *RowChannel) Close(err error) {
	if err != nil {
		rc.dataChan <- StreamMsg{Row: nil, Err: err}
	}
	close(rc.dataChan)
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

// NoMoreRows causes future PushRow calls to return false. The caller should
// still drain the channel to make sure the sender is not blocked.
func (rc *RowChannel) NoMoreRows() {
	atomic.StoreUint32(&rc.noMoreRows, 1)
}

// MultiplexedRowChannel is a RowChannel wrapper which allows multiple row
// producers to push rows on the same channel.
type MultiplexedRowChannel struct {
	rowChan RowChannel
	// numSenders is an atomic counter that keeps track of how many senders have
	// yet to call Close().
	numSenders int32
	firstErr   error
}

var _ RowReceiver = &MultiplexedRowChannel{}
var _ RowSource = &MultiplexedRowChannel{}

// Init initializes the MultiplexedRowChannel with the default buffer size.
func (mrc *MultiplexedRowChannel) Init(numSenders int) {
	mrc.rowChan.Init()
	atomic.StoreInt32(&mrc.numSenders, int32(numSenders))
	mrc.firstErr = nil
}

// PushRow is part of the RowReceiver interface.
func (mrc *MultiplexedRowChannel) PushRow(row sqlbase.EncDatumRow) bool {
	return mrc.rowChan.PushRow(row)
}

// Close is part of the RowReceiver interface.
func (mrc *MultiplexedRowChannel) Close(err error) {
	if err != nil {
		mrc.firstErr = err
	}
	newVal := atomic.AddInt32(&mrc.numSenders, -1)
	if newVal < 0 {
		panic("too many Close() calls")
	}
	if newVal == 0 {
		mrc.rowChan.Close(mrc.firstErr)
	}
}

// NextRow is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) NextRow() (sqlbase.EncDatumRow, error) {
	return mrc.rowChan.NextRow()
}

// RowBuffer is an implementation of RowReceiver that buffers (accumulates)
// results in memory, as well as an implementation of RowSource that returns
// rows from a row buffer.
type RowBuffer struct {
	rows sqlbase.EncDatumRows
	err  error

	// closed is used when the RowBuffer is used as a RowReceiver; it is set to
	// true when the sender calls Close.
	closed bool

	// done is used when the RowBuffer is used as a RowSource; it is set to true
	// when the receiver read all the rows.
	done bool
}

var _ RowReceiver = &RowBuffer{}
var _ RowSource = &RowBuffer{}

// PushRow is part of the RowReceiver interface.
func (rb *RowBuffer) PushRow(row sqlbase.EncDatumRow) bool {
	rowCopy := append(sqlbase.EncDatumRow(nil), row...)
	rb.rows = append(rb.rows, rowCopy)
	return true
}

// Close is part of the RowReceiver interface.
func (rb *RowBuffer) Close(err error) {
	rb.err = err
	rb.closed = true
}

// NextRow is part of the RowSource interface.
func (rb *RowBuffer) NextRow() (sqlbase.EncDatumRow, error) {
	if rb.err != nil {
		return nil, rb.err
	}
	if len(rb.rows) == 0 {
		rb.done = true
		return nil, nil
	}
	row := rb.rows[0]
	rb.rows = rb.rows[1:]
	return row, nil
}

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
