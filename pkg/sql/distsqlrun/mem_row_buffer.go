// Copyright 2017 The Cockroach Authors.
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

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"
)

const memRowBufferBufSize = rowChannelBufSize

// memRowBuffer is a simple wrapper around memRowContainer that implements
// RowReceiver and RowSource.
// It can be used as an overflow-safe buffer to temporarily buffer rows between
// row senders and row receivers.
// It does not store any ProducerMetadata during Push.
type memRowBuffer struct {
	ctx context.Context

	// Schema of the rows in this buffer.
	types []sqlbase.ColumnType

	mu struct {
		syncutil.Mutex

		// cond is signaled everytime a producer adds a new row or
		// invokes ProducerDone.
		cond *sync.Cond

		// Level "1" row buffer is used first for few number of rows.  This is
		// a circular FIFO queue with rowBufLen elements and the oldest
		// element (the leftmost) at rowBufFirstIdx.
		rowBuf                [memRowBufferBufSize]sqlbase.EncDatumRow
		rowBufLeft, rowBufLen uint32

		// Level "2" row buffer is a memory-accounted container.
		// This buffer always contains rows "older" than those in rowBuf.
		rowContainer memRowContainer

		// consumeDone is set to true when the receiver read all the rows and
		// calls ConsumerDone().
		consumerDone   bool
		consumerClosed bool

		// producerDone is set to true when the sender acks that it is finished
		// producing rows and calls ProducerDone().
		producerDone bool
	}
}

var _ RowSource = &memRowBuffer{}

func makeMemRowBuffer(
	ctx context.Context, evalCtx *tree.EvalContext, colTypes []sqlbase.ColumnType,
) *memRowBuffer {
	rb := &memRowBuffer{
		ctx:   ctx,
		types: colTypes,
	}
	rb.mu.cond = sync.NewCond(&rb.mu.Mutex)
	rb.mu.rowContainer.init(nil /* ordering */, colTypes, evalCtx)
	return rb
}

func (rb *memRowBuffer) addRow(row sqlbase.EncDatumRow) (ConsumerStatus, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.mu.consumerClosed {
		return ConsumerClosed, nil
	}
	if rb.mu.consumerDone {
		return DrainRequested, nil
	}

	if rb.mu.rowBufLen == memRowBufferBufSize {
		// Take out the oldest row in rowBuf and put it in rowContainer.
		evictedRow := rb.mu.rowBuf[rb.mu.rowBufLeft]
		if err := rb.mu.rowContainer.AddRow(rb.ctx, evictedRow); err != nil {
			return 0, err
		}

		rb.mu.rowBufLeft = (rb.mu.rowBufLeft + 1) % memRowBufferBufSize
		rb.mu.rowBufLen--
	}
	rb.mu.rowBuf[(rb.mu.rowBufLeft+rb.mu.rowBufLen)%routerRowBufSize] = row
	rb.mu.rowBufLen++

	rb.mu.cond.Signal()

	return NeedMoreRows, nil
}

func (rb *memRowBuffer) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	// We pull this out of the critical section since
	// we know the length of the row(s) beforehand.
	ret := make(sqlbase.EncDatumRow, len(rb.types))

	rb.mu.Lock()
	defer rb.mu.Unlock()

	for {
		// Check if there are any rows remaining.
		if rb.mu.rowContainer.Len() > 0 || rb.mu.rowBufLen > 0 {
			var row sqlbase.EncDatumRow

			// Check row container first.
			if rb.mu.rowContainer.Len() > 0 {
				row = rb.mu.rowContainer.EncRow(0)
				rb.mu.rowContainer.PopFirst()
			} else {
				row = rb.mu.rowBuf[rb.mu.rowBufLeft]
				rb.mu.rowBufLeft = (rb.mu.rowBufLeft + 1) % memRowBufferBufSize
				rb.mu.rowBufLen--
			}
			copy(ret, row)
			return ret, ProducerMetadata{}
		}

		// No more rows.
		if rb.mu.producerDone {
			return nil, ProducerMetadata{}
		}

		// Wait for any new event.
		rb.mu.cond.Wait()
	}
}

// ProducerDone can be invoked by the sender to signal that no more rows
// will be produced.
func (rb *memRowBuffer) ProducerDone() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.mu.producerDone = true
	rb.mu.cond.Signal()
}

// Types implements the RowSource interface.
func (rb *memRowBuffer) Types() []sqlbase.ColumnType {
	return rb.types
}

// ConsumerDone implements the RowSource interface.
func (rb *memRowBuffer) ConsumerDone() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.mu.consumerDone = true
	rb.mu.rowContainer.Close(rb.ctx)
	rb.mu.cond.Signal()
}

// ConsumerClosed implements the RowSource interface.
func (rb *memRowBuffer) ConsumerClosed() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.mu.consumerClosed = true
	rb.mu.cond.Signal()
}
