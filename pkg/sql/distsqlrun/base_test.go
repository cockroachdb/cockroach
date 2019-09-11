// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// bufferedRecord represents a row or metadata record that has been buffered
// inside a RowBuffer.
type bufferedRecord struct {
	row  sqlbase.EncDatumRow
	meta *distsqlpb.ProducerMetadata
}

// RowBuffer is an implementation of RowReceiver that buffers (accumulates)
// results in memory, as well as an implementation of RowSource that returns
// records from a record buffer. Just for tests.
type RowBuffer struct {
	mu struct {
		syncutil.Mutex

		// producerClosed is used when the RowBuffer is used as a RowReceiver; it is
		// set to true when the sender calls ProducerDone().
		producerClosed bool

		// records represent the data that has been buffered. Push appends a row
		// to the back, Next removes a row from the front.
		records []bufferedRecord
	}

	// Done is used when the RowBuffer is used as a RowSource; it is set to true
	// when the receiver read all the rows.
	Done bool

	ConsumerStatus distsql.ConsumerStatus

	// Schema of the rows in this buffer.
	types []types.T

	args rowBufferArgs
}

var _ distsql.RowReceiver = &RowBuffer{}
var _ distsql.RowSource = &RowBuffer{}

// rowBufferArgs contains testing-oriented parameters for a RowBuffer.
type rowBufferArgs struct {
	// If not set, then the RowBuffer will behave like a RowChannel and not
	// accumulate rows after it's been put in draining mode. If set, rows will still
	// be accumulated. Useful for tests that want to observe what rows have been
	// pushed after draining.
	AccumulateRowsWhileDraining bool
	// OnConsumerDone, if specified, is called as the first thing in the
	// ConsumerDone() method.
	OnConsumerDone func(*RowBuffer)
	// OnConsumerClose, if specified, is called as the first thing in the
	// ConsumerClosed() method.
	OnConsumerClosed func(*RowBuffer)
	// OnNext, if specified, is called as the first thing in the Next() method.
	// If it returns an empty row and metadata, then RowBuffer.Next() is allowed
	// to run normally. Otherwise, the values are returned from RowBuffer.Next().
	OnNext func(*RowBuffer) (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata)
	// OnPush, if specified, is called as the first thing in the Push() method.
	OnPush func(sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata)
}

// newRowBuffer creates a RowBuffer with the given schema and initial rows.
func newRowBuffer(types []types.T, rows sqlbase.EncDatumRows, hooks rowBufferArgs) *RowBuffer {
	if types == nil {
		panic("types required")
	}
	wrappedRows := make([]bufferedRecord, len(rows))
	for i, row := range rows {
		wrappedRows[i].row = row
	}
	rb := &RowBuffer{types: types, args: hooks}
	rb.mu.records = wrappedRows
	return rb
}

// Push is part of the RowReceiver interface.
func (rb *RowBuffer) Push(
	row sqlbase.EncDatumRow, meta *distsqlpb.ProducerMetadata,
) distsql.ConsumerStatus {
	if rb.args.OnPush != nil {
		rb.args.OnPush(row, meta)
	}
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.mu.producerClosed {
		panic("Push called after ProducerDone")
	}
	// We mimic the behavior of RowChannel.
	storeRow := func() {
		rowCopy := append(sqlbase.EncDatumRow(nil), row...)
		rb.mu.records = append(rb.mu.records, bufferedRecord{row: rowCopy, meta: meta})
	}
	status := distsql.ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if rb.args.AccumulateRowsWhileDraining {
		storeRow()
	} else {
		switch status {
		case distsql.NeedMoreRows:
			storeRow()
		case distsql.DrainRequested:
			if meta != nil {
				storeRow()
			}
		case distsql.ConsumerClosed:
		}
	}
	return status
}

// ProducerClosed is a utility function used by tests to check whether the
// RowBuffer has had ProducerDone() called on it.
func (rb *RowBuffer) ProducerClosed() bool {
	rb.mu.Lock()
	c := rb.mu.producerClosed
	rb.mu.Unlock()
	return c
}

// ProducerDone is part of the RowSource interface.
func (rb *RowBuffer) ProducerDone() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.mu.producerClosed {
		panic("RowBuffer already closed")
	}
	rb.mu.producerClosed = true
}

// Types is part of the RowReceiver interface.
func (rb *RowBuffer) Types() []types.T {
	return rb.types
}

// OutputTypes is part of the RowSource interface.
func (rb *RowBuffer) OutputTypes() []types.T {
	if rb.types == nil {
		panic("not initialized")
	}
	return rb.types
}

// Start is part of the RowSource interface.
func (rb *RowBuffer) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
//
// There's no synchronization here with Push(). The assumption is that these
// two methods are not called concurrently.
func (rb *RowBuffer) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	if rb.args.OnNext != nil {
		row, meta := rb.args.OnNext(rb)
		if row != nil || meta != nil {
			return row, meta
		}
	}
	if len(rb.mu.records) == 0 {
		rb.Done = true
		return nil, nil
	}
	rec := rb.mu.records[0]
	rb.mu.records = rb.mu.records[1:]
	return rec.row, rec.meta
}

// ConsumerDone is part of the RowSource interface.
func (rb *RowBuffer) ConsumerDone() {
	if atomic.CompareAndSwapUint32((*uint32)(&rb.ConsumerStatus),
		uint32(distsql.NeedMoreRows), uint32(distsql.DrainRequested)) {
		if rb.args.OnConsumerDone != nil {
			rb.args.OnConsumerDone(rb)
		}
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rb *RowBuffer) ConsumerClosed() {
	status := distsql.ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if status == distsql.ConsumerClosed {
		log.Fatalf(context.Background(), "RowBuffer already closed")
	}
	atomic.StoreUint32((*uint32)(&rb.ConsumerStatus), uint32(distsql.ConsumerClosed))
	if rb.args.OnConsumerClosed != nil {
		rb.args.OnConsumerClosed(rb)
	}
}

// NextNoMeta is a version of Next which fails the test if
// it encounters any metadata.
func (rb *RowBuffer) NextNoMeta(tb testing.TB) sqlbase.EncDatumRow {
	row, meta := rb.Next()
	if meta != nil {
		tb.Fatalf("unexpected metadata: %v", meta)
	}
	return row
}

// GetRowsNoMeta returns the rows in the buffer; it fails the test if it
// encounters any metadata.
func (rb *RowBuffer) GetRowsNoMeta(t *testing.T) sqlbase.EncDatumRows {
	var res sqlbase.EncDatumRows
	for {
		row := rb.NextNoMeta(t)
		if row == nil {
			break
		}
		res = append(res, row)
	}
	return res
}
