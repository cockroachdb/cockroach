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
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// ConsumerStatus is the type returned by RowReceiver.Push(), informing a
// producer of a consumer's state.
type ConsumerStatus uint32

const (
	// NeedMoreRows indicates that the consumer is still expecting more rows.
	NeedMoreRows ConsumerStatus = iota
	// DrainRequested indicates that the consumer will not process any more data
	// rows, but will accept trailing metadata from the producer.
	DrainRequested
	// ConsumerClosed indicates that the consumer will not process any more data
	// rows or metadata. This is also commonly returned in case the consumer has
	// encountered an error.
	ConsumerClosed
)

// RowReceiver is any component of a flow that receives rows from another
// component. It can be an input synchronizer, a router, or a mailbox.
type RowReceiver interface {
	// Push sends a record to the consumer of this RowReceiver. Exactly one of the
	// row/meta must be specified (i.e. either row needs to be non-nil or meta
	// needs to be non-Empty()). May block.
	//
	// The return value indicates the current status of the consumer. Depending on
	// it, producers are expected to drain or shut down. In all cases,
	// ProducerDone() needs to be called (after draining is done, if draining was
	// requested).
	//
	// The sender must not modify the row after calling this function.
	//
	// After DrainRequested is returned, it is expected that all future calls only
	// carry metadata (however that is not enforced and implementations should be
	// prepared to discard non-metadata rows). If ConsumerClosed is returned,
	// implementations have to ignore further calls to Push() (such calls are
	// allowed because there might be multiple producers for a single RowReceiver
	// and they might not all be aware of the last status returned).
	//
	// Implementations of Push() must be thread-safe.
	Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus

	// ProducerDone is called when the producer has pushed all the rows and
	// metadata; it causes the RowReceiver to process all rows and clean up.
	//
	// ProducerDone() cannot be called concurrently with Push(), and after it
	// is called, no other method can be called.
	ProducerDone()
}

// RowSource is any component of a flow that produces rows that cam be consumed
// by another component.
type RowSource interface {
	// Types returns the schema for the rows in this source.
	Types() []sqlbase.ColumnType

	// Next returns the next record that a producer has pushed into this
	// RowSource. At most one of the return values will be non-empty. Both of them
	// can be empty when the RowSource has been exhausted - no more records are
	// coming and any further method calls will be no-ops.
	//
	// A ProducerMetadata record may contain an error. In that case, this
	// interface is oblivious about the semantics: implementers may continue
	// returning different rows on future calls, or may return an empty record
	// (thus asking the consumer to stop asking for rows). In particular,
	// implementers are not required to only return metadata records from this
	// point on (which means, for example, that they're not required to
	// automatically ask every producer to drain, in case there's multiple
	// producers). Therefore, consumers need to be aware that some rows might have
	// been skipped in case they continue to consume rows. Usually a consumer
	// should react to an error by calling ConsumerDone(), thus asking the
	// RowSource to drain, and separately discard any future data rows.
	Next() (sqlbase.EncDatumRow, ProducerMetadata)

	// ConsumerDone lets the producer know that we will not need any more data
	// rows. The producer is expected to start draining and only send metadata
	// rows.
	//
	// May block. If the consumer of the source stops consuming rows before
	// Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerDone()

	// ConsumerClosed informs the producer that the consumer will not be reading
	// any more rows. The producer is expected to shut down without sending
	// anything else.
	//
	// Like ConsumerDone(), if the consumer of the source stops consuming rows
	// before Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerClosed()
}

// DrainAndForwardMetadata calls src.ConsumerDone() (thus asking src for
// draining metadata) and then forwards all the metadata to dst.
//
// When this returns, src has been properly closed (regardless of the presence
// or absence of an error). dst, however, has not been closed; someone else must
// call dst.ProducerDone() when all producers have finished draining.
//
// It is OK to call DrainAndForwardMetadata() multiple times concurrently on the
// same dst (as RowReceiver.Push() is guaranteed to be thread safe).
//
// TODO(andrei): errors seen while draining should be reported to the gateway,
// but they shouldn't fail a SQL query.
func DrainAndForwardMetadata(ctx context.Context, src RowSource, dst RowReceiver) {
	src.ConsumerDone()
	for {
		row, meta := src.Next()
		if meta.Empty() {
			if row == nil {
				return
			}
			continue
		}
		if row != nil {
			log.Fatalf(ctx, "both row data and metadata in the same record. row: %s meta: %s",
				row, meta)
		}

		switch dst.Push(row, meta) {
		case ConsumerClosed:
			src.ConsumerClosed()
			return
		case NeedMoreRows:
		case DrainRequested:
		}
	}
}

// DrainAndClose is a version of DrainAndForwardMetadata that drains multiple
// sources. These sources are assumed to be the only producers left for dst, so
// dst is closed once they're all exhausted (this is different from
// DrainAndForwardMetadata).
//
// If cause is specified, it is forwarded to the consumer before all the drain
// metadata. This is intended to have been the error, if any, that caused the
// draining.
//
// srcs can be nil.
//
// All errors are forwarded to the producer.
func DrainAndClose(ctx context.Context, dst RowReceiver, cause error, srcs ...RowSource) {
	if cause != nil {
		// We ignore the returned ConsumerStatus and rely on the
		// DrainAndForwardMetadata() calls below to close srcs in all cases.
		_ = dst.Push(nil /* row */, ProducerMetadata{Err: cause})
	}
	var wg sync.WaitGroup
	for _, input := range srcs {
		wg.Add(1)
		go func(input RowSource) {
			DrainAndForwardMetadata(ctx, input, dst)
			wg.Done()
		}(input)
	}
	wg.Wait()
	dst.ProducerDone()
}

// NoMetadataRowSource is a wrapper on top of a RowSource that automatically
// forwards metadata to a RowReceiver. Data rows are returned through an
// interface similar to RowSource, except that, since metadata is taken care of,
// only the data rows are returned.
//
// The point of this struct is that it'd be burdensome for some row consumers to
// have to deal with metadata.
type NoMetadataRowSource struct {
	src          RowSource
	metadataSink RowReceiver
}

// MakeNoMetadataRowSource builds a NoMetadataRowSource.
func MakeNoMetadataRowSource(src RowSource, sink RowReceiver) NoMetadataRowSource {
	return NoMetadataRowSource{src: src, metadataSink: sink}
}

// NextRow is analogous to RowSource.Next. If the producer sends an error, we
// can't just forward it to metadataSink. We need to let the consumer know so
// that it's not under the impression that everything is hunky-dory and it can
// continue consuming rows. So, this interface returns the error. Just like with
// a raw RowSource, the consumer should generally call ConsumerDone() and drain.
func (rs *NoMetadataRowSource) NextRow() (sqlbase.EncDatumRow, error) {
	for {
		row, meta := rs.src.Next()
		if meta.Err != nil {
			return nil, meta.Err
		}
		if meta.Empty() {
			return row, nil
		}
		// We forward the metadata and ignore the returned ConsumerStatus. There's
		// no good way to use that status here; eventually the consumer of this
		// NoMetadataRowSource will figure out the same status and act on it as soon
		// as a non-metadata row is received.
		_ = rs.metadataSink.Push(nil /* row */, meta)
	}
}

// RowChannelMsg is the message used in the channels that implement
// local physical streams (i.e. the RowChannel's).
type RowChannelMsg struct {
	// Only one of these fields will be set.
	Row  sqlbase.EncDatumRow
	Meta ProducerMetadata
}

// ProducerMetadata represents a metadata record flowing through a DistSQL flow.
type ProducerMetadata struct {
	// Only one of these fields will be set. If this ever changes, note that
	// there's consumers out there that extract the error and, if there is one,
	// forward it in isolation and drop the rest of the record.
	Ranges []roachpb.RangeInfo
	// TODO(vivek): change to type Error
	Err error
}

// Empty returns true if none of the fields in metadata are populated.
func (meta ProducerMetadata) Empty() bool {
	return meta.Ranges == nil && meta.Err == nil
}

// RowChannel is a thin layer over a RowChannelMsg channel, which can be used to
// transfer rows between goroutines.
type RowChannel struct {
	types []sqlbase.ColumnType

	// The channel on which rows are delivered.
	C <-chan RowChannelMsg

	// dataChan is the same channel as C.
	dataChan chan RowChannelMsg

	// consumerStatus is an atomic that signals whether the RowChannel is only
	// accepting draining metadata or is no longer accepting any rows via Push.
	consumerStatus ConsumerStatus
}

var _ RowReceiver = &RowChannel{}
var _ RowSource = &RowChannel{}

// InitWithBufSize initializes the RowChannel with a given buffer size.
func (rc *RowChannel) InitWithBufSize(types []sqlbase.ColumnType, chanBufSize int) {
	rc.types = types
	rc.dataChan = make(chan RowChannelMsg, chanBufSize)
	rc.C = rc.dataChan
}

// Init initializes the RowChannel with the default buffer size.
func (rc *RowChannel) Init(types []sqlbase.ColumnType) {
	rc.InitWithBufSize(types, rowChannelBufSize)
}

// Push is part of the RowReceiver interface.
func (rc *RowChannel) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	consumerStatus := ConsumerStatus(
		atomic.LoadUint32((*uint32)(&rc.consumerStatus)))
	switch consumerStatus {
	case NeedMoreRows:
		rc.dataChan <- RowChannelMsg{Row: row, Meta: meta}
	case DrainRequested:
		// If we're draining, only forward metadata.
		if !meta.Empty() {
			rc.dataChan <- RowChannelMsg{Meta: meta}
		}
	case ConsumerClosed:
		// If the consumer is gone, swallow all the rows.
	}
	return consumerStatus
}

// ProducerDone is part of the RowReceiver interface.
func (rc *RowChannel) ProducerDone() {
	close(rc.dataChan)
}

// Types is part of the RowSource interface.
func (rc *RowChannel) Types() []sqlbase.ColumnType {
	return rc.types
}

// Next is part of the RowSource interface.
func (rc *RowChannel) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	d, ok := <-rc.C
	if !ok {
		// No more rows.
		return nil, ProducerMetadata{}
	}
	return d.Row, d.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rc *RowChannel) ConsumerDone() {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rc.consumerStatus)))
	if status != NeedMoreRows {
		log.Fatalf(context.TODO(), "RowChannel already done or closed: %d", status)
	}
	atomic.StoreUint32((*uint32)(&rc.consumerStatus), uint32(DrainRequested))
}

// ConsumerClosed is part of the RowSource interface.
func (rc *RowChannel) ConsumerClosed() {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rc.consumerStatus)))
	if status == ConsumerClosed {
		panic("RowChannel already closed")
	}
	atomic.StoreUint32((*uint32)(&rc.consumerStatus), uint32(ConsumerClosed))
	// Read (at most) one message in case the producer is blocked trying to emit a
	// row. Note that, if the producer is done, then it has also closed the
	// channel this will not block. The producer might be neither blocked nor
	// closed, though; hence the default case.
	select {
	case <-rc.dataChan:
	default:
	}
}

// MultiplexedRowChannel is a RowChannel wrapper which allows multiple row
// producers to push rows on the same channel.
type MultiplexedRowChannel struct {
	rowChan RowChannel
	// numSenders is an atomic counter that keeps track of how many senders have
	// yet to call ProducerDone().
	numSenders int32
}

var _ RowReceiver = &MultiplexedRowChannel{}
var _ RowSource = &MultiplexedRowChannel{}

// Init initializes the MultiplexedRowChannel with the default buffer size.
func (mrc *MultiplexedRowChannel) Init(numSenders int, types []sqlbase.ColumnType) {
	mrc.rowChan.Init(types)
	atomic.StoreInt32(&mrc.numSenders, int32(numSenders))
}

// Push is part of the RowReceiver interface.
func (mrc *MultiplexedRowChannel) Push(
	row sqlbase.EncDatumRow, meta ProducerMetadata,
) ConsumerStatus {
	return mrc.rowChan.Push(row, meta)
}

// ProducerDone is part of the RowReceiver interface.
func (mrc *MultiplexedRowChannel) ProducerDone() {
	newVal := atomic.AddInt32(&mrc.numSenders, -1)
	if newVal < 0 {
		panic("too many ProducerDone() calls")
	}
	if newVal == 0 {
		mrc.rowChan.ProducerDone()
	}
}

// Types is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) Types() []sqlbase.ColumnType {
	return mrc.rowChan.types
}

// Next is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) Next() (row sqlbase.EncDatumRow, meta ProducerMetadata) {
	return mrc.rowChan.Next()
}

// ConsumerDone is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) ConsumerDone() {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&mrc.rowChan.consumerStatus)))
	if status != NeedMoreRows {
		log.Fatalf(context.TODO(), "MultiplexedRowChannel already done or closed: %d", status)
	}
	atomic.StoreUint32(
		(*uint32)(&mrc.rowChan.consumerStatus), uint32(DrainRequested))
}

// ConsumerClosed is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) ConsumerClosed() {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&mrc.rowChan.consumerStatus)))
	if status == ConsumerClosed {
		panic("MultiplexedRowChannel already closed")
	}
	atomic.StoreUint32(
		(*uint32)(&mrc.rowChan.consumerStatus), uint32(ConsumerClosed))
	numSenders := atomic.LoadInt32(&mrc.numSenders)
	// Drain (at most) numSenders messages in case senders are blocked trying to
	// emit a row.
	for i := int32(0); i < numSenders; i++ {
		if _, ok := <-mrc.rowChan.dataChan; !ok {
			break
		}
	}
}

// BufferedRecord represents a row or metadata record that has been buffered
// inside a RowBuffer.
type BufferedRecord struct {
	Row  sqlbase.EncDatumRow
	Meta ProducerMetadata
}

// RowBuffer is an implementation of RowReceiver that buffers (accumulates)
// results in memory, as well as an implementation of RowSource that returns
// records from a record buffer. Just for tests.
type RowBuffer struct {
	mu struct {
		syncutil.Mutex

		// records represent the data that has been buffered. Push appends a row
		// to the back, Next removes a row from the front.
		records []BufferedRecord
	}

	// ProducerClosed is used when the RowBuffer is used as a RowReceiver; it is
	// set to true when the sender calls ProducerDone().
	ProducerClosed bool

	// Done is used when the RowBuffer is used as a RowSource; it is set to true
	// when the receiver read all the rows.
	Done bool

	ConsumerStatus ConsumerStatus

	// Schema of the rows in this buffer.
	types []sqlbase.ColumnType

	args RowBufferArgs
}

var _ RowReceiver = &RowBuffer{}
var _ RowSource = &RowBuffer{}

// RowBufferArgs contains testing-oriented parameters for a RowBuffer.
type RowBufferArgs struct {
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
	OnNext func(*RowBuffer) (sqlbase.EncDatumRow, ProducerMetadata)
}

// NewRowBuffer creates a RowBuffer with the given schema and initial rows. The
// types are optional if there is at least one row.
func NewRowBuffer(
	types []sqlbase.ColumnType, rows sqlbase.EncDatumRows, hooks RowBufferArgs,
) *RowBuffer {
	wrappedRows := make([]BufferedRecord, len(rows))
	for i, row := range rows {
		wrappedRows[i].Row = row
	}
	rb := &RowBuffer{types: types, args: hooks}
	rb.mu.records = wrappedRows

	if len(rb.mu.records) > 0 && rb.types == nil {
		rb.types = make([]sqlbase.ColumnType, len(rb.mu.records[0].Row))
		for i, d := range rb.mu.records[0].Row {
			rb.types[i] = d.Type
		}
	}
	return rb
}

// Push is part of the RowReceiver interface.
func (rb *RowBuffer) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	if rb.ProducerClosed {
		panic("Push called after ProducerDone")
	}
	// We mimic the behavior of RowChannel.
	storeRow := func() {
		rowCopy := append(sqlbase.EncDatumRow(nil), row...)
		rb.mu.Lock()
		rb.mu.records = append(rb.mu.records, BufferedRecord{Row: rowCopy, Meta: meta})
		rb.mu.Unlock()
	}
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if rb.args.AccumulateRowsWhileDraining {
		storeRow()
	} else {
		switch status {
		case NeedMoreRows:
			storeRow()
		case DrainRequested:
			if !meta.Empty() {
				storeRow()
			}
		case ConsumerClosed:
		}
	}
	return status
}

// ProducerDone is part of the interface.
func (rb *RowBuffer) ProducerDone() {
	if rb.ProducerClosed {
		panic("RowBuffer already closed")
	}
	rb.ProducerClosed = true
}

// Types is part of the RowSource interface.
func (rb *RowBuffer) Types() []sqlbase.ColumnType {
	if rb.types == nil {
		panic("not initialized with types")
	}
	return rb.types
}

// Next is part of the RowSource interface.
//
// There's no synchronization here with Push(). The assumption is that these
// two methods are not called concurrently.
func (rb *RowBuffer) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	if rb.args.OnNext != nil {
		row, meta := rb.args.OnNext(rb)
		if row != nil || !meta.Empty() {
			return row, meta
		}
	}
	if len(rb.mu.records) == 0 {
		rb.Done = true
		return nil, ProducerMetadata{}
	}
	rec := rb.mu.records[0]
	rb.mu.records = rb.mu.records[1:]
	return rec.Row, rec.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rb *RowBuffer) ConsumerDone() {
	rb.ConsumerStatus = DrainRequested
	if rb.args.OnConsumerDone != nil {
		rb.args.OnConsumerDone(rb)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rb *RowBuffer) ConsumerClosed() {
	rb.ConsumerStatus = ConsumerClosed
	if rb.args.OnConsumerClosed != nil {
		rb.args.OnConsumerClosed(rb)
	}
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

// String implements fmt.Stringer.
func (e *Error) String() string {
	if err := e.ErrorDetail(); err != nil {
		return err.Error()
	}
	return "<nil>"
}

// NewError creates an Error from an error.
func NewError(err error) *Error {
	pgErr, ok := pgerror.GetPGCause(err)
	if !ok {
		pgErr = pgerror.NewError(pgerror.CodeInternalError, err.Error()).(*pgerror.Error)
	}
	return &Error{Detail: &Error_PGError{PGError: pgErr}}
}

// ErrorDetail returns the payload as a Go error.
func (e *Error) ErrorDetail() error {
	if e == nil {
		return nil
	}
	switch t := e.Detail.(type) {
	case *Error_PGError:
		return t.PGError

	default:
		panic(fmt.Sprintf("bad error detail: %+v", t))
	}
}
