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

package distsqlrun

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus

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
	// OutputTypes returns the schema for the rows in this source.
	OutputTypes() []sqlbase.ColumnType

	// Next returns the next record from the source. At most one of the return
	// values will be non-empty. Both of them can be empty when the RowSource has
	// been exhausted - no more records are coming and any further method calls
	// will be no-ops.
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
	Next() (sqlbase.EncDatumRow, *ProducerMetadata)

	// ConsumerDone lets the source know that we will not need any more data
	// rows. The source is expected to start draining and only send metadata
	// rows. May be called multiple times on a RowSource, even after
	// ConsumerClosed has been called.
	//
	// May block. If the consumer of the source stops consuming rows before
	// Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerDone()

	// ConsumerClosed informs the source that the consumer is done and will not
	// make any more calls to Next(). Must only be called once on a given
	// RowSource.
	//
	// Like ConsumerDone(), if the consumer of the source stops consuming rows
	// before Next indicates that there are no more rows, ConsumerDone() and/or
	// ConsumerClosed() must be called; it is a no-op to call these methods after
	// all the rows were consumed (i.e. after Next() returned an empty row).
	ConsumerClosed()
}

// Run reads records from the source and outputs them to the receiver, properly
// draining the source of metadata and closing both the source and receiver.
func Run(ctx context.Context, src RowSource, dst RowReceiver) {
	for {
		row, meta := src.Next()
		// Emit the row; stop if no more rows are needed.
		if row != nil || meta != nil {
			switch dst.Push(row, meta) {
			case NeedMoreRows:
				continue
			case DrainRequested:
				DrainAndForwardMetadata(ctx, src, dst)
				dst.ProducerDone()
				return
			case ConsumerClosed:
				src.ConsumerClosed()
				dst.ProducerDone()
				return
			}
		}
		// row == nil && meta == nil: the source has been fully drained.
		dst.ProducerDone()
		return
	}
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
		if meta == nil {
			if row == nil {
				return
			}
			continue
		}
		if row != nil {
			log.Fatalf(
				ctx, "both row data and metadata in the same record. row: %s meta: %+v",
				row.String(src.OutputTypes()), meta,
			)
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

func getTraceData(ctx context.Context) []tracing.RecordedSpan {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		return tracing.GetRecording(sp)
	}
	return nil
}

// sendTraceData collects the tracing information from the ctx and pushes it to
// dst. The ConsumerStatus returned by dst is ignored.
//
// Note that the tracing data is distinct between different processors, since
// each one gets its own trace "recording group".
func sendTraceData(ctx context.Context, dst RowReceiver) {
	if rec := getTraceData(ctx); rec != nil {
		dst.Push(nil /* row */, &ProducerMetadata{TraceData: rec})
	}
}

// sendTxnCoordMetaMaybe reads the txn metadata from a leaf transactions and
// sends it to dst, so that it eventually makes it to the root txn. The
// ConsumerStatus returned by dst is ignored.
//
// If the txn is a root txn, this is a no-op.
//
// NOTE(andrei): As of 04/2018, the txn is shared by all processors scheduled on
// a node, and so it's possible for multiple processors to send the same
// TxnCoordMeta. The root TxnCoordSender doesn't care if it receives the same
// thing multiple times.
func sendTxnCoordMetaMaybe(txn *client.Txn, dst RowReceiver) {
	if txn.Type() == client.RootTxn {
		return
	}
	txnMeta := txn.GetTxnCoordMeta()
	if txnMeta.Txn.ID != (uuid.UUID{}) {
		dst.Push(nil /* row */, &ProducerMetadata{TxnMeta: &txnMeta})
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
// pushTrailingMeta is called after draining the sources and before calling
// dst.ProducerDone(). It gives the caller the opportunity to push some trailing
// metadata (e.g. tracing information and txn updates, if applicable).
//
// srcs can be nil.
//
// All errors are forwarded to the producer.
func DrainAndClose(
	ctx context.Context,
	dst RowReceiver,
	cause error,
	pushTrailingMeta func(context.Context),
	srcs ...RowSource,
) {
	if cause != nil {
		// We ignore the returned ConsumerStatus and rely on the
		// DrainAndForwardMetadata() calls below to close srcs in all cases.
		_ = dst.Push(nil /* row */, &ProducerMetadata{Err: cause})
	}
	if len(srcs) > 0 {
		var wg sync.WaitGroup
		for _, input := range srcs[1:] {
			wg.Add(1)
			go func(input RowSource) {
				DrainAndForwardMetadata(ctx, input, dst)
				wg.Done()
			}(input)
		}
		DrainAndForwardMetadata(ctx, srcs[0], dst)
		wg.Wait()
	}
	pushTrailingMeta(ctx)
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
		if meta == nil {
			return row, nil
		}
		if meta.Err != nil {
			return nil, meta.Err
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
	Meta *ProducerMetadata
}

// ProducerMetadata represents a metadata record flowing through a DistSQL flow.
type ProducerMetadata struct {
	// Only one of these fields will be set. If this ever changes, note that
	// there're consumers out there that extract the error and, if there is one,
	// forward it in isolation and drop the rest of the record.
	Ranges []roachpb.RangeInfo
	// TODO(vivek): change to type Error
	Err error
	// TraceData is sent if snowball tracing is enabled.
	TraceData []tracing.RecordedSpan
	// TxnMeta contains the updated transaction coordinator metadata,
	// to be sent from leaf transactions to augment the root transaction,
	// held by the flow's ultimate receiver.
	TxnMeta *roachpb.TxnCoordMeta
	// RowNum corresponds to a row produced by a "source" processor that takes no
	// inputs. It is used in tests to verify that all metadata is forwarded
	// exactly once to the receiver on the gateway node.
	RowNum *RemoteProducerMetadata_RowNum
}

// RowChannel is a thin layer over a RowChannelMsg channel, which can be used to
// transfer rows between goroutines.
type RowChannel struct {
	rowSourceBase

	types []sqlbase.ColumnType

	// The channel on which rows are delivered.
	C <-chan RowChannelMsg

	// dataChan is the same channel as C.
	dataChan chan RowChannelMsg
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
func (rc *RowChannel) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
	consumerStatus := ConsumerStatus(
		atomic.LoadUint32((*uint32)(&rc.consumerStatus)))
	switch consumerStatus {
	case NeedMoreRows:
		rc.dataChan <- RowChannelMsg{Row: row, Meta: meta}
	case DrainRequested:
		// If we're draining, only forward metadata.
		if meta != nil {
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

// OutputTypes is part of the RowSource interface.
func (rc *RowChannel) OutputTypes() []sqlbase.ColumnType {
	return rc.types
}

// Next is part of the RowSource interface.
func (rc *RowChannel) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	d, ok := <-rc.C
	if !ok {
		// No more rows.
		return nil, nil
	}
	return d.Row, d.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rc *RowChannel) ConsumerDone() {
	rc.consumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (rc *RowChannel) ConsumerClosed() {
	rc.consumerClosed("RowChannel")
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
	row sqlbase.EncDatumRow, meta *ProducerMetadata,
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

// OutputTypes is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) OutputTypes() []sqlbase.ColumnType {
	return mrc.rowChan.types
}

// Next is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) Next() (row sqlbase.EncDatumRow, meta *ProducerMetadata) {
	return mrc.rowChan.Next()
}

// ConsumerDone is part of the RowSource interface.
func (mrc *MultiplexedRowChannel) ConsumerDone() {
	mrc.rowChan.ConsumerDone()
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
	Meta *ProducerMetadata
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
	OnNext func(*RowBuffer) (sqlbase.EncDatumRow, *ProducerMetadata)
}

// NewRowBuffer creates a RowBuffer with the given schema and initial rows.
func NewRowBuffer(
	types []sqlbase.ColumnType, rows sqlbase.EncDatumRows, hooks RowBufferArgs,
) *RowBuffer {
	if types == nil {
		panic("types required")
	}
	wrappedRows := make([]BufferedRecord, len(rows))
	for i, row := range rows {
		wrappedRows[i].Row = row
	}
	rb := &RowBuffer{types: types, args: hooks}
	rb.mu.records = wrappedRows
	return rb
}

// Push is part of the RowReceiver interface.
func (rb *RowBuffer) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
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
			if meta != nil {
				storeRow()
			}
		case ConsumerClosed:
		}
	}
	return status
}

// ProducerDone is part of the RowSource interface.
func (rb *RowBuffer) ProducerDone() {
	if rb.ProducerClosed {
		panic("RowBuffer already closed")
	}
	rb.ProducerClosed = true
}

// OutputTypes is part of the RowSource interface.
func (rb *RowBuffer) OutputTypes() []sqlbase.ColumnType {
	if rb.types == nil {
		panic("not initialized")
	}
	return rb.types
}

// Next is part of the RowSource interface.
//
// There's no synchronization here with Push(). The assumption is that these
// two methods are not called concurrently.
func (rb *RowBuffer) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
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
	return rec.Row, rec.Meta
}

// ConsumerDone is part of the RowSource interface.
func (rb *RowBuffer) ConsumerDone() {
	if atomic.CompareAndSwapUint32((*uint32)(&rb.ConsumerStatus),
		uint32(NeedMoreRows), uint32(DrainRequested)) {
		if rb.args.OnConsumerDone != nil {
			rb.args.OnConsumerDone(rb)
		}
	}
}

// ConsumerClosed is part of the RowSource interface.
func (rb *RowBuffer) ConsumerClosed() {
	status := ConsumerStatus(atomic.LoadUint32((*uint32)(&rb.ConsumerStatus)))
	if status == ConsumerClosed {
		log.Fatalf(context.Background(), "RowBuffer already closed")
	}
	atomic.StoreUint32((*uint32)(&rb.ConsumerStatus), uint32(ConsumerClosed))
	if rb.args.OnConsumerClosed != nil {
		rb.args.OnConsumerClosed(rb)
	}
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	if err := e.ErrorDetail(); err != nil {
		return err.Error()
	}
	return "<nil>"
}

// NewError creates an Error from an error, to be sent on the wire. It will
// recognize certain errors and marshall them accordingly, and everything
// unrecognized is turned into a PGError with code "internal".
func NewError(err error) *Error {
	if pgErr, ok := pgerror.GetPGCause(err); ok {
		return &Error{Detail: &Error_PGError{PGError: pgErr}}
	} else if retryErr, ok := err.(*roachpb.UnhandledRetryableError); ok {
		return &Error{
			Detail: &Error_RetryableTxnError{
				RetryableTxnError: retryErr,
			}}
	} else {
		// Anything unrecognized is an "internal error".
		return &Error{
			Detail: &Error_PGError{
				PGError: pgerror.NewError(
					pgerror.CodeInternalError, err.Error())}}
	}
}

// ErrorDetail returns the payload as a Go error.
func (e *Error) ErrorDetail() error {
	if e == nil {
		return nil
	}
	switch t := e.Detail.(type) {
	case *Error_PGError:
		return t.PGError
	case *Error_RetryableTxnError:
		return t.RetryableTxnError
	default:
		panic(fmt.Sprintf("bad error detail: %+v", t))
	}
}
