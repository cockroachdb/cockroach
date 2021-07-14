// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colmeta

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// colmeta package introduces facilities that allow for intertwining data
// (either rowenc.EncDatumRows or coldata.Batches) with the requests to
// propagate the metadata in a streaming fashion. It is used by the vectorized
// execution engine because there, by default, we buffer all metadata to be
// emitted only during the draining phase of running the vectorized flow.
//
// However, such handling of the metadata is not sufficient for some uses cases
// when we want to actually propagate the metadata to the client (the
// DistSQLReceiver on the gateway node) while the flow is running. An example of
// such a use case is how we're emitting metrics about the number of rows read
// by the scan operators in order to estimate the progress of the query.
//
// This package adds such a facility that allows for propagating the metadata in
// a streaming fashion without having to change colexecop.Operator interface and
// to teach each operator about the metadata. It is achieved by asking the root
// components of the vectorized flows to split up the work of reading data from
// the input tree and pushing it to the output into two goroutines. This
// introduces a point of synchronization into which we can insert the streaming
// metadata while keeping all other non-root components unaware of the metadata.
//
// The outline of how the interfaces in this file are designed to be used.
//
//             ┌──────────────────────────────────────────────────────────────────────────────────────┐
//             │                                      DataConsumer                                    │
//             │                         (main goroutine of the root component)                       │
//             └──┬───────────────────────────────────────────────────────────────────────────────────┘
//                │                                   │                                             │
//                ▼                                   ▼                                             ▼
// ┌──────────────────────────────┐  ┌────────────────────────────────────┐       ┌────────────────────────────────────┐
// │         DataProducer         │  │   StreamingMetadataProducer 1      │       │    StreamingMetadataProducer N     │
// │         ------------         │  │   ---------------------------      │       │    ---------------------------     │
// │ (goroutine reading from the  │  │      (any goroutine running        │  ...  │      (any goroutine running        │
// │ input of the root component) │  │ colexecop.StreamingMetadataSource) │       │ colexecop.StreamingMetadataSource) │
// └──────────────────────────────┘  └────────────────────────────────────┘       └────────────────────────────────────┘
//
// Once the DataProducer goroutine exits, all streaming metadata will not be
// delivered.

// DataProducer should be used by the goroutine that is receiving data from some
// input component and wants to push that data to the consumer in synchronous
// fashion.
type DataProducer interface {
	// WaitForConsumer returns the channel from which the producer must receive
	// in order to block itself until the consumer arrives.
	WaitForConsumer() <-chan struct{}

	// SendRow pushes a rowenc.EncDatumRow to the consumer. It blocks until the
	// row has been received, and the caller is then free to reuse the row. A
	// context cancellation error can be returned in which case the producer
	// should exit right away.
	SendRow(context.Context, rowenc.EncDatumRow) error
	// SendBatch pushes a coldata.Batch to the consumer. It blocks until the
	// batch has been received, and the caller is then free to reuse the batch.
	// A context cancellation error can be returned in which case the producer
	// should exit right away.
	SendBatch(context.Context, coldata.Batch, *colmem.Allocator) error
	// SendRemoteProducerMessage pushes an execinfrapb.ProducerMessage to the
	// consumer. It blocks until the message has been received, and the caller
	// is then free to reuse the message. A context cancellation error can be
	// returned in which case the producer should exit right away.
	SendRemoteProducerMessage(context.Context, *execinfrapb.ProducerMessage) error
	// SendMeta pushes an execinfrapb.ProducerMetadata to the consumer. It
	// blocks until the meta has been received, and the caller is then free to
	// reuse the meta. A context cancellation error can be returned in which
	// case the producer should exit right away.
	SendMeta(context.Context, *execinfrapb.ProducerMetadata) error

	// ProducerDone needs to be called once the producer has no more data. No
	// other calls are allowed after this.
	ProducerDone()
}

// StreamingMetadataProducer helps implement colexecop.StreamingMetadataReceiver
// interface and is safe to be used from any goroutine (except for the one using
// the DataConsumer interface).
type StreamingMetadataProducer interface {
	// SendStreamingMeta pushes a "local" metadata in a streaming fashion to the
	// consumer. It blocks until either the metadata is delivered or an error
	// occurs. See the comment on PushStreamingMeta for more details.
	SendStreamingMeta(context.Context, *execinfrapb.ProducerMetadata) error
	// SendRemoteStreamingMeta pushes a "remote" metadata in a streaming fashion
	// to the consumer. It blocks until either the metadata is delivered or an
	// error occurs. See the comment on PushStreamingMeta for more details.
	SendRemoteStreamingMeta(context.Context, *execinfrapb.ProducerMessage) error
}

// DataConsumer should be used by the main goroutine of the root component that
// is consuming the data coming from the DataProducer goroutine intertwined with
// the streaming metadata coming from StreamingMetadataProducer goroutines.
type DataConsumer interface {
	// ConsumerArrived notifies the DataProducer goroutine that the consumer is
	// ready to receive data. It must be called exactly once before any other
	// methods.
	ConsumerArrived()

	// NextRowAndMeta returns the next row and metadata objects to consume. It
	// blocks until there is something to consume or the DataProducer exits.
	NextRowAndMeta() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata)
	// NextBatchAndMeta returns the next batch and metadata objects to consume.
	// It blocks until there is something to consume or the DataProducer exits.
	NextBatchAndMeta() (coldata.Batch, *execinfrapb.ProducerMetadata)
	// NextRemoteProducerMsg returns the next remote producer message to
	// consume. It blocks until there is something to consume or the
	// DataProducer exits.
	NextRemoteProducerMsg(context.Context) *execinfrapb.ProducerMessage
}

// StreamingMetadataHandler implements the logic of intertwining the data coming
// from the input to the root component with the requests to propagate a
// metadata in a streaming fashion.
type StreamingMetadataHandler struct {
	producerMu struct {
		syncutil.Mutex
		// nextCh is used to intertwine data produced by the input reading
		// goroutine with the requests to propagate streaming metadata. It is
		// closed when the DataProducer goroutine exits.
		nextCh chan *nextChMsg
		// done indicates whether the DataProducer goroutine exited (meaning
		// nextCh has already been closed). All requests to propagate streaming
		// meta will error out once it is set to true.
		done bool
	}

	producerExitMu struct {
		syncutil.Mutex
		// err stores the context cancellation error if the DataProducer
		// encountered it. The DataConsumer will unset it once the error is
		// retrieved.
		err error
	}

	// waitForConsumer is used to block the DataProducer goroutine until the
	// DataConsumer goroutine arrives.
	waitForConsumer chan struct{}

	producerScratch struct {
		nextIdx int
		row     [2]rowenc.EncDatumRow
		batch   [2]coldata.Batch
		typs    []*types.T
	}
}

var _ DataProducer = &StreamingMetadataHandler{}
var _ DataConsumer = &StreamingMetadataHandler{}
var _ StreamingMetadataProducer = &StreamingMetadataHandler{}

type nextChMsg struct {
	// Only one of the next four fields can be set.
	row   rowenc.EncDatumRow
	batch coldata.Batch
	msg   *execinfrapb.ProducerMessage
	meta  *execinfrapb.ProducerMetadata
}

var nextChMsgPool = sync.Pool{
	New: func() interface{} {
		return &nextChMsg{}
	},
}

func (msg *nextChMsg) release() {
	*msg = nextChMsg{}
	nextChMsgPool.Put(msg)
}

// Init initializes the handler.
func (h *StreamingMetadataHandler) Init() {
	h.producerMu.nextCh = make(chan *nextChMsg)
	h.waitForConsumer = make(chan struct{})
}

// WaitForConsumer is part of the DataProducer interface.
func (h *StreamingMetadataHandler) WaitForConsumer() <-chan struct{} {
	return h.waitForConsumer
}

// sendInputMsg sends the scratch input message on nextCh.
//
// An error is returned if the context is canceled and the producer should exit.
func (h *StreamingMetadataHandler) sendInputMsg(ctx context.Context, msg *nextChMsg) error {
	defer func() {
		msg.release()
		if ctx.Err() != nil {
			h.producerExitMu.Lock()
			h.producerExitMu.err = ctx.Err()
			h.producerExitMu.Unlock()
		}
	}()
	select {
	// No need to hold the mutex here since the current goroutine is the only
	// that can close nextCh.
	case h.producerMu.nextCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendRow is part of the DataProducer interface.
func (h *StreamingMetadataHandler) SendRow(ctx context.Context, row rowenc.EncDatumRow) error {
	msg := nextChMsgPool.Get().(*nextChMsg)
	if h.producerScratch.row[h.producerScratch.nextIdx] == nil {
		h.producerScratch.row[h.producerScratch.nextIdx] = make(rowenc.EncDatumRow, len(row))
	}
	msg.row = h.producerScratch.row[h.producerScratch.nextIdx]
	h.producerScratch.nextIdx = (h.producerScratch.nextIdx + 1) % 2
	copy(msg.row, row)
	return h.sendInputMsg(ctx, msg)
}

// SendBatch is part of the DataProducer interface.
func (h *StreamingMetadataHandler) SendBatch(
	ctx context.Context, batch coldata.Batch, allocator *colmem.Allocator,
) error {
	msg := nextChMsgPool.Get().(*nextChMsg)
	if h.producerScratch.typs == nil {
		h.producerScratch.typs = make([]*types.T, batch.Width())
		for i := range h.producerScratch.typs {
			h.producerScratch.typs[i] = batch.ColVec(i).Type()
		}
	}
	h.producerScratch.batch[h.producerScratch.nextIdx], _ = allocator.ResetMaybeReallocate(
		h.producerScratch.typs,
		h.producerScratch.batch[h.producerScratch.nextIdx],
		batch.Length(),
		math.MaxInt64,
	)
	msg.batch = h.producerScratch.batch[h.producerScratch.nextIdx]
	h.producerScratch.nextIdx = (h.producerScratch.nextIdx + 1) % 2
	allocator.PerformOperation(msg.batch.ColVecs(), func() {
		for colIdx, col := range msg.batch.ColVecs() {
			col.Copy(coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					Src:       batch.ColVec(colIdx),
					Sel:       batch.Selection(),
					SrcEndIdx: batch.Length(),
				},
			})
		}
		msg.batch.SetLength(batch.Length())
	})
	return h.sendInputMsg(ctx, msg)
}

// SendRemoteProducerMessage is part of the DataProducer interface.
func (h *StreamingMetadataHandler) SendRemoteProducerMessage(
	ctx context.Context, producerMsg *execinfrapb.ProducerMessage,
) error {
	msg := nextChMsgPool.Get().(*nextChMsg)
	// TODO: deep copy.
	msg.msg = producerMsg
	return h.sendInputMsg(ctx, msg)
}

// SendMeta is part of the DataProducer interface.
func (h *StreamingMetadataHandler) SendMeta(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	msg := nextChMsgPool.Get().(*nextChMsg)
	msg.meta = meta
	return h.sendInputMsg(ctx, msg)
}

// ProducerDone is part of the DataProducer interface.
func (h *StreamingMetadataHandler) ProducerDone() {
	h.producerMu.Lock()
	close(h.producerMu.nextCh)
	h.producerMu.done = true
	h.producerMu.Unlock()
}

var errNextChClosed = errors.New("nextCh is closed")

// sendStreamingMsg sends a message (which supposed to be a streaming metadata
// object, either local or remote) to the DataConsumer, might block. If the
// DataProducer goroutine has already exited, an error is returned.
func (h *StreamingMetadataHandler) sendStreamingMsg(ctx context.Context, msg *nextChMsg) error {
	h.producerMu.Lock()
	defer h.producerMu.Unlock()
	if h.producerMu.done {
		return errNextChClosed
	}
	select {
	case h.producerMu.nextCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendStreamingMeta is part of the StreamingMetadataProducer interface.
func (h *StreamingMetadataHandler) SendStreamingMeta(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	return h.sendStreamingMsg(ctx, &nextChMsg{meta: meta})
}

// SendRemoteStreamingMeta is part of the StreamingMetadataProducer interface.
func (h *StreamingMetadataHandler) SendRemoteStreamingMeta(
	ctx context.Context, msg *execinfrapb.ProducerMessage,
) error {
	return h.sendStreamingMsg(ctx, &nextChMsg{msg: msg})
}

// ConsumerArrived is part of the DataConsumer interface.
func (h *StreamingMetadataHandler) ConsumerArrived() {
	close(h.waitForConsumer)
}

// next retrieves the next piece of data to consume, might block. If the
// DataProducer goroutine exited because of the context cancellation, that error
// is returned as an execinfrapb.ProducerMetadata.
func (h *StreamingMetadataHandler) next() (*nextChMsg, *execinfrapb.ProducerMetadata) {
	msg, ok := <-h.producerMu.nextCh
	if !ok {
		// The DataProducer goroutine has exited. Check whether it was because
		// of the context cancellation.
		h.producerExitMu.Lock()
		err := h.producerExitMu.err
		// Unset the error in order to return an empty nextChMsg on the next
		// call.
		h.producerExitMu.err = nil
		h.producerExitMu.Unlock()
		if err != nil {
			meta := execinfrapb.GetProducerMeta()
			meta.Err = err
			return nil, meta
		}
		return nil, nil
	}
	return msg, nil
}

// NextRowAndMeta is part of the DataConsumer interface.
func (h *StreamingMetadataHandler) NextRowAndMeta() (
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	msg, meta := h.next()
	if msg != nil {
		return msg.row, msg.meta
	}
	if meta != nil {
		return nil, meta
	}
	return nil, nil
}

// NextBatchAndMeta is part of the DataConsumer interface.
func (h *StreamingMetadataHandler) NextBatchAndMeta() (
	coldata.Batch,
	*execinfrapb.ProducerMetadata,
) {
	msg, meta := h.next()
	if msg != nil {
		return msg.batch, msg.meta
	}
	if meta != nil {
		return nil, meta
	}
	return nil, nil
}

// NextRemoteProducerMsg is part of the DataConsumer interface.
func (h *StreamingMetadataHandler) NextRemoteProducerMsg(
	ctx context.Context,
) *execinfrapb.ProducerMessage {
	msg, meta := h.next()
	if msg != nil {
		return msg.msg
	}
	if meta != nil {
		errMsg := &execinfrapb.ProducerMessage{}
		errMsg.Data.Metadata = []execinfrapb.RemoteProducerMetadata{execinfrapb.LocalMetaToRemoteProducerMeta(ctx, *meta)}
		return errMsg
	}
	return nil
}
