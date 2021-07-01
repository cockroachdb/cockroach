// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colrpc

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

// flowStreamServer is a utility interface used to mock out the RPC layer.
type flowStreamServer interface {
	Send(*execinfrapb.ConsumerSignal) error
	Recv() (*execinfrapb.ProducerMessage, error)
}

// Inbox is used to expose data from remote flows through a colexecop.Operator
// interface. FlowStream RPC handlers should call RunWithStream (which blocks
// until operation terminates, gracefully or unexpectedly) to pass the stream
// to the inbox. Next may be called before RunWithStream, it will just block
// until the stream is made available or its context is canceled. Note that
// ownership of the stream is passed from the RunWithStream goroutine to the
// Next goroutine. In exchange, the RunWithStream goroutine receives the first
// context passed into Next and listens for cancellation. Returning from
// RunWithStream (or more specifically, the RPC handler) will unblock Next by
// closing the stream.
type Inbox struct {
	colexecop.ZeroInputNode
	colexecop.InitHelper

	typs []*types.T

	allocator  *colmem.Allocator
	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

	// streamID is used to overwrite a caller's streamID
	// in the ctx argument of Next and DrainMeta.
	streamID execinfrapb.StreamID

	// streamCh is the channel over which the stream is passed from the stream
	// handler to the reader goroutine.
	streamCh chan flowStreamServer
	// contextCh is the channel over which the reader goroutine passes the
	// context to the stream handler so that it can listen for context
	// cancellation.
	contextCh chan context.Context

	// timeoutCh is the channel over which an error will be sent if the reader
	// goroutine should exit while waiting for a stream.
	timeoutCh chan error

	// errCh is the channel that RunWithStream will block on, waiting until the
	// Inbox does not need a stream any more. An error will only be sent on this
	// channel in the event of a cancellation or a non-io.EOF error originating
	// from a stream.Recv.
	errCh chan error

	// ctxInterceptorFn is a callback to expose the inbox's context
	// right after init. To be used for unit testing.
	ctxInterceptorFn func(context.Context)

	// done prevents double closing. It should not be used by the RunWithStream
	// goroutine.
	done bool
	// bufferedMeta buffers any metadata found in Next when reading from the
	// stream and is returned by DrainMeta.
	bufferedMeta []execinfrapb.ProducerMetadata

	// stream is the RPC stream. It is set when RunWithStream is called but
	// only the Next/DrainMeta goroutine may access it.
	stream flowStreamServer

	// statsAtomics are the execution statistics that need to be atomically
	// accessed. This is necessary since Get*() methods can be called from
	// different goroutine than Next().
	statsAtomics struct {
		// rowsRead contains the total number of rows Inbox has read so far.
		rowsRead int64
		// bytesRead contains the number of bytes sent to the Inbox.
		bytesRead int64
		// numMessages contains the number of messages received by the Inbox.
		numMessages int64
	}

	// deserializationStopWatch records the time Inbox spends deserializing
	// batches. Note that the stop watch is safe for concurrent use, so it
	// doesn't have to have an explicit synchronization like fields above.
	deserializationStopWatch *timeutil.StopWatch

	scratch struct {
		data []*array.Data
		b    coldata.Batch
	}
}

var _ colexecop.Operator = &Inbox{}

// NewInbox creates a new Inbox.
func NewInbox(
	allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID,
) (*Inbox, error) {
	c, err := colserde.NewArrowBatchConverter(typs)
	if err != nil {
		return nil, err
	}
	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	i := &Inbox{
		typs:                     typs,
		allocator:                allocator,
		converter:                c,
		serializer:               s,
		streamID:                 streamID,
		streamCh:                 make(chan flowStreamServer, 1),
		contextCh:                make(chan context.Context, 1),
		timeoutCh:                make(chan error, 1),
		errCh:                    make(chan error, 1),
		deserializationStopWatch: timeutil.NewStopWatch(),
	}
	i.scratch.data = make([]*array.Data, len(typs))
	return i, nil
}

// close closes the inbox, ensuring that any call to RunWithStream will return
// immediately. close is idempotent.
// NOTE: it is very important to close the Inbox only when execution terminates
// in one way or another. DrainMeta will use the stream to read any remaining
// metadata after Next returns a zero-length batch during normal execution.
func (i *Inbox) close() {
	if !i.done {
		i.done = true
		close(i.errCh)
	}
}

// RunWithStream sets the Inbox's stream and waits until either streamCtx is
// canceled, a caller of Next cancels the context passed into Init, or any error
// is encountered on the stream by the Next goroutine.
//
// flowCtxDone is listened on only during the setup of the handler, before the
// readerCtx is received. This is needed in case Inbox.Init is never called.
func (i *Inbox) RunWithStream(
	streamCtx context.Context, stream flowStreamServer, flowCtxDone <-chan struct{},
) error {
	streamCtx = logtags.AddTag(streamCtx, "streamID", i.streamID)
	log.VEvent(streamCtx, 2, "Inbox handling stream")
	defer log.VEvent(streamCtx, 2, "Inbox exited stream handler")
	// Pass the stream to the reader goroutine (non-blocking) and get the
	// context to listen for cancellation.
	i.streamCh <- stream
	var readerCtx context.Context
	select {
	case err := <-i.errCh:
		// nil will be read from errCh when the channel is closed.
		return err
	case readerCtx = <-i.contextCh:
		log.VEvent(streamCtx, 2, "Inbox reader arrived")
	case <-streamCtx.Done():
		return fmt.Errorf("%s: streamCtx while waiting for reader (remote client canceled)", streamCtx.Err())
	case <-flowCtxDone:
		// The flow context of the inbox host has been canceled. This can occur
		// e.g. when the query is canceled, or when another stream encountered
		// an unrecoverable error forcing it to shutdown the flow.
		return cancelchecker.QueryCanceledError
	}

	// Now wait for one of the events described in the method comment. If a
	// cancellation is encountered, nothing special must be done to cancel the
	// reader goroutine as returning from the handler will close the stream.
	//
	// Note that we don't listen for cancellation on flowCtxDone because
	// readerCtx must be the child of the flow context.
	select {
	case err := <-i.errCh:
		// nil will be read from errCh when the channel is closed.
		return err
	case <-readerCtx.Done():
		// The reader canceled the stream meaning that it no longer needs any
		// more data from the outbox. This is a graceful termination, so we
		// return nil.
		return nil
	case <-streamCtx.Done():
		// The client canceled the stream.
		return fmt.Errorf("%s: streamCtx in Inbox stream handler (remote client canceled)", streamCtx.Err())
	}
}

// Timeout sends the given error to any readers waiting for a stream to be
// established (i.e. RunWithStream to be called).
func (i *Inbox) Timeout(err error) {
	i.timeoutCh <- err
}

// Init is part of the Operator interface.
func (i *Inbox) Init(ctx context.Context) {
	if !i.InitHelper.Init(ctx) {
		return
	}

	i.Ctx = logtags.AddTag(i.Ctx, "streamID", i.streamID)
	// Initializes the Inbox for operation by blocking until RunWithStream sets
	// the stream to read from.
	if err := func() error {
		// Wait for the stream to be initialized. We're essentially waiting for
		// the remote connection.
		select {
		case i.stream = <-i.streamCh:
		case err := <-i.timeoutCh:
			i.errCh <- fmt.Errorf("%s: remote stream arrived too late", err)
			return err
		case <-i.Ctx.Done():
			// Our reader canceled the context meaning that it no longer needs
			// any more data from the outbox. This is a graceful termination, so
			// we don't send any error on errCh and only return an error. This
			// will close the inbox (making the stream handler exit gracefully)
			// and will stop the current goroutine from proceeding further.
			return i.Ctx.Err()
		}

		if i.ctxInterceptorFn != nil {
			i.ctxInterceptorFn(i.Ctx)
		}
		i.contextCh <- i.Ctx
		return nil
	}(); err != nil {
		// An error occurred while initializing the Inbox and is likely caused
		// by the connection issues. It is expected that such an error can
		// occur. The Inbox must still be closed.
		i.close()
		log.VEventf(ctx, 1, "Inbox encountered an error in Init: %v", err)
		colexecerror.ExpectedError(err)
	}
}

// Next returns the next batch. It will block until there is data available.
// The Inbox will exit when either the context passed in Init() is canceled or
// when DrainMeta goroutine tells it to do so.
func (i *Inbox) Next() coldata.Batch {
	if i.done {
		return coldata.ZeroBatch
	}

	defer func() {
		// Catch any panics that occur and close the Inbox in order to not leak
		// the goroutine listening for context cancellation. The Inbox must
		// still be closed during normal termination.
		if panicObj := recover(); panicObj != nil {
			// Only close the Inbox here in case of an ungraceful termination.
			i.close()
			err := logcrash.PanicAsError(0, panicObj)
			log.VEventf(i.Ctx, 1, "Inbox encountered an error in Next: %v", err)
			// Note that here we use InternalError to propagate the error
			// consciously - the code below is careful to mark all expected
			// errors as "expected", and we want to keep that distinction.
			colexecerror.InternalError(err)
		}
	}()

	i.deserializationStopWatch.Start()
	defer i.deserializationStopWatch.Stop()
	for {
		i.deserializationStopWatch.Stop()
		m, err := i.stream.Recv()
		i.deserializationStopWatch.Start()
		atomic.AddInt64(&i.statsAtomics.numMessages, 1)
		if err != nil {
			if err == io.EOF {
				// Done.
				i.close()
				return coldata.ZeroBatch
			}
			// Note that here err can be stream's context cancellation.
			// Regardless of the cause we want to propagate such an error as
			// expected on in all cases so that the caller could decide on how
			// to handle it.
			err = pgerror.Newf(pgcode.InternalConnectionFailure, "inbox communication error: %s", err)
			i.errCh <- err
			colexecerror.ExpectedError(err)
		}
		if len(m.Data.Metadata) != 0 {
			for _, rpm := range m.Data.Metadata {
				meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(i.Ctx, rpm)
				if !ok {
					continue
				}
				if meta.Err != nil {
					// If an error was encountered, it needs to be propagated
					// immediately. All other metadata will simply be buffered
					// and returned in DrainMeta.
					colexecerror.ExpectedError(meta.Err)
				}
				i.bufferedMeta = append(i.bufferedMeta, meta)
			}
			// Continue until we get the next batch or EOF.
			continue
		}
		if len(m.Data.RawBytes) == 0 {
			// Protect against Deserialization panics by skipping empty messages.
			continue
		}
		atomic.AddInt64(&i.statsAtomics.bytesRead, int64(len(m.Data.RawBytes)))
		i.scratch.data = i.scratch.data[:0]
		batchLength, err := i.serializer.Deserialize(&i.scratch.data, m.Data.RawBytes)
		// Eagerly throw away the RawBytes memory.
		m.Data.RawBytes = nil
		if err != nil {
			colexecerror.InternalError(err)
		}
		// For now, we don't enforce any footprint-based memory limit.
		// TODO(yuzefovich): refactor this.
		const maxBatchMemSize = math.MaxInt64
		i.scratch.b, _ = i.allocator.ResetMaybeReallocate(i.typs, i.scratch.b, batchLength, maxBatchMemSize)
		if err := i.converter.ArrowToBatch(i.scratch.data, batchLength, i.scratch.b); err != nil {
			colexecerror.InternalError(err)
		}
		atomic.AddInt64(&i.statsAtomics.rowsRead, int64(i.scratch.b.Length()))
		return i.scratch.b
	}
}

// GetBytesRead returns the number of bytes received by the Inbox.
func (i *Inbox) GetBytesRead() int64 {
	return atomic.LoadInt64(&i.statsAtomics.bytesRead)
}

// GetRowsRead returns the number of rows received by the Inbox.
func (i *Inbox) GetRowsRead() int64 {
	return atomic.LoadInt64(&i.statsAtomics.rowsRead)
}

// GetDeserializationTime returns the amount of time the Inbox spent
// deserializing batches.
func (i *Inbox) GetDeserializationTime() time.Duration {
	return i.deserializationStopWatch.Elapsed()
}

// GetNumMessages returns the number of messages received by the Inbox.
func (i *Inbox) GetNumMessages() int64 {
	return atomic.LoadInt64(&i.statsAtomics.numMessages)
}

func (i *Inbox) sendDrainSignal(ctx context.Context) error {
	log.VEvent(ctx, 2, "Inbox sending drain signal to Outbox")
	if err := i.stream.Send(&execinfrapb.ConsumerSignal{DrainRequest: &execinfrapb.DrainRequest{}}); err != nil {
		if log.V(1) {
			log.Warningf(ctx, "Inbox unable to send drain signal to Outbox: %+v", err)
		}
		return err
	}
	return nil
}

// DrainMeta is part of the colexecop.MetadataSource interface. DrainMeta may
// not be called concurrently with Next.
func (i *Inbox) DrainMeta() []execinfrapb.ProducerMetadata {
	allMeta := i.bufferedMeta
	i.bufferedMeta = i.bufferedMeta[:0]

	if i.done {
		// Next exhausted the stream of metadata.
		return allMeta
	}
	defer i.close()

	if err := i.sendDrainSignal(i.Ctx); err != nil {
		return allMeta
	}

	for {
		msg, err := i.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if log.V(1) {
				log.Warningf(i.Ctx, "Inbox Recv connection error while draining metadata: %+v", err)
			}
			return allMeta
		}
		for _, remoteMeta := range msg.Data.Metadata {
			meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(i.Ctx, remoteMeta)
			if !ok {
				continue
			}
			allMeta = append(allMeta, meta)
		}
	}

	return allMeta
}
