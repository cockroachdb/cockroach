// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colrpc

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	allocator    *colmem.Allocator
	deserializer colexecutils.Deserializer

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

	// flowCtxDone is the Done() channel of the flow context of the Inbox host.
	flowCtxDone <-chan struct{}

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

	admissionQ    *admission.WorkQueue
	admissionInfo admission.WorkInfo

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
}

var _ colexecop.Operator = &Inbox{}

// NewInbox creates a new Inbox.
func NewInbox(
	allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID,
) (*Inbox, error) {
	i := &Inbox{
		typs:                     typs,
		allocator:                allocator,
		streamID:                 streamID,
		streamCh:                 make(chan flowStreamServer, 1),
		contextCh:                make(chan context.Context, 1),
		timeoutCh:                make(chan error, 1),
		errCh:                    make(chan error, 1),
		deserializationStopWatch: timeutil.NewStopWatch(),
	}
	err := i.deserializer.Init(allocator, typs, false /* alwaysReallocate */)
	return i, err
}

// NewInboxWithFlowCtxDone creates a new Inbox when the done channel of the flow
// context is available.
func NewInboxWithFlowCtxDone(
	allocator *colmem.Allocator,
	typs []*types.T,
	streamID execinfrapb.StreamID,
	flowCtxDone <-chan struct{},
) (*Inbox, error) {
	i, err := NewInbox(allocator, typs, streamID)
	if err != nil {
		return nil, err
	}
	i.flowCtxDone = flowCtxDone
	return i, nil
}

// NewInboxWithAdmissionControl creates a new Inbox that does admission
// control on responses received from DistSQL.
func NewInboxWithAdmissionControl(
	allocator *colmem.Allocator,
	typs []*types.T,
	streamID execinfrapb.StreamID,
	flowCtxDone <-chan struct{},
	admissionQ *admission.WorkQueue,
	admissionInfo admission.WorkInfo,
) (*Inbox, error) {
	i, err := NewInboxWithFlowCtxDone(allocator, typs, streamID, flowCtxDone)
	if err != nil {
		return nil, err
	}
	i.admissionQ = admissionQ
	i.admissionInfo = admissionInfo
	return i, err
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

// checkFlowCtxCancellation returns an error if the flow context has already
// been canceled.
func (i *Inbox) checkFlowCtxCancellation() error {
	select {
	case <-i.flowCtxDone:
		return cancelchecker.QueryCanceledError
	default:
		return nil
	}
}

// RunWithStream sets the Inbox's stream and waits until either streamCtx is
// canceled, the Inbox's host cancels the flow context, a caller of Next cancels
// the context passed into Init, or any error is encountered on the stream by
// the Next goroutine.
func (i *Inbox) RunWithStream(streamCtx context.Context, stream flowStreamServer) error {
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
		return errors.Wrap(streamCtx.Err(), "streamCtx error while waiting for reader (remote client canceled)")
	case <-i.flowCtxDone:
		// The flow context of the inbox host has been canceled. This can occur
		// e.g. when the query is canceled, or when another stream encountered
		// an unrecoverable error forcing it to shutdown the flow.
		return cancelchecker.QueryCanceledError
	}

	// Now wait for one of the events described in the method comment. If a
	// cancellation is encountered, nothing special must be done to cancel the
	// reader goroutine as returning from the handler will close the stream.
	select {
	case err := <-i.errCh:
		// nil will be read from errCh when the channel is closed.
		return err
	case <-i.flowCtxDone:
		// The flow context of the inbox host has been canceled. This can occur
		// e.g. when the query is canceled, or when another stream encountered
		// an unrecoverable error forcing it to shutdown the flow.
		return cancelchecker.QueryCanceledError
	case <-readerCtx.Done():
		// readerCtx is canceled, but we don't know whether it was because the
		// flow context was canceled or for other reason. In the former case we
		// have an ungraceful shutdown whereas in the latter case we have a
		// graceful one.
		return i.checkFlowCtxCancellation()
	case <-streamCtx.Done():
		// The client canceled the stream.
		return errors.Wrap(streamCtx.Err(), "streamCtx error in Inbox stream handler (remote client canceled)")
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
			i.errCh <- errors.Wrap(err, "remote stream arrived too late")
			return err
		case <-i.flowCtxDone:
			i.errCh <- cancelchecker.QueryCanceledError
			return cancelchecker.QueryCanceledError
		case <-i.Ctx.Done():
			// errToThrow is propagated to the reader of the Inbox.
			errToThrow := i.Ctx.Err()
			if err := i.checkFlowCtxCancellation(); err != nil {
				// This is an ungraceful termination because the flow context
				// has been canceled.
				i.errCh <- err
				errToThrow = err
			}
			return errToThrow
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

	var ungracefulStreamTermination bool
	defer func() {
		// Catch any panics that occur and close the Inbox in order to not leak
		// the goroutine listening for context cancellation. The Inbox must
		// still be closed during normal termination.
		if panicObj := recover(); panicObj != nil {
			if ungracefulStreamTermination {
				// Only close the Inbox here in case of an ungraceful
				// termination.
				i.close()
			}
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
			// expected one in all cases so that the caller could decide on how
			// to handle it.
			log.VEventf(i.Ctx, 2, "Inbox communication error: %v", err)
			err = pgerror.Wrap(err, pgcode.InternalConnectionFailure, "inbox communication error")
			i.errCh <- err
			ungracefulStreamTermination = true
			colexecerror.ExpectedError(err)
		}
		if len(m.Data.Metadata) != 0 {
			log.VEvent(i.Ctx, 2, "Inbox received metadata")
			// If an error was encountered, it needs to be propagated
			// immediately. All other metadata will simply be buffered and
			// returned in DrainMeta.
			var receivedErr error
			for _, rpm := range m.Data.Metadata {
				meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(i.Ctx, rpm)
				if !ok {
					continue
				}
				if meta.Err != nil && receivedErr == nil {
					receivedErr = meta.Err
				} else {
					// Note that if multiple errors are sent in a single
					// message, then we'll propagate the first one right away
					// (via a panic below) and will buffer the rest to be
					// returned in DrainMeta. The caller will catch the panic
					// and will transition to draining, so this all works out.
					//
					// We choose this way of handling multiple errors rather
					// than something like errors.CombineErrors() since we want
					// to keep errors unchanged (e.g. kvpb.ErrPriority() will
					// be called on each error in the DistSQLReceiver).
					i.bufferedMeta = append(i.bufferedMeta, meta)
					colexecutils.AccountForMetadata(i.Ctx, i.allocator.Acc(), i.bufferedMeta[len(i.bufferedMeta)-1:])
				}
			}
			if receivedErr != nil {
				colexecerror.ExpectedError(receivedErr)
			}
			// Continue until we get the next batch or EOF.
			continue
		}
		log.VEvent(i.Ctx, 2, "Inbox received batch")
		if len(m.Data.RawBytes) == 0 {
			// Protect against Deserialization panics by skipping empty messages.
			continue
		}
		numSerializedBytes := int64(len(m.Data.RawBytes))
		atomic.AddInt64(&i.statsAtomics.bytesRead, numSerializedBytes)
		// Update the allocator since we're holding onto the serialized bytes
		// for now.
		i.allocator.AdjustMemoryUsageAfterAllocation(numSerializedBytes)
		// Do admission control after memory accounting for the serialized bytes
		// and before deserialization.
		if i.admissionQ != nil {
			if _, err := i.admissionQ.Admit(i.Ctx, i.admissionInfo); err != nil {
				// err includes the case of context cancellation while waiting for
				// admission.
				colexecerror.ExpectedError(err)
			}
		}
		batch := i.deserializer.Deserialize(m.Data.RawBytes)
		// Eagerly throw away the RawBytes memory.
		m.Data.RawBytes = nil
		// At this point, we have lost all references to the serialized bytes
		// (because ArrowToBatch nils out elements in i.scratch.data once
		// processed), so we update the allocator accordingly.
		i.allocator.AdjustMemoryUsage(-numSerializedBytes)
		atomic.AddInt64(&i.statsAtomics.rowsRead, int64(batch.Length()))
		return batch
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
		log.VWarningf(ctx, 1, "Inbox unable to send drain signal to Outbox: %+v", err)
		return err
	}
	return nil
}

// DrainMeta is part of the colexecop.MetadataSource interface. DrainMeta may
// not be called concurrently with Next.
func (i *Inbox) DrainMeta() []execinfrapb.ProducerMetadata {
	allMeta := i.bufferedMeta
	// Eagerly lose the reference to the metadata since it might be of
	// non-trivial footprint.
	i.bufferedMeta = nil
	// We also no longer need the deserializer.
	i.deserializer.Close(i.Ctx)
	// The allocator tracks the memory usage for a few things (the scratch batch
	// as well as the metadata), and when this function returns, we no longer
	// reference any of those, so we can release all of the allocations.
	defer i.allocator.ReleaseAll()

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
			log.VEventf(i.Ctx, 1, "Inbox communication error while draining metadata: %v", err)
			return allMeta
		}
		if len(msg.Data.Metadata) == 0 {
			log.VEvent(i.Ctx, 2, "Inbox received batch while draining metadata, ignoring")
			continue
		}
		log.VEvent(i.Ctx, 2, "Inbox received metadata while draining metadata")
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
