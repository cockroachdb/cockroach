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
	"sync"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
)

// flowStreamServer is a utility interface used to mock out the RPC layer.
type flowStreamServer interface {
	Send(*execinfrapb.ConsumerSignal) error
	Recv() (*execinfrapb.ProducerMessage, error)
}

// Inbox is used to expose data from remote flows through an exec.Operator
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
	colexecbase.ZeroInputNode
	typs []*types.T

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

	// streamID is used to overwrite a caller's streamID
	// in the ctx argument of Next and DrainMeta.
	streamID execinfrapb.StreamID

	// streamCh is the channel over which the stream is passed from the stream
	// handler to the reader goroutine.
	streamCh chan flowStreamServer
	// contextCh is the channel over which the reader goroutine passes the first
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

	// We need two mutexes because a single mutex is insufficient to handle
	// concurrent calls to Next() and DrainMeta(). See comment in DrainMeta.
	stateMu struct {
		syncutil.Mutex
		// initialized prevents double initialization. Should not be used by the
		// RunWithStream goroutine.
		initialized bool
		// done prevents double closing. It should not be used by the RunWithStream
		// goroutine.
		done bool
		// nextRunning indicates whether Next goroutine is running at the moment.
		nextRunning bool
		// nextExited is a condition variable on which DrainMeta might block in
		// order to wait for Next goroutine to exit.
		nextExited *sync.Cond
		// nextShouldExit indicates to Next goroutine that it should exit. It must
		// only be updated by DrainMeta goroutine.
		nextShouldExit bool
		// bufferedMeta buffers any metadata found in Next when reading from the
		// stream and is returned by DrainMeta.
		bufferedMeta []execinfrapb.ProducerMetadata
	}

	streamMu struct {
		syncutil.Mutex
		// stream is the RPC stream. It is set when RunWithStream is called but
		// only the Next and DrainMeta goroutines may access it.
		stream flowStreamServer
	}

	scratch struct {
		data []*array.Data
		b    coldata.Batch
	}
}

var _ colexecbase.Operator = &Inbox{}

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
		typs:       typs,
		converter:  c,
		serializer: s,
		streamID:   streamID,
		streamCh:   make(chan flowStreamServer, 1),
		contextCh:  make(chan context.Context, 1),
		timeoutCh:  make(chan error, 1),
		errCh:      make(chan error, 1),
	}
	i.scratch.data = make([]*array.Data, len(typs))
	i.scratch.b = allocator.NewMemBatch(typs)
	i.stateMu.bufferedMeta = make([]execinfrapb.ProducerMetadata, 0)
	i.stateMu.nextExited = sync.NewCond(&i.stateMu)
	return i, nil
}

// maybeInitLocked calls Inbox.initLocked if the inbox is not initialized and
// returns an error if the initialization was not successful. Usually this is
// because the given context is canceled before the remote stream arrives.
// NOTE: i.stateMu *must* be held when calling this function.
func (i *Inbox) maybeInitLocked(ctx context.Context) error {
	if !i.stateMu.initialized {
		if err := i.initLocked(ctx); err != nil {
			return err
		}
		i.stateMu.initialized = true
	}
	return nil
}

// initLocked initializes the Inbox for operation by blocking until
// RunWithStream sets the stream to read from. ctx ownership is retained until
// the stream arrives (to allow for unblocking the wait for a stream), at which
// point ownership is transferred to RunWithStream. This should only be called
// from the reader goroutine when it needs a stream.
// NOTE: i.stateMu *must* be held when calling this function because it is
// sufficient to protect access to i.streamMu.stream since the stream will only
// be accessed after the initialization.
func (i *Inbox) initLocked(ctx context.Context) error {
	// Wait for the stream to be initialized. We're essentially waiting for the
	// remote connection.
	select {
	case i.streamMu.stream = <-i.streamCh:
	case err := <-i.timeoutCh:
		i.errCh <- fmt.Errorf("%s: remote stream arrived too late", err)
		return err
	case <-ctx.Done():
		i.errCh <- fmt.Errorf("%s: Inbox while waiting for stream", ctx.Err())
		return ctx.Err()
	}

	if i.ctxInterceptorFn != nil {
		i.ctxInterceptorFn(ctx)
	}
	i.contextCh <- ctx
	return nil
}

// closeLocked closes the inbox, ensuring that any call to RunWithStream will
// return immediately. closeLocked is idempotent.
// NOTE: i.stateMu *must* be held when calling this function.
func (i *Inbox) closeLocked() {
	if !i.stateMu.done {
		i.stateMu.done = true
		close(i.errCh)
	}
}

// RunWithStream sets the Inbox's stream and waits until either streamCtx is
// canceled, a caller of Next cancels the first context passed into Next, or
// an EOF is encountered on the stream by the Next goroutine.
func (i *Inbox) RunWithStream(streamCtx context.Context, stream flowStreamServer) error {
	streamCtx = logtags.AddTag(streamCtx, "streamID", i.streamID)
	log.VEvent(streamCtx, 2, "Inbox handling stream")
	defer log.VEvent(streamCtx, 2, "Inbox exited stream handler")
	// Pass the stream to the reader goroutine (non-blocking) and get the context
	// to listen for cancellation.
	i.streamCh <- stream
	var readerCtx context.Context
	select {
	case err := <-i.errCh:
		return err
	case readerCtx = <-i.contextCh:
		log.VEvent(streamCtx, 2, "Inbox reader arrived")
	case <-streamCtx.Done():
		return fmt.Errorf("%s: streamCtx while waiting for reader (remote client canceled)", streamCtx.Err())
	}

	// Now wait for one of the events described in the method comment. If a
	// cancellation is encountered, nothing special must be done to cancel the
	// reader goroutine as returning from the handler will close the stream.
	select {
	case err := <-i.errCh:
		// nil will be read from errCh when the channel is closed.
		return err
	case <-readerCtx.Done():
		// The reader canceled the stream.
		return fmt.Errorf("%s: readerCtx in Inbox stream handler (local reader canceled)", readerCtx.Err())
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
func (i *Inbox) Init() {}

// Next returns the next batch. It will block until there is data available.
// The Inbox will exit when either the context passed in on the first call to
// Next is canceled or when DrainMeta goroutine tells it to do so.
func (i *Inbox) Next(ctx context.Context) coldata.Batch {
	i.stateMu.Lock()
	stateMuLocked := true
	i.stateMu.nextRunning = true
	defer func() {
		i.stateMu.nextRunning = false
		i.stateMu.nextExited.Signal()
		i.stateMu.Unlock()
	}()
	if i.stateMu.done {
		return coldata.ZeroBatch
	}

	ctx = logtags.AddTag(ctx, "streamID", i.streamID)

	defer func() {
		// Catch any panics that occur and close the errCh in order to not leak the
		// goroutine listening for context cancellation. errCh must still be closed
		// during normal termination.
		if err := recover(); err != nil {
			if !stateMuLocked {
				// The panic occurred while we were Recv'ing when we were holding
				// i.streamMu and were not holding i.stateMu.
				i.stateMu.Lock()
				i.streamMu.Unlock()
			}
			i.closeLocked()
			colexecerror.InternalError(err)
		}
	}()

	// NOTE: It is very important to close i.errCh only when execution terminates
	// ungracefully or when DrainMeta has been called (which indicates a graceful
	// termination). DrainMeta will use the stream to read any remaining metadata
	// after Next returns a zero-length batch during normal execution.
	if err := i.maybeInitLocked(ctx); err != nil {
		// An error occurred while initializing the Inbox and is likely caused by
		// the connection issues. It is expected that such an error can occur.
		colexecerror.ExpectedError(err)
	}

	for {
		// DrainMeta goroutine indicated to us that we should exit. We do so
		// without closing errCh since DrainMeta still needs the stream.
		if i.stateMu.nextShouldExit {
			return coldata.ZeroBatch
		}

		i.stateMu.Unlock()
		stateMuLocked = false
		i.streamMu.Lock()
		m, err := i.streamMu.stream.Recv()
		i.streamMu.Unlock()
		i.stateMu.Lock()
		stateMuLocked = true
		if err != nil {
			if err == io.EOF {
				// Done.
				i.closeLocked()
				return coldata.ZeroBatch
			}
			i.errCh <- err
			colexecerror.ExpectedError(err)
		}
		if len(m.Data.Metadata) != 0 {
			for _, rpm := range m.Data.Metadata {
				meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(ctx, rpm)
				if !ok {
					continue
				}
				i.stateMu.bufferedMeta = append(i.stateMu.bufferedMeta, meta)
			}
			// Continue until we get the next batch or EOF.
			continue
		}
		if len(m.Data.RawBytes) == 0 {
			// Protect against Deserialization panics by skipping empty messages.
			continue
		}
		i.scratch.data = i.scratch.data[:0]
		if err := i.serializer.Deserialize(&i.scratch.data, m.Data.RawBytes); err != nil {
			colexecerror.InternalError(err)
		}
		if err := i.converter.ArrowToBatch(i.scratch.data, i.scratch.b); err != nil {
			colexecerror.InternalError(err)
		}
		return i.scratch.b
	}
}

func (i *Inbox) sendDrainSignal(ctx context.Context) error {
	log.VEvent(ctx, 2, "Inbox sending drain signal to Outbox")
	// It is safe to Send without holding the mutex because it is legal to call
	// Send and Recv from different goroutines.
	if err := i.streamMu.stream.Send(&execinfrapb.ConsumerSignal{DrainRequest: &execinfrapb.DrainRequest{}}); err != nil {
		if log.V(1) {
			log.Warningf(ctx, "Inbox unable to send drain signal to Outbox: %+v", err)
		}
		return err
	}
	return nil
}

// DrainMeta is part of the MetadataGenerator interface. DrainMeta may be
// called concurrently with Next.
// Note: DrainMeta will cause Next goroutine to finish.
func (i *Inbox) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	allMeta := i.stateMu.bufferedMeta
	i.stateMu.bufferedMeta = i.stateMu.bufferedMeta[:0]

	if i.stateMu.done {
		return allMeta
	}

	ctx = logtags.AddTag(ctx, "streamID", i.streamID)

	// We want draining the Inbox to work regardless of whether or not we have a
	// goroutine in Next. We essentially need to do two things: 1) Is the stream
	// safe to use? If yes, then 2) Make sure nobody else is receiving.
	// Unfortunately, there is no way to cancel a Recv on a stream, so we need to
	// do this by sending the message. However, we can't unconditionally send a
	// message since we don't know the state of the stream (is it initialized?).
	// This leaves us with having two separate mutexes, one for the state and
	// another one for the stream (to make sure we wait until the Next goroutine
	// has finished Recv'ing).
	drainSignalSent := false
	if i.stateMu.initialized {
		if err := i.sendDrainSignal(ctx); err != nil {
			return allMeta
		}
		drainSignalSent = true
		i.stateMu.nextShouldExit = true
		for i.stateMu.nextRunning {
			i.stateMu.nextExited.Wait()
		}
		// It is possible that Next goroutine has buffered more metadata, so we
		// need to grab it.
		allMeta = append(allMeta, i.stateMu.bufferedMeta...)
		i.stateMu.bufferedMeta = i.stateMu.bufferedMeta[:0]
	}

	// Note that unlocking defer from above will execute after this defer because
	// the unlocking one will be pushed below on the stack, so we still will have
	// the lock when this one is executed.
	defer i.closeLocked()

	if err := i.maybeInitLocked(ctx); err != nil {
		if log.V(1) {
			log.Warningf(ctx, "Inbox unable to initialize stream while draining metadata: %+v", err)
		}
		return allMeta
	}
	if !drainSignalSent {
		if err := i.sendDrainSignal(ctx); err != nil {
			return allMeta
		}
	}

	i.streamMu.Lock()
	defer i.streamMu.Unlock()
	for {
		msg, err := i.streamMu.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if log.V(1) {
				log.Warningf(ctx, "Inbox Recv connection error while draining metadata: %+v", err)
			}
			return allMeta
		}
		for _, remoteMeta := range msg.Data.Metadata {
			meta, ok := execinfrapb.RemoteProducerMetaToLocalMeta(ctx, remoteMeta)
			if !ok {
				continue
			}
			allMeta = append(allMeta, meta)
		}
	}

	return allMeta
}
