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
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// flowStreamServer is a utility interface used to mock out the RPC layer.
type flowStreamServer interface {
	Send(*distsqlpb.ConsumerSignal) error
	Recv() (*distsqlpb.ProducerMessage, error)
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
	typs []types.T

	zeroBatch coldata.Batch

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

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

	// errChClosed indicates whether errCh has been closed. This must be accessed
	// atomically.
	errChClosed uint32

	mu struct {
		syncutil.Mutex
		// initialized prevents double initialization. Should not be used by the
		// RunWithStream goroutine.
		initialized bool
		// done prevents double closing and used as a signal from DrainMeta
		// goroutine to Next goroutine that the latter should finish. Should not be
		// used by the RunWithStream goroutine.
		done bool
		// stream is the RPC stream. It is set when RunWithStream is called but
		// only the Next and DrainMeta goroutines may access it.
		stream flowStreamServer
		// bufferedMeta buffers any metadata found in Next when reading from the
		// stream and is returned by DrainMeta.
		bufferedMeta []distsqlpb.ProducerMetadata
	}

	scratch struct {
		data []*array.Data
		b    coldata.Batch
	}
}

const (
	errChOpen   = 0
	errChClosed = 1
)

var _ exec.Operator = &Inbox{}
var _ exec.StaticMemoryOperator = &Inbox{}

// NewInbox creates a new Inbox.
func NewInbox(typs []types.T) (*Inbox, error) {
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
		zeroBatch:  coldata.NewMemBatchWithSize(typs, 0),
		converter:  c,
		serializer: s,
		streamCh:   make(chan flowStreamServer, 1),
		contextCh:  make(chan context.Context, 1),
		timeoutCh:  make(chan error, 1),
		errCh:      make(chan error, 1),
	}
	i.zeroBatch.SetLength(0)
	i.scratch.data = make([]*array.Data, len(typs))
	i.scratch.b = coldata.NewMemBatch(typs)
	i.mu.bufferedMeta = make([]distsqlpb.ProducerMetadata, 0)
	return i, nil
}

// EstimateStaticMemoryUsage implements the StaticMemoryOperator interface.
func (i *Inbox) EstimateStaticMemoryUsage() int {
	return exec.EstimateBatchSizeBytes(i.typs, coldata.BatchSize)
}

// maybeInitLocked calls Inbox.initLocked if the inbox is not initialized and
// returns an error if the initialization was not successful. Usually this is
// because the given context is canceled before the remote stream arrives.
// NOTE: i.mu *must* be held when calling this function.
func (i *Inbox) maybeInitLocked(ctx context.Context) error {
	if !i.mu.initialized {
		if err := i.initLocked(ctx); err != nil {
			return err
		}
		i.mu.initialized = true
	}
	return nil
}

// initLocked initializes the Inbox for operation by blocking until
// RunWithStream sets the stream to read from. ctx ownership is retained until
// the stream arrives (to allow for unblocking the wait for a stream), at which
// point ownership is transferred to RunWithStream. This should only be called
// from the reader goroutine when it needs a stream.
// NOTE: i.mu *must* be held when calling this function.
func (i *Inbox) initLocked(ctx context.Context) error {
	// Wait for the stream to be initialized. We're essentially waiting for the
	// remote connection.
	select {
	case i.mu.stream = <-i.streamCh:
	case err := <-i.timeoutCh:
		i.errCh <- fmt.Errorf("%s: remote stream arrived too late", err)
		return err
	case <-ctx.Done():
		i.errCh <- fmt.Errorf("%s: Inbox while waiting for stream", ctx.Err())
		return ctx.Err()
	}
	i.contextCh <- ctx
	return nil
}

func (i *Inbox) close() {
	if atomic.CompareAndSwapUint32(&i.errChClosed, errChOpen, errChClosed) {
		close(i.errCh)
	}
}

// closeLocked closes the inbox, ensuring that any call to RunWithStream will
// return immediately. closeLocked is idempotent.
// NOTE: i.mu *must* be held when calling this function.
func (i *Inbox) closeLocked() {
	i.close()
	i.mu.done = true
}

// RunWithStream sets the Inbox's stream and waits until either streamCtx is
// canceled, a caller of Next cancels the first context passed into Next, or
// an EOF is encountered on the stream by the Next goroutine.
func (i *Inbox) RunWithStream(streamCtx context.Context, stream flowStreamServer) error {
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
// For simplicity, the Inbox will only listen for cancellation of the context
// passed in to the first Next call.
func (i *Inbox) Next(ctx context.Context) coldata.Batch {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.mu.done {
		return i.zeroBatch
	}

	defer func() {
		// Catch any panics that occur and close the errCh in order to not leak the
		// goroutine listening for context cancellation. errCh must still be closed
		// during normal termination.
		if err := recover(); err != nil {
			// Note that there are situations when we are (we got an error when
			// Recv'ing) and are not (panic while Recv'ing) holding the mutex, so we
			// cannot call i.closeLocked().
			i.close()
			panic(err)
		}
	}()

	// NOTE: It is very important to close i.errCh only when execution terminates
	// ungracefully or when DrainMeta has been called (which indicates a graceful
	// termination). DrainMeta will use the stream to read any remaining metadata
	// after Next returns a zero-length batch during normal execution.
	if err := i.maybeInitLocked(ctx); err != nil {
		panic(err)
	}

	for {
		// It is possible that DrainMeta has been called concurrently in which case
		// we should finish without closing errCh since DrainMeta goroutine still
		// might need the stream.
		if i.mu.done {
			return i.zeroBatch
		}

		i.mu.Unlock()
		m, err := i.mu.stream.Recv()
		i.mu.Lock()
		// TODO(yuzefovich): do we want to check whether i.mu.done is true here?
		// I think it is possible that DrainMeta goroutine was called while Next
		// goroutine was blocked on Recv, so now Next goroutine should just exit
		// right away.
		if err != nil {
			if err == io.EOF {
				// Done.
				i.closeLocked()
				return i.zeroBatch
			}
			i.errCh <- err
			panic(err)
		}
		if len(m.Data.Metadata) != 0 {
			for _, rpm := range m.Data.Metadata {
				meta, ok := distsqlpb.RemoteProducerMetaToLocalMeta(ctx, rpm)
				if !ok {
					continue
				}
				i.mu.bufferedMeta = append(i.mu.bufferedMeta, meta)
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
			panic(err)
		}
		if err := i.converter.ArrowToBatch(i.scratch.data, i.scratch.b); err != nil {
			panic(err)
		}
		return i.scratch.b
	}
}

// DrainMeta is part of the MetadataGenerator interface. DrainMeta may be
// called concurrently with Next.
// Note: when DrainMeta is called, it updates done field of Inbox which will
// cause Next goroutine to finish.
func (i *Inbox) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	i.mu.Lock()
	defer i.mu.Unlock()
	allMeta := i.mu.bufferedMeta
	i.mu.bufferedMeta = i.mu.bufferedMeta[:0]

	if i.mu.done {
		return allMeta
	}

	// Note that unlocking defer from above will execute after this defer because
	// the unlocking one will be pushed below on the stack, so we still will have
	// the lock when this one is executed.
	defer func() {
		// Catch any panics that occur and close the errCh in order to not leak the
		// goroutine listening for context cancellation. errCh must still be closed
		// during normal termination.
		if err := recover(); err != nil {
			i.closeLocked()
			panic(err)
		}
	}()
	defer i.closeLocked()

	if err := i.maybeInitLocked(ctx); err != nil {
		log.Warningf(ctx, "Inbox unable to initialize stream while draining metadata: %+v", err)
		return allMeta
	}
	log.VEvent(ctx, 2, "Inbox sending drain signal to Outbox")
	if err := i.mu.stream.Send(&distsqlpb.ConsumerSignal{DrainRequest: &distsqlpb.DrainRequest{}}); err != nil {
		log.Warningf(ctx, "Inbox unable to send drain signal to Outbox: %+v", err)
		return allMeta
	}
	for {
		msg, err := i.mu.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warningf(ctx, "Inbox Recv connection error while draining metadata: %+v", err)
			return allMeta
		}
		for _, remoteMeta := range msg.Data.Metadata {
			meta, ok := distsqlpb.RemoteProducerMetaToLocalMeta(ctx, remoteMeta)
			if !ok {
				continue
			}
			allMeta = append(allMeta, meta)
		}
	}

	return allMeta
}
