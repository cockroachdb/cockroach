// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package colrpc

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// initialized and done prevent double initialization/closing. Should not be
	// used by the RunWithStream goroutine.
	initialized bool
	done        bool
	// stream is the RPC stream. It is set when RunWithStream is called but only
	// the Next goroutine may access it.
	stream flowStreamServer
	// streamCh is the channel over which the stream is passed from the stream
	// handler to the reader goroutine.
	streamCh chan flowStreamServer
	// contextCh is the channel over which the reader goroutine passes the first
	// context to the stream handler, so that it can listen for context
	// cancellation.
	contextCh chan context.Context

	// errCh is that channel that RunWithStream will block on, waiting until the
	// Inbox does not need a stream any more. An error will only be sent on this
	// channel in the event of a cancellation or a non-io.EOF error originating
	// from a stream.Recv.
	errCh chan error

	// bufferedMeta buffers any metadata found in Next when reading from the
	// stream and is returned by DrainMeta.
	bufferedMeta []distsqlpb.ProducerMetadata

	scratch struct {
		data []*array.Data
	}
}

var _ exec.Operator = &Inbox{}

// NewInbox creates a new Inbox.
func NewInbox(typs []types.T) (*Inbox, error) {
	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	i := &Inbox{
		typs:         typs,
		zeroBatch:    coldata.NewMemBatchWithSize(typs, 0),
		converter:    colserde.NewArrowBatchConverter(typs),
		serializer:   s,
		streamCh:     make(chan flowStreamServer, 1),
		contextCh:    make(chan context.Context, 1),
		errCh:        make(chan error, 1),
		bufferedMeta: make([]distsqlpb.ProducerMetadata, 0),
	}
	i.zeroBatch.SetLength(0)
	i.scratch.data = make([]*array.Data, len(typs))
	return i, nil
}

// maybeInit calls Inbox.init if the inbox is not initialized and returns an
// error if the initialization was not successful. Usually this is because the
// given context is canceled before the remote stream arrives.
func (i *Inbox) maybeInit(ctx context.Context) error {
	if !i.initialized {
		if err := i.init(ctx); err != nil {
			return err
		}
		i.initialized = true
	}
	return nil
}

// init initializes the Inbox for operation by blocking until RunWithStream
// sets the stream to read from. ctx ownership is retained until the stream
// arrives (to allow for unblocking the wait for a stream), at which point
// ownership is transferred to RunWithStream. This should only be called from
// the reader goroutine when it needs a stream.
func (i *Inbox) init(ctx context.Context) error {
	// Wait for the stream to be initialized. We're essentially waiting for the
	// remote connection.
	select {
	case i.stream = <-i.streamCh:
	case <-ctx.Done():
		i.errCh <- fmt.Errorf("%s: Inbox while waiting for stream", ctx.Err())
		return ctx.Err()
	}
	i.contextCh <- ctx
	return nil
}

// close closes the inbox, ensuring that any call to RunWithStream will return
// immediately. close is idempotent.
func (i *Inbox) close() {
	if !i.done {
		i.done = true
		close(i.errCh)
	}
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
	// reader goroutine, as returning from the handler will close the stream.
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

// Init is part of the Operator interface.
func (i *Inbox) Init() {}

// Next returns the next batch. It will block until there is data available.
// For simplicity, the Inbox will only listen for cancellation of the context
// passed in to the first Next call.
func (i *Inbox) Next(ctx context.Context) coldata.Batch {
	if i.done {
		return i.zeroBatch
	}

	defer func() {
		// Catch any panics that occur and close the errCh in order to not leak the
		// goroutine listening for context cancellation. errCh must still be closed
		// during normal termination.
		if err := recover(); err != nil {
			i.close()
			panic(err)
		}
	}()

	// NOTE: It is very important to close i.errCh only when execution terminates
	// ungracefully. DrainMeta will use the stream to read any remaining metadata
	// after Next returns a zero-length batch during normal execution.
	if err := i.maybeInit(ctx); err != nil {
		panic(err)
	}

	for {
		m, err := i.stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Done.
				i.close()
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
				i.bufferedMeta = append(i.bufferedMeta, meta)
			}
			// Continue until we get the next batch or EOF.
			continue
		}
		if len(m.Data.RawBytes) == 0 {
			// Protect against Deserialization panics by skipping empty messages.
			// TODO(asubiotto): I don't think we're using NumEmptyRows, right?
			continue
		}
		i.scratch.data = i.scratch.data[:0]
		if err := i.serializer.Deserialize(&i.scratch.data, m.Data.RawBytes); err != nil {
			panic(err)
		}
		b, err := i.converter.ArrowToBatch(i.scratch.data)
		if err != nil {
			panic(err)
		}
		return b
	}
}

// DrainMeta is part of the MetadataGenerator interface. DrainMeta may not be
// called concurrently with Next.
func (i *Inbox) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	allMeta := i.bufferedMeta
	i.bufferedMeta = i.bufferedMeta[:0]

	if i.done {
		return allMeta
	}

	defer i.close()
	if err := i.maybeInit(ctx); err != nil {
		log.Warningf(ctx, "Inbox unable to initialize stream while draining metadata: %s", err)
		return allMeta
	}
	log.VEvent(ctx, 2, "Inbox sending drain signal to Outbox")
	if err := i.stream.Send(&distsqlpb.ConsumerSignal{DrainRequest: &distsqlpb.DrainRequest{}}); err != nil {
		log.Warningf(ctx, "Inbox unable to send drain signal to Outbox: %s", err)
		return allMeta
	}
	for {
		msg, err := i.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warningf(ctx, "Inbox Recv connection error while draining metadata: %s", err)
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
