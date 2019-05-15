// Copyright 2019 The Cockroach Authors.
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

package colrpc

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
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

	initialized bool
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

	// errCh is that channel that RunWithStream will block on, waiting until Next
	// encounters an exit condition. An error will only be sent on this channel
	// in the event of a cancellation.
	errCh chan error

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
		typs:       typs,
		zeroBatch:  coldata.NewMemBatchWithSize(typs, 0),
		converter:  colserde.NewArrowBatchConverter(typs),
		serializer: s,
		streamCh:   make(chan flowStreamServer, 1),
		contextCh:  make(chan context.Context, 1),
		errCh:      make(chan error, 1),
	}
	i.zeroBatch.SetLength(0)
	i.scratch.data = make([]*array.Data, len(typs))
	return i, nil
}

// init initializes the Inbox for operation by blocking until RunWithStream
// sets the stream to read from. ctx ownership is retained until the stream
// arrives (to allow for unblocking the wait for a stream), at which point
// ownership is transferred to RunWithStream. This should only be called from
// the reader goroutine when it needs a stream.
func (i *Inbox) init(ctx context.Context) bool {
	// Wait for the stream to be initialized. We're essentially waiting for the
	// remote connection.
	select {
	case i.stream = <-i.streamCh:
	case <-ctx.Done():
		i.errCh <- fmt.Errorf("%s: Inbox while waiting for stream", ctx.Err())
		return false
	}
	i.contextCh <- ctx
	return true
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
	defer func() {
		// Catch any panics that occur and close the errCh in order to not leak the
		// goroutine listening for context cancellation. errCh must still be closed
		// during normal termination.
		if err := recover(); err != nil {
			close(i.errCh)
			panic(err)
		}
	}()

	// NOTE: It is very important to close i.errCh on termination of execution
	// (normally or when panicking), otherwise we might leak RunWithStream.
	if !i.initialized {
		if !i.init(ctx) {
			close(i.errCh)
			return i.zeroBatch
		}
		i.initialized = true
	}

	m, err := i.stream.Recv()
	if err != nil {
		if err == io.EOF {
			close(i.errCh)
			return i.zeroBatch
		}
		panic(err)
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

// DrainMeta is part of the MetadataGenerator interface.
// TODO(asubiotto): Implement this.
func (i *Inbox) DrainMeta(context.Context) []distsqlrun.ProducerMetadata {
	return nil
}
