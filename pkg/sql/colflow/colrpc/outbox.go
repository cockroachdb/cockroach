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
	"bytes"
	"context"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

// flowStreamClient is a utility interface used to mock out the RPC layer.
type flowStreamClient interface {
	Send(*execinfrapb.ProducerMessage) error
	Recv() (*execinfrapb.ConsumerSignal, error)
	CloseSend() error
}

// Dialer is used for dialing based on node IDs. It extracts out the single
// method that Outbox.Run needs from nodedialer.Dialer so that we can mock it
// in tests outside of this package.
type Dialer interface {
	Dial(context.Context, roachpb.NodeID, rpc.ConnectionClass) (*grpc.ClientConn, error)
}

// Outbox is used to push data from local flows to a remote endpoint. Run may
// be called with the necessary information to establish a connection to a
// given remote endpoint.
type Outbox struct {
	colexec.OneInputNode

	typs []*types.T
	// batch is the last batch received from the input.
	batch coldata.Batch

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

	// draining is an atomic that represents whether the Outbox is draining.
	draining        uint32
	metadataSources []execinfrapb.MetadataSource
	// closers is a slice of Closers that need to be Closed on termination.
	closers []colexec.IdempotentCloser

	scratch struct {
		buf *bytes.Buffer
		msg *execinfrapb.ProducerMessage
	}

	// A copy of Run's caller ctx, with no StreamID tag.
	// Used to pass a clean context to the input.Next.
	runnerCtx context.Context
}

// NewOutbox creates a new Outbox.
func NewOutbox(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	typs []*types.T,
	metadataSources []execinfrapb.MetadataSource,
	toClose []colexec.IdempotentCloser,
) (*Outbox, error) {
	c, err := colserde.NewArrowBatchConverter(typs)
	if err != nil {
		return nil, err
	}
	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	o := &Outbox{
		// Add a deselector as selection vectors are not serialized (nor should they
		// be).
		OneInputNode:    colexec.NewOneInputNode(colexec.NewDeselectorOp(allocator, input, typs)),
		typs:            typs,
		converter:       c,
		serializer:      s,
		metadataSources: metadataSources,
		closers:         toClose,
	}
	o.scratch.buf = &bytes.Buffer{}
	o.scratch.msg = &execinfrapb.ProducerMessage{}
	return o, nil
}

func (o *Outbox) close(ctx context.Context) {
	for _, closer := range o.closers {
		if err := closer.IdempotentClose(ctx); err != nil {
			if log.V(1) {
				log.Infof(ctx, "error closing Closer: %v", err)
			}
		}
	}
}

// Run starts an outbox by connecting to the provided node and pushing
// coldata.Batches over the stream after sending a header with the provided flow
// and stream ID. Note that an extra goroutine is spawned so that Recv may be
// called concurrently wrt the Send goroutine to listen for drain signals.
// If an io.EOF is received while sending, the outbox will call cancelFn to
// indicate an unexpected termination of the stream.
// If an error is encountered that cannot be sent over the stream, the error
// will be logged but not returned.
// There are several ways the bidirectional FlowStream RPC may terminate.
// 1) Execution is finished. In this case, the upstream operator signals
//    termination by returning a zero-length batch. The Outbox will drain its
//    metadata sources, send the metadata, and then call CloseSend on the
//    stream. The Outbox will wait until its Recv goroutine receives a non-nil
//    error to not leak resources.
// 2) A cancellation happened. This can come from the provided context or the
//    remote reader. Refer to tests for expected behavior.
// 3) A drain signal was received from the server (consumer). In this case, the
//    Outbox goes through the same steps as 1).
func (o *Outbox) Run(
	ctx context.Context,
	dialer Dialer,
	nodeID roachpb.NodeID,
	flowID execinfrapb.FlowID,
	streamID execinfrapb.StreamID,
	cancelFn context.CancelFunc,
) {
	o.runnerCtx = ctx
	ctx = logtags.AddTag(ctx, "streamID", streamID)
	log.VEventf(ctx, 2, "Outbox Dialing %s", nodeID)

	var stream execinfrapb.DistSQL_FlowStreamClient
	if err := func() error {
		conn, err := dialer.Dial(ctx, nodeID, rpc.DefaultClass)
		if err != nil {
			log.Warningf(
				ctx,
				"Outbox Dial connection error, distributed query will fail: %+v",
				err,
			)
			return err
		}

		client := execinfrapb.NewDistSQLClient(conn)
		stream, err = client.FlowStream(ctx)
		if err != nil {
			log.Warningf(
				ctx,
				"Outbox FlowStream connection error, distributed query will fail: %+v",
				err,
			)
			return err
		}

		log.VEvent(ctx, 2, "Outbox sending header")
		// Send header message to establish the remote server (consumer).
		if err := stream.Send(
			&execinfrapb.ProducerMessage{Header: &execinfrapb.ProducerHeader{FlowID: flowID, StreamID: streamID}},
		); err != nil {
			log.Warningf(
				ctx,
				"Outbox Send header error, distributed query will fail: %+v",
				err,
			)
			return err
		}
		return nil
	}(); err != nil {
		// error during stream set up.
		o.close(ctx)
		return
	}

	log.VEvent(ctx, 2, "Outbox starting normal operation")
	o.runWithStream(ctx, stream, cancelFn)
	log.VEvent(ctx, 2, "Outbox exiting")
}

// handleStreamErr is a utility method used to handle an error when calling
// a method on a flowStreamClient. If err is an io.EOF, cancelFn is called. The
// given error is logged with the associated opName.
func (o *Outbox) handleStreamErr(
	ctx context.Context, opName string, err error, cancelFn context.CancelFunc,
) {
	if err == io.EOF {
		if log.V(1) {
			log.Infof(ctx, "Outbox calling cancelFn after %s EOF", opName)
		}
		cancelFn()
	} else {
		if log.V(1) {
			log.Warningf(ctx, "Outbox %s connection error: %+v", opName, err)
		}
	}
}

func (o *Outbox) moveToDraining(ctx context.Context) {
	if atomic.CompareAndSwapUint32(&o.draining, 0, 1) {
		log.VEvent(ctx, 2, "Outbox moved to draining")
	}
}

// sendBatches reads from the Outbox's input in a loop and sends the
// coldata.Batches over the stream. A boolean is returned, indicating whether
// execution completed gracefully (either received a zero-length batch or a
// drain signal) as well as an error which is non-nil if an error was
// encountered AND the error should be sent over the stream as metadata. The for
// loop continues iterating until one of the following conditions becomes true:
// 1) A zero-length batch is received from the input. This indicates graceful
//    termination. true, nil is returned.
// 2) Outbox.draining is observed to be true. This is also considered graceful
//    termination. true, nil is returned.
// 3) An error unrelated to the stream occurs (e.g. while deserializing a
//    coldata.Batch). false, err is returned. This err should be sent over the
//    stream as metadata.
// 4) An error related to the stream occurs. In this case, the error is logged
//    but not returned, as there is no way to propagate this error anywhere
//    meaningful. false, nil is returned. NOTE: io.EOF is a special case. This
//    indicates non-graceful termination initiated by the remote Inbox. cancelFn
//    will be called in this case.
func (o *Outbox) sendBatches(
	ctx context.Context, stream flowStreamClient, cancelFn context.CancelFunc,
) (terminatedGracefully bool, _ error) {
	nextBatch := func() {
		if o.runnerCtx == nil {
			o.runnerCtx = ctx
		}
		o.batch = o.Input().Next(o.runnerCtx)
	}
	serializeBatch := func() {
		o.scratch.buf.Reset()
		d, err := o.converter.BatchToArrow(o.batch)
		if err != nil {
			colexecerror.InternalError(errors.Wrap(err, "Outbox BatchToArrow data serialization error"))
		}
		if _, _, err := o.serializer.Serialize(o.scratch.buf, d); err != nil {
			colexecerror.InternalError(errors.Wrap(err, "Outbox Serialize data error"))
		}
	}
	for {
		if atomic.LoadUint32(&o.draining) == 1 {
			return true, nil
		}

		if err := colexecerror.CatchVectorizedRuntimeError(nextBatch); err != nil {
			if log.V(1) {
				log.Warningf(ctx, "Outbox Next error: %+v", err)
			}
			return false, err
		}
		if o.batch.Length() == 0 {
			return true, nil
		}

		if err := colexecerror.CatchVectorizedRuntimeError(serializeBatch); err != nil {
			log.Errorf(ctx, "%+v", err)
			return false, err
		}
		o.scratch.msg.Data.RawBytes = o.scratch.buf.Bytes()

		// o.scratch.msg can be reused as soon as Send returns since it returns as
		// soon as the message is written to the control buffer. The message is
		// marshaled (bytes are copied) before writing.
		if err := stream.Send(o.scratch.msg); err != nil {
			o.handleStreamErr(ctx, "Send (batches)", err, cancelFn)
			return false, nil
		}
	}
}

// sendMetadata drains the Outbox.metadataSources and sends the metadata over
// the given stream, returning the Send error, if any. sendMetadata also sends
// errToSend as metadata if non-nil.
func (o *Outbox) sendMetadata(ctx context.Context, stream flowStreamClient, errToSend error) error {
	msg := &execinfrapb.ProducerMessage{}
	if errToSend != nil {
		msg.Data.Metadata = append(
			msg.Data.Metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, execinfrapb.ProducerMetadata{Err: errToSend}),
		)
	}
	for _, src := range o.metadataSources {
		for _, meta := range src.DrainMeta(ctx) {
			msg.Data.Metadata = append(msg.Data.Metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, meta))
		}
	}
	if len(msg.Data.Metadata) == 0 {
		return nil
	}
	return stream.Send(msg)
}

// runWithStream should be called after sending the ProducerHeader on the
// stream. It implements the behavior described in Run.
func (o *Outbox) runWithStream(
	ctx context.Context, stream flowStreamClient, cancelFn context.CancelFunc,
) {
	o.Input().Init()

	waitCh := make(chan struct{})
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					if log.V(1) {
						log.Warningf(ctx, "Outbox Recv connection error: %+v", err)
					}
				}
				break
			}
			switch {
			case msg.Handshake != nil:
				log.VEventf(ctx, 2, "Outbox received handshake: %v", msg.Handshake)
			case msg.DrainRequest != nil:
				o.moveToDraining(ctx)
			}
		}
		close(waitCh)
	}()

	terminatedGracefully, errToSend := o.sendBatches(ctx, stream, cancelFn)
	if terminatedGracefully || errToSend != nil {
		o.moveToDraining(ctx)
		if err := o.sendMetadata(ctx, stream, errToSend); err != nil {
			o.handleStreamErr(ctx, "Send (metadata)", err, cancelFn)
		} else {
			// Close the stream. Note that if this block isn't reached, the stream
			// is unusable.
			// The receiver goroutine will read from the stream until io.EOF is
			// returned.
			if err := stream.CloseSend(); err != nil {
				o.handleStreamErr(ctx, "CloseSend", err, cancelFn)
			}
		}
	}

	o.close(ctx)
	<-waitCh
}
