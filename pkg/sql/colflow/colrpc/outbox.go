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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colmeta"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// flowStreamClient is a utility interface used to mock out the RPC layer.
type flowStreamClient interface {
	Send(*execinfrapb.ProducerMessage) error
	Recv() (*execinfrapb.ConsumerSignal, error)
	CloseSend() error
}

// Outbox is used to push data from local flows to a remote endpoint. Run may
// be called with the necessary information to establish a connection to a
// given remote endpoint.
type Outbox struct {
	colexecop.OneInputNode

	// handler should not be accessed after the initialization.
	handler colmeta.StreamingMetadataHandler
	// These three fields point to the same handler object but provide different
	// interfaces in order to have a clear distinction of which methods are
	// allowed to be used by each of the goroutines.
	producer      colmeta.DataProducer
	consumer      colmeta.DataConsumer
	streamingMeta colmeta.StreamingMetadataProducer

	// inputMetaInfo contains all of the meta components that the outbox is
	// responsible for. OneInputNode.Input is the deselector operator with Root
	// field as its input. Notably StatsCollectors are not accessed directly -
	// instead, getStats is used for those.
	inputMetaInfo    colexecargs.OpWithMetaInfo
	inputInitialized bool

	typs []*types.T

	allocator  *colmem.Allocator
	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

	// draining is an atomic that represents whether the Outbox is draining.
	draining uint32

	// scratch contains fields that are reused by the goroutine reading from the
	// input. These fields are safe to be reused because we block the input
	// reading (producer) goroutine until the message is sent on the gRPC stream
	// by the main (consumer) goroutine.
	scratch struct {
		buf *bytes.Buffer
		msg *execinfrapb.ProducerMessage
	}

	span *tracing.Span
	// getStats, when non-nil, returns all of the execution statistics of the
	// operators that are in the same tree as this Outbox. The stats will be
	// added into the span as Structured payload and returned to the gateway as
	// execinfrapb.ProducerMetadata.
	getStats func() []*execinfrapb.ComponentStats

	// A copy of Run's caller ctx, with no StreamID tag. Used to pass a clean
	// context to the input reading (producer) goroutine.
	producerCtx context.Context

	testingKnobs struct {
		// skipCloseSend, if true, will make the main (producer) goroutine not
		// call CloseSend on the gRPC stream.
		skipCloseSend bool
	}
}

var _ colexecop.StreamingMetadataReceiver = &Outbox{}

// NewOutbox creates a new Outbox.
// - getStats, when non-nil, returns all of the execution statistics of the
//   operators that are in the same tree as this Outbox.
func NewOutbox(
	allocator *colmem.Allocator,
	input colexecargs.OpWithMetaInfo,
	typs []*types.T,
	getStats func() []*execinfrapb.ComponentStats,
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
		OneInputNode:  colexecop.NewOneInputNode(colexecutils.NewDeselectorOp(allocator, input.Root, typs)),
		inputMetaInfo: input,
		typs:          typs,
		allocator:     allocator,
		converter:     c,
		serializer:    s,
		getStats:      getStats,
	}
	o.handler.Init()
	o.producer = &o.handler
	o.consumer = &o.handler
	o.streamingMeta = &o.handler
	o.scratch.buf = &bytes.Buffer{}
	o.scratch.msg = &execinfrapb.ProducerMessage{}
	for _, s := range input.StreamingMetadataSources {
		s.SetReceiver(o)
	}
	return o, nil
}

// Run starts an outbox by connecting to the provided node and pushing
// coldata.Batches over the stream after sending a header with the provided flow
// and stream ID. Note that an extra goroutine is spawned so that Recv may be
// called concurrently wrt the Send goroutine to listen for drain signals.
// If an io.EOF is received while sending, the outbox will cancel all components
// from the same tree as the outbox.
// If non-io.EOF is received while sending, the outbox will call flowCtxCancel
// to shutdown all parts of the flow on this node.
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
	dialer execinfra.Dialer,
	nodeID roachpb.NodeID,
	flowID execinfrapb.FlowID,
	streamID execinfrapb.StreamID,
	flowCtxCancel context.CancelFunc,
	connectionTimeout time.Duration,
) {
	// Derive a child context so that we can cancel all components rooted in
	// this outbox.
	var outboxCtxCancel context.CancelFunc
	ctx, outboxCtxCancel = context.WithCancel(ctx)
	// Calling outboxCtxCancel is not strictly necessary, but we do it just to
	// be safe.
	defer outboxCtxCancel()

	ctx, o.span = execinfra.ProcessorSpan(ctx, "outbox")
	if o.span != nil {
		defer o.span.Finish()
	}
	defer func() {
		o.scratch.buf = nil
		o.scratch.msg = nil
		// Unset the input (which is a deselector operator) so that its output
		// batch could be garbage collected. This allows us to release all
		// memory registered with the allocator (the allocator is shared by the
		// outbox and the deselector).
		o.Input = nil
		o.allocator.ReleaseMemory(o.allocator.Used())
		o.inputMetaInfo.ToClose.CloseAndLogOnErr(ctx, "outbox")
	}()

	o.producerCtx = ctx
	ctx = logtags.AddTag(ctx, "streamID", streamID)
	log.VEventf(ctx, 2, "Outbox Dialing %s", nodeID)

	var stream execinfrapb.DistSQL_FlowStreamClient
	if err := func() error {
		conn, err := execinfra.GetConnForOutbox(ctx, dialer, nodeID, connectionTimeout)
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
		return
	}

	log.VEvent(ctx, 2, "Outbox starting normal operation")
	o.runWithStream(ctx, stream, flowCtxCancel, outboxCtxCancel)
	log.VEvent(ctx, 2, "Outbox exiting")
}

// handleStreamErr is a utility method used to handle an error when calling
// a method on a flowStreamClient. If err is an io.EOF, outboxCtxCancel is
// called, for all other errors flowCtxCancel is. The given error is logged with
// the associated opName.
func handleStreamErr(
	ctx context.Context, opName string, err error, flowCtxCancel, outboxCtxCancel context.CancelFunc,
) {
	if err == io.EOF {
		if log.V(1) {
			log.Infof(ctx, "Outbox calling outboxCtxCancel after %s EOF", opName)
		}
		outboxCtxCancel()
	} else {
		log.Warningf(ctx, "Outbox calling flowCtxCancel after %s connection error: %+v", opName, err)
		flowCtxCancel()
	}
}

func (o *Outbox) moveToDraining(ctx context.Context, reason string) {
	if atomic.CompareAndSwapUint32(&o.draining, 0, 1) {
		log.VEventf(ctx, 2, "Outbox moved to draining (%s)", reason)
	}
}

// pushBatches reads from the Outbox's input in a loop and pushes the
// coldata.Batches to the consumer goroutine to be send over the gRPC stream.
//
// A boolean is returned, indicating whether draining of metadata sources should
// be skipped as well as an error which is non-nil if an error was encountered
// AND the error should be sent over the stream as metadata. The for loop
// continues iterating until one of the following conditions becomes true:
// 1) A zero-length batch is received from the input. This indicates graceful
//    termination. false, nil is returned.
// 2) Outbox.draining is observed to be true. This is also considered graceful
//    termination. false, nil is returned.
// 3) A context cancellation error is encountered while pushing a message to
//    the consumer goroutine, indicating an ungraceful termination. true, nil is
//    returned.
// 4) Any other error occurs (e.g. while deserializing a coldata.Batch).
//    false, err is returned. This err should be sent over the stream as
//    metadata.
func (o *Outbox) pushBatches() (skipDraining bool, errToSend error) {
	errToSend = colexecerror.CatchVectorizedRuntimeError(func() {
		o.Input.Init(o.producerCtx)
		o.inputInitialized = true
		for {
			if atomic.LoadUint32(&o.draining) == 1 {
				return
			}

			batch := o.Input.Next()
			n := batch.Length()
			if n == 0 {
				return
			}

			// Note that for certain types (like Decimals, Intervals,
			// datum-backed types) BatchToArrow allocates some memory in order
			// to perform the conversion, and we consciously choose to ignore it
			// for the purposes of the memory accounting because the references
			// to those slices are lost in Serialize call below.
			d, err := o.converter.BatchToArrow(batch)
			if err != nil {
				colexecerror.InternalError(errors.Wrap(err, "Outbox BatchToArrow data serialization error"))
			}

			oldBufCap := o.scratch.buf.Cap()
			o.scratch.buf.Reset()
			if _, _, err := o.serializer.Serialize(o.scratch.buf, d, n); err != nil {
				colexecerror.InternalError(errors.Wrap(err, "Outbox Serialize data error"))
			}
			// Account for the increase in the capacity of the scratch buffer.
			// Note that because we never truncate the buffer, we are only
			// adjusting the memory usage whenever the buffer's capacity
			// increases (if it didn't increase, this call becomes a noop).
			o.allocator.AdjustMemoryUsage(int64(o.scratch.buf.Cap() - oldBufCap))
			o.scratch.msg.Data.RawBytes = o.scratch.buf.Bytes()
			if err := o.producer.SendRemoteProducerMessage(o.producerCtx, o.scratch.msg); err != nil {
				skipDraining = true
				// Note that there is no need to communicate the error to the
				// consumer goroutine because it is a context cancellation error
				// in which case the stream context has also been canceled. This
				// means that the inbox will get the error automatically. The
				// gRPC stream is unusable at this point anyway.
				return
			}
		}
	})
	return skipDraining, errToSend
}

// pushMetadata drains all non-streaming metadata sources from the input tree
// and sends the metadata to the consumer goroutine to be propagated on the
// gRPC stream. pushMetadata also pushes errToSend as metadata if non-nil.
func (o *Outbox) pushMetadata(ctx context.Context, errToSend error) {
	msg := &execinfrapb.ProducerMessage{}
	if errToSend != nil {
		log.VEventf(ctx, 1, "Outbox sending an error as metadata: %v", errToSend)
		msg.Data.Metadata = append(
			msg.Data.Metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, execinfrapb.ProducerMetadata{Err: errToSend}),
		)
	}
	if o.inputInitialized {
		// Retrieving stats and draining the metadata is only safe if the input
		// to the outbox was properly initialized.
		if o.span != nil && o.getStats != nil {
			for _, s := range o.getStats() {
				o.span.RecordStructured(s)
			}
		}
		for _, meta := range o.inputMetaInfo.MetadataSources.DrainMeta() {
			msg.Data.Metadata = append(msg.Data.Metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, meta))
		}
	}
	if trace := execinfra.GetTraceData(ctx); trace != nil {
		msg.Data.Metadata = append(msg.Data.Metadata, execinfrapb.RemoteProducerMetadata{
			Value: &execinfrapb.RemoteProducerMetadata_TraceData_{
				TraceData: &execinfrapb.RemoteProducerMetadata_TraceData{
					CollectedSpans: trace,
				},
			},
		})
	}
	if len(msg.Data.Metadata) == 0 {
		return
	}
	// We're not interested in any errors since the producer goroutine is
	// essentially done at this point.
	_ = o.producer.SendRemoteProducerMessage(ctx, msg)
}

// PushStreamingMeta is part of the colexecop.StreamingMetadataReceiver
// interface.
func (o *Outbox) PushStreamingMeta(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
	msg := &execinfrapb.ProducerMessage{
		Data: execinfrapb.ProducerData{
			Metadata: []execinfrapb.RemoteProducerMetadata{
				execinfrapb.LocalMetaToRemoteProducerMeta(ctx, *meta),
			},
		},
	}
	return o.streamingMeta.SendRemoteStreamingMeta(ctx, msg)
}

// runWithStream should be called after sending the ProducerHeader on the
// stream. It implements the behavior described in Run.
func (o *Outbox) runWithStream(
	streamCtx context.Context,
	stream flowStreamClient,
	flowCtxCancel, outboxCtxCancel context.CancelFunc,
) {
	// Cancellation functions might be nil in some tests, but we'll make them
	// noops for convenience.
	if flowCtxCancel == nil {
		flowCtxCancel = func() {}
	}
	if outboxCtxCancel == nil {
		outboxCtxCancel = func() {}
	}

	var wg sync.WaitGroup
	// TODO(yuzefovich): consider using stopper to run this goroutine.
	wg.Add(1)
	go func(stream flowStreamClient) {
		// This goroutine's job is to listen continually on the stream from the
		// consumer of FlowStream RPC for errors or drain requests, while the
		// remainder of this function concurrently is producing data (in the
		// second goroutine) and sending it over the network (in the main
		// goroutine).
		//
		// This goroutine will tear down the flow if non-io.EOF error is
		// received - without it, a producer goroutine might spin doing work
		// forever after a connection is closed, since it wouldn't notice a
		// closed connection until it tried to Send over that connection.
		//
		// Similarly, if an io.EOF error is received, it indicates that the
		// server side of FlowStream RPC (the inbox) has exited gracefully, so
		// the inbox doesn't need anything else from this outbox, and this
		// goroutine will shut down the tree of operators rooted in this outbox.
		defer wg.Done()
		for {
			msg, err := stream.Recv()
			if err != nil {
				handleStreamErr(streamCtx, "watchdog Recv", err, flowCtxCancel, outboxCtxCancel)
				return
			}
			switch {
			case msg.Handshake != nil:
				log.VEventf(streamCtx, 2, "Outbox received handshake: %v", msg.Handshake)
			case msg.DrainRequest != nil:
				log.VEventf(streamCtx, 2, "Outbox received drain request")
				o.moveToDraining(streamCtx, "consumer requested draining" /* reason */)
			}
		}
	}(stream)

	// TODO(yuzefovich): consider using stopper to run this goroutine.
	wg.Add(1)
	go func() {
		// This goroutine's job is to read continuously from the input of the
		// Outbox and communicate each piece of data to the main goroutine. This
		// goroutine works in sync with the main goroutine (by blocking on
		// o.producer) in order to not read more data than necessary.
		//
		// See the comment on pushBatches on when this goroutine exits.
		defer wg.Done()
		defer o.producer.ProducerDone()
		<-o.producer.WaitForConsumer()
		// Check whether we have been canceled while we were waiting for the
		// consumer to arrive.
		if err := streamCtx.Err(); err != nil {
			log.VEventf(streamCtx, 1, "%s", err.Error())
			return
		}
		if o.producerCtx == nil {
			// In the non-testing path, producerCtx has been set in Run()
			// method; however, the tests might use runWithStream() directly in
			// which case producerCtx will remain unset, so we have this check.
			o.producerCtx = streamCtx
		}
		skipDraining, errToSend := o.pushBatches()
		if !skipDraining {
			reason := "terminated gracefully"
			if errToSend != nil {
				reason = fmt.Sprintf("encountered error when reading batches: %v", errToSend)
			}
			o.moveToDraining(o.producerCtx, reason)
			o.pushMetadata(o.producerCtx, errToSend)
		}
	}()

	// This is the body of the main (consumer) goroutine of the Outbox that
	// receives messages from the producer goroutine (as well as - possibly -
	// from streaming metadata sources) and sends them over the gRPC stream.
	//
	// If an error sending on the gRPC stream occurs, then if
	// - it is a non-io.EOF error (indicating an ungraceful shutdown of the
	//   stream), then flowCtxCancel is called;
	// - it is an io.EOF (indicating a graceful shutdown initiated by the
	//   remote Inbox), outboxCtxCancel is called.
	// In both cases, this goroutine exits.
	defer func() {
		if stream != nil && !o.testingKnobs.skipCloseSend {
			// Close the stream if it hasn't been unset.
			if err := stream.CloseSend(); err != nil {
				handleStreamErr(streamCtx, "CloseSend", err, flowCtxCancel, outboxCtxCancel)
			}
		}
		wg.Wait()
	}()

	o.consumer.ConsumerArrived()
	for {
		msg := o.consumer.NextRemoteProducerMsg(streamCtx)
		if msg == nil {
			return
		}
		if err := stream.Send(msg); err != nil {
			handleStreamErr(streamCtx, "Send", err, flowCtxCancel, outboxCtxCancel)
			// The stream is unusable, so don't attempt to close it.
			stream = nil
			return
		}
	}
}
