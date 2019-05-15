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
	"bytes"
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
)

// flowStreamClient is a utility interface used to mock out the RPC layer.
type flowStreamClient interface {
	Send(*distsqlpb.ProducerMessage) error
	Recv() (*distsqlpb.ConsumerSignal, error)
	CloseSend() error
}

// Outbox is used to push data from local flows to a remote endpoint. Run may
// be called with the necessary information to establish a connection to a
// given remote endpoint.
type Outbox struct {
	input exec.Operator
	typs  []types.T

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer

	scratch struct {
		buf *bytes.Buffer
		msg *distsqlpb.ProducerMessage
	}
}

// NewOutbox creates a new Outbox.
func NewOutbox(input exec.Operator, typs []types.T) (*Outbox, error) {
	s, err := colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		// TODO(asubiotto): Remove err? The only case in which an error is returned
		// is with zero-length types. Might be good to just panic in this case.
		return nil, err
	}
	o := &Outbox{
		// Add a deselector as selection vectors are not serialized (nor should they
		// be).
		input:      exec.NewDeselectorOp(input, typs),
		typs:       typs,
		converter:  colserde.NewArrowBatchConverter(typs),
		serializer: s,
	}
	o.scratch.buf = &bytes.Buffer{}
	o.scratch.msg = &distsqlpb.ProducerMessage{}
	return o, nil
}

// Get rid of unused warning.
// TODO(asubiotto): Remove this once Outbox is used.
var _ = (&Outbox{}).Run

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
// termination by returning a zero-length batch. The Outbox will call CloseSend
// on the stream. The Outbox will wait until its Recv goroutine receives a
// non-nil error to not leak resources.
// 2) A cancellation happened. This can come from the provided context or the
// remote reader. TODO(asubiotto): Exact behavior explanation to be explained
// in following PR.
// 3) A drain signal was received from the server (consumer). In this case, the
// Outbox drains all its metadata sources and sends the metadata to the consumer
// before going through the same steps as 1). TODO(asubiotto): Expand.
func (o *Outbox) Run(
	ctx context.Context,
	dialer *nodedialer.Dialer,
	nodeID roachpb.NodeID,
	flowID distsqlpb.FlowID,
	streamID distsqlpb.StreamID,
	cancelFn context.CancelFunc,
) {
	ctx = logtags.AddTag(ctx, "streamID", streamID)

	// TODO(asubiotto): Currently runWithStream waits for the Recv goroutine to
	// return. Should we just assume that this context cancellation will clean
	// that up? If not, this context cancellation doesn't seem useful. I would
	// prefer to just have runWithStream keep responsibility of the Recv
	// goroutine.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.VEventf(ctx, 2, "Outbox Dialing %s", nodeID)
	conn, err := dialer.Dial(ctx, nodeID)
	if err != nil {
		log.Warningf(
			ctx,
			"Outbox Dial connection error, distributed query will fail: %s",
			err,
		)
		return
	}

	client := distsqlpb.NewDistSQLClient(conn)
	stream, err := client.FlowStream(ctx)
	if err != nil {
		log.Warningf(
			ctx,
			"Outbox FlowStream connection error, distributed query will fail: %s",
			err,
		)
		return
	}

	log.VEvent(ctx, 2, "Outbox sending header")
	// Send header message to establish the remote server (consumer).
	if err := stream.Send(
		&distsqlpb.ProducerMessage{Header: &distsqlpb.ProducerHeader{FlowID: flowID, StreamID: streamID}},
	); err != nil {
		log.Warningf(
			ctx,
			"Outbox Send header error, distributed query will fail: %s",
			err,
		)
		return
	}

	log.VEvent(ctx, 2, "Outbox starting normal operation")
	o.runWithStream(ctx, stream, cancelFn)
}

// runwWithStream should be called after sending the ProducerHeader on the
// stream. It implements the behavior described in Run.
func (o *Outbox) runWithStream(
	ctx context.Context, stream flowStreamClient, cancelFn context.CancelFunc,
) {
	o.input.Init()

	waitCh := make(chan struct{})
	go func() {
		// TODO(asubiotto): Receive drain signals.
		for {
			_, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					log.Warningf(ctx, "Outbox Recv connection error: %s", err)
				}
				break
			}
		}
		close(waitCh)
	}()
	for {
		var b coldata.Batch
		if err := exec.CatchVectorizedRuntimeError(func() { b = o.input.Next(ctx) }); err != nil {
			// TODO(asubiotto): Send this error as metadata.
			log.Warningf(ctx, "Outbox Next error: %s", err)
			break
		}
		if b.Length() == 0 {
			if err := stream.CloseSend(); err != nil {
				if err == io.EOF {
					cancelFn()
					break
				}
				log.Warningf(ctx, "Outbox CloseSend connection error: %s", err)
				break
			}
			// The receiver goroutine will read from the stream until io.EOF is
			// returned.
			break
		}

		o.scratch.buf.Reset()
		d, err := o.converter.BatchToArrow(b)
		if err != nil {
			// TODO(asubiotto): Send this error as metadata.
			log.Warningf(ctx, "Outbox BatchToArrow data serialization error: %s", err)
			break
		}
		if err := o.serializer.Serialize(o.scratch.buf, d); err != nil {
			// TODO(asubiotto): Send this error as metadata.
			log.Warningf(ctx, "Outbox Serialize data error: %s", err)
			break
		}
		o.scratch.msg.Data.RawBytes = o.scratch.buf.Bytes()

		// o.scratch.msg can be reused as soon as Send returns since it returns as
		// soon as the message is written to the control buffer. The message is
		// marshaled (bytes are copied) before writing.
		if err := stream.Send(o.scratch.msg); err != nil {
			if err == io.EOF {
				cancelFn()
				break
			}
			log.Warningf(
				ctx,
				"Outbox Send data error, distributed query will fail: %s",
				err,
			)
			break
		}
	}
	<-waitCh
}
