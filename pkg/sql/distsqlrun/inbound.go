// Copyright 2016 The Cockroach Authors.
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

package distsqlrun

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// ProcessInboundStream receives rows from a DistSQL_FlowStreamServer and sends
// them to a RowReceiver. Optionally processes an initial StreamMessage that was
// already received (because the first message contains the flow and stream IDs,
// it needs to be received before we can get here).
func ProcessInboundStream(
	ctx context.Context,
	stream DistSQL_FlowStreamServer,
	firstMsg *ProducerMessage,
	dst RowReceiver,
	f *Flow,
) error {

	err := processInboundStreamHelper(ctx, stream, firstMsg, dst, f)

	// err, if set, will also be propagated to the producer
	// as the last record that the producer gets.
	if err != nil {
		log.VEventf(ctx, 1, "inbound stream error: %s", err)
		return err
	}
	log.VEventf(ctx, 1, "inbound stream done")
	// We are now done. The producer, if it's still around, will receive an EOF
	// error over its side of the stream.
	return nil
}

func processInboundStreamHelper(
	ctx context.Context,
	stream DistSQL_FlowStreamServer,
	firstMsg *ProducerMessage,
	dst RowReceiver,
	f *Flow,
) error {
	var rs readState
	var sd StreamDecoder

	sendErrToConsumer := func(err error) {
		if err != nil {
			dst.Push(nil, &ProducerMetadata{Err: err})
		}
		dst.ProducerDone()
	}

	if firstMsg != nil {
		if res := processProducerMessage(
			ctx, stream, dst, &sd, &rs, firstMsg,
		); res.err != nil {
			sendErrToConsumer(res.err)
			return res.err
		} else if res.consumerClosed {
			sendErrToConsumer(nil)
			return nil
		}
	}

	// There's two goroutines involved in handling the RPC - the current one (the
	// "parent"), which is watching for context cancellation, and a "reader" one
	// that receives messages from the stream. This is all because a stream.Recv()
	// call doesn't react to context cancellation. The idea is that, if the parent
	// detects a canceled context, it will return from this RPC handler, which
	// will cause the stream to be closed. Because the parent cannot wait for the
	// reader to finish (that being the whole point of the different goroutines),
	// the reader sending an error to the parent might race with the parent
	// finishing. In that case, nobody cares about the reader anymore and so its
	// result channel is buffered.
	errChan := make(chan error, 1)

	f.waitGroup.Add(1)
	go func() {
		defer f.waitGroup.Done()
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					// Communication error.
					err = errors.Wrap(
						err, log.MakeMessage(ctx, "communication error", nil /* args */))
					sendErrToConsumer(err)
					errChan <- err
					return
				}
				// End of the stream.
				sendErrToConsumer(rs.finalErr)
				errChan <- rs.finalErr
				return
			}

			if res := processProducerMessage(
				ctx, stream, dst, &sd, &rs, msg,
			); res.err != nil {
				sendErrToConsumer(res.err)
				errChan <- res.err
				return
			} else if res.consumerClosed {
				sendErrToConsumer(rs.finalErr)
				errChan <- rs.finalErr
				return
			}
		}
	}()

	// Check for context cancellation while reading from the stream on another
	// goroutine.
	select {
	case <-f.Ctx.Done():
		return sqlbase.NewQueryCanceledError()
	case err := <-errChan:
		return err
	}
}

// sendDrainSignalToProducer is called when the consumer wants to signal the
// producer that it doesn't need any more rows and the producer should drain. A
// signal is sent on stream to the producer to ask it to send metadata.
func sendDrainSignalToStreamProducer(ctx context.Context, stream DistSQL_FlowStreamServer) error {
	log.VEvent(ctx, 1, "sending drain signal to producer")
	sig := ConsumerSignal{DrainRequest: &DrainRequest{}}
	return stream.Send(&sig)
}

// processProducerMessage is a helper function to read data from the producer
// and send it along to the consumer. It uses rs to keep track of its state
// between runs. If the returned error is set, the caller must return the error
// to the producer. If the returned bool is true, the consumer is in a
// ConsumerClosed state.
func processProducerMessage(
	ctx context.Context,
	stream DistSQL_FlowStreamServer,
	dst RowReceiver,
	sd *StreamDecoder,
	rs *readState,
	msg *ProducerMessage,
) processMessageResult {
	err := sd.AddMessage(msg)
	if err != nil {
		return processMessageResult{
			err:            errors.Wrap(err, log.MakeMessage(ctx, "decoding error", nil /* args */)),
			consumerClosed: false,
		}
	}
	var types []sqlbase.ColumnType
	for {
		row, meta, err := sd.GetRow(nil /* rowBuf */)
		if err != nil {
			return processMessageResult{err: err, consumerClosed: false}
		}
		if row == nil && meta == nil {
			// No more rows in the last message.
			return processMessageResult{err: nil, consumerClosed: false}
		}

		if log.V(3) {
			if types == nil {
				types = sd.Types()
			}
			log.Infof(ctx, "inbound stream pushing row %s", row.String(types))
		}
		if rs.draining && meta == nil {
			// Don't forward data rows when we're draining.
			continue
		}
		switch dst.Push(row, meta) {
		case NeedMoreRows:
			continue
		case DrainRequested:
			// The rest of rows are not needed by the consumer. We'll send a drain
			// signal to the producer and expect it to quickly send trailing
			// metadata and close its side of the stream, at which point we also
			// close the consuming side of the stream and call dst.ProducerDone().
			if !rs.draining {
				rs.draining = true
				if err := sendDrainSignalToStreamProducer(ctx, stream); err != nil {
					// We remember to forward this error to the consumer, but we
					// continue forwarding the rows we've already buffered.
					// NOTE(andrei): I'm not sure what to do with this error. If we
					// failed to send the drain signal to the producer, we're probably
					// (guaranteed?) also going to fail the next stream.Recv() call with
					// something other than io.EOF, in which case that error will
					// override rs.finalErr. Assuming io.EOF were to be returned by the
					// following stream.Recv(), we also don't care about this error; the
					// draining would be completed successfully regardless of the
					// failure to send this signal. This suggests that maybe we want to
					// swallow this error...
					rs.finalErr = err
				}
			}
		case ConsumerClosed:
			return processMessageResult{err: rs.finalErr, consumerClosed: true}
		}
	}
}

// readState is used to keep track of processProducerMessage's state between
// calls.
type readState struct {
	draining bool
	// finalErr is the error that will be returned to the producer after the
	// consumer is closed successfully, or if the producer has exited.
	finalErr error
}

type processMessageResult struct {
	err            error
	consumerClosed bool
}
