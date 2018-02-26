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

	// err, if set, will be propagated to the producer and the consumer as the
	// last message from us.
	if err != nil {
		log.VEventf(ctx, 1, "inbound stream error: %s", err)
		dst.Push(nil, &ProducerMetadata{Err: err})
		dst.ProducerDone()
		return err
	}
	log.VEventf(ctx, 1, "inbound stream done")
	dst.ProducerDone()
	// The consumer is now done. The producer, if it's still around, will
	// receive an EOF error over its side of the stream.
	return nil
}

func processInboundStreamHelper(
	ctx context.Context,
	stream DistSQL_FlowStreamServer,
	firstMsg *ProducerMessage,
	dst RowReceiver,
	f *Flow,
) error {
	var finalErr error
	draining := false
	var sd StreamDecoder

	// processProducerMessage is a helper function to read data from the producer
	// and send it along to the consumer. If err is set, the caller must return
	// the error to the producer. If useFinalErr is set, the caller must return
	// finalErr, even if it's nil.
	processProducerMessage := func(msg *ProducerMessage) (err error, useFinalErr bool) {
		err = sd.AddMessage(msg)
		if err != nil {
			return errors.Wrap(err, log.MakeMessage(ctx, "decoding error", nil /* args */)), false
		}
		var types []sqlbase.ColumnType
		for {
			row, meta, err := sd.GetRow(nil /* rowBuf */)
			if err != nil {
				return err, false
			}
			if row == nil && meta == nil {
				// No more rows in the last message.
				return nil, false
			}

			if log.V(3) {
				if types == nil {
					types = sd.Types()
				}
				log.Infof(ctx, "inbound stream pushing row %s", row.String(types))
			}
			if draining && meta == nil {
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
				if !draining {
					draining = true
					if err := sendDrainSignalToStreamProducer(ctx, stream); err != nil {
						// We remember to forward this error to the consumer, but we
						// continue forwarding the rows we've already buffered.
						// NOTE(andrei): I'm not sure what to do with this error. If we
						// failed to send the drain signal to the producer, we're probably
						// (guaranteed?) also going to fail the next stream.Recv() call with
						// something other than io.EOF, in which case that error will override
						// finalErr. Assuming io.EOF were to be returned by the following
						// stream.Recv(), we also don't care about this error; the draining
						// would be completed successfully regardless of the failure to send
						// this signal. This suggests that maybe we want to swallow this
						// error...
						finalErr = err
					}
				}
			case ConsumerClosed:
				return finalErr, true
			}
		}
	}

	if firstMsg != nil {
		if err, useFinalErr := processProducerMessage(firstMsg); err != nil {
			return err
		} else if useFinalErr {
			return finalErr
		}
	}

	// If the main goroutine detects a context cancellation just before
	// the RPC-receiving goroutine sends an error, the error won't be consumed,
	// and the RPC goroutine will deadlock. To prevent this, make errChan a
	// buffered channel.
	errChan := make(chan error, 1)

	f.waitGroup.Add(1)
	go func() {
		defer f.waitGroup.Done()
		for {
			// Check for cancellation before recv()ing the next message.
			select {
			case <-f.Ctx.Done():
				return
			default:
			}

			msg, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					// Communication error.
					errChan <- errors.Wrap(
						err, log.MakeMessage(ctx, "communication error", nil /* args */))
					return
				}
				// End of the stream.
				errChan <- finalErr
				return
			}

			if err, useFinalErr := processProducerMessage(msg); err != nil {
				errChan <- err
				return
			} else if useFinalErr {
				errChan <- finalErr
				return
			}
		}
	}()

	// Check for context cancellation while recv()ing the FlowStram on another
	// goroutine.
	select {
	case <-f.Ctx.Done():
		// The flow's context is canceled. Error out the RPC to make the outbox on
		// the producer node cancel its own flow context.
		return sqlbase.NewQueryCanceledError()
	case err := <-errChan:
		// The producer is done, or the RPC errored out. Return the error to close
		// the stream.
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
