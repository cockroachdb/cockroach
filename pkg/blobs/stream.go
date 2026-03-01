// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobs

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

// Within the blob service, streaming is used in two functions:
//   - GetStream, streaming from server to client
//   - PutStream, streaming from client to server
// These functions are used to read or write files on a remote node.
// The io.ReadCloser we implement here are used on the _receiver's_
// side, to read from either Blob_GetStreamClient or Blob_PutStreamServer.
// The function streamContent() is used on the _sender's_ side to split
// the content and send it using Blob_GetStreamServer or Blob_PutStreamClient.

// ChunkSize was decided to be 128K after running an experiment benchmarking
// ReadFile and WriteFile. It seems like the benefits of streaming do not appear
// until files of 1 MB or larger, and for those files, 128K chunks are optimal.
// For ReadFile, larger chunks are more efficient but the gains are not as significant
// past 128K. For WriteFile, 128K chunks perform best, and past that, performance
// starts decreasing.
const ChunkSize = 128 * 1 << 10

// FlowControlWindow is a cluster setting that controls the number of
// unacknowledged chunks the blob service can have in flight at any time.
// Setting this to 0 means the window is unbounded (no flow control).
var FlowControlWindow = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"bulkio.blob.flow_control_window",
	"number of unacknowledged chunks the blob service can have in flight; 0 means unbounded",
	16,
	settings.IntInRange(0, 64),
)

// chunkBufferPool is a pool of byte slice buffers used for streaming chunks.
// Using a pool reduces allocations during high-throughput file transfers.
var chunkBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, ChunkSize)
		return &buf
	},
}

// blobStreamReader implements a ReadCloser which receives
// gRPC streaming messages.
var _ ioctx.ReadCloserCtx = &blobStreamReader{}

type streamReceiver interface {
	SendAndClose(*blobspb.StreamResponse) error
	Recv() (*blobspb.StreamChunk, error)
}

// closeFuncSendAndClose creates a GetStreamClient that calls a close function
// when SendAndClose is called. The close function is expected to cancel the context
// and cause the stream to clean itself up.
type closeFuncSendAndClose struct {
	blobspb.RPCBlob_GetStreamClient
	close func()
}

func (c *closeFuncSendAndClose) SendAndClose(*blobspb.StreamResponse) error {
	c.close()
	return nil
}

// newGetStreamReader creates an io.ReadCloser that uses gRPC's streaming API
// to read chunks of data.
func newGetStreamReader(client blobspb.RPCBlob_GetStreamClient, close func()) ioctx.ReadCloserCtx {
	return &blobStreamReader{
		stream: &closeFuncSendAndClose{client, close},
	}
}

// newPutStreamReader creates an io.ReadCloser that uses gRPC's streaming API
// to read chunks of data.
func newPutStreamReader(client blobspb.RPCBlob_PutStreamStream) ioctx.ReadCloserCtx {
	return &blobStreamReader{stream: client}
}

type blobStreamReader struct {
	lastPayload []byte
	lastOffset  int
	stream      streamReceiver
	EOFReached  bool
}

func (r *blobStreamReader) Read(ctx context.Context, out []byte) (int, error) {
	if r.EOFReached {
		return 0, io.EOF
	}

	offset := 0
	// Use the last payload.
	if r.lastPayload != nil {
		offset = len(r.lastPayload) - r.lastOffset
		if len(out) < offset {
			copy(out, r.lastPayload[r.lastOffset:])
			r.lastOffset += len(out)
			return len(out), nil
		}
		copy(out[:offset], r.lastPayload[r.lastOffset:])
		r.lastPayload = nil
	}
	for offset < len(out) {
		chunk, err := r.stream.Recv()
		if err == io.EOF {
			r.EOFReached = true
			break
		}
		if err != nil {
			return offset, err
		}
		var lenToWrite int
		if len(out)-offset >= len(chunk.Payload) {
			lenToWrite = len(chunk.Payload)
		} else {
			lenToWrite = len(out) - offset
			// Need to cache payload.
			r.lastPayload = chunk.Payload
			r.lastOffset = lenToWrite
		}
		copy(out[offset:offset+lenToWrite], chunk.Payload[:lenToWrite])
		offset += lenToWrite
	}
	return offset, nil
}

func (r *blobStreamReader) Close(ctx context.Context) error {
	return r.stream.SendAndClose(&blobspb.StreamResponse{})
}

type streamSender interface {
	Send(*blobspb.StreamChunk) error
}

// streamContent splits the content into chunks, of size `ChunkSize`,
// and streams those chunks to sender.
// Note: This does not close the stream.
func streamContent(ctx context.Context, sender streamSender, content ioctx.ReaderCtx) error {
	bufPtr := chunkBufferPool.Get().(*[]byte)
	defer chunkBufferPool.Put(bufPtr)
	payload := *bufPtr
	var chunk blobspb.StreamChunk
	for {
		n, err := content.Read(ctx, payload)
		if n > 0 {
			chunk.Payload = payload[:n]
			err = sender.Send(&chunk)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// streamContentWithFlowControl streams file content with explicit acknowledgments.
// It maintains a sliding window of in-flight chunks and waits for acks before
// sending more data.
func streamContentWithFlowControl(
	ctx context.Context,
	stream blobspb.RPCBlob_GetStreamFlowControlledStream,
	content ioctx.ReaderCtx,
	window int,
) error {
	bufPtr := chunkBufferPool.Get().(*[]byte)
	defer chunkBufferPool.Put(bufPtr)
	payload := (*bufPtr)[:ChunkSize]
	inFlight := 0
	eofReached := false

	for {
		// Send chunks until window is full or we reach EOF.
		for inFlight < window && !eofReached {
			n, readErr := content.Read(ctx, payload)
			if n > 0 {
				msg := &blobspb.FlowControlledServerMessage{
					Msg: &blobspb.FlowControlledServerMessage_Chunk{
						Chunk: &blobspb.StreamChunk{Payload: payload[:n]},
					},
				}
				if err := stream.Send(msg); err != nil {
					return errors.Wrap(err, "sending chunk")
				}
				inFlight++
			}

			if readErr == io.EOF {
				eofReached = true
				// Send EOF marker.
				eofMsg := &blobspb.FlowControlledServerMessage{
					Msg: &blobspb.FlowControlledServerMessage_Eof{Eof: true},
				}
				if err := stream.Send(eofMsg); err != nil {
					return errors.Wrap(err, "sending EOF")
				}
				break
			}
			if readErr != nil {
				return errors.Wrap(readErr, "reading content")
			}
		}

		// If we've sent EOF and all chunks are acknowledged, we're done.
		if eofReached && inFlight == 0 {
			return nil
		}

		// Wait for an ack from the client.
		ackMsg, err := stream.Recv()
		if err == io.EOF {
			// Client closed the stream.
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "waiting for ack")
		}
		// Ignore messages that are not acks. This allows us to adjust the protocol
		// in the future without breaking compatibility.
		if _, ok := ackMsg.GetMsg().(*blobspb.FlowControlledClientMessage_Ack); !ok {
			continue
		}
		ackCount := ackMsg.GetAck()
		if ackCount <= 0 {
			return errors.AssertionFailedf("unexpected negative ack message: %d", ackCount)
		}
		inFlight -= int(ackCount)
		if inFlight < 0 {
			inFlight = 0
		}
	}
}

// flowControlledStreamReader implements a ReadCloser for flow-controlled streams.
// It reads chunks from the server and sends acknowledgments back.
type flowControlledStreamReader struct {
	stream      blobspb.RPCBlob_GetStreamFlowControlledClient
	cancel      func()
	lastPayload []byte
	lastOffset  int
	eofReached  bool
	pending     int // chunks received but not yet acked
}

var _ ioctx.ReadCloserCtx = &flowControlledStreamReader{}

func newFlowControlledStreamReader(
	stream blobspb.RPCBlob_GetStreamFlowControlledClient, cancel func(),
) ioctx.ReadCloserCtx {
	return &flowControlledStreamReader{
		stream: stream,
		cancel: cancel,
	}
}

func (r *flowControlledStreamReader) Read(ctx context.Context, out []byte) (int, error) {
	if r.eofReached {
		return 0, io.EOF
	}

	offset := 0

	// Use any remaining data from the last chunk.
	if r.lastPayload != nil {
		remaining := len(r.lastPayload) - r.lastOffset
		if len(out) < remaining {
			copy(out, r.lastPayload[r.lastOffset:])
			r.lastOffset += len(out)
			return len(out), nil
		}
		copy(out[:remaining], r.lastPayload[r.lastOffset:])
		offset = remaining
		r.lastPayload = nil
		r.pending++
	}

	for offset < len(out) {
		// Send acks if we have pending chunks to acknowledge.
		if r.pending > 0 {
			ackMsg := &blobspb.FlowControlledClientMessage{
				Msg: &blobspb.FlowControlledClientMessage_Ack{
					Ack: int32(r.pending),
				},
			}
			if err := r.stream.Send(ackMsg); err != nil {
				return offset, errors.Wrap(err, "sending ack")
			}
			r.pending = 0
		}

		// Receive the next message.
		msg, err := r.stream.Recv()
		if err == io.EOF {
			r.eofReached = true
			break
		}
		if err != nil {
			return offset, err
		}

		// Check if this is an EOF marker.
		if msg.GetEof() {
			r.eofReached = true
			break
		}

		chunk := msg.GetChunk()
		if chunk == nil {
			return offset, errors.New("unexpected message type")
		}

		payload := chunk.Payload
		if len(out)-offset >= len(payload) {
			copy(out[offset:offset+len(payload)], payload)
			offset += len(payload)
			r.pending++
		} else {
			// Partial read, cache the remainder.
			toWrite := len(out) - offset
			copy(out[offset:], payload[:toWrite])
			r.lastPayload = payload
			r.lastOffset = toWrite
			offset = len(out)
		}
	}

	return offset, nil
}

func (r *flowControlledStreamReader) Close(ctx context.Context) error {
	r.cancel()
	return nil
}
