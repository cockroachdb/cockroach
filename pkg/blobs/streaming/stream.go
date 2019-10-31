// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streaming

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
)

const chunkSize = 4 * 1024

// blobStreamReader implements a ReadCloser which receives
// gRPC streaming messages.
var _ io.ReadCloser = &blobStreamReader{}

type streamReceiver interface {
	SendAndClose(*blobspb.StreamResponse) error
	Recv() (*blobspb.StreamChunk, error)
}

// nopSendAndClose creates a GetStreamClient that has a nop SendAndClose function
type nopSendAndClose struct {
	blobspb.Blob_GetStreamClient
}

func (*nopSendAndClose) SendAndClose(*blobspb.StreamResponse) error {
	return nil
}

// NewGetStreamReader creates an io.ReadCloser that uses gRPC's streaming API
// to read chunks of data.
func NewGetStreamReader(client blobspb.Blob_GetStreamClient) io.ReadCloser {
	return &blobStreamReader{
		lastPayload: nil,
		lastOffset:  0,
		stream:      &nopSendAndClose{client},
		EOFReached:  false,
	}
}

// NewPutStreamReader creates an io.ReadCloser that uses gRPC's streaming API
// to read chunks of data.
func NewPutStreamReader(client blobspb.Blob_PutStreamServer) io.ReadCloser {
	return &blobStreamReader{
		lastPayload: nil,
		lastOffset:  0,
		stream:      client,
		EOFReached:  false,
	}
}

type blobStreamReader struct {
	lastPayload []byte
	lastOffset  int
	stream      streamReceiver
	EOFReached  bool
}

func (r *blobStreamReader) Read(out []byte) (int, error) {
	if r.EOFReached {
		return 0, io.EOF
	}

	offset := 0
	if r.lastPayload != nil {
		// Use the last payload.
		offset = len(r.lastPayload) - r.lastOffset
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

	// Fill remaining space with 0, this should only happen if we reach EOF.
	if offset < len(out) {
		for i := offset; i < len(out); i++ {
			out[i] = 0
		}
	}
	return offset, nil
}

func (r *blobStreamReader) Close() error {
	return r.stream.SendAndClose(&blobspb.StreamResponse{})
}

type streamSender interface {
	Send(*blobspb.StreamChunk) error
}

// Send splits the content into chunks, of size `chunkSize`,
// and streams those chunks to sender.
func Send(sender streamSender, content io.Reader) error {
	for {
		payload := make([]byte, chunkSize)
		n, err := content.Read(payload)
		if n > 0 {
			err = sender.Send(&blobspb.StreamChunk{
				Payload: payload[:n],
			})
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
