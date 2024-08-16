// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import "github.com/cockroachdb/cockroach/pkg/kv/kvpb"

// BufferedStreamSender is a StreamSender that buffers events before sending
// them to the underlying rpc ServerStreamSender stream.
type BufferedStreamSender struct {
	ServerStreamSender
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender. It returns an error if the buffer is full or has been
// stopped. BufferedStreamSender is responsible for properly releasing it from
// now on. The event is guaranteed to be sent unless the buffered stream
// terminates before sending (e.g. broken grpc stream).
func (bs *BufferedStreamSender) SendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	// Currently, this is only used in testing. For simplicity, we just send to
	// underlying stream directly. In the future, we will start buffering events.
	return bs.SendUnbuffered(event)
}
