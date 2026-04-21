// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Stream is an object capable of transmitting TxnFeedEvents from a server
// TxnFeed to a client. It supports both buffered and unbuffered sends:
//
//   - SendBuffered is non-blocking and pushes events into the BufferedSender's
//     shared queue. Used for live event delivery after the catch-up scan.
//   - SendUnbuffered sends directly to the gRPC stream, bypassing the buffer.
//     Used during the catch-up scan.
//   - SendError sends a completion error without blocking. Safe to call under
//     raftMu.
type Stream interface {
	// SendBuffered buffers the event for asynchronous delivery via the
	// BufferedSender. This call must not block.
	SendBuffered(event *kvpb.TxnFeedEvent) error
	// SendUnbuffered sends the event directly to the underlying gRPC stream,
	// bypassing the buffer. This call may block. Used during catch-up scan.
	SendUnbuffered(event *kvpb.TxnFeedEvent) error
	// SendError sends an error to the stream. Must not block on IO. May be
	// called by the processor while holding raftMu.
	SendError(err *kvpb.Error)
}

// Disconnector defines an interface for disconnecting a TxnFeed registration.
// It is returned to the StreamManager to allow node-level stream management
// to disconnect a registration.
type Disconnector interface {
	// Disconnect disconnects the registration with the provided error. Safe to
	// run multiple times; subsequent errors are discarded.
	Disconnect(pErr *kvpb.Error)
	// IsDisconnected returns whether the registration has been disconnected.
	// Disconnected is a permanent state; once IsDisconnected returns true, it
	// always returns true.
	IsDisconnected() bool
	// Unregister is called when an error has finally been delivered to the
	// underlying stream.
	Unregister()
}

// PerRangeTxnFeedSink wraps each TxnFeedEvent with rangeID and streamID
// metadata for multiplexed streams. It implements Stream by delegating to a
// BufferedSender for both buffered and unbuffered sends.
type PerRangeTxnFeedSink struct {
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *BufferedSender
}

func newPerRangeTxnFeedSink(
	rangeID roachpb.RangeID, streamID int64, wrapped *BufferedSender,
) *PerRangeTxnFeedSink {
	return &PerRangeTxnFeedSink{
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ Stream = (*PerRangeTxnFeedSink)(nil)

// SendBuffered implements Stream. It buffers the event in the BufferedSender
// for asynchronous delivery to the gRPC stream.
func (s *PerRangeTxnFeedSink) SendBuffered(event *kvpb.TxnFeedEvent) error {
	return s.wrapped.sendBuffered(&kvpb.MuxTxnFeedEvent{
		TxnFeedEvent: *event,
		RangeID:      s.rangeID,
		StreamID:     s.streamID,
	})
}

// SendUnbuffered implements Stream. It sends the event directly to the
// underlying gRPC stream, bypassing the buffer. This call may block.
func (s *PerRangeTxnFeedSink) SendUnbuffered(event *kvpb.TxnFeedEvent) error {
	return s.wrapped.sendUnbuffered(&kvpb.MuxTxnFeedEvent{
		TxnFeedEvent: *event,
		RangeID:      s.rangeID,
		StreamID:     s.streamID,
	})
}

// SendError implements Stream. It sends a txnfeed completion error back to
// the client without blocking. This may be called by the processor while
// holding raftMu, so it is important that this function doesn't block on IO.
func (s *PerRangeTxnFeedSink) SendError(err *kvpb.Error) {
	if err == nil {
		err = kvpb.NewError(
			kvpb.NewTxnFeedRetryError(kvpb.TxnFeedRetryError_REASON_TXNFEED_CLOSED))
	}
	ev := &kvpb.MuxTxnFeedEvent{
		RangeID:  s.rangeID,
		StreamID: s.streamID,
	}
	ev.Error = &kvpb.TxnFeedError{
		Error: *err,
	}
	// Silence the error: expected to happen when the buffered sender is
	// closed or stopped.
	_ = s.wrapped.sendBuffered(ev)
}
