// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

//            ┌─────────────────┐
//            │Node.MuxRangefeed│
//            └──────┬───┬──────┘
//  Sender.AddStream │   │LockedMuxStream.Send ───────────────────┐────────────────────────────────┐
//        ┌──────────┴─▼─┴────────────┐                           │                                │
//        │ Buffered/Unbuffered Sender├───────────┐               │                                │
//        └────────────┬──────────────┘           │               │                                │
//                     │                          │               │                                │
//            ┌────────▼─────────┐                │               │                                │
//            │ Stores.Rangefeed │                │               │                                │
//            └────────┬─────────┘                │               │                                │
//                     │                          │               │                                │
//             ┌───────▼─────────┐         BufferedSender      BufferedSender                      │
//             │ Store.Rangefeed │ SendUnbuffered/SendBuffered SendBufferedError ─────► BufferedSender.run
//             └───────┬─────────┘ (catch-up scan)(live raft)     ▲
//                     │                          ▲               │
//            ┌────────▼──────────┐               │               │
//            │ Replica.Rangefeed │               │               │
//            └────────┬──────────┘               │               │
//                     │                          │               │
//             ┌───────▼──────┐                   │               │
//             │ Registration │                   │               │
//             └──────┬───────┘                   │               │
//                    │                           │               │
//                    │                           │               │
//                    └───────────────────────────┘───────────────┘
//               BufferedPerRangeEventSink.Send    BufferedPerRangeEventSink.SendError

// BufferedSender is embedded in every rangefeed.BufferedPerRangeEventSink,
// serving as a helper which buffers events before forwarding events to the
// underlying gRPC stream.
//
// Refer to the comments above UnbufferedSender for more details on the role of
// senders in the entire rangefeed architecture.
type BufferedSender struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender
}

func NewBufferedSender(sender ServerStreamSender) *BufferedSender {
	bs := &BufferedSender{
		sender: sender,
	}
	return bs
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
//
// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc
// and release it if the return error is non-nil. Note that it is safe to send
// error events without being blocked for too long.
func (bs *BufferedSender) send(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// run forwards buffered events back to the client. run is expected to be called
// in a goroutine and will block until the context is done or the stopper is
// quiesced. BufferedSender will stop forwarding events after run completes. It
// may still buffer more events in the buffer, but they will be cleaned up soon
// during bs.Stop(), and there should be no new events buffered after that.
func (bs *BufferedSender) run(
	ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// cleanup is called when the sender is stopped. It is expected to free up
// buffer queue and no new events should be buffered after this.
func (bs *BufferedSender) cleanup() {
	panic("unimplemented: buffered sender for rangefeed #126560")
}
