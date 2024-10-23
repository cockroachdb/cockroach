// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

//// BufferedPerRangeEventSink is an implementation of BufferedStream which is
//// similar to PerRangeEventSink but buffers events in BufferedSender before
//// forwarding events to the underlying grpc stream.
//type BufferedPerRangeEventSink struct {
//	*PerRangeEventSink
//	sendBuffered func(*kvpb.MuxRangeFeedEvent, *SharedBudgetAllocation) error
//}

// var _ kvpb.RangeFeedEventSink = (*BufferedPerRangeEventSink)(nil)
// var _ Stream = (*BufferedPerRangeEventSink)(nil)
// var _ BufferedStream = (*BufferedPerRangeEventSink)(nil)
//
// SendBuffered buffers the event in BufferedSender and transfers the ownership
// of SharedBudgetAllocation to BufferedSender. BufferedSender is responsible
// for properly using and releasing it when an error occurs or when the event is
// sent. The event is guaranteed to be sent unless BufferedSender terminates
// before sending (such as due to broken grpc stream).
//
// If the function returns an error, it is safe to disconnect the stream and
// assume that all future SendBuffered on this stream will return an error.
//func (s *BufferedPerRangeEventSink) SendBuffered(
//	event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
//) error {
//	rfe := &kvpb.MuxRangeFeedEvent{
//		RangeFeedEvent: *event,
//		RangeID:        s.rangeID,
//		StreamID:       s.streamID,
//	}
//	return s.sendBuf(rfe, alloc)
//}
