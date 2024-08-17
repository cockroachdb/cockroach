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

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/errors"
)

// BufferedStreamSender is a wrapper around ServerStreamSender which buffers
// events before sending them to the underlying rpc ServerStreamSender stream.
// It should be treated as a black box which only buffers and sends events. It
// does not hold any stream specific information.
type BufferedStreamSender struct {
	ServerStreamSender
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender. It returns an error if the buffer is full or has been
// stopped. If it does not return an error, the event is guaranteed to be sent
// unless the BufferedStreamSender is stopped before sending it (such as due to
// broken grpc stream). It does not check for any stream-specific information.
// It is the caller's responsibility to check whether the stream is active and
// ensure events are sent in order.
//
// Note that the ownership of SharedBudgetAllocation has now been transferred to
// BufferedStreamSender. It is responsible for using and releasing the budget
// from now on.
func (bs *BufferedStreamSender) SendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	return errors.Errorf("unimplemented: buffered stream sender")
}
