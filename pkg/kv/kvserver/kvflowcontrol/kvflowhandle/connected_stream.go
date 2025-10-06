// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowhandle

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
)

// connectedStream is a concrete implementation of the
// kvflowcontrol.ConnectedStream interface. It's used to unblock requests
// waiting for flow tokens over a stream that has since disconnected.
type connectedStream struct {
	stream       kvflowcontrol.Stream
	ch           chan struct{}
	disconnected int32
}

var _ kvflowcontrol.ConnectedStream = &connectedStream{}

func newConnectedStream(stream kvflowcontrol.Stream) *connectedStream {
	return &connectedStream{
		stream: stream,
		ch:     make(chan struct{}),
	}
}

// Stream is part of the kvflowcontrol.ConnectedStream interface.
func (b *connectedStream) Stream() kvflowcontrol.Stream {
	return b.stream
}

// Disconnected is part of the kvflowcontrol.ConnectedStream interface.
func (b *connectedStream) Disconnected() <-chan struct{} {
	return b.ch
}

// Disconnect is used to disconnect the underlying replication stream,
// unblocking all waiting requests.
func (b *connectedStream) Disconnect() {
	if atomic.CompareAndSwapInt32(&b.disconnected, 0, 1) {
		close(b.ch)
	}
}
