// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// testConnHandle is a test connection handle that only implements a small
// subset of methods used for testing.
type testConnHandle struct {
	ConnectionHandle
	id                   int
	ctx                  context.Context
	onClose              func()
	onTransferConnection func() error

	mu struct {
		syncutil.Mutex
		// Connection handles are active by default (i.e. idle = false).
		idle                      bool
		onTransferConnectionCount int
	}
}

var _ ConnectionHandle = &testConnHandle{}

// Context implements the ConnectionHandle interface.
func (h *testConnHandle) Context() context.Context {
	if h.ctx != nil {
		return h.ctx
	}
	return context.Background()
}

// Close implements the ConnectionHandle interface.
func (h *testConnHandle) Close() {
	h.onClose()
}

// TransferConnection implements the ConnectionHandle interface.
func (h *testConnHandle) TransferConnection() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.onTransferConnectionCount++

	if h.ctx != nil && h.ctx.Err() != nil {
		return h.ctx.Err()
	}
	return h.onTransferConnection()
}

// IsIdle implements the ConnectionHandle interface.
func (h *testConnHandle) IsIdle() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.idle
}

// setIdle updates the idle state of the connection handle.
func (h *testConnHandle) setIdle(idle bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.idle = idle
}

// transferConnectionCount returns the number of times TransferConnection is
// called.
func (h *testConnHandle) transferConnectionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.onTransferConnectionCount
}
