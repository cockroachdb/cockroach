// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

// transferConnectionCount returns the number of times TransferConnection is
// called.
func (h *testConnHandle) transferConnectionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.onTransferConnectionCount
}
