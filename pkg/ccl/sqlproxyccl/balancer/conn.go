// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import "context"

// ConnectionHandle corresponds to the connection's handle, which will always
// be instances of the forwarder.
type ConnectionHandle interface {
	// Context returns the context object associated with the handle.
	Context() context.Context

	// Close closes the connection handle.
	Close()

	// TransferConnection performs a connection migration on the connection
	// handle. Invoking this blocks until the connection migration process has
	// been completed.
	TransferConnection() error

	// IsIdle returns true if the connection is idle, and false otherwise.
	IsIdle() bool
}
