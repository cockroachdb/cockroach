// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

	// ServerRemoteAddr returns the remote address of the connection between
	// the proxy and the server, which is basically the SQL pod's address
	// (e.g. 10.15.42.36:26257). This will be used to identify which pod the
	// connection handle is attached to.
	ServerRemoteAddr() string

	// IsIdle returns true if the connection is idle, and false otherwise.
	IsIdle() bool
}
