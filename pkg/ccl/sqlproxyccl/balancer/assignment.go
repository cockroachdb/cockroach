// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ServerAssignment represents an assignment to a SQL pod.
type ServerAssignment struct {
	owner   ConnectionHandle
	addr    string
	onClose struct {
		sync.Once
		closerFn func()
	}
}

// NewServerAssignment returns a new instance of ServerAssignment. Instances of
// ServerAssignment must be closed through Close once no longer in use.
func NewServerAssignment(
	tenantID roachpb.TenantID, tracker *ConnTracker, owner ConnectionHandle, addr string,
) *ServerAssignment {
	sa := &ServerAssignment{owner: owner, addr: addr}
	sa.onClose.closerFn = func() {
		tracker.unregisterAssignment(tenantID, sa)
	}
	tracker.registerAssignment(tenantID, sa)
	return sa
}

// Owner returns the connection handle associated with the server assignment.
// The connection handle may not be initialized yet, so callers will need to
// check for that where necessary.
func (sa *ServerAssignment) Owner() ConnectionHandle {
	return sa.owner
}

// Addr returns the address of the server assignment.
func (sa *ServerAssignment) Addr() string {
	return sa.addr
}

// Close cleans up the server assignment and deregisters it from the connection
// tracker. This is idempotent.
func (sa *ServerAssignment) Close() {
	// Tests may create a ServerAssignment without a closerFn.
	if sa.onClose.closerFn != nil {
		sa.onClose.Do(sa.onClose.closerFn)
	}
}
