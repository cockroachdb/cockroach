// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		// Since closerFn is used within Close, operations in closerFn should
		// not invoke Close on the server assignment, or else a cyclic call may
		// occur, which will result in a deadlock.
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
