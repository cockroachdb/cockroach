// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"fmt"
	"net"
)

// TestAddr is an address to use for test servers. Listening on port 0
// causes the kernel to allocate an unused port.
var TestAddr = NewUnresolvedAddr("tcp", "127.0.0.1:0")

// IsolatedTestAddr is initialized in testaddr_*.go

// IsolatedTestAddr is an address to use for tests that need extra
// isolation by using more addresses than 127.0.0.1 (support for this
// is platform-specific and only enabled on Linux). Both TestAddr and
// IsolatedTestAddr guarantee that the chosen port is not in use when
// allocated, but IsolatedTestAddr draws from a larger pool of
// addresses so that when tests are run in a tight loop the system is
// less likely to run out of available ports or give a port to one
// test immediately after it was closed by another.
//
// IsolatedTestAddr should be used for tests that open and close a
// large number of sockets, or tests which stop a server and rely on
// seeing a "connection refused" error afterwards. It cannot be used
// with tests that operate in secure mode since our test certificates
// are only valid for 127.0.0.1.
var IsolatedTestAddr *UnresolvedAddr

// MakeUnresolvedAddr populates an UnresolvedAddr from a network and raw
// address string.
func MakeUnresolvedAddr(network, addr string) UnresolvedAddr {
	return UnresolvedAddr{
		NetworkField: network,
		AddressField: addr,
	}
}

// NewUnresolvedAddr creates a new UnresolvedAddr from a network and raw
// address string.
func NewUnresolvedAddr(network, addr string) *UnresolvedAddr {
	return &UnresolvedAddr{
		NetworkField: network,
		AddressField: addr,
	}
}

// Note that we make *UnresolvedAddr implement the net.Addr interface, not
// UnresolvedAddr. This is done because assigning a non-empty struct to an
// interface requires an allocation, while assigning a pointer to an interface
// is allocation free. Using an *UnresolvedAddr makes it both clear that an
// allocation is occurring and allows us to avoid an allocation when an
// UnresolvedAddr is a field of a struct (e.g. NodeDescriptor.Address).
var _ net.Addr = &UnresolvedAddr{}

// Network returns the address's network name.
func (a *UnresolvedAddr) Network() string {
	return a.NetworkField
}

// IsEmpty returns true if the address has no network or address specified.
func (a UnresolvedAddr) IsEmpty() bool {
	return a == (UnresolvedAddr{})
}

// String returns the address's string form.
func (a UnresolvedAddr) String() string {
	return a.AddressField
}

// Resolve attempts to resolve a into a net.Addr.
func (a UnresolvedAddr) Resolve() (net.Addr, error) {
	switch a.NetworkField {
	case "tcp", "tcp4", "tcp6":
		return net.ResolveTCPAddr(a.NetworkField, a.AddressField)
	case "udp", "udp4", "udp6":
		return net.ResolveUDPAddr(a.NetworkField, a.AddressField)
	case "unix", "unixgram", "unixpacket":
		return net.ResolveUnixAddr(a.NetworkField, a.AddressField)
	}
	return nil, fmt.Errorf("network %s not supported", a.NetworkField)
}
