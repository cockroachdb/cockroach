// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"fmt"
	"net"
)

// TestAddr is an address to use for test servers. Listening on port 0 causes
// the kernel to allocate an unused port.
var TestAddr = NewUnresolvedAddr("tcp", "127.0.0.1:0")

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

// String returns the address's string form.
func (a *UnresolvedAddr) String() string {
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
