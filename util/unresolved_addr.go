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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"fmt"
	"net"
)

// MakeUnresolvedAddr creates a new UnresolvedAddr from a network
// and raw address string.
func MakeUnresolvedAddr(network string, str string) UnresolvedAddr {
	return UnresolvedAddr{
		NetworkField: network,
		AddressField: str,
	}
}

// Network returns the address's network name.
func (a UnresolvedAddr) Network() string {
	return a.NetworkField
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
