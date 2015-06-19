// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"net"

	"github.com/cockroachdb/cockroach/util"
)

// FromNetAddr returns an Addr object based on the supplied net.Addr.
func FromNetAddr(addr net.Addr) *Addr {
	return &Addr{
		Network: addr.Network(),
		Address: addr.String(),
	}
}

// NetAddr returns a net.Addr object.
func (a Addr) NetAddr() (net.Addr, error) {
	switch a.Network {
	case "tcp", "tcp4", "tcp6":
		return net.ResolveTCPAddr(a.Network, a.Address)
	case "udp", "udp4", "udp6":
		return net.ResolveUDPAddr(a.Network, a.Address)
	case "unix", "unixgram", "unixpacket":
		return net.ResolveUnixAddr(a.Network, a.Address)
	}
	return nil, util.Errorf("network %s not supported", a.Network)
}

// GetUser implements UserRequest.
// Gossip messages are always sent by the node user.
func (m *GossipRequest) GetUser() string {
	// TODO(marc): we should use security.NodeUser here, but we need to break cycles first.
	return "node"
}
