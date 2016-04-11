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
// Author: Marc Berhault (marc@cockroachlabs.com)

package resolver

import (
	"net"
	"os"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
)

// Resolver is an interface which provides an abstract factory for
// net.Addr addresses. Resolvers are not thread safe.
type Resolver interface {
	Type() string
	Addr() string
	GetAddress() (net.Addr, error)
}

// NewResolver takes an address and returns a new resolver.
func NewResolver(context *base.Context, address string) (Resolver, error) {
	if len(address) == 0 {
		return nil, util.Errorf("invalid address value: %q", address)
	}

	// Ensure addr has port and host set.
	address = ensureHostPort(address, base.DefaultPort)
	return &socketResolver{typ: "tcp", addr: address}, nil
}

// NewResolverFromAddress takes a net.Addr and constructs a resolver.
func NewResolverFromAddress(addr net.Addr) (Resolver, error) {
	switch addr.Network() {
	case "tcp":
		return &socketResolver{typ: addr.Network(), addr: addr.String()}, nil
	default:
		return nil, util.Errorf("unknown address network %q for %v", addr.Network(), addr)
	}
}

// NewResolverFromUnresolvedAddr takes a util.UnresolvedAddr and constructs a resolver.
func NewResolverFromUnresolvedAddr(addr util.UnresolvedAddr) (Resolver, error) {
	return NewResolverFromAddress(&addr)
}

// ensureHostPort takes a host:port addr, where the host and port are optional. If host and port are
// present, the output is equal to addr. If port is not present, defaultPort is used. If host is not
// present, hostname (or "127.0.0.1" as a fallback) is used.
func ensureHostPort(addr string, defaultPort string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	if host == "" {
		host, err = os.Hostname()
		if err != nil {
			host = "127.0.0.1"
		}
	}
	if port == "" {
		port = defaultPort
	}

	return net.JoinHostPort(host, port)
}
