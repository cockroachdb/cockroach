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
// Author: Marc Berhault (marc@cockroachlabs.com)

package util

import (
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/util/log"
)

// Resolver is an interface which provides an abstract factory for
// net.Addr addresses.
type Resolver interface {
	Type() string
	Addr() string
	GetAddress() (net.Addr, error)
	IsExhausted() bool
}

// socketResolver represents the different types of socket-based
// address resolvers. Based on the Type, it may return the same
// address multiple times (eg: "lb") or not (eg: "tcp").
type socketResolver struct {
	typ       string
	addr      string
	exhausted bool // Can we try this resolver again?
}

// Type returns the resolver type.
func (sr *socketResolver) Type() string { return sr.typ }

// Addr returns the resolver address.
func (sr *socketResolver) Addr() string { return sr.addr }

// GetAddress returns a net.Addr or error.
func (sr *socketResolver) GetAddress() (net.Addr, error) {
	switch sr.typ {
	case "unix":
		addr, err := net.ResolveUnixAddr("unix", sr.addr)
		if err != nil {
			return nil, err
		}
		sr.exhausted = true
		return addr, nil
	case "tcp", "lb":
		_, err := net.ResolveTCPAddr("tcp", sr.addr)
		if err != nil {
			return nil, err
		}
		if sr.typ == "tcp" {
			// "tcp" resolvers point to a single host. "lb" have an unknown of number of backends.
			sr.exhausted = true
		}
		return MakeUnresolvedAddr("tcp", sr.addr), nil
	}
	return nil, Errorf("unknown address type: %q", sr.typ)
}

// IsExhausted returns whether the resolver can yield further
// addresses.
func (sr *socketResolver) IsExhausted() bool { return sr.exhausted }

var validTypes = map[string]struct{}{
	"tcp":  struct{}{},
	"lb":   struct{}{},
	"unix": struct{}{},
}

// NewResolver takes a resolver specification and returns a new resolver.
// A specification is of the form: [<network type>=]<address>
// Network type can be one of:
// - tcp: plain hostname of ip address
// - lb: load balancer host name or ip: points to an unknown number of backends
// - unix: unix sockets
// If "network type" is not specified, "tcp" is assumed.
func NewResolver(spec string) (Resolver, error) {
	parts := strings.Split(spec, "=")
	var typ, addr string
	if len(parts) == 1 {
		// No type specified: assume "tcp".
		typ = "tcp"
		addr = strings.TrimSpace(parts[0])
	} else if len(parts) == 2 {
		typ = strings.TrimSpace(parts[0])
		addr = strings.TrimSpace(parts[1])
	} else {
		return nil, Errorf("unable to parse gossip resolver spec: %q", spec)
	}

	// We should not have an empty address at this point.
	if len(addr) == 0 {
		return nil, Errorf("invalid address value in gossip resolver spec: %q", spec)
	}

	// Validate the type.
	if _, ok := validTypes[typ]; !ok {
		return nil, Errorf("unknown address type in gossip resolver spec: %q, "+
			"valid types are %s", spec, validTypes)
	}

	// If we're on tcp or lb make sure we fill in the host when not specified (eg: ":8080")
	if typ == "tcp" || typ == "lb" {
		addr = EnsureHost(addr)
	}

	return &socketResolver{typ: typ, addr: addr}, nil
}

// NewResolverFromAddress takes a net.Addr and contructs a resolver.
func NewResolverFromAddress(addr net.Addr) Resolver {
	switch addr.Network() {
	case "tcp", "unix":
		return &socketResolver{typ: addr.Network(), addr: addr.String()}
	default:
		log.Fatalf("unknown address network %q for %v", addr.Network(), addr)
		return nil
	}
}
