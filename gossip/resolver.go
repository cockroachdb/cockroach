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

package gossip

import (
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Resolver represents the different types of node resolvers.
// Based on the ResolverType, it will return the same
// address multiple times (eg: "lb") or not (eg: "tcp").
type Resolver struct {
	ResolverType string
	Address      string
	// indicates whether we should try this resolver again.
	IsExhausted bool
}

// GetAddress returns a net.Addr.
func (r *Resolver) GetAddress() (net.Addr, error) {
	switch r.ResolverType {
	case "unix":
		addr, err := net.ResolveUnixAddr("unix", r.Address)
		if err != nil {
			return nil, err
		}
		r.IsExhausted = true
		return addr, nil
	case "tcp", "lb":
		_, err := net.ResolveTCPAddr("tcp", r.Address)
		if err != nil {
			return nil, err
		}
		if r.ResolverType == "tcp" {
			// "tcp" resolvers point to a single host. "lb" have an unknown of number of backends.
			r.IsExhausted = true
		}
		return util.MakeRawAddr("tcp", r.Address), nil
	}
	return nil, util.Errorf("unknown address type: %q", r.ResolverType)
}

// NewResolver takes a resolver specification and returns a new resolver.
// A specification is of the form: [<network type>=]<address>
// Network type can be one of:
// - tcp: plain hostname of ip address
// - lb: load balancer host name or ip: points to an unknown number of backends
// - unix: unix sockets
// If "network type" is not specified, "tcp" is assumed.
func NewResolver(spec string) (*Resolver, error) {
	parts := strings.Split(spec, "=")
	var resolverType, address string
	if len(parts) == 1 {
		// No type specified: assume "tcp".
		resolverType = "tcp"
		address = strings.TrimSpace(parts[0])
	} else if len(parts) == 2 {
		resolverType = strings.TrimSpace(parts[0])
		address = strings.TrimSpace(parts[1])
	} else {
		return nil, util.Errorf("unable to parse gossip resolver spec: %q", spec)
	}

	// We should not have an empty address at this point.
	if len(address) == 0 {
		return nil, util.Errorf("invalid address value in gossip resolver spec: %q", spec)
	}

	// Validate the type.
	if resolverType != "tcp" && resolverType != "lb" && resolverType != "unix" {
		return nil, util.Errorf("unknown address type in gossip resolver spec: %q, "+
			"valid types are 'tcp', 'lb', 'unix'", spec)
	}

	// If we're on tcp or lb make sure we fill in the host when not specified (eg: ":8080")
	if resolverType == "tcp" || resolverType == "lb" {
		address = util.EnsureHost(address)
	}

	return &Resolver{resolverType, address, false}, nil
}

// NewResolverFromAddress take a net.Addr and contructs a resolver.
func NewResolverFromAddress(addr net.Addr) *Resolver {
	switch addr.Network() {
	case "tcp", "unix":
		return &Resolver{addr.Network(), addr.String(), false}
	default:
		log.Fatalf("unknown address network %q for %v", addr.Network(), addr)
		return nil
	}
}
