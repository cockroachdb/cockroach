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

package resolver

import (
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
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

var validTypes = map[string]struct{}{
	"tcp":     {},
	"lb":      {},
	"unix":    {},
	"http-lb": {},
}

// NewResolver takes a resolver specification and returns a new resolver.
// A specification is of the form: [<network type>=]<address>
// Network type can be one of:
// - tcp: plain hostname of ip address
// - lb: load balancer host name or ip: points to an unknown number of backends
// - unix: unix sockets
// - http-lb: http load balancer: queries http(s)://<lb>/_status/local for node addresses
// If "network type" is not specified, "tcp" is assumed.
func NewResolver(context *base.Context, spec string) (Resolver, error) {
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
		return nil, util.Errorf("unable to parse gossip resolver spec: %q", spec)
	}

	// We should not have an empty address at this point.
	if len(addr) == 0 {
		return nil, util.Errorf("invalid address value in gossip resolver spec: %q", spec)
	}

	// Validate the type.
	if _, ok := validTypes[typ]; !ok {
		return nil, util.Errorf("unknown address type %q in gossip resolver spec: %q, "+
			"valid types are %s", typ, spec, validTypes)
	}

	// For non-unix resolvers, make sure we fill in the host when not specified (eg: ":8080")
	if typ != "unix" {
		addr = util.EnsureHost(addr)
	}

	// Create the actual resolver.
	if typ == "http-lb" {
		return &nodeLookupResolver{context: context, typ: typ, addr: addr}, nil
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
