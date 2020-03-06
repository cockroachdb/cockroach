// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolver

import (
	"fmt"
	"net"
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/pkg/errors"
)

var (
	lookupSRV = net.LookupSRV
)

// Resolver is an interface which provides an abstract factory for
// net.Addr addresses. Resolvers are not thread safe.
type Resolver interface {
	Type() string
	Addr() string
	GetAddress() (net.Addr, error)
}

// NewResolver takes an address and returns a new resolver.
func NewResolver(address string) (Resolver, error) {
	if len(address) == 0 {
		return nil, errors.Errorf("invalid address value: %q", address)
	}

	// Ensure addr has port and host set.
	address = ensureHostPort(address, base.DefaultPort)
	return &socketResolver{typ: "tcp", addr: address}, nil
}

// SRV returns a slice of addresses from SRV record lookup
func SRV(name string) ([]string, error) {
	// Ignore port
	name, _, err := netutil.SplitHostPort(name, base.DefaultPort)
	if err != nil {
		return nil, err
	}

	if name == "" {
		// nolint:returnerrcheck
		return nil, nil
	}

	// "" as the addr and proto forces the direct look up of the name
	_, recs, err := lookupSRV("", "", name)
	if err != nil {
		if dnsErr, ok := err.(*net.DNSError); ok && dnsErr.Err == "no such host" {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "failed to lookup SRV record for %q", name)
	}

	var addrs = make([]string, len(recs))
	for i, r := range recs {
		addrs[i] = net.JoinHostPort(r.Target, fmt.Sprintf("%d", r.Port))
	}

	return addrs, nil
}

// NewResolverFromAddress takes a net.Addr and constructs a resolver.
func NewResolverFromAddress(addr net.Addr) (Resolver, error) {
	switch addr.Network() {
	case "tcp":
		return &socketResolver{typ: addr.Network(), addr: addr.String()}, nil
	default:
		return nil, errors.Errorf("unknown address network %q for %v", addr.Network(), addr)
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
