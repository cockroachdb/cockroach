// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package addr

import (
	"net"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// AddrWithDefaultLocalhost returns addr with the host set
// to localhost if it is empty.
func AddrWithDefaultLocalhost(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port), nil
}

// SplitHostPort is like net.SplitHostPort however it supports
// addresses without a port number. In that case, the provided port
// number is used.
func SplitHostPort(v string, defaultPort string) (addr string, port string, err error) {
	addr, port, err = net.SplitHostPort(v)
	if err != nil {
		var aerr *net.AddrError
		if errors.As(err, &aerr) {
			if strings.HasPrefix(aerr.Err, "too many colons") {
				// Maybe this was an IPv6 address using the deprecated syntax
				// without '[...]'? Try that to help the user with a hint.
				// Note: the following is valid even if defaultPort is empty.
				// (An empty port number is always a valid listen address.)
				maybeAddr := "[" + v + "]:" + defaultPort
				addr, port, err = net.SplitHostPort(maybeAddr)
				if err == nil {
					err = errors.WithHintf(
						errors.Newf("invalid address format: %q", v),
						"enclose IPv6 addresses within [...], e.g. \"[%s]\"", v)
				}
			} else if strings.HasPrefix(aerr.Err, "missing port") {
				// It's inconvenient that SplitHostPort doesn't know how to ignore
				// a missing port number. Oh well.
				addr, port, err = net.SplitHostPort(v + ":" + defaultPort)
			}
		}
	}
	if err == nil && port == "" {
		port = defaultPort
	}
	return addr, port, err
}

type addrSetter struct {
	addr *string
	port *string
}

// NewAddrSetter creates a new pflag.Value raps a address/port
// configuration option pair and enables setting them both with a
// single command-line flag.
func NewAddrSetter(hostOption, portOption *string) pflag.Value {
	return &addrSetter{addr: hostOption, port: portOption}
}

// String implements the pflag.Value interface.
func (a addrSetter) String() string {
	return net.JoinHostPort(*a.addr, *a.port)
}

// Type implements the pflag.Value interface.
func (a addrSetter) Type() string { return "<addr/host>[:<port>]" }

// Set implements the pflag.Value interface.
func (a addrSetter) Set(v string) error {
	addr, port, err := SplitHostPort(v, *a.port)
	if err != nil {
		return err
	}
	*a.addr = addr
	*a.port = port
	return nil
}
