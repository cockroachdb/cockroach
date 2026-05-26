// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package addr

import (
	"fmt"
	"net"
	"strconv"
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

type portRangeSetter struct {
	lower *int
	upper *int
}

// NewPortRangeSetter creates a new pflag.Value that allows setting a
// lower and upper bound of a port range with a single setting.
func NewPortRangeSetter(lower, upper *int) pflag.Value {
	return portRangeSetter{lower: lower, upper: upper}
}

// String implements the pflag.Value interface.
func (a portRangeSetter) String() string {
	return fmt.Sprintf("%d-%d", *a.lower, *a.upper)
}

// Type implements the pflag.Value interface.
func (a portRangeSetter) Type() string { return "<lower>-<upper>" }

// Set implements the pflag.Value interface.
func (a portRangeSetter) Set(v string) error {
	parts := strings.Split(v, "-")
	if len(parts) > 2 {
		return errors.New("invalid port range: too many parts")
	}

	if len(parts) < 2 || parts[1] == "" {
		return errors.New("invalid port range: too few parts")
	}

	lower, err := strconv.Atoi(parts[0])
	if err != nil {
		return errors.Wrap(err, "invalid port range")
	}

	upper, err := strconv.Atoi(parts[1])
	if err != nil {
		return errors.Wrap(err, "invalid port range")
	}

	if lower > upper {
		return errors.Newf("invalid port range: lower bound (%d) > upper bound (%d)", lower, upper)
	}
	*a.lower = lower
	*a.upper = upper

	return nil
}
