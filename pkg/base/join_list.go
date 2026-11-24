// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"bytes"
	"fmt"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/errors"
)

// JoinListType is a slice of strings that implements pflag's value
// interface.
type JoinListType []string

// String returns a string representation of all the JoinListType. This is part
// of pflag's value interface.
func (jls JoinListType) String() string {
	var buffer bytes.Buffer
	for _, jl := range jls {
		fmt.Fprintf(&buffer, "--join=%s ", jl)
	}
	// Trim the extra space from the end if it exists.
	if l := buffer.Len(); l > 0 {
		buffer.Truncate(l - 1)
	}
	return buffer.String()
}

// Type returns the underlying type in string form. This is part of pflag's
// value interface.
func (jls *JoinListType) Type() string {
	return "string"
}

// Set adds a new value to the JoinListType. It is the important part of
// pflag's value interface.
func (jls *JoinListType) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		// No value, likely user error.
		return errors.New("no address specified in --join")
	}
	for _, v := range strings.Split(value, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			// --join=a,,b  equivalent to --join=a,b
			continue
		}
		// Try splitting the address. This validates the format
		// of the address and tolerates a missing delimiter colon
		// between the address and port number.
		addr, port, err := addr.SplitHostPort(v, "")
		if err != nil {
			return err
		}
		// Default the port if unspecified.
		if len(port) == 0 {
			port = DefaultPort
		}
		// Re-join the parts. This guarantees an address that
		// will be valid for net.SplitHostPort().
		*jls = append(*jls, net.JoinHostPort(addr, port))
	}
	return nil
}
