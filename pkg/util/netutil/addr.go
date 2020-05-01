// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package netutil

import (
	"net"
	"strings"

	"github.com/cockroachdb/errors"
)

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
	return addr, port, err
}
