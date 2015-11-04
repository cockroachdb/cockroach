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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package util

import (
	"net"
	"os"
)

const (
	defaultPort = "26257"
)

// EnsureHostPort takes a host:port pair, where the host and port are optional.
// If host and port are present, the output is equal to the input. If port is
// not present, use default port 26257. If host is not present, host will be
// equal to the hostname (or "127.0.0.1" as a fallback).
func EnsureHostPort(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if host != "" || err != nil {
		if port == "" {
			port = defaultPort
			return net.JoinHostPort(addr, port)
		}

		return addr
	}

	host, err = os.Hostname()
	if err != nil {
		host = "127.0.0.1"
	}

	if port == "" {
		port = defaultPort
	}

	return net.JoinHostPort(host, port)
}
