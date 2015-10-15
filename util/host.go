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

// EnsureHost takes a host:port pair, where the host portion is optional.
// If a host is present, the output is equal to the input. Otherwise,
// the output will contain a host portion equal to the hostname (or
// "127.0.0.1" as a fallback).
func EnsureHost(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if host != "" || err != nil {
		return addr
	}
	host, err = os.Hostname()
	if err != nil {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// EnsureFullAddr check if addr is a host:port pair. It make sure to have both.
func EnsureFullAddr(addr string) string {
	// Ensure host first
	addr = EnsureHost(addr)

	// Check port
	_, port, _ := net.SplitHostPort(addr)
	if port != "" {
		return addr
	}

	// Fill default port 26257
	return net.JoinHostPort(addr, defaultPort)
}
