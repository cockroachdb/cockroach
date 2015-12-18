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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package codec

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// Connected status response message for HTTP CONNECT to rpcPath.
const Connected = "200 Connected to Go RPC"

// tlsDial wraps either net.Dial or crypto/tls.Dial, depending on the contents of
// the passed TLS Config.
func tlsDial(network, address string, timeout time.Duration, config *tls.Config) (net.Conn, error) {
	defaultDialer := net.Dialer{Timeout: timeout}
	if config == nil {
		return defaultDialer.Dial(network, address)
	}
	return tls.DialWithDialer(&defaultDialer, network, address, config)
}

// TLSDialHTTP connects to an HTTP RPC server at the specified address.
func TLSDialHTTP(network, address string, timeout time.Duration, config *tls.Config) (net.Conn, error) {
	conn, err := tlsDial(network, address, timeout, config)
	if err != nil {
		return conn, err
	}

	// Note: this code was adapted from net/rpc.DialHTTPPath.
	if _, err := io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n"); err != nil {
		return conn, err
	}

	// Require successful HTTP response before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil {
		if resp.Status == Connected {
			return conn, nil
		}
		err = util.Errorf("unexpected HTTP response: %s", resp.Status)
	}
	conn.Close()
	return nil, err
}
