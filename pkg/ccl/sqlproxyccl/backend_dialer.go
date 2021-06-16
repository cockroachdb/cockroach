// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// backendLookupAddr looks up the given address using usual DNS resolution
// mechanisms. It returns the first resolved address, or an error if the lookup
// failed in some way. This can be overridden in tests.
var backendLookupAddr = func(ctx context.Context, addr string) (string, error) {
	// Address must have a port. SplitHostPort returns an error if the address
	// does not have a port.
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		return "", fmt.Errorf("no host was provided for '%s'", addr)
	}
	if port == "" {
		return "", fmt.Errorf("no port was provided for '%s'", addr)
	}

	// Note that LookupAddr might return an IPv6 address if no IPv4 addresses
	// are found. We will punt on that since this function will go away soon,
	// and we don't currently use IPv6.
	ip, err := base.LookupAddr(ctx, net.DefaultResolver, host)
	if err != nil {
		// Assume that any errors are due to missing or mismatched tenant.
		return "", errors.Wrapf(err, "DNS lookup failed for '%s'", addr)
	}

	return net.JoinHostPort(ip, port), nil
}

// backendDial is an example backend dialer that does a TCP/IP connection
// to a backend, SSL and forwards the start message. It is defined as a variable
// so it can be redirected for testing.
var backendDial = func(
	msg *pgproto3.StartupMessage, outgoingAddress string, tlsConfig *tls.Config,
) (net.Conn, error) {
	conn, err := net.Dial("tcp", outgoingAddress)
	if err != nil {
		return nil, newErrorf(
			codeBackendDown, "unable to reach backend SQL server: %v", err,
		)
	}
	conn, err = sslOverlay(conn, tlsConfig)
	if err != nil {
		return nil, err
	}
	err = relayStartupMsg(conn, msg)
	if err != nil {
		return nil, newErrorf(
			codeBackendDown, "relaying StartupMessage to target server %v: %v",
			outgoingAddress, err)
	}
	return conn, nil
}

// sslOverlay attempts to upgrade the PG connection to use SSL if a tls.Config
// is specified.
func sslOverlay(conn net.Conn, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		return conn, nil
	}

	var err error
	// Send SSLRequest.
	if err := binary.Write(conn, binary.BigEndian, pgSSLRequest); err != nil {
		return nil, newErrorf(
			codeBackendDown, "sending SSLRequest to target server: %v", err,
		)
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(conn, response); err != nil {
		return nil,
			newErrorf(codeBackendDown, "reading response to SSLRequest")
	}

	if response[0] != pgAcceptSSLRequest {
		return nil, newErrorf(
			codeBackendRefusedTLS, "target server refused TLS connection",
		)
	}

	outCfg := tlsConfig.Clone()
	return tls.Client(conn, outCfg), nil
}

// relayStartupMsg forwards the start message on the backend connection.
func relayStartupMsg(conn net.Conn, msg *pgproto3.StartupMessage) (err error) {
	_, err = conn.Write(msg.Encode(nil))
	return
}
