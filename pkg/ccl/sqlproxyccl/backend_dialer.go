// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"

	"github.com/jackc/pgproto3/v2"
)

// BackendDial is an example backend dialer that does a TCP/IP connection
// to a backend, SSL and forwards the start message. It is defined as a variable
// so it can be redirected for testing.
var BackendDial = func(
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
