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
)

// BackendDialer connect to the SQL server indicated by BackendConfig, establishing TLS connection.
func BackendDialer(backendConfig *BackendConfig) (net.Conn, *CodeError) {
	crdbConn, err := net.Dial("tcp", backendConfig.OutgoingAddress)
	if err != nil {
		return nil, NewErrorf(CodeBackendDown, "dialing backend server: %v", err)
	}

	if backendConfig.TLSConf != nil {
		// Send SSLRequest.
		if err := binary.Write(crdbConn, binary.BigEndian, pgSSLRequest); err != nil {
			return nil, NewErrorf(CodeBackendDown, "sending SSLRequest to target server: %v", err)
		}

		response := make([]byte, 1)
		if _, err = io.ReadFull(crdbConn, response); err != nil {
			return nil, NewErrorf(CodeBackendDown, "reading response to SSLRequest")
		}

		if response[0] != pgAcceptSSLRequest {
			return nil, NewErrorf(CodeBackendRefusedTLS, "target server refused TLS connection")
		}

		outCfg := backendConfig.TLSConf.Clone()
		crdbConn = tls.Client(crdbConn, outCfg)
	}
	if backendConfig.IdleTimeout != 0 {
		crdbConn = NewIdleDisconnectConnection(crdbConn, backendConfig.IdleTimeout)
	}
	return crdbConn, nil
}
