// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// errorCode classifies errors emitted by Proxy().
//go:generate stringer -type=errorCode
type errorCode int

const (
	_ errorCode = iota

	// codeAuthFailed indicates that client authentication attempt has failed and
	// backend has closed the connection.
	codeAuthFailed

	// codeBackendReadFailed indicates an error reading from backend connection.
	codeBackendReadFailed
	// codeBackendWriteFailed indicates an error writing to backend connection.
	codeBackendWriteFailed

	// codeClientReadFailed indicates an error reading from the client connection
	codeClientReadFailed
	// codeClientWriteFailed indicates an error writing to the client connection.
	codeClientWriteFailed

	// codeUnexpectedInsecureStartupMessage indicates that the client sent a
	// StartupMessage which was unexpected. Typically this means that an
	// SSLRequest was expected but the client attempted to go ahead without TLS,
	// or vice versa.
	codeUnexpectedInsecureStartupMessage

	// codeSNIRoutingFailed indicates an error choosing a backend address based on
	// the client's SNI header.
	codeSNIRoutingFailed

	// codeUnexpectedStartupMessage indicates an unexpected startup message
	// received from the client after TLS negotiation.
	codeUnexpectedStartupMessage

	// codeParamsRoutingFailed indicates an error choosing a backend address based
	// on the client's session parameters.
	codeParamsRoutingFailed

	// codeBackendDown indicates an error establishing or maintaining a connection
	// to the backend SQL server.
	codeBackendDown

	// codeBackendRefusedTLS indicates that the backend SQL server refused a TLS-
	// enabled SQL connection.
	codeBackendRefusedTLS

	// codeBackendDisconnected indicates that the backend disconnected (with a
	// connection error) while serving client traffic.
	codeBackendDisconnected

	// codeClientDisconnected indicates that the client disconnected unexpectedly
	// (with a connection error) while in a session with backend SQL server.
	codeClientDisconnected

	// codeProxyRefusedConnection indicates that the proxy refused the connection
	// request due to high load or too many connection attempts.
	codeProxyRefusedConnection

	// codeExpiredClientConnection indicates that proxy connection to the client
	// has expired and should be closed.
	codeExpiredClientConnection

	// codeIdleDisconnect indicates that the connection was disconnected for
	// being idle for longer than the specified timeout.
	codeIdleDisconnect
)

// codeError is combines an error with one of the above codes to ease
// the processing of the errors.
type codeError struct {
	code errorCode
	err  error
}

func (e *codeError) Error() string {
	return fmt.Sprintf("%s: %s", e.code, e.err)
}

// newErrorf returns a new codeError out of the supplied args.
func newErrorf(code errorCode, format string, args ...interface{}) error {
	return &codeError{
		code: code,
		err:  errors.Errorf(format, args...),
	}
}
