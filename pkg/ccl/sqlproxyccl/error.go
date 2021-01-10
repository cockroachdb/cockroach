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

// ErrorCode classifies errors emitted by Proxy().
//go:generate stringer -type=ErrorCode
type ErrorCode int

const (
	_ ErrorCode = iota

	// CodeAuthFailed indicates that client authentication attempt has failed and
	// backend has closed the connection.
	CodeAuthFailed

	// CodeBackendReadFailed indicates an error reading from backend connection.
	CodeBackendReadFailed
	// CodeBackendWriteFailed indicates an error writing to backend connection.
	CodeBackendWriteFailed

	// CodeClientReadFailed indicates an error reading from the client connection
	CodeClientReadFailed
	// CodeClientWriteFailed indicates an error writing to the client connection.
	CodeClientWriteFailed

	// CodeUnexpectedInsecureStartupMessage indicates that the client sent a
	// StartupMessage which was unexpected. Typically this means that an
	// SSLRequest was expected but the client attempted to go ahead without TLS,
	// or vice versa.
	CodeUnexpectedInsecureStartupMessage

	// CodeSNIRoutingFailed indicates an error choosing a backend address based on
	// the client's SNI header.
	CodeSNIRoutingFailed

	// CodeUnexpectedStartupMessage indicates an unexpected startup message
	// received from the client after TLS negotiation.
	CodeUnexpectedStartupMessage

	// CodeParamsRoutingFailed indicates an error choosing a backend address based
	// on the client's session parameters.
	CodeParamsRoutingFailed

	// CodeBackendDown indicates an error establishing or maintaining a connection
	// to the backend SQL server.
	CodeBackendDown

	// CodeBackendRefusedTLS indicates that the backend SQL server refused a TLS-
	// enabled SQL connection.
	CodeBackendRefusedTLS

	// CodeBackendDisconnected indicates that the backend disconnected (with a
	// connection error) while serving client traffic.
	CodeBackendDisconnected

	// CodeClientDisconnected indicates that the client disconnected unexpectedly
	// (with a connection error) while in a session with backend SQL server.
	CodeClientDisconnected

	// CodeProxyRefusedConnection indicates that the proxy refused the connection
	// request due to high load or too many connection attempts.
	CodeProxyRefusedConnection

	// CodeExpiredClientConnection indicates that proxy connection to the client
	// has expired and should be closed.
	CodeExpiredClientConnection

	// CodeIdleDisconnect indicates that the connection was disconnected for
	// being idle for longer than the specified timeout.
	CodeIdleDisconnect
)

// CodeError is combines an error with one of the above codes to ease
// the processing of the errors.
type CodeError struct {
	code ErrorCode
	err  error
}

func (e *CodeError) Error() string {
	return fmt.Sprintf("%s: %s", e.code, e.err)
}

// NewErrorf returns a new CodeError out of the supplied args.
func NewErrorf(code ErrorCode, format string, args ...interface{}) error {
	return &CodeError{
		code: code,
		err:  errors.Errorf(format, args...),
	}
}
