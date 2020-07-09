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
)

type codeError struct {
	code ErrorCode
	err  error
}

func (e *codeError) Error() string {
	return fmt.Sprintf("%s: %s", e.code, e.err)
}

func newErrorf(code ErrorCode, format string, args ...interface{}) error {
	return &codeError{
		code: code,
		err:  errors.Errorf(format, args...),
	}
}
