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
//
//go:generate stringer -type=errorCode
type errorCode int

const (
	codeNone errorCode = iota

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

	// codeUnavailable indicates that the backend SQL server exists but is not
	// accepting connections. For example, a tenant cluster that has maxPods set to 0.
	codeUnavailable
)

// errWithCode combines an error with one of the above codes to ease
// the processing of the errors.
// This follows the same pattern used by cockroachdb/errors that allows
// decorating errors with additional information. Check WithStack, WithHint,
// WithDetail etc.
// By using the pattern, the decorations are chained and allow searching and
// extracting later on. See getErrorCode bellow.
type errWithCode struct {
	code  errorCode
	cause error
}

var _ error = (*errWithCode)(nil)
var _ fmt.Formatter = (*errWithCode)(nil)

func (e *errWithCode) Error() string {
	if e.code == 0 {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: %v", e.code, e.cause)
}
func (e *errWithCode) Cause() error  { return e.cause }
func (e *errWithCode) Unwrap() error { return e.cause }

func (e *errWithCode) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }
func (e *errWithCode) FormatError(p errors.Printer) (next error) {
	p.Print(e.code)
	return e.cause
}

// withCode decorates an error with a proxy status specific code.
func withCode(err error, code errorCode) error {
	if err == nil {
		return nil
	}

	return &errWithCode{cause: err, code: code}
}

// getErrorCode extracts the error code from the error (if any) or returns
// codeNone
func getErrorCode(err error) errorCode {
	if ewc := (*errWithCode)(nil); errors.As(err, &ewc) {
		return ewc.code
	}
	return codeNone
}
