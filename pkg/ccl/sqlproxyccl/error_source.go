// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"net"

	"github.com/cockroachdb/errors"
)

// errorSourceConn is used to annotate errors returned by the connection. The
// errors make it possible to determine which side of an io.Copy broke.
type errorSourceConn struct {
	net.Conn
	readErrMarker  error
	writeErrMarker error
}

// errClientWrite indicates the error occurred when attempting to write to the
// client connection.
var errClientWrite = errors.New("client write error")

// errClientRead indicates the error occurred when attempting to read from the
// client connection.
var errClientRead = errors.New("client read error")

// errServerWrite indicates the error occurred when attempting to write to the
// sql server.
var errServerWrite = errors.New("server write error")

// errServerRead indicates the error occurred when attempting to read from the
// sql server.
var errServerRead = errors.New("server read error")

// wrapConnectionError wraps the error with newErrorf and the appropriate code
// if it is a known connection error. nil is returned if the error is not
// recognized.
func wrapConnectionError(err error) error {
	switch {
	case errors.Is(err, errClientRead):
		return withCode(errors.Wrap(err, "unable to read from client"), codeClientReadFailed)
	case errors.Is(err, errClientWrite):
		return withCode(errors.Wrap(err, "unable to write to client"), codeClientWriteFailed)
	case errors.Is(err, errServerRead):
		return withCode(errors.Wrap(err, "unable to read from sql server"), codeBackendReadFailed)
	case errors.Is(err, errServerWrite):
		return withCode(errors.Wrap(err, "unable to write to sql server"), codeBackendWriteFailed)
	}
	return nil
}

// Read wraps net.Conn.Read and annotates the returned error.
func (conn *errorSourceConn) Read(b []byte) (n int, err error) {
	n, err = conn.Conn.Read(b)
	if err != nil {
		err = errors.Mark(err, conn.readErrMarker)
	}
	return n, err
}

// Write wraps net.Conn.Read and annotates the returned error.
func (conn *errorSourceConn) Write(b []byte) (n int, err error) {
	n, err = conn.Conn.Write(b)
	if err != nil {
		err = errors.Mark(err, conn.writeErrMarker)
	}
	return n, err
}
