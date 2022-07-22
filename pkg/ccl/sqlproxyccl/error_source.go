// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

// errClientWrite indicates the error occured when attempting to write to the
// client connection.
var errClientWrite = errors.New("client write error")

// errClientRead indicates the error occured when attempting to read from the
// client connection.
var errClientRead = errors.New("client read error")

// errServerWrite indicates the error occured when attempting to write to the
// sql server.
var errServerWrite = errors.New("server write error")

// errServerRead indicates the error occured when attempting to read from the
// sql server.
var errServerRead = errors.New("server read error")

// wrapConnectionError wraps the error with newErrorf and the appropriate code
// if it is a known connection error. nil is returned if the error is not
// recognized.
func wrapConnectionError(err error) error {
	switch {
	case errors.Is(err, errClientRead):
		return newErrorf(codeClientReadFailed, "unable to read from client: %s", err)
	case errors.Is(err, errClientWrite):
		return newErrorf(codeClientWriteFailed, "unable to write to client: %s", err)
	case errors.Is(err, errServerRead):
		return newErrorf(codeBackendReadFailed, "unable to read from sql server: %s", err)
	case errors.Is(err, errServerWrite):
		return newErrorf(codeBackendWriteFailed, "unable to write to sql server: %s", err)
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
