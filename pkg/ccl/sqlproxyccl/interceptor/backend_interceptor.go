// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/jackc/pgproto3/v2"
)

// BackendInterceptor is a server int/erceptor for the Postgres backend protocol.
type BackendInterceptor pgInterceptor

// NewBackendInterceptor creates a BackendInterceptor. bufSize must be at least
// the size of a pgwire message header.
func NewBackendInterceptor(src io.Reader, dst io.Writer, bufSize int) (*BackendInterceptor, error) {
	pgi, err := newPgInterceptor(src, dst, bufSize)
	if err != nil {
		return nil, err
	}
	return (*BackendInterceptor)(pgi), nil
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor.
//
// See pgInterceptor.PeekMsg for more information.
func (bi *BackendInterceptor) PeekMsg() (typ pgwirebase.ClientMessageType, size int, err error) {
	byteType, size, err := (*pgInterceptor)(bi).PeekMsg()
	return pgwirebase.ClientMessageType(byteType), size, err
}

// WriteMsg writes the given bytes to the writer dst.
//
// See pgInterceptor.WriteMsg for more information.
func (bi *BackendInterceptor) WriteMsg(data pgproto3.FrontendMessage) (n int, err error) {
	return (*pgInterceptor)(bi).WriteMsg(data.Encode(nil))
}

// ReadMsg decodes the current pgwire message and returns a FrontendMessage.
// This also advances the interceptor to the next message.
//
// See pgInterceptor.ReadMsg for more information.
func (bi *BackendInterceptor) ReadMsg() (msg pgproto3.FrontendMessage, err error) {
	msgBytes, err := (*pgInterceptor)(bi).ReadMsg()
	if err != nil {
		return nil, err
	}
	// errPanicWriter is used here because Receive must not Write.
	return pgproto3.NewBackend(newChunkReader(msgBytes), &errPanicWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message.
//
// See pgInterceptor.ForwardMsg for more information.
func (bi *BackendInterceptor) ForwardMsg() (n int, err error) {
	return (*pgInterceptor)(bi).ForwardMsg()
}

// Close closes the interceptor, and prevents further operations on it.
func (bi *BackendInterceptor) Close() {
	(*pgInterceptor)(bi).Close()
}
