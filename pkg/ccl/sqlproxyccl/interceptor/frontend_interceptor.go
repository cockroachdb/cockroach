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

// FrontendInterceptor is a client interceptor for the Postgres frontend
// protocol.
type FrontendInterceptor pgInterceptor

// NewFrontendInterceptor creates a FrontendInterceptor. bufSize must be at
// least the size of a pgwire message header.
func NewFrontendInterceptor(
	src io.Reader, dst io.Writer, bufSize int,
) (*FrontendInterceptor, error) {
	pgi, err := newPgInterceptor(src, dst, bufSize)
	if err != nil {
		return nil, err
	}
	return (*FrontendInterceptor)(pgi), nil
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor.
//
// See pgInterceptor.PeekMsg for more information.
func (fi *FrontendInterceptor) PeekMsg() (typ pgwirebase.ServerMessageType, size int, err error) {
	byteType, size, err := (*pgInterceptor)(fi).PeekMsg()
	return pgwirebase.ServerMessageType(byteType), size, err
}

// WriteMsg writes the given bytes to the writer dst.
//
// See pgInterceptor.WriteMsg for more information.
func (fi *FrontendInterceptor) WriteMsg(data pgproto3.BackendMessage) (n int, err error) {
	return (*pgInterceptor)(fi).WriteMsg(data.Encode(nil))
}

// ReadMsg decodes the current pgwire message and returns a BackendMessage.
// This also advances the interceptor to the next message.
//
// See pgInterceptor.ReadMsg for more information.
func (fi *FrontendInterceptor) ReadMsg() (msg pgproto3.BackendMessage, err error) {
	msgBytes, err := (*pgInterceptor)(fi).ReadMsg()
	if err != nil {
		return nil, err
	}
	// errPanicWriter is used here because Receive must not Write.
	return pgproto3.NewFrontend(newChunkReader(msgBytes), &errPanicWriter{}).Receive()
}

// ForwardMsg sends the current pgwire message to the destination without any
// decoding, and advances the interceptor to the next message.
//
// See pgInterceptor.ForwardMsg for more information.
func (fi *FrontendInterceptor) ForwardMsg() (n int, err error) {
	return (*pgInterceptor)(fi).ForwardMsg()
}

// Close closes the interceptor, and prevents further operations on it.
func (fi *FrontendInterceptor) Close() {
	(*pgInterceptor)(fi).Close()
}
