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

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

var errInvalidRead = errors.New("invalid read in chunkReader")

var _ pgproto3.ChunkReader = &chunkReader{}

// chunkReader is a wrapper on a single Postgres message, and is meant to be
// used with the Receive method on pgproto3.{NewFrontend, NewBackend}.
type chunkReader struct {
	msg []byte
	pos int
}

func newChunkReader(msg []byte) pgproto3.ChunkReader {
	return &chunkReader{msg: msg}
}

// Next implements the pgproto3.ChunkReader interface. An io.EOF will be
// returned once the entire message has been read. If the caller tries to read
// more bytes than it could, an errInvalidRead will be returned.
func (cr *chunkReader) Next(n int) (buf []byte, err error) {
	// pgproto3's Receive methods will still invoke Next even if the body size
	// is 0. We shouldn't return an EOF in that case.
	if n == 0 {
		return []byte{}, nil
	}
	if cr.pos == len(cr.msg) {
		return nil, io.EOF
	}
	if cr.pos+n > len(cr.msg) {
		return nil, errInvalidRead
	}
	buf = cr.msg[cr.pos : cr.pos+n]
	cr.pos += n
	return buf, nil
}
