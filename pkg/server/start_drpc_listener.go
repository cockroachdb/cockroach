// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"io"
	"net"

	"storj.io/drpc/drpcmigrate"
)

var drpcMatcher = func(reader io.Reader) bool {
	buf := make([]byte, len(drpcmigrate.DRPCHeader))
	if _, err := io.ReadFull(reader, buf); err != nil {
		return false
	}
	return bytes.Equal(buf, []byte(drpcmigrate.DRPCHeader))
}

type dropDRPCHeaderListener struct {
	wrapped net.Listener
}

func (ln *dropDRPCHeaderListener) Accept() (net.Conn, error) {
	conn, err := ln.wrapped.Accept()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(drpcmigrate.DRPCHeader))
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return conn, nil
}

func (ln *dropDRPCHeaderListener) Close() error {
	return ln.wrapped.Close()
}

func (ln *dropDRPCHeaderListener) Addr() net.Addr {
	return ln.wrapped.Addr()
}
