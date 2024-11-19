// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"net/http"
)

// sessionWriter implements http.ResponseWriter. It is used
// to extract cookies written to the header when passed in to
// a ServeHTTP function.
type sessionWriter struct {
	header http.Header
	buf    bytes.Buffer
	code   int
}

var _ http.ResponseWriter = &sessionWriter{}

func (sw *sessionWriter) Header() http.Header {
	return sw.header
}

func (sw *sessionWriter) WriteHeader(statusCode int) {
	sw.code = statusCode
}

func (sw *sessionWriter) Write(data []byte) (int, error) {
	return sw.buf.Write(data)
}
