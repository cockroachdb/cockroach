// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
