// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pprofui

import (
	"io"
	"net/http"
)

// responseBridge is a helper for fetching from the pprof profile handlers.
// Their interface wants a http.ResponseWriter, so we give it one. The writes
// are passed through to an `io.Writer` of our choosing.
type responseBridge struct {
	target     io.Writer
	statusCode int
}

var _ http.ResponseWriter = &responseBridge{}

func (r *responseBridge) Header() http.Header {
	return http.Header{}
}

func (r *responseBridge) Write(b []byte) (int, error) {
	return r.target.Write(b)
}

func (r *responseBridge) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}
