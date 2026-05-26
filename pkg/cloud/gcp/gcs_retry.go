// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"io"
	"net"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultShouldRetry is google-cloud's default predicate for determining
// whether an error can be retried.
//
// TODO(rui): Currently this code is copied as-is from the google-cloud-go SDK
// in order to get the default retry behavior on top of our own customizations.
// There's currently a PR in google-cloud-go that exposes the default retry
// function, so this can be removed when it is merged:
// https://github.com/googleapis/google-cloud-go/pull/6370
func defaultShouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	switch e := err.(type) {
	case *net.OpError:
		if strings.Contains(e.Error(), "use of closed network connection") {
			// TODO: check against net.ErrClosed (go 1.16+) instead of string
			return true
		}
	case *googleapi.Error:
		// Retry on 408, 429, and 5xx, according to
		// https://cloud.google.com/storage/docs/exponential-backoff.
		return e.Code == 408 || e.Code == 429 || (e.Code >= 500 && e.Code < 600)
	case *url.Error:
		// Retry socket-level errors ECONNREFUSED and ECONNRESET (from syscall).
		// Unfortunately the error type is unexported, so we resort to string
		// matching.
		retriable := []string{"connection refused", "connection reset"}
		for _, s := range retriable {
			if strings.Contains(e.Error(), s) {
				return true
			}
		}
	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}
	// HTTP 429, 502, 503, and 504 all map to gRPC UNAVAILABLE per
	// https://grpc.github.io/grpc/core/md_doc_http-grpc-status-mapping.html.
	//
	// This is only necessary for the experimental gRPC-based media operations.
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return true
	}
	// Unwrap is only supported in go1.13.x+
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return defaultShouldRetry(e.Unwrap())
	}
	return false
}
