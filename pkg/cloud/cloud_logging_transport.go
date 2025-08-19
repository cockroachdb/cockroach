// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/redact"
)

// maybeAddLogging wraps the provided http.RoundTripper with a logging
// transport if verbose logging is enabled.
func maybeAddLogging(inner http.RoundTripper) http.RoundTripper {
	if log.V(1) {
		return &loggingTransport{inner: inner}
	}
	return inner
}

type loggingTransport struct {
	inner http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (l *loggingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	requestWatcher := &requestBodyTracker{inner: request.Body}
	if request.Body != nil {
		// NOTE: Body can be nil if the request has no body.
		request.Body = requestWatcher
	}

	now := timeutil.Now()
	resp, err := l.inner.RoundTrip(request)
	if err != nil {
		log.Warningf(request.Context(), "%s %s: %v", request.Method, request.URL.String(), err)
		return resp, err
	}

	logCtx, span := tracing.ForkSpan(request.Context(), "cloud-logging-transport")

	resp.Body = &responseBodyTracker{
		inner: resp.Body,
		ctx:   logCtx,
		span:  span,

		status:         redact.SafeString(resp.Status),
		method:         redact.SafeString(request.Method),
		url:            request.URL.String(),
		requestLatency: timeutil.Since(now),
		requestBytes:   requestWatcher.readBytes,
		responseBytes:  resp.ContentLength,
	}
	return resp, nil
}

var _ http.RoundTripper = &loggingTransport{}

// requestBodyTracker is a wrapper around io.ReadCloser that tracks the number of
// bytes read from the request body.
type requestBodyTracker struct {
	inner io.ReadCloser
	// readBytes is the number of bytes returned by the underly Read calls.
	readBytes int64
}

func (s *requestBodyTracker) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	s.readBytes += int64(n)
	return n, err
}

func (s *requestBodyTracker) Close() error {
	return s.inner.Close()
}

// responseBodyTracker is an io.ReadCloser that wraps the original response
// body. It counts the number of bytes read from the response body and logs
// stats about the request when the body is closed.
type responseBodyTracker struct {
	inner io.ReadCloser
	ctx   context.Context
	span  *tracing.Span

	// status is the HTTP status code of the response. (e.g. "200 OK", "404 Not Found").
	status redact.SafeString
	// method is the HTTP method of the request (e.g., GET, POST).
	method redact.SafeString
	// url is the URL of the request.
	url string
	// requestLatency is the amount of time spent waiting for the response header.
	requestLatency time.Duration
	// requestBytes is the number of bytes sent in the request body.
	requestBytes int64
	// number of bytes in the response body. We often do not read the entire
	// body.
	responseBytes int64

	// readErr is the error returned by the last Read() call on the response body. This is
	// expected to be io.EOF when the body is fully read, but may also be `nil` if we are closing
	// an incomplete response body or some network error that occurred after recieving the response
	// header but before reading the whole body.
	readErr error
	// readBytes is the number of bytes read from the response body. This is tracked
	// because we often close a request before reading the entire body.
	readBytes int64
	// readTime is the amount of time spent waiting in Read() calls.
	readTime time.Duration

	closeOnce sync.Once // ensures Close() is only called once
}

func (l *responseBodyTracker) Read(p []byte) (int, error) {
	start := timeutil.Now()

	n, err := l.inner.Read(p)

	l.readBytes += int64(n)
	l.readTime += timeutil.Since(start)
	l.readErr = err
	return n, err
}

func (l *responseBodyTracker) Close() error {
	l.closeOnce.Do(func() {
		if l.readErr == io.EOF || (l.readErr == nil && l.readBytes == l.responseBytes) {
			log.Infof(l.ctx, "%s %s (%s) sent=%d,recv=%d,request_latency=%s,read_latency=%s",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.requestLatency, l.readTime)
		} else if l.readErr != nil {
			log.Infof(l.ctx, "%s %s (%s) sent=%d,recv=(%d/%d),request_latency=%s,read_latency=%s: %v",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.responseBytes, l.requestLatency, l.readTime, l.readErr)
		} else {
			log.Infof(l.ctx, "%s %s (%s) sent=%d,recv=(%d/%d),request_latency=%s,read_latency=%s: closed early",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.responseBytes, l.requestLatency, l.readTime)
		}
		l.span.Finish()
	})
	return l.inner.Close()
}
