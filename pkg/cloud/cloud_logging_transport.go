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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/crlib/crtime"
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

	// NOTE: Body can be nil if the request has no body.
	requestWatcher := &requestBodyTracker{inner: request.Body}
	if request.Body != nil {
		request = request.Clone(request.Context())
		request.Body = requestWatcher
	}

	now := crtime.NowMono()
	resp, err := l.inner.RoundTrip(request)
	if err != nil {
		log.Dev.Warningf(request.Context(), "%s %s: %v", redact.SafeString(request.Method), request.URL.String(), err)
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
		requestLatency: now.Elapsed(),
		requestBytes:   requestWatcher.readBytes.Load(),
		responseBytes:  resp.ContentLength,
	}
	return resp, nil
}

var _ http.RoundTripper = &loggingTransport{}

// requestBodyTracker is a wrapper around io.ReadCloser that tracks the number of
// bytes read from the request body.
type requestBodyTracker struct {
	inner io.ReadCloser
	// readBytes is the number of bytes returned by the underlying Read calls.
	// NOTE(jeffswenson): in practice, I don't think this actually needs to be an
	// atomic value. By the time we observe the value, the request body should be
	// fully consumed. But syscalls in golang are not defined to be memory
	// barriers. So from the perspective of the go race detector there is no
	// synchronization between the goroutine reading the request body and the
	// goroutine sending the request.
	readBytes atomic.Int64
}

func (s *requestBodyTracker) Read(p []byte) (int, error) {
	n, err := s.inner.Read(p)
	s.readBytes.Add(int64(n))
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
	// an incomplete response body or some network error that occurred after receiving the response
	// header but before reading the whole body.
	readErr error
	// readBytes is the number of bytes read from the response body. This is tracked
	// because we often close a request before reading the entire body.
	readBytes int64
	// readTime is the amount of time spent waiting in Read() calls.
	readTime time.Duration

	// closeOnce ensures Close() logic is only called once. Some callers of
	// Close() are sloppy and call it multiple times. The double log lines are
	// annoying, but the real issue is the *tracing.Span. The tracing
	// infrastructure reuses memory internally, so it is essential that
	// span.Finish is called exactly once.
	closeOnce sync.Once
}

func (l *responseBodyTracker) Read(p []byte) (int, error) {
	start := crtime.NowMono()

	n, err := l.inner.Read(p)

	l.readBytes += int64(n)
	l.readTime += start.Elapsed()
	l.readErr = err
	return n, err
}

func (l *responseBodyTracker) Close() error {
	l.closeOnce.Do(func() {
		if l.readErr == io.EOF || (l.readErr == nil && l.readBytes == l.responseBytes) {
			log.Dev.Infof(l.ctx, "%s %s (%s) sent=%d,recv=%d,request_latency=%s,read_latency=%s",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.requestLatency, l.readTime)
		} else if l.readErr != nil {
			log.Dev.Warningf(l.ctx, "%s %s (%s) sent=%d,recv=(%d/%d),request_latency=%s,read_latency=%s: %v",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.responseBytes, l.requestLatency, l.readTime, l.readErr)
		} else {
			log.Dev.Infof(l.ctx, "%s %s (%s) sent=%d,recv=(%d/%d),request_latency=%s,read_latency=%s: closed early",
				l.method, l.url, l.status,
				l.requestBytes, l.readBytes, l.responseBytes, l.requestLatency, l.readTime)
		}
		l.span.Finish()
	})
	return l.inner.Close()
}
