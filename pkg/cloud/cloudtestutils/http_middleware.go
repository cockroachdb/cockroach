// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudtestutils

import (
	"io"
	"math/rand"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// BrownoutMiddleware creates an HTTP middleware that injects 500 errors during
// a specific time window relative to when the middleware was created.
func BrownoutMiddleware(startAfter, endAfter time.Duration) cloud.HttpMiddleware {
	now := timeutil.Now()
	return func(next http.RoundTripper) http.RoundTripper {
		return &brownoutRoundTripper{
			next:         next,
			startTime:    now.Add(startAfter),
			endTime:      now.Add(endAfter),
			maxReadBytes: 1024,
		}
	}
}

type brownoutRoundTripper struct {
	next         http.RoundTripper
	maxReadBytes int
	startTime    time.Time
	endTime      time.Time
}

func (b *brownoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	now := timeutil.Now()
	if !(now.After(b.startTime) && now.Before(b.endTime)) {
		return b.next.RoundTrip(req)
	}
	if rand.Intn(2) == 0 {
		return b.injectHttpError(req)
	}
	return b.injectReadError(req)
}

func (b *brownoutRoundTripper) injectHttpError(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "500 Injected Server Error",
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

func (b *brownoutRoundTripper) injectReadError(req *http.Request) (*http.Response, error) {
	resp, err := b.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	resp.Body = &brownOutBody{
		body:             resp.Body,
		allowedReadBytes: rand.Intn(b.maxReadBytes),
	}
	return resp, nil
}

type brownOutBody struct {
	body             io.ReadCloser
	allowedReadBytes int
}

func (b *brownOutBody) Close() error {
	return b.body.Close()
}

func (b *brownOutBody) Read(p []byte) (int, error) {
	readSize, err := b.body.Read(p)
	if readSize < b.allowedReadBytes {
		b.allowedReadBytes -= readSize
		return readSize, err
	}
	return 0, errors.Wrap(&net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: syscall.ECONNRESET,
	}, "injected fault")
}
