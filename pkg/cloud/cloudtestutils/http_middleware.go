// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloudtestutils

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// BrownoutMiddleware creates an HTTP middleware that injects 500 errors during
// a specific time window relative to when the middleware was created.
func BrownoutMiddleware(startAfter, endAfter time.Duration) cloud.HttpMiddleware {
	now := timeutil.Now()
	return func(next http.RoundTripper) http.RoundTripper {
		return &brownoutRoundTripper{
			next:      next,
			startTime: now.Add(startAfter),
			endTime:   now.Add(endAfter),
		}
	}
}

type brownoutRoundTripper struct {
	next      http.RoundTripper
	startTime time.Time
	endTime   time.Time
}

func (b *brownoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	now := timeutil.Now()
	if now.After(b.startTime) && now.Before(b.endTime) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Injected Server Error",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    req,
		}, nil
	}

	return b.next.RoundTrip(req)
}
