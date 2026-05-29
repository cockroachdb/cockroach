// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package google

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestShouldRetryGoogleAPIRequest(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "request timeout",
			err:  &googleapi.Error{Code: http.StatusRequestTimeout},
			want: true,
		},
		{
			name: "too many requests",
			err:  &googleapi.Error{Code: http.StatusTooManyRequests},
			want: true,
		},
		{
			name: "service unavailable",
			err:  &googleapi.Error{Code: http.StatusServiceUnavailable},
			want: true,
		},
		{
			name: "wrapped service unavailable",
			err:  errors.Wrap(&googleapi.Error{Code: http.StatusServiceUnavailable}, "wrapped"),
			want: true,
		},
		{
			name: "bad request",
			err:  &googleapi.Error{Code: http.StatusBadRequest},
			want: false,
		},
		{
			name: "not found",
			err:  &googleapi.Error{Code: http.StatusNotFound},
			want: false,
		},
		{
			name: "unexpected eof",
			err:  io.ErrUnexpectedEOF,
			want: true,
		},
		{
			name: "connection reset",
			err:  &url.Error{Op: "Post", URL: "https://sheets.googleapis.com", Err: fmt.Errorf("connection reset by peer")},
			want: true,
		},
		{
			name: "plain error",
			err:  errors.New("plain error"),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldRetryGoogleAPIRequest(tc.err))
		})
	}
}

func TestRetryGoogleAPIRequest(t *testing.T) {
	defer setGoogleAPIRequestRetryOptionsForTest(t, retry.Options{
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
		Multiplier:          1,
		RandomizationFactor: -1,
		MaxRetries:          2,
	})()

	var attempts int
	err := retryGoogleAPIRequest(context.Background(), "test operation", func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &googleapi.Error{Code: http.StatusServiceUnavailable}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryGoogleAPIRequestStopsOnNonRetryableError(t *testing.T) {
	defer setGoogleAPIRequestRetryOptionsForTest(t, retry.Options{
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
		Multiplier:          1,
		RandomizationFactor: -1,
		MaxRetries:          2,
	})()

	var attempts int
	err := retryGoogleAPIRequest(context.Background(), "test operation", func(context.Context) error {
		attempts++
		return &googleapi.Error{Code: http.StatusBadRequest}
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts)
}

func setGoogleAPIRequestRetryOptionsForTest(t *testing.T, opts retry.Options) func() {
	t.Helper()
	oldOpts := googleAPIRequestRetryOptions
	googleAPIRequestRetryOptions = opts
	return func() {
		googleAPIRequestRetryOptions = oldOpts
	}
}
