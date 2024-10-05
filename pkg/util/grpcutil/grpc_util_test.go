// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcutil_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestIsWaitingForInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := map[string]struct {
		err    error
		expect bool
	}{
		"waiting for init":  {server.NewWaitingForInitError("foo"), true},
		"unavailable error": {status.Errorf(codes.Unavailable, "foo"), false},
		"non-grpc":          {fmt.Errorf("node waiting for init"), false},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, grpcutil.IsWaitingForInit(tc.err))
		})
	}
}

func TestRequestDidNotStart_Errors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := map[string]struct {
		err    error
		expect bool
	}{
		"failed heartbeat":    {&netutil.InitialHeartbeatFailedError{}, true},
		"waiting for init":    {server.NewWaitingForInitError("foo"), true},
		"unauthenticated":     {status.Error(codes.Unauthenticated, "unauthenticated"), true},
		"permission denied":   {status.Error(codes.PermissionDenied, "permission denied"), true},
		"failed precondition": {status.Error(codes.FailedPrecondition, "failed precondition"), true},
		"circuit breaker":     {circuit.ErrBreakerOpen, false},
		"plain":               {errors.New("foo"), false},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// Make sure the error is properly detected both bare and wrapped.
			require.Equal(t, tc.expect, grpcutil.RequestDidNotStart(tc.err))
			require.Equal(t, tc.expect, grpcutil.RequestDidNotStart(errors.Wrap(tc.err, "wrapped")))
		})
	}
}
