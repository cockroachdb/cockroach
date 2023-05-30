// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package grpcutil

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestGRPCErrRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("prints gogo status errors", func(t *testing.T) {
		s := status.Newf(codes.Unauthenticated, "%d %s %s", 1, "two", redact.Safe("three"))
		err := errors.Wrap(s.Err(), "boom")
		require.EqualValues(t, `boom: grpc: ‹1 two three› [code 16/Unauthenticated]`, redact.Sprint(err))
	})
	t.Run("does not handle nil status.Status", func(t *testing.T) {
		e := &testingErrWithGRPCStatus{}
		err := errors.Wrap(e, "boom")
		require.EqualValues(t, `boom: ‹test error›`, redact.Sprint(err))
	})
}

type testingErrWithGRPCStatus struct{}

func (e *testingErrWithGRPCStatus) GRPCStatus() *grpcStatus.Status { return nil }
func (e *testingErrWithGRPCStatus) Error() string                  { return "test error" }
