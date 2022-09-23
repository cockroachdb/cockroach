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
)

func TestGRPCErrRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := status.Newf(codes.Unauthenticated, "%d %s %s", 1, "two", redact.Safe("three"))

	err := errors.Wrap(s.Err(), "boom")
	require.EqualValues(t, `boom: grpc: ‹1 two three› [code 16/Unauthenticated]`, redact.Sprint(err))
}
