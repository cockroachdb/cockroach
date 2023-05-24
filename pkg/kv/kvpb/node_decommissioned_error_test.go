// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestNewDecommissionedStatusErrorf(t *testing.T) {
	ctx := context.Background()
	err := errors.Wrap(NewDecommissionedStatusErrorf(codes.Unauthenticated, "hello %s", "world"), "!")
	require.True(t, IsDecommissionedStatusErr(err))
	ee := errors.EncodeError(ctx, err)
	require.True(t, IsDecommissionedStatusErr(errors.DecodeError(ctx, ee)))
}
