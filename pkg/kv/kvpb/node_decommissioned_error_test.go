// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
