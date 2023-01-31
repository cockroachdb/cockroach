// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestFastFromIncomingContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	md := metadata.MD{"hello": []string{"world", "universe"}}

	ctx := metadata.NewIncomingContext(context.Background(), md)
	md2, ok := FastFromIncomingContext(ctx)
	require.True(t, ok)
	require.Equal(t, md2, md)

	v, ok := FastFirstValueFromIncomingContext(ctx, "hello")
	require.True(t, ok)
	require.Equal(t, v, "world")
}
