// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contextutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWithoutCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	child := WithoutCancel(ctx)
	require.Nil(t, child.Done())

	cancel()
	require.NotNil(t, ctx.Err())
	require.Nil(t, child.Err())
}

func TestInheritsCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type myKey string

	c1, _ := context.WithCancel(context.Background())
	c2 := context.WithValue(c1, myKey("a"), nil)
	c3 := WithoutCancel(c2)
	c4, _ := context.WithCancel(c3)
	c5, _ := context.WithCancel(c4)
	c6 := WithoutCancel(c5)
	c7 := context.WithValue(c6, myKey("a"), nil)

	require.True(t, InheritsCancellation(c2, c1))
	require.False(t, InheritsCancellation(c3, c2))
	require.False(t, InheritsCancellation(c7, c2))
	require.True(t, InheritsCancellation(c5, c4))
	require.False(t, InheritsCancellation(c6, c5))
	require.False(t, InheritsCancellation(c7, c5))
}
