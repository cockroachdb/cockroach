// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cancelchecker

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCancelChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var checker CancelChecker

	checker.Reset(ctx1)
	require.NoError(t, checker.Check())

	checker.Reset(ctx2)
	require.NoError(t, checker.Check())

	cancel1()
	require.NoError(t, checker.Check()) // Using ctx2.

	cancel2()
	require.True(t, errors.Is(checker.Check(), QueryCanceledError))
}

// Old implementation:
// BenchmarkCancelChecker-10    	133185742	   8.790 ns/op
// New Implementation:
// BenchmarkCancelChecker-10    	1000000000	 0.5804 ns/op
func BenchmarkCancelChecker(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var checker CancelChecker
	checker.Reset(ctx)
	for i := 0; i < b.N; i++ {
		_ = checker.Check()
	}
}
