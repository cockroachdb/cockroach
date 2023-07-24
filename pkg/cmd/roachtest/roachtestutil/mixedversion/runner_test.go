// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/stretchr/testify/require"
)

func Test_runSingleStep(t *testing.T) {
	tr := testTestRunner()

	// steps that run without errors do not return errors
	successStep := newTestStep(func() error {
		return nil
	})
	err := tr.runSingleStep(ctx, successStep, nilLogger)
	require.NoError(t, err)

	// steps that return an error have that error surfaced
	errorStep := newTestStep(func() error {
		return fmt.Errorf("oops")
	})
	err = tr.runSingleStep(ctx, errorStep, nilLogger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "oops")

	// steps that panic cause an error to be returned
	panicStep := newTestStep(func() error {
		var ids []int
		if ids[0] > 42 {
			return nil
		}
		return fmt.Errorf("unreachable")
	})
	err = nil
	require.NotPanics(t, func() {
		err = tr.runSingleStep(ctx, panicStep, nilLogger)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic (stack trace above): runtime error: index out of range [0] with length 0")
}

func testTestRunner() *testRunner {
	runnerCtx, cancel := context.WithCancel(ctx)
	return &testRunner{
		ctx:        runnerCtx,
		cancel:     cancel,
		logger:     nilLogger,
		crdbNodes:  nodes,
		background: newBackgroundRunner(runnerCtx, nilLogger),
		seed:       seed,
	}
}

type testSingleStep struct {
	runFunc func() error
}

func (testSingleStep) ID() int                { return 42 }
func (testSingleStep) Description() string    { return "testSingleStep" }
func (testSingleStep) Background() shouldStop { return nil }

func (tss testSingleStep) Run(
	_ context.Context, _ *logger.Logger, _ cluster.Cluster, _ *Helper,
) error {
	return tss.runFunc()
}

func newTestStep(f func() error) singleStep {
	return testSingleStep{runFunc: f}
}
