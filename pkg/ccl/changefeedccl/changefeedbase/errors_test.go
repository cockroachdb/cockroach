// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type drainHelper bool

func (h drainHelper) IsDraining() bool {
	return bool(h)
}

var nodeIsDraining drainHelper = true
var nodeIsNotDraining drainHelper = false

func TestAsTerminalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("context error", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Regardless of the state of the node drain, or the type of error,
		// context error takes precedence.
		require.Regexp(t, context.Canceled,
			changefeedbase.AsTerminalError(canceledCtx, nodeIsDraining, errors.New("ignored")))
		require.Regexp(t, context.Canceled,
			changefeedbase.AsTerminalError(canceledCtx, nodeIsNotDraining, errors.New("ignored")))
	})

	t.Run("node drain marked as job retry", func(t *testing.T) {
		cause := errors.New("some error happened")
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsDraining, cause)
		require.Regexp(t, cause.Error(), termErr)
		require.True(t, jobs.IsRetryJobError(termErr))
	})

	t.Run("terminal errors are terminal", func(t *testing.T) {
		// Errors explicitly marked as terminal are really terminal
		cause := changefeedbase.WithTerminalError(
			changefeedbase.MarkRetryableError(errors.New("confusing error")))
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Regexp(t, cause.Error(), termErr)
	})

	t.Run("assertion failures are terminal", func(t *testing.T) {
		// Assertion failures are terminal, even if marked as retry-able.
		cause := changefeedbase.MarkRetryableError(errors.AssertionFailedf("though shall not pass"))
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Regexp(t, cause.Error(), termErr)
	})

	t.Run("gc error is terminal", func(t *testing.T) {
		cause := changefeedbase.MarkRetryableError(&kvpb.BatchTimestampBeforeGCError{})
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Regexp(t, cause.Error(), termErr)
	})
}
