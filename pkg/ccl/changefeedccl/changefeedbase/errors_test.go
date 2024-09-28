// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		require.Regexp(t, context.Canceled.Error(),
			changefeedbase.AsTerminalError(canceledCtx, nodeIsDraining, errors.New("ignored")).Error())
		require.Regexp(t, context.Canceled.Error(),
			changefeedbase.AsTerminalError(canceledCtx, nodeIsNotDraining, errors.New("ignored")).Error())
	})

	t.Run("node drain marked as job retry", func(t *testing.T) {
		cause := errors.New("some error happened")
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsDraining, cause)
		require.Contains(t, cause.Error(), termErr.Error())
		require.True(t, jobs.IsRetryJobError(termErr))
	})

	t.Run("terminal errors are terminal", func(t *testing.T) {
		// Errors explicitly marked as terminal are really terminal
		cause := changefeedbase.WithTerminalError(
			changefeedbase.MarkRetryableError(errors.New("confusing error")))
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Contains(t, cause.Error(), termErr.Error())
	})

	t.Run("assertion failures are terminal", func(t *testing.T) {
		// Assertion failures are terminal, even if marked as retry-able.
		cause := changefeedbase.MarkRetryableError(errors.AssertionFailedf("though shall not pass"))
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Contains(t, cause.Error(), termErr.Error())
	})

	t.Run("gc error is terminal", func(t *testing.T) {
		cause := changefeedbase.MarkRetryableError(&kvpb.BatchTimestampBeforeGCError{})
		termErr := changefeedbase.AsTerminalError(context.Background(), nodeIsNotDraining, cause)
		require.Contains(t, cause.Error(), termErr.Error())
	})
}
