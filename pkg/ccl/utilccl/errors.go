package utilccl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

const retryableJobsFlowError = "retryable jobs error"

type retryableError struct {
	wrapped error
}

// MarkRetryableError wraps the given error, marking it as retryable to
// jobs.
func MarkRetryableError(e error) error {
	return &retryableError{wrapped: e}
}

// Error implements the error interface.
func (e *retryableError) Error() string {
	return fmt.Sprintf("%s: %s", retryableJobsFlowError, e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *retryableError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *retryableError) Unwrap() error { return e.wrapped }

// IsDistSQLRetryableError returns true if the supplied error, or any of its parent
// causes, is a IsDistSQLRetryableError.
func isDistSQLRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// TODO(knz): this is a bad implementation. Make it go away
	// by avoiding string comparisons.

	errStr := err.Error()
	if strings.Contains(errStr, `rpc error`) {
		// When a crdb node dies, any DistSQL flows with processors scheduled on
		// it get an error with "rpc error" in the message from the call to
		// `(*DistSQLPlanner).Run`.
		return true
	}
	return false
}

// RetryDistSQLFlowCustomRetryable retries the given func in the context of a
// long running DistSQL flow which is used by all jobs. If a node were to fail,
// either the work func should be retried, or the error returned will be a job
// retry error that will retry the entire job in the case of the coordinator
// node being drained.
//
// This is maintained to support old-version nodes running CDC that may return
// with CDC specific retryable errors.
// TODO(pbardea): In 20.2, remove the isRetryable argument.
func RetryDistSQLFlowCustomRetryable(
	ctx context.Context,
	isRetryable func(error) bool,
	retryable func(ctx context.Context) error,
	logRetryableError func(error),
) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
	}

	var err error

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		err = retryable(ctx)
		if err == nil {
			return nil
		}

		isCustomRetryable := false
		if isRetryable != nil && isRetryable(err) {
			isCustomRetryable = true
		}
		if retryable := isDistSQLRetryableError(err) || isCustomRetryable; !retryable {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if flowinfra.IsFlowRetryableError(err) {
				// We don't want to retry flowinfra retryable error in the retry loop
				// above. This error currently indicates that this node is being
				// drained.  As such, retries will not help.
				// Instead, we want to make sure that the job is not marked failed due
				// to a transient, retryable error.
				err = jobs.NewRetryJobError(fmt.Sprintf("retryable flow error: %+v", err))
			}

			log.Warningf(ctx, `returning with error: %+v`, err)
			return err
		}

		if logRetryableError != nil {
			logRetryableError(err)
		}
	}

	// We only hit this if `r.Next()` returns false, which right now only happens
	// on context cancellation.
	return errors.Wrap(err, `ran out of retries`)
}
