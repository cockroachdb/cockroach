// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// QueryNotRunningInHomeRegionMessagePrefix is the common message prefix for
// erroring out queries with no home region when the enforce_home_region session
// flag is set.
const QueryNotRunningInHomeRegionMessagePrefix = "Query is not running in its home region"

// dynamicQueryHasNoHomeRegionError is a wrapper for a
// "query has no home region" error to flag the query as retryable.
type dynamicQueryHasNoHomeRegionError struct {
	err error
}

// NewDynamicQueryHasNoHomeRegionError returns a dynamic no home region error
// wrapping the original error. This error triggers a transaction retry.
func NewDynamicQueryHasNoHomeRegionError(err error) error {
	return &dynamicQueryHasNoHomeRegionError{err: err}
}

var _ pgerror.ClientVisibleRetryError = (*dynamicQueryHasNoHomeRegionError)(nil)

// dynamicQueryHasNoHomeRegionError implements the error interface.
func (e *dynamicQueryHasNoHomeRegionError) Error() string {
	return e.err.Error()
}

// Cause returns the wrapped error inside this error.
func (e *dynamicQueryHasNoHomeRegionError) Cause() error {
	return e.err
}

// ClientVisibleRetryError is detected by the pgwire layer and will convert
// this error into an error to be retried. See pgcode.ClientVisibleRetryError.
func (e *dynamicQueryHasNoHomeRegionError) ClientVisibleRetryError() {}

// IsDynamicQueryHasNoHomeRegionError tests if `err` is a
// dynamicQueryHasNoHomeRegionError.
func IsDynamicQueryHasNoHomeRegionError(err error) bool {
	if err == nil {
		return false
	}
	for {
		// If the error is not a `dynamicQueryHasNoHomeRegionError`, unwrap the
		// root cause.
		if !errors.HasType(err, (*dynamicQueryHasNoHomeRegionError)(nil)) {
			err = errors.UnwrapOnce(err)
			if err == nil {
				break
			}
			continue
		}
		break
	}
	nhrErr := (*dynamicQueryHasNoHomeRegionError)(nil)
	return errors.As(err, &nhrErr)
}

// MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError tests if `err` is a
// dynamicQueryHasNoHomeRegionError. If it is, it returns the error embedded
// in this structure (which should be a non-retryable error), otherwise returns
// the original error.
func MaybeGetNonRetryableDynamicQueryHasNoHomeRegionError(err error) error {
	if err == nil {
		return nil
	}
	for {
		// If the error is not a `dynamicQueryHasNoHomeRegionError`, unwrap the
		// root cause.
		if !errors.HasType(err, (*dynamicQueryHasNoHomeRegionError)(nil)) {
			err = errors.UnwrapOnce(err)
			if err == nil {
				break
			}
			continue
		}
		break
	}
	if errors.HasType(err, (*dynamicQueryHasNoHomeRegionError)(nil)) {
		return errors.UnwrapOnce(err)
	}
	return err
}
