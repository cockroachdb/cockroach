// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// QueryNotRunningInHomeRegionMessagePrefix is the common message prefix for
// erroring out queries with no home region when the enforce_home_region session
// flag is set.
const QueryNotRunningInHomeRegionMessagePrefix = "Query is not running in its home region"

var AllowEnforceHomeRegionFollowerReads = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.multiregion.enforce_home_region_follower_reads.enabled",
	"allows the use of follower reads to dynamically detect a query's home region",
	false,
).WithPublic()

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
	// If the error is wrapped in a notInternalError, skip to the root cause.
	for {
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
	// If the error is wrapped in a notInternalError, skip to the root cause.
	for {
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
