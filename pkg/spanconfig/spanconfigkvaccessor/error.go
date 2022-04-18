// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// retryableLeaseExpiredError is emitted by the KVAccessor when while performing
// an update request the supplied lease expired, but is likely to succeed if
// retried again with an extended release.
type retryableLeaseExpiredError struct{}

// newRetryableLeaseExpiredError constructes a retryableLeaseExpiredError.
func newRetryableLeaseExpiredError() retryableLeaseExpiredError {
	return retryableLeaseExpiredError{}
}

// IsRetryableLeaseExpiredError detects whether an error emitted by the
// KVAccessor failed because the lease expired while performing an update but
// is likely to succeed if retried again.
func IsRetryableLeaseExpiredError(err error) bool {
	return errors.Is(err, retryableLeaseExpiredError{})
}

// Error implements the error interface.
func (e retryableLeaseExpiredError) Error() string { return "lease expired" }

func decodeRetryableLeaseExpiredError(context.Context, string, []string, proto.Message) error {
	return newRetryableLeaseExpiredError()
}

func init() {
	errors.RegisterLeafDecoder(
		errors.GetTypeKey((*retryableLeaseExpiredError)(nil)), decodeRetryableLeaseExpiredError,
	)
}
