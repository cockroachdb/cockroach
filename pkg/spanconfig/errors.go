// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfig

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type mismatchedDescriptorTypesError struct {
	d1, d2 catalog.DescriptorType
}

// NewMismatchedDescriptorTypesError constructs a mismatchedDescriptorTypesError.
func NewMismatchedDescriptorTypesError(d1, d2 catalog.DescriptorType) error {
	return mismatchedDescriptorTypesError{d1, d2}
}

// IsMismatchedDescriptorTypesError returns whether the given error is the
// mismatchedDescriptorTypesError kind.
func IsMismatchedDescriptorTypesError(err error) bool {
	return errors.HasType(err, mismatchedDescriptorTypesError{})
}

// Error implements the error interface.
func (e mismatchedDescriptorTypesError) Error() string {
	return fmt.Sprintf("mismatched descriptor types (%s, %s) for the same id", e.d1, e.d2)
}

// commitTimestampOutOfBoundsError is returned when it's not possible to commit
// an update within the specified time interval.
type commitTimestampOutOfBoundsError struct{}

// NewCommitTimestampOutOfBoundsError constructs a commitTimestampOutOfBoundsError.
func NewCommitTimestampOutOfBoundsError() error {
	return commitTimestampOutOfBoundsError{}
}

// IsCommitTimestampOutOfBoundsError returns whether the given error is the
// commitTimestampOutOfBoundsError kind.
func IsCommitTimestampOutOfBoundsError(err error) bool {
	return errors.Is(err, commitTimestampOutOfBoundsError{})
}

// Error implements the error interface.
func (e commitTimestampOutOfBoundsError) Error() string { return "lease expired" }

func decodeRetryableLeaseExpiredError(context.Context, string, []string, proto.Message) error {
	return NewCommitTimestampOutOfBoundsError()
}

func init() {
	errors.RegisterLeafDecoder(
		errors.GetTypeKey((*commitTimestampOutOfBoundsError)(nil)), decodeRetryableLeaseExpiredError,
	)
}
