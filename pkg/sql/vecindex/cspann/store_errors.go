// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import "github.com/cockroachdb/errors"

// ErrPartitionNotFound is returned by the store when the requested partition
// cannot be found because it has been deleted by another agent.
var ErrPartitionNotFound = errors.New("partition not found")

// ErrRestartOperation is returned by the store when it needs the last index
// operation (e.g. search or insert) to be restarted. This should only be
// returned in situations where restarting the operation is guaranteed to make
// progress, in order to avoid an infinite loop. An example is a stale root
// partition, where a restarted operation can make progress after the cache has
// been refreshed.
var ErrRestartOperation = errors.New("conflict detected, restart operation")

// ConditionFailedError is returned by an operation that fails because a
// partition's metadata does not match some expected value. This is used to
// detect races between multiple agents operating on a partition.
type ConditionFailedError struct {
	Actual PartitionMetadata
}

// NewConditionFailedError constructs a new instance of ConditionFailedError.
func NewConditionFailedError(actual PartitionMetadata) *ConditionFailedError {
	return &ConditionFailedError{Actual: actual}
}

// Error implements the error interface.
func (e *ConditionFailedError) Error() string {
	return "metadata for partition did not match expected value"
}
