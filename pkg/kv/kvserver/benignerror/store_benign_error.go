// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package benignerror

import "github.com/cockroachdb/errors"

// StoreBenignError may be used for declaring an error that is less serious i.e.
// benign, originated from a store, and requires a retry. These errors are
// tracked in a metric away from other benign errors.
type StoreBenignError struct {
	cause error
}

// NewStoreBenign returns a new store benign error with the given error cause.
func NewStoreBenign(cause error) *StoreBenignError {
	return &StoreBenignError{cause: cause}
}

func (be *StoreBenignError) Error() string { return be.cause.Error() }
func (be *StoreBenignError) Cause() error  { return be.cause }

func IsStoreBenign(err error) bool {
	return errors.HasType(err, (*StoreBenignError)(nil))
}
