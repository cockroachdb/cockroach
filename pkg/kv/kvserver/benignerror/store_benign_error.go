// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
