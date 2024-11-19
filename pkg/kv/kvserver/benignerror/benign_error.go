// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package benignerror

import "github.com/cockroachdb/errors"

// BenignError may be used for declaring an error that is less serious i.e.
// benign.
type BenignError struct {
	cause error
}

// New returns a new benign error with the given error cause.
func New(cause error) *BenignError {
	return &BenignError{cause: cause}
}

func (be *BenignError) Error() string { return be.cause.Error() }
func (be *BenignError) Cause() error  { return be.cause }

func IsBenign(err error) bool {
	return errors.HasType(err, (*BenignError)(nil))
}
