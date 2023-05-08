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
