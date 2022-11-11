// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvserverbase

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// NonDeterministicFailureError is an error type that indicates that a state machine
// transition failed due to an unexpected error. Failure to perform a state
// transition is a form of non-determinism, so it can't be permitted for any
// reason during the application phase of state machine replication. The only
// acceptable recourse is to signal that the replica has become corrupted.
//
// All errors returned by replicaDecoder and replicaStateMachine will be instances
// of this type.
type NonDeterministicFailureError struct {
	wrapped error
	expl    redact.RedactableString
}

// NewNonDeterministicFailureErrorf creates a NonDeterministicFailureError.
func NewNonDeterministicFailureErrorf(format string, args ...interface{}) error {
	err := errors.AssertionFailedWithDepthf(1, format, args...)
	return &NonDeterministicFailureError{
		wrapped: err,
		expl:    redact.Sprintf(format, args...),
	}
}

// NonDeterministicFailureErrorWrapf wraps the provided error with a NonDeterministicFailureError.
func NonDeterministicFailureErrorWrapf(err error, format string, args ...interface{}) error {
	return &NonDeterministicFailureError{
		wrapped: errors.Wrapf(err, format, args...),
		expl:    redact.Sprintf(format, args...),
	}
}

// Error implements the error interface.
func (e *NonDeterministicFailureError) Error() string {
	return fmt.Sprintf("non-deterministic failure: %s", e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *NonDeterministicFailureError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *NonDeterministicFailureError) Unwrap() error { return e.wrapped }

// GetRedactedNonDeterministicFailureExplanation loads message from the first wrapped
// NonDeterministicFailureError, if any. The returned message is *redacted*,
// i.e. contains no sensitive information.
func GetRedactedNonDeterministicFailureExplanation(err error) redact.RedactableString {
	if nd := (*NonDeterministicFailureError)(nil); errors.As(err, &nd) {
		return nd.expl.Redact()
	}
	return "???"
}
