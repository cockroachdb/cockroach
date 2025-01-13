// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

// CriticalError is an error that will cause the application to stop.
type CriticalError struct {
	Err error
}

// Error implements the error interface.
func (e CriticalError) Error() string {
	return e.Err.Error()
}

// NewCriticalError creates a new CriticalError.
func NewCriticalError(err error) *CriticalError {
	return &CriticalError{Err: err}
}

// PublicError is an error that can be exposed to the user in the API.
type PublicError struct {
	Err error
}

// Error implements the error interface.
func (e PublicError) Error() string {
	return e.Err.Error()
}

// NewPublicError creates a new PublicError.
func NewPublicError(err error) *PublicError {
	return &PublicError{Err: err}
}
