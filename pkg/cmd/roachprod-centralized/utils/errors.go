// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

// CriticalError represents an unrecoverable error that requires application shutdown.
// These errors indicate fundamental system failures that cannot be handled gracefully,
// such as database connection loss or configuration corruption.
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

// PublicError represents an error that is safe to expose to API clients.
// These errors contain user-friendly messages without sensitive internal details,
// making them suitable for inclusion in HTTP responses and user interfaces.
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
