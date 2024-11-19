// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
)

// Error is an error object which, when propagated through
// the CLI main function, causes the process to exit with
// a given exit code.
type Error struct {
	exitCode exit.Code
	severity logpb.Severity
	cause    error
}

// NewError instantiates an Error.
func NewError(cause error, exitCode exit.Code) error {
	return NewErrorWithSeverity(cause, exitCode, severity.UNKNOWN)
}

// NewErrorWithSeverity instantiates an Error with an explicit severity.
func NewErrorWithSeverity(cause error, exitCode exit.Code, severity logpb.Severity) error {
	return &Error{
		exitCode: exitCode,
		severity: severity,
		cause:    cause,
	}
}

// GetExitCode extracts the exit code.
func (e *Error) GetExitCode() exit.Code { return e.exitCode }

// GetSeverity extracts the severity.
func (e *Error) GetSeverity() logpb.Severity { return e.severity }

// Error implements the error interface.
func (e *Error) Error() string { return e.cause.Error() }

// Cause implements causer for compatibility
// with pkg/errors.
// NB: this is an obsolete method, use Unwrap() instead.
func (e *Error) Cause() error { return e.cause }

// Unwrap implements errors.Wrapper.
func (e *Error) Unwrap() error { return e.cause }

// Format implements fmt.Formatter.
func (e *Error) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *Error) FormatError(p errors.Printer) error {
	if p.Detail() {
		p.Printf("error with exit code: %d", e.exitCode)
	}
	return e.cause
}
