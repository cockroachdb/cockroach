// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

const terminalErrorString = "terminal changefeed error"

type terminalError struct {
	wrapped error
}

// MarkTerminalError wraps the given error, marking it as non-retryable to
// changefeeds.
func MarkTerminalError(e error) error {
	return &terminalError{wrapped: e}
}

// Error implements the error interface.
func (e *terminalError) Error() string {
	return fmt.Sprintf("%s: %s", terminalErrorString, e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *terminalError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *terminalError) Unwrap() error { return e.wrapped }

// IsTerminalError returns true if the supplied error, or any of its parent
// causes, is a IsTerminalError.
func IsTerminalError(err error) bool {
	for {
		if err == nil {
			return false
		}
		if _, ok := err.(*terminalError); ok {
			return true
		}
		if _, ok := err.(*pgerror.Error); ok {
			return strings.Contains(err.Error(), terminalErrorString)
		}
		if e, ok := err.(interface{ Unwrap() error }); ok {
			err = e.Unwrap()
			continue
		}
		return false
	}
}

// MaybeStripTerminalErrorMarker performs some minimal attempt to clean the
// TerminalError marker out. This won't do anything if the TerminalError itself
// has been wrapped, but that's okay, we'll just have an uglier string.
func MaybeStripTerminalErrorMarker(err error) error {
	if e, ok := err.(*terminalError); ok {
		err = e.wrapped
	}
	return err
}
