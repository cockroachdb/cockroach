// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// WithCommandDetails is an error that describes the
// result of executing a command. In particular, it
// allows access to Stderr and Stdout.
type WithCommandDetails struct {
	Wrapped error
	Cmd     string
	Stderr  string
	Stdout  string
}

var _ error = (*WithCommandDetails)(nil)
var _ errors.Formatter = (*WithCommandDetails)(nil)

// Error implements error.
func (e *WithCommandDetails) Error() string { return e.Wrapped.Error() }

// Cause implements causer.
func (e *WithCommandDetails) Cause() error { return e.Wrapped }

// Format implements fmt.Formatter.
func (e *WithCommandDetails) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *WithCommandDetails) FormatError(p errors.Printer) error {
	p.Printf("%s returned", e.Cmd)
	if p.Detail() {
		p.Printf("stderr:\n%s\nstdout:\n%s", e.Stderr, e.Stdout)
	}
	return e.Wrapped
}

// GetStderr retrieves the stderr output of a command that
// returned with an error, or the empty string if there was no stderr.
func GetStderr(err error) string {
	var c *WithCommandDetails
	if errors.As(err, &c) {
		return c.Stderr
	}
	return ""
}
