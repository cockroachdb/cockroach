// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// InitialSQLConnectionError indicates that an error was encountered
// during the initial set-up of a SQL connection.
type InitialSQLConnectionError struct {
	err error
}

// Error implements the error interface.
func (i *InitialSQLConnectionError) Error() string { return i.err.Error() }

// Cause implements causer for compatibility with pkg/errors.
// NB: this is obsolete. Use Unwrap() instead.
func (i *InitialSQLConnectionError) Cause() error { return i.err }

// Unwrap implements errors.Wrapper.
func (i *InitialSQLConnectionError) Unwrap() error { return i.err }

// Format implements fmt.Formatter.
func (i *InitialSQLConnectionError) Format(s fmt.State, verb rune) { errors.FormatError(i, s, verb) }

// FormatError implements errors.Formatter.
func (i *InitialSQLConnectionError) FormatError(p errors.Printer) error {
	if p.Detail() {
		p.Print("error while establishing the SQL session")
	}
	return i.err
}
