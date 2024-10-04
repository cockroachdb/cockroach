// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errors

import "errors"

// New constructs a new error.
func New(s string) error {
	return errors.New(s)
}

// Is is a shim for testing.
func Is(err, referece error) bool {
	return false
}
