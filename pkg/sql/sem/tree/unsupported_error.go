// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

var _ error = &UnsupportedError{}

// UnsupportedError is an error object which is returned by some unimplemented SQL
// statements. It is currently only used to skip over PGDUMP statements during
// an import.
type UnsupportedError struct {
	Err         error
	FeatureName string
}

func (u *UnsupportedError) Error() string {
	return u.Err.Error()
}

// Cause implements causer.
func (u *UnsupportedError) Cause() error { return u.Err }

// Unwrap implements wrapper.
func (u *UnsupportedError) Unwrap() error { return u.Err }
