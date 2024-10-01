// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errors

import "fmt"

func New(msg string) error {
	return fmt.Errorf("abc")
}

func Newf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
