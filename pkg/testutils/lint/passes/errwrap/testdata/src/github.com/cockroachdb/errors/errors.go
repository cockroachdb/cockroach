// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errors

import "fmt"

func New(msg string) error {
	return fmt.Errorf(msg)
}

func Newf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func Wrap(_ error, msg string) error {
	return fmt.Errorf(msg)
}

func Wrapf(_ error, format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func WrapWithDepth(depth int, err error, msg string) error {
	return fmt.Errorf(msg)
}

func WrapWithDepthf(depth int, err error, format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func NewWithDepth(_ int, msg string) error {
	return fmt.Errorf(msg)
}

func NewWithDepthf(_ int, format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func AssertionFailedf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func AssertionFailedWithDepthf(_ int, format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func NewAssertionErrorWithWrappedErrf(_ error, format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
