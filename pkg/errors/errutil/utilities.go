// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errutil

import (
	goErr "errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/safedetails"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
	pkgErr "github.com/pkg/errors"
)

// New creates an error with a simple error message.
// A stack trace is retained.
func New(msg string) error {
	return NewWithDepth(1, msg)
}

// NewWithDepth is like New() except the depth to capture the stack
// trace is configurable.
func NewWithDepth(depth int, msg string) error {
	err := goErr.New(msg)
	err = withstack.WithStackDepth(err, 1+depth)
	return err
}

// Newf creates an error with a formatted error message.
// A stack trace is retained.
func Newf(format string, args ...interface{}) error {
	return NewWithDepthf(1, format, args...)
}

// NewWithDepthf is like Newf() except the depth to capture the stack
// trace is configurable.
func NewWithDepthf(depth int, format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	err = safedetails.WithSafeDetails(err, format, args...)
	err = withstack.WithStackDepth(err, 1+depth)
	return err
}

// Errorf aliases Newf and is provided for compatibility with other packages.
var Errorf func(format string, args ...interface{}) error = Newf

// WithMessage wraps an error with a simple error message prefix.
var WithMessage func(err error, msg string) error = pkgErr.WithMessage

// WithMessagef wraps an error with a simple error message prefix.
func WithMessagef(err error, format string, args ...interface{}) error {
	err = pkgErr.WithMessagef(err, format, args...)
	err = safedetails.WithSafeDetails(err, format, args...)
	return err
}

// Wrap wraps an error with a message prefix.
// A stack trace is retained.
func Wrap(err error, msg string) error {
	return WrapWithDepth(1, err, msg)
}

// WrapWithDepth is like Wrap except the depth to capture the stack
// trace is configurable.
func WrapWithDepth(depth int, err error, msg string) error {
	if msg != "" {
		err = pkgErr.WithMessage(err, msg)
	}
	err = withstack.WithStackDepth(err, depth+1)
	return err
}

// Wrapf wraps an error with a formatted message prefix. A stack
// trace is also retained. If the format is empty, no prefix is added,
// but the extra arguments are still processed for reportable strings.
func Wrapf(err error, format string, args ...interface{}) error {
	return WrapWithDepthf(1, err, format, args...)
}

// WrapWithDepthf is like Wrapf except the depth to capture the stack
// trace is configurable.
func WrapWithDepthf(depth int, err error, format string, args ...interface{}) error {
	if format != "" {
		err = pkgErr.WithMessagef(err, format, args...)
	}
	err = safedetails.WithSafeDetails(err, format, args...)
	err = withstack.WithStackDepth(err, depth+1)
	return err
}
