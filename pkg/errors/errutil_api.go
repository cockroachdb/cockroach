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

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/errutil"

// New forwards a definition.
var New func(msg string) error = errutil.New

// NewWithDepth forwards a definition.
var NewWithDepth func(depth int, msg string) error = errutil.NewWithDepth

// Newf forwards a definition.
var Newf func(format string, args ...interface{}) error = errutil.Newf

// NewWithDepthf forwards a definition.
var NewWithDepthf func(depth int, format string, args ...interface{}) error = errutil.NewWithDepthf

// Errorf forwards a definition.
var Errorf func(format string, args ...interface{}) error = errutil.Errorf

// Cause forwards a definition.
var Cause func(error) error = errutil.Cause

// WithMessage forwards a definition.
var WithMessage func(err error, msg string) error = errutil.WithMessage

// WithMessagef forwards a definition.
var WithMessagef func(err error, format string, args ...interface{}) error = errutil.WithMessagef

// Wrap forwards a definition.
var Wrap func(err error, msg string) error = errutil.Wrap

// WrapWithDepth forwards a definition.
var WrapWithDepth func(depth int, err error, msg string) error = errutil.WrapWithDepth

// Wrapf forwards a definition.
var Wrapf func(err error, format string, args ...interface{}) error = errutil.Wrapf

// WrapWithDepthf forwards a definition.
var WrapWithDepthf func(depth int, err error, format string, args ...interface{}) error = errutil.WrapWithDepthf

// AssertionFailedf forwards a definition.
var AssertionFailedf func(format string, args ...interface{}) error = errutil.AssertionFailedf

// AssertionFailedWithDepthf forwards a definition.
var AssertionFailedWithDepthf func(depth int, format string, args ...interface{}) error = errutil.AssertionFailedWithDepthf

// NewAssertionErrorWithWrappedErrf forwards a definition.
var NewAssertionErrorWithWrappedErrf func(origErr error, format string, args ...interface{}) error = errutil.NewAssertionErrorWithWrappedErrf
