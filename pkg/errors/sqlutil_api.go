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

import "github.com/cockroachdb/cockroach/pkg/errors/sqlutil"

// NewWithCode is like New except it also proposes a pg code.
var NewWithCode func(code string, msg string) error = sqlutil.NewWithCode

// WrapWithCode is like Wrap except it also proposes a pg code.
var WrapWithCode func(err error, code string, msg string) error = sqlutil.WrapWithCode

// NewWithCodef is like Newf except it also proposes a pg code.
var NewWithCodef func(code string, format string, args ...interface{}) error = sqlutil.NewWithCodef

// WrapWithCodef is like Wrapf except it also proposes a pg code.
var WrapWithCodef func(err error, code string, format string, args ...interface{}) error = sqlutil.WrapWithCodef

// NewWithCodeDepthf is like NewWithDepthfd except
// it also proposes a pg code.
var NewWithCodeDepthf func(depth int, code string, format string, args ...interface{}) error = sqlutil.NewWithCodeDepthf

// WrapWithCodeDepthf is like Wrapf except it also proposes a pg code.
var WrapWithCodeDepthf func(depth int, err error, code string, format string, args ...interface{}) error = sqlutil.WrapWithCodeDepthf

// DangerousStatementf creates a new error for "rejected dangerous
// statements".
var DangerousStatementf func(format string, args ...interface{}) error = sqlutil.DangerousStatementf
