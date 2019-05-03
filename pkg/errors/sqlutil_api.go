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

// NewWithCode forwards a definition.
var NewWithCode func(code string, msg string) error = sqlutil.NewWithCode

// WrapWithCode forwards a definition.
var WrapWithCode func(err error, code string, msg string) error = sqlutil.WrapWithCode

// NewWithCodef forwards a definition.
var NewWithCodef func(code string, format string, args ...interface{}) error = sqlutil.NewWithCodef

// WrapWithCodef forwards a definition.
var WrapWithCodef func(err error, code string, format string, args ...interface{}) error = sqlutil.WrapWithCodef

// NewWithCodeDepthf forwards a definition.
var NewWithCodeDepthf func(depth int, code string, format string, args ...interface{}) error = sqlutil.NewWithCodeDepthf

// WrapWithCodeDepthf forwards a definition.
var WrapWithCodeDepthf func(depth int, err error, code string, format string, args ...interface{}) error = sqlutil.WrapWithCodeDepthf

// DangerousStatementf forwards a definition.
var DangerousStatementf func(format string, args ...interface{}) error = sqlutil.DangerousStatementf
