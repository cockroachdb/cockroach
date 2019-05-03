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

import "github.com/cockroachdb/cockroach/pkg/errors/hintdetail"

// WithHint decorates an error with a textual hint.
// The hint may contain PII and thus will not reportable.
var WithHint func(err error, msg string) error = hintdetail.WithHint

// WithHintf formats the hint.
var WithHintf func(err error, format string, args ...interface{}) error = hintdetail.WithHintf

// WithDetail decorates an error with a textual detail.
// The detail may contain PII and thus will not reportable.
var WithDetail func(err error, msg string) error = hintdetail.WithDetail

// WithDetailf formats the detail string.
var WithDetailf func(err error, format string, args ...interface{}) error = hintdetail.WithDetailf

// GetAllHints retrieves the hints from the error using in post-order
// traversal. The hints are de-duplicated. Assertion failures
// and unimplemented errors are detected and receive standard hints.
var GetAllHints func(err error) []string = hintdetail.GetAllHints

// FlattenHints retrieves the hints as per GetAllHints() and
// concatenates them into a single string.
var FlattenHints func(err error) string = hintdetail.FlattenHints

// GetAllDetails retrieves the details from the error using in post-order
// traversal.
var GetAllDetails func(err error) []string = hintdetail.GetAllDetails

// FlattenDetails retrieves the details as per GetAllDetails() and
// concatenates them into a single string.
var FlattenDetails func(err error) string = hintdetail.FlattenDetails
