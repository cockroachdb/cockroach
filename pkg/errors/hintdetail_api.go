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

// WithHint forwards a definition.
var WithHint func(err error, msg string) error = hintdetail.WithHint

// WithHintf forwards a definition.
var WithHintf func(err error, format string, args ...interface{}) error = hintdetail.WithHintf

// WithDetail forwards a definition.
var WithDetail func(err error, msg string) error = hintdetail.WithDetail

// WithDetailf forwards a definition.
var WithDetailf func(err error, format string, args ...interface{}) error = hintdetail.WithDetailf

// GetAllHints forwards a definition.
var GetAllHints func(err error) []string = hintdetail.GetAllHints

// FlattenHints forwards a definition.
var FlattenHints func(err error) string = hintdetail.FlattenHints

// GetAllDetails forwards a definition.
var GetAllDetails func(err error) []string = hintdetail.GetAllDetails

// FlattenDetails forwards a definition.
var FlattenDetails func(err error) string = hintdetail.FlattenDetails
