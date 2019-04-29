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

import "github.com/cockroachdb/cockroach/pkg/errors/markers"

// Is determines whether one of the causes of the given error or any
// of its causes is equivalent to some reference error.
var Is func(err, reference error) bool = markers.Is

// If returns a predicate's return value the first time the predicate returns true.
var If func(err error, pred func(err error) (interface{}, bool)) (interface{}, bool) = markers.If

// IsAny is like Is except that multiple references are compared.
var IsAny func(err error, references ...error) bool = markers.IsAny

// Mark creates an explicit mark for the given error, using
// the same mark as some reference error.
var Mark func(err error, reference error) error = markers.Mark
