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

import "github.com/cockroachdb/cockroach/pkg/errors/safedetails"

// WithSafeDetails annotates an error with the given reportable details.
// The format is made available as a PII-free string, alongside
// with a PII-free representation of every additional argument.
// Arguments can be reported as-is (without redaction) by wrapping
// them using the Safe() function.
//
// The annotated strings are not visible in the resulting error's
// main message rechable via Error().
var WithSafeDetails func(err error, format string, args ...interface{}) error = safedetails.WithSafeDetails

// A SafeType object can be reported verbatim, i.e. does not leak
// information. A nil `*SafeType` is not valid for use and may cause
// panics.
//
// Additional data can be attached to the safe value
// using its WithCause() method.
// Note: errors objects should not be attached using WithCause().
// Instead prefer WithSecondaryError().
type SafeType = safedetails.SafeType

// Safe constructs a SafeType.
var Safe func(v interface{}) SafeType = safedetails.Safe
