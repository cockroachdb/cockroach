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

package safedetails

import "fmt"

// WithSafeDetails annotates an error with the given reportable details.
// The format is made available as a PII-free string, alongside
// with a PII-free representation of every additional argument.
// Arguments can be reported as-is (without redaction) by wrapping
// them using the Safe() function.
//
// The annotated strings are not visible in the resulting error's
// main message rechable via Error().
func WithSafeDetails(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	details := make([]string, 1, 1+len(args))
	details[0] = format
	for i, a := range args {
		details = append(details, fmt.Sprintf("-- arg %d: %s", i, Redact(a)))
	}
	return &withSafeDetails{cause: err, safeDetails: details}
}

// SafeMessager is implemented by objects which have a way of representing
// themselves suitably redacted for anonymized reporting.
type SafeMessager interface {
	SafeMessage() string
}

// Safe wraps the given object into an opaque struct that implements
// SafeMessager: its contents can be included as-is in PII-free
// strings in error objects and reports.
func Safe(v interface{}) SafeMessager {
	return safeType{V: v}
}

// A safeType panic can be reported verbatim, i.e. does not leak
// information. A nil `*safeType` is not valid for use and may cause
// panics.
type safeType struct {
	V interface{}
}

var _ SafeMessager = safeType{}

// SafeMessage implements SafeMessager.
func (st safeType) SafeMessage() string {
	return fmt.Sprintf("%+v", st.V)
}

// safeType implements fmt.Stringer as a convenience.
func (st safeType) String() string {
	return st.SafeMessage()
}
