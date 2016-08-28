// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package testutils

import (
	"bytes"
	"fmt"
	"regexp"
	"runtime"

	"github.com/cockroachdb/cockroach/roachpb"
)

// IsError returns true if err is non-nil and the error string matches the
// supplied regexp.
func IsError(err error, re string) bool {
	if err == nil {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}

// IsPError returns true if pErr is non-nil and the error message matches the
// supplied regexp.
func IsPError(pErr *roachpb.Error, re string) bool {
	if pErr == nil {
		return false
	}
	matched, merr := regexp.MatchString(re, pErr.Message)
	if merr != nil {
		return false
	}
	return matched
}

// IsSQLRetryError returns true if err is retryable. This is true for errors
// that show a connection issue or an issue with the node itself. This can
// occur when a node is restarting or is unstable in some other way.
func IsSQLRetryError(err error) bool {
	return IsError(err, "(connection reset by peer|connection refused|failed to send RPC|EOF|context deadline exceeded)")
}

// Caller returns filename and line number info for the specified stack
// depths. The info is formated as <file>:<line> and each entry is separated
// for a space.
func Caller(depth ...int) string {
	var sep string
	var buf bytes.Buffer
	for _, d := range depth {
		_, file, line, _ := runtime.Caller(d + 1)
		fmt.Fprintf(&buf, "%s%s:%d", sep, file, line)
		sep = " "
	}
	return buf.String()
}

// MakeCaller returns a function which will invoke Caller with the specified
// arguments.
func MakeCaller(depth ...int) func() string {
	return func() string {
		return Caller(depth...)
	}
}
