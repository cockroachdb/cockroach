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

package testutils

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
)

// IsError returns true if the error string matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	errString := pgerror.FullError(err)
	matched, merr := regexp.MatchString(re, errString)
	if merr != nil {
		return false
	}
	return matched
}

// IsPError returns true if pErr's message matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsPError(pErr *roachpb.Error, re string) bool {
	if pErr == nil && re == "" {
		return true
	}
	if pErr == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, pErr.Message)
	if merr != nil {
		return false
	}
	return matched
}

// IsSQLRetryableError returns true if err is retryable. This is true
// for errors that show a connection issue or an issue with the node
// itself. This can occur when a node is restarting or is unstable in
// some other way. Note that retryable errors may occur event in cases
// where the SQL execution ran to completion.
//
// TODO(bdarnell): Why are RPC errors in this list? These should
// generally be retried on the server side or transformed into
// ambiguous result errors ("connection reset/refused" are needed for
// the pgwire connection, but anything RPC-related should be handled
// within the cluster).
func IsSQLRetryableError(err error) bool {
	// Don't forget to update the corresponding test when making adjustments
	// here.
	return IsError(err, "(no inbound stream connection|connection reset by peer|connection refused|failed to send RPC|rpc error: code = Unavailable|EOF|result is ambiguous)")
}

// Caller returns filename and line number info for the specified stack
// depths. The info is formated as <file>:<line> and each entry is separated
// for a space.
func Caller(depth ...int) string {
	var sep string
	var buf bytes.Buffer
	for _, d := range depth {
		file, line, _ := caller.Lookup(d + 1)
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
