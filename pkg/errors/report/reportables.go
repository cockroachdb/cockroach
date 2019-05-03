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

package report

import (
	"bytes"
	"fmt"

	raven "github.com/getsentry/raven-go"
)

// ReportableObject is an interface suitable for the extra detail
// objects provided to SendReport().
type ReportableObject = raven.Interface

// NewReportMessage constructs string objects that can be added to
// calls to SendReport().
func NewReportMessage(msg string) ReportableObject {
	return &raven.Message{Message: msg}
}

// StackTrace is an object suitable for inclusion in errors that can
// ultimately be reported with ReportInternalError() or similar.
type StackTrace = raven.Stacktrace

// It also implements the interface ReportableObjet below and is
// thus suitable for use with SendReport().
var _ ReportableObject = &StackTrace{}

// PrintStackTrace produces a human-readable partial representation of
// the stack trace.
func PrintStackTrace(s *StackTrace) string {
	var buf bytes.Buffer
	for i := len(s.Frames) - 1; i >= 0; i-- {
		f := s.Frames[i]
		fmt.Fprintf(&buf, "%s:%d: in %s()\n", f.Filename, f.Lineno, f.Function)
	}
	return buf.String()
}
