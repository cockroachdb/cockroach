// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stacktrace

import (
	"bytes"
	"encoding/json"
	"fmt"

	raven "github.com/getsentry/raven-go"
)

// ReportableObject is an interface suitable for the extra detail
// objects provided to SendReport().
type ReportableObject = raven.Interface

// StackTrace is an object suitable for inclusion in errors that can
// ultimately be reported with ReportInternalError() or similar.
type StackTrace = raven.Stacktrace

// It also implements the interface ReportableObject below and is
// thus suitable for use with SendReport().
var _ ReportableObject = &StackTrace{}

// NewStackTrace generates a stacktrace suitable for inclusion in
// error reports.
func NewStackTrace(depth int) *StackTrace {
	const contextLines = 3
	return raven.NewStacktrace(depth+1, contextLines, crdbPaths)
}

var crdbPaths = []string{
	"github.com/cockroachdb/cockroach",
	"go.etcd.io/etcd/raft",
}

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

// EncodeStackTrace produces a decodable string representation of the
// stack trace.  This never fails.
func EncodeStackTrace(s *StackTrace) (string, error) {
	v, err := json.Marshal(s)
	if err != nil {
		return "<invalid stack trace>", err
	}
	return string(v), nil
}

// DecodeStackTrace produces a stack trace from the encoded string.
// If decoding fails, a boolean false is returned. In that case the
// caller is invited to include the string in the final reportable
// object, as a fallback (instead of discarding the stack trace
// entirely).
func DecodeStackTrace(s string) (*StackTrace, error) {
	var st raven.Stacktrace
	err := json.Unmarshal([]byte(s), &st)
	return &st, err
}
