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

import "github.com/cockroachdb/cockroach/pkg/errors/issuelink"

// WithIssueLink adds an annotation to a know issue
// on a web issue tracker.
//
// The url and detail strings may contain PII and will
// be considered reportable.
var WithIssueLink func(err error, issue IssueLink) error = issuelink.WithIssueLink

// IssueLink is the payload for a linked issue annotation.
type IssueLink = issuelink.IssueLink

// UnimplementedError creates a new leaf error that indicates that
// some feature was not (yet) implemented.
var UnimplementedError func(issueLink IssueLink, msg string) error = issuelink.UnimplementedError

// UnimplementedErrorf creates a new leaf error that indicates that
// some feature was not (yet) implemented. The message is formatted.
var UnimplementedErrorf func(issueLink IssueLink, format string, args ...interface{}) error = issuelink.UnimplementedErrorf

// GetAllIssueLinks retrieves the linked issue carried
// by the error or its direct causes.
var GetAllIssueLinks func(err error) (issues []IssueLink) = issuelink.GetAllIssueLinks

// HasIssueLink returns true iff the error or one of its
// causes has a linked issue payload.
var HasIssueLink func(err error) bool = issuelink.HasIssueLink

// IsIssueLink returns true iff the error (not its
// causes) has a linked issue payload.
var IsIssueLink func(err error) bool = issuelink.IsIssueLink

// HasUnimplementedError returns iff if err or its cause is an
// unimplemented error.
var HasUnimplementedError func(err error) bool = issuelink.HasUnimplementedError

// IsUnimplementedError returns iff if err is an unimplemented error.
var IsUnimplementedError func(err error) bool = issuelink.IsUnimplementedError
