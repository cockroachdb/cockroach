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

// WithIssueLink forwards a definition.
var WithIssueLink func(err error, issue IssueLink) error = issuelink.WithIssueLink

// IssueLink forwards a definition.
type IssueLink = issuelink.IssueLink

// UnimplementedError forwards a definition.
var UnimplementedError func(issueLink IssueLink, msg string) error = issuelink.UnimplementedError

// UnimplementedErrorf forwards a definition.
var UnimplementedErrorf func(issueLink IssueLink, format string, args ...interface{}) error = issuelink.UnimplementedErrorf

// GetAllIssueLinks forwards a definition.
var GetAllIssueLinks func(err error) (issues []IssueLink) = issuelink.GetAllIssueLinks

// HasIssueLink forwards a definition.
var HasIssueLink func(err error) bool = issuelink.HasIssueLink

// IsIssueLink forwards a definition.
var IsIssueLink func(err error) bool = issuelink.IsIssueLink

// HasUnimplementedError forwards a definition.
var HasUnimplementedError func(err error) bool = issuelink.HasUnimplementedError

// IsUnimplementedError forwards a definition.
var IsUnimplementedError func(err error) bool = issuelink.IsUnimplementedError
