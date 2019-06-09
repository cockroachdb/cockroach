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

import "github.com/cockroachdb/cockroach/pkg/errors/errutil"

// Unimplemented forwards a definition.
var Unimplemented func(feature, msg string) error = errutil.Unimplemented

// Unimplementedf forwards a definition.
var Unimplementedf func(feature, format string, args ...interface{}) error = errutil.Unimplementedf

// UnimplementedWithDepthf forwards a definition.
var UnimplementedWithDepthf func(depth int, feature, format string, args ...interface{}) error = errutil.UnimplementedWithDepthf

// UnimplementedWithIssue forwards a definition.
var UnimplementedWithIssue func(issue int, msg string) error = errutil.UnimplementedWithIssue

// UnimplementedWithIssuef forwards a definition.
var UnimplementedWithIssuef func(issue int, format string, args ...interface{}) error = errutil.UnimplementedWithIssuef

// UnimplementedWithIssueHint forwards a definition.
var UnimplementedWithIssueHint func(issue int, msg, hint string) error = errutil.UnimplementedWithIssueHint

// UnimplementedWithIssueDetail forwards a definition.
var UnimplementedWithIssueDetail func(issue int, detail, msg string) error = errutil.UnimplementedWithIssueDetail

// UnimplementedWithIssueDetailf forwards a definition.
var UnimplementedWithIssueDetailf func(issue int, detail, format string, args ...interface{}) error = errutil.UnimplementedWithIssueDetailf
