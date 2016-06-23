// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package util

import "fmt"

// UnimplementedWithIssueError is an error that links unimplemented functionality back
// to its issue on GitHub.
type UnimplementedWithIssueError struct {
	issue int
	msg   string
}

func (e UnimplementedWithIssueError) Error() string {
	var fmtMsg string
	if e.msg != "" {
		fmtMsg = fmt.Sprintf(": %s", e.msg)
	}
	return fmt.Sprintf("unimplemented%s (see issue "+
		"https://github.com/cockroachdb/cockroach/issues/%d)", fmtMsg, e.issue)
}

// UnimplementedWithIssueErrorf constructs an UnimplementedWithIssueError with the
// provided issue and formatted message.
func UnimplementedWithIssueErrorf(issue int, msg string, args ...interface{}) error {
	return UnimplementedWithIssueError{
		issue: issue,
		msg:   fmt.Sprintf(msg, args...),
	}
}
