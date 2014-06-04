// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"fmt"
	"path/filepath"
	"runtime"
)

const defaultSkip = 2
const errorPrefixFormat string = "%s:%d: "

// getPrefix skips "skip" stack frames to get the file & line number
// of original caller.
func getPrefix(skip int, format string) string {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return fmt.Sprintf(format, filepath.Base(file), line)
	}
	return ""
}

// Errorf is a passthrough to fmt.Errorf, with an additional prefix
// containing the filename and line number.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(getPrefix(defaultSkip, errorPrefixFormat)+format, a...)
}

// ErrorfSkipFrames allows the skip count for stack frames to be
// specified. This is useful when generating errors via helper
// methods. Skip should be specified as the number of additional stack
// frames between the location at which the error is caused and the
// location at which the error is generated.
func ErrorfSkipFrames(skip int, format string, a ...interface{}) error {
	return fmt.Errorf(getPrefix(defaultSkip+skip, errorPrefixFormat)+format, a...)
}

// Error is a passthrough to fmt.Error, with an additional prefix
// containing the filename and line number.
func Error(a ...interface{}) error {
	return ErrorSkipFrames(1, a...)
}

// ErrorSkipFrames allows the skip count for stack frames to be
// specified. See the comments for ErrorfSkip.
func ErrorSkipFrames(skip int, a ...interface{}) error {
	prefix := getPrefix(defaultSkip+skip, errorPrefixFormat)
	if prefix != "" {
		a = append([]interface{}{prefix}, a...)
	}
	return fmt.Errorf("%s", fmt.Sprint(a...))
}
