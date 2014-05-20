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

const errorPrefixFormat string = "%s:%d: "

// getPrefix skips two stack frames to get the file & line number of
// original caller.
func getPrefix(format string) string {
	if _, file, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf(format, filepath.Base(file), line)
	}
	return ""
}

// Errorf is a passthrough to fmt.Errorf, with an additional prefix
// containing the filename and line number.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(getPrefix(errorPrefixFormat)+format, a...)
}

// Error is a passthrough to fmt.Error, with an additional prefix
// containing the filename and line number.
func Error(a ...interface{}) error {
	prefix := getPrefix(errorPrefixFormat)
	if prefix != "" {
		a = append([]interface{}{prefix}, a...)
	}
	return fmt.Errorf("%s", fmt.Sprint(a...))
}
