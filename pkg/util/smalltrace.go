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

package util

import (
	"runtime"
	"strconv"
	"strings"
)

var prefix = func() string {
	result := "github.com/cockroachdb/cockroach/pkg/"
	if runtime.Compiler == "gccgo" {
		result = strings.Replace(result, ".", "_", -1)
		result = strings.Replace(result, "/", "_", -1)
	}
	return result
}()

// GetSmallTrace returns a comma-separated string containing the top
// 5 callers from a given skip level.
func GetSmallTrace(skip int) string {
	var pcs [5]uintptr
	nCallers := runtime.Callers(skip, pcs[:])
	callers := make([]string, 0, nCallers)
	frames := runtime.CallersFrames(pcs[:])

	for {
		f, more := frames.Next()
		function := strings.TrimPrefix(f.Function, prefix)
		callers = append(callers, function+":"+strconv.Itoa(f.Line))
		if !more {
			break
		}
	}

	return strings.Join(callers, ",")
}
