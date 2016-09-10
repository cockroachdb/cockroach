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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package util

import (
	"runtime"
	"strings"
)

// GetTopCallers populates an array with the names of the topmost 5
// caller functions in the stack after skipping a given number of
// frames. We use this to provide context to allocations in the logs
// with high verbosity.
func GetTopCallers(callers []string, skip int) {
	var pc [5]uintptr
	nCallers := runtime.Callers(skip+1, pc[:])
	for i := 0; i < nCallers; i++ {
		name := runtime.FuncForPC(pc[i]).Name()
		const crl = "github.com/cockroachdb/cockroach/"
		if strings.HasPrefix(name, crl) {
			name = name[len(crl):]
		}
		callers[i] = name
	}
}

// GetSmallTrace produces a ":"-separated single line containing the
// topmost 5 callers from a given skip level.
func GetSmallTrace(skip int) string {
	var callers [5]string
	GetTopCallers(callers[0:], skip+2)
	return strings.Join(callers[0:], ":")
}
