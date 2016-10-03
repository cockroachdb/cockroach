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

// GetSmallTrace produces a ":"-separated single line containing the
// topmost 5 callers from a given skip level.
func GetSmallTrace(skip int) string {
	pc := make([]uintptr, 5)
	nCallers := runtime.Callers(skip, pc[:])
	callers := make([]string, nCallers)

	for i := range callers {
		callers[i] = strings.TrimPrefix(
			runtime.FuncForPC(pc[i]).Name(),
			"github.com/cockroachdb/cockroach/",
		)
	}

	return strings.Join(callers, ":")
}
