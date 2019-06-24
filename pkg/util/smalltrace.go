// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		file := f.File
		if index := strings.LastIndexByte(file, '/'); index >= 0 {
			file = file[index+1:]
		}
		callers = append(callers, file+":"+strconv.Itoa(f.Line)+":"+function)
		if !more {
			break
		}
	}

	return strings.Join(callers, ",")
}
