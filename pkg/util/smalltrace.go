// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"runtime"
	"strings"

	"github.com/cockroachdb/redact"
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
func GetSmallTrace(skip int) redact.RedactableString {
	var pcs [5]uintptr
	runtime.Callers(skip, pcs[:])
	frames := runtime.CallersFrames(pcs[:])
	var callers redact.StringBuilder

	var callerPrefix redact.RedactableString
	for {
		f, more := frames.Next()
		function := strings.TrimPrefix(f.Function, prefix)
		file := f.File
		if index := strings.LastIndexByte(file, '/'); index >= 0 {
			file = file[index+1:]
		}
		callers.Printf("%s%s:%d:%s", callerPrefix, redact.SafeString(file), f.Line, redact.SafeString(function))
		callerPrefix = ","
		if !more {
			break
		}
	}

	return callers.RedactableString()
}
