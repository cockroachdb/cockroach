// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package util

import (
	"fmt"
	"runtime"
)

// const char* compilerVersion() {
// #if defined(__clang__)
// 	return __VERSION__;
// #elif defined(__GNUC__) || defined(__GNUG__)
// 	return "gcc " __VERSION__;
// #else
// 	return "non-gcc, non-clang (or an unrecognized version)";
// #endif
// }
import "C"

var (
	// These variables are initialized via the linker -X flag in the
	// top-level Makefile when compiling release binaries.
	buildTag         string // Tag of this build (git describe)
	buildTime        string // Build time in UTC (year/month/day hour:min:sec)
	buildDeps        string // Git SHAs of dependencies
	buildCgoCompiler = C.GoString(C.compilerVersion())
	buildPlatform    = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
)

// Short returns a pretty printed build and version summary.
func (b BuildInfo) Short() string {
	return fmt.Sprintf("CockroachDB %s (%s, built %s, %s)", b.Tag, b.Platform, b.Time, b.GoVersion)
}

// GetBuildInfo ...
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		GoVersion:   runtime.Version(),
		Tag:         buildTag,
		Time:        buildTime,
		Deps:        buildDeps,
		CgoCompiler: buildCgoCompiler,
		Platform:    buildPlatform,
	}
}
