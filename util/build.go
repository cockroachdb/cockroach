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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package util

import "runtime"

var (
	// These variables are initialized via the linker -X flag in the
	// top-level Makefile when compiling release binaries.
	buildVers string // Go Version
	buildTag  string // Tag of this build (git describe)
	buildTime string // Build time in UTC (year/month/day hour:min:sec)
	buildDeps string // Git SHAs of dependencies
)

// BuildInfo ...
type BuildInfo struct {
	Vers string `json:"goVersion"`
	Tag  string `json:"tag"`
	Time string `json:"time"`
	Deps string `json:"dependencies"`
}

// GetBuildInfo ...
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		Vers: runtime.Version(),
		Tag:  buildTag,
		Time: buildTime,
		Deps: buildDeps,
	}
}
