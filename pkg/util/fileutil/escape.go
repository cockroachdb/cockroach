// Copyright 2017 The Cockroach Authors.
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
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package fileutil

import "regexp"

// EscapeFilename replaces bad characters in a filename with safe equivalents.
// The only character disallowed on Unix systems is the path separator "/".
// Windows is more restrictive; banned characters on Windows are listed here:
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
func EscapeFilename(s string) string {
	return regexp.MustCompile(`[<>:"\/|?*\x00-\x1f]`).ReplaceAllString(s, "_")
}
