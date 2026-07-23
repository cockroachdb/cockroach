// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fileutil

import "regexp"

// EscapeFilename replaces bad characters in a filename with safe equivalents.
// The only character disallowed on Unix systems is the path separator "/".
// Windows is more restrictive; banned characters on Windows are listed here:
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
func EscapeFilename(s string) string {
	return regexp.MustCompile(`[<>:"\/|?*\x00-\x1f]`).ReplaceAllString(s, "_")
}
