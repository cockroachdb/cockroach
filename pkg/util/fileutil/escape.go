// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fileutil

import "regexp"

// EscapeFilename replaces bad characters in a filename with safe equivalents.
// The only character disallowed on Unix systems is the path separator "/".
// Windows is more restrictive; banned characters on Windows are listed here:
// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
func EscapeFilename(s string) string {
	return regexp.MustCompile(`[<>:"\/|?*\x00-\x1f]`).ReplaceAllString(s, "_")
}
