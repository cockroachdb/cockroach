// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build windows
// +build windows

package protoc

import (
	"path/filepath"
)

// isOutNotAFullPath checks if we need to consider the path to be a full path.
//
// This is always true in unix, and is true if the path is not absolute in windows.
// This is needed because i.e.:
//
//   # unix
//   --go_out=opt:foo/bar
//   --go_out=foo/bar
//   # windows
//   --go_out=opt:C:\foo\bar
//   --go_out=C:\foo\bar
//
// protoc uses : in both unix and windows to separate the opt and out, but in windows,
// if a full path is given, then we don't want it interpreted as something we should split.
func isOutNotAFullPath(path string) bool {
	return !filepath.IsAbs(path)
}
