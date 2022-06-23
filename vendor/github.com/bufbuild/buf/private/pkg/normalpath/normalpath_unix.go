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

// Matching the unix-like build tags in the Golang standard library based on the dependency
// on "path/filepath", i.e. https://cs.opensource.google/go/go/+/refs/tags/go1.17:src/path/filepath/path_unix.go;l=5-6

//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd js,wasm linux netbsd openbsd solaris

package normalpath

import (
	"path/filepath"
	"strings"
)

// NormalizeAndValidate normalizes and validates the given path.
//
// This calls Normalize on the path.
// Returns Error if the path is not relative or jumps context.
// This can be used to validate that paths are valid to use with Buckets.
// The error message is safe to pass to users.
func NormalizeAndValidate(path string) (string, error) {
	normalizedPath := Normalize(path)
	if filepath.IsAbs(normalizedPath) {
		return "", NewError(path, errNotRelative)
	}
	// https://github.com/bufbuild/buf/issues/51
	if strings.HasPrefix(normalizedPath, normalizedRelPathJumpContextPrefix) {
		return "", NewError(path, errOutsideContextDir)
	}
	return normalizedPath, nil
}

// EqualsOrContainsPath returns true if the value is equal to or contains the path.
//
// The path and value are expected to be normalized and validated if Relative is used.
// The path and value are expected to be normalized and absolute if Absolute is used.
func EqualsOrContainsPath(value string, path string, pathType PathType) bool {
	pathRoot := stringOSPathSeparator
	if pathType == Relative {
		pathRoot = "."
	}

	if value == pathRoot {
		return true
	}

	// Walk up the path and compare at each directory level until there is a
	// match or the path reaches its root (either `/` or `.`).
	for curPath := path; curPath != pathRoot; curPath = Dir(curPath) {
		if value == curPath {
			return true
		}
	}
	return false
}

// MapHasEqualOrContainingPath returns true if the path matches any file or directory in the map.
//
// The path and keys in m are expected to be normalized and validated if Relative is used.
// The path and keys in m are expected to be normalized and absolute if Absolute is used.
//
// If the map is empty, returns false.
func MapHasEqualOrContainingPath(m map[string]struct{}, path string, pathType PathType) bool {
	if len(m) == 0 {
		return false
	}

	pathRoot := stringOSPathSeparator
	if pathType == Relative {
		pathRoot = "."
	}

	if _, ok := m[pathRoot]; ok {
		return true
	}
	for curPath := path; curPath != pathRoot; curPath = Dir(curPath) {
		if _, ok := m[curPath]; ok {
			return true
		}
	}
	return false
}

// MapAllEqualOrContainingPathMap returns the paths in m that are equal to, or contain
// path, in a new map.
//
// The path and keys in m are expected to be normalized and validated if Relative is used.
// The path and keys in m are expected to be normalized and absolute if Absolute is used.
//
// If the map is empty, returns nil.
func MapAllEqualOrContainingPathMap(m map[string]struct{}, path string, pathType PathType) map[string]struct{} {
	if len(m) == 0 {
		return nil
	}

	pathRoot := stringOSPathSeparator
	if pathType == Relative {
		pathRoot = "."
	}

	n := make(map[string]struct{})
	if _, ok := m[pathRoot]; ok {
		// also covers if path == separator.
		n[pathRoot] = struct{}{}
	}
	for potentialMatch := range m {
		for curPath := path; curPath != pathRoot; curPath = Dir(curPath) {
			if potentialMatch == curPath {
				n[potentialMatch] = struct{}{}
				break
			}
		}
	}
	return n
}

// Components splits the path into it's components.
//
// This calls filepath.Split repeatedly.
//
// The path is expected to be normalized.
func Components(path string) []string {
	var components []string
	dir := Unnormalize(path)
	for {
		var file string
		dir, file = filepath.Split(dir)
		// puts in reverse
		components = append(components, file)
		if dir == stringOSPathSeparator {
			components = append(components, dir)
			break
		}
		dir = strings.TrimSuffix(dir, stringOSPathSeparator)
		if dir == "" {
			break
		}
	}
	// https://github.com/golang/go/wiki/SliceTricks#reversing
	for i := len(components)/2 - 1; i >= 0; i-- {
		opp := len(components) - 1 - i
		components[i], components[opp] = components[opp], components[i]
	}
	for i, component := range components {
		components[i] = Normalize(component)
	}
	return components
}
