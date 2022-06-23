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

package normalpath

import (
	"os"
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
	if filepath.IsAbs(normalizedPath) || (len(normalizedPath) > 0 && normalizedPath[0] == '/') {
		// the stdlib implementation of `IsAbs` assumes that a volume name is required for a path to
		// be absolute, however Windows treats a `/` (normalized) rooted path as absolute _within_ the current volume.
		// In the context of validating that a path is _not_ relative, we need to reject a path that begins
		// with `/`.
		return "", NewError(path, errNotRelative)
	}
	// https://github.com/bufbuild/buf/issues/51
	if strings.HasPrefix(normalizedPath, normalizedRelPathJumpContextPrefix) {
		return "", NewError(path, errOutsideContextDir)
	}
	return normalizedPath, nil
}

// EqualsOrContainsPath returns true if the value is equal to or contains the path.
// path is compared at each directory level to value for equivalency under simple unicode
// codepoint folding. This means it is context and locale independent. This matching
// will not support the few rare cases, primarily in Turkish and Lithuanian, noted
// in the caseless matching section of Unicode 13.0 https://www.unicode.org/versions/Unicode13.0.0/ch05.pdf#page=47.
//
// The path and value are expected to be normalized and validated if Relative is used.
// The path and value are expected to be normalized and absolute if Absolute is used.
func EqualsOrContainsPath(value string, path string, pathType PathType) bool {
	pathRoot := "."
	if pathType == Absolute {
		// To check an absolute path on Windows we must first determine
		// the root volume. This can appear in one of 3 forms (examples are after Normalize())
		// c.f. https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file?redirectedfrom=MSDN#fully-qualified-vs-relative-paths
		// * A disk designator: `C:/`
		// * A UNC Path: `//servername/share/`
		//   c.f. https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dfsc/149a3039-98ce-491a-9268-2f5ddef08192
		// * A "current volume absolute path" `/`
		//   This refers to the root of the current volume
		//
		// We do not support paths with string parsing disabled such as
		// `\\?\path`
		volumeName := filepath.VolumeName(path)

		// If value has a disk designator or is a UNC path, we'll get a volume name with no trailing separator.
		// However, filepath.Dir() will return a path component with a trailing separator so we need to add one.
		// If value is a current volume absolute path, volumeName will be "".
		// Therefore, we unconditionally add `/` to the volume name to get to the root path component
		// at which we should stop iterating
		pathRoot = volumeName + "/"
	}

	if pathRoot == value {
		return true
	}

	// Walk up the path and compare at each directory level until there is a
	// match or the path reaches its root (either `/` or `.`).
	for curPath := path; !strings.EqualFold(curPath, pathRoot); curPath = Dir(curPath) {
		if strings.EqualFold(value, curPath) {
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

	for value := range m {
		if EqualsOrContainsPath(value, path, pathType) {
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

	n := make(map[string]struct{})

	for potentialMatch := range m {
		if EqualsOrContainsPath(potentialMatch, path, pathType) {
			n[potentialMatch] = struct{}{}
		}
	}
	return n
}

// Components splits the path into its components.
//
// This calls filepath.Split repeatedly.
//
// The path is expected to be normalized.
func Components(path string) []string {
	var components []string

	if len(path) < 1 {
		return []string{"."}
	}

	dir := Unnormalize(path)

	volumeComponent := filepath.VolumeName(dir)
	if len(volumeComponent) > 0 {
		// On Windows the volume of an absolute path could be one of the following 3 forms
		// c.f. https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file?redirectedfrom=MSDN#fully-qualified-vs-relative-paths
		// * A disk designator: `C:\`
		// * A UNC Path: `\\servername\share\`
		//   c.f. https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dfsc/149a3039-98ce-491a-9268-2f5ddef08192
		// * A "current volume absolute path" `\`
		//   This refers to the root of the current volume
		//
		// We do not support paths with string parsing disabled such as
		// `\\?\path`
		//
		// If we did extract a volume name, we need to add a path separator to turn it into
		// a path component. Volume Names without path separators have an implied "current directory"
		// when performing a join operation, or using them as a path directly, which is not the
		// intention of `Split` so we ensure they always mean "the root of this volume".
		volumeComponent = volumeComponent + stringOSPathSeparator
	}
	if len(volumeComponent) < 1 && dir[0] == os.PathSeparator {
		// If we didn't extract a volume name then the path is either
		// absolute and starts with an os.PathSeparator (it must be exactly 1
		// otherwise its a UNC path and we would have found a volume above) or it is relative.
		// If it is absolute, we set the expected volume component to os.PathSeparator.
		// otherwise we leave it as an empty string.
		volumeComponent = stringOSPathSeparator
	}
	for {
		var file string
		dir, file = filepath.Split(dir)
		// puts in reverse
		components = append(components, file)

		if dir == volumeComponent {
			if volumeComponent != "" {
				components = append(components, dir)
			}
			break
		}

		dir = strings.TrimSuffix(dir, stringOSPathSeparator)
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
