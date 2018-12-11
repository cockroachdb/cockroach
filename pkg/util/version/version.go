// Copyright 2018 The Cockroach Authors.
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

package version

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// Version represents a semantic version; see
// https://semver.org/spec/v2.0.0.html.
type Version struct {
	major      int32
	minor      int32
	patch      int32
	preRelease string
	metadata   string
}

// Major returns the major (first) version number.
func (v *Version) Major() int {
	return int(v.major)
}

// Minor returns the minor (second) version number.
func (v *Version) Minor() int {
	return int(v.minor)
}

// Patch returns the patch (third) version number.
func (v *Version) Patch() int {
	return int(v.patch)
}

// PreRelease returns the pre-release version (if present).
func (v *Version) PreRelease() string {
	return v.preRelease
}

// Metadata returns the metadata (if present).
func (v *Version) Metadata() string {
	return v.metadata
}

// String returns the string representation, in the format:
//   "v1.2.3-beta+md"
func (v Version) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "v%d.%d.%d", v.major, v.minor, v.patch)
	if v.preRelease != "" {
		fmt.Fprintf(&b, "-%s", v.preRelease)
	}
	if v.metadata != "" {
		fmt.Fprintf(&b, "+%s", v.metadata)
	}
	return b.String()
}

// versionRE is the regexp that is used to verify that a version string is
// of the form "vMAJOR.MINOR.PATCH[-PRERELEASE][+METADATA]". This
// conforms to https://semver.org/spec/v2.0.0.html
var versionRE = regexp.MustCompile(
	`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-.]+)?(\+[0-9A-Za-z-.]+|)?$`,
	// ^major           ^minor           ^patch         ^preRelease       ^metadata
)

// Parse creates a version from a string. The string must be a valid semantic
// version (as per https://semver.org/spec/v2.0.0.html) in the format:
//   "vMINOR.MAJOR.PATCH[-PRERELEASE][+METADATA]".
// MINOR, MAJOR, and PATCH are numeric values (without any leading 0s).
// PRERELEASE and METADATA can contain ASCII characters and digits, hyphens and
// dots.
func Parse(str string) (*Version, error) {
	if !versionRE.MatchString(str) {
		return nil, errors.Errorf("invalid version string '%s'", str)
	}

	var v Version
	r := strings.NewReader(str)
	// Read the major.minor.patch part.
	_, err := fmt.Fscanf(r, "v%d.%d.%d", &v.major, &v.minor, &v.patch)
	if err != nil {
		panic(fmt.Sprintf("invalid version '%s' passed the regex: %s", str, err))
	}
	remaining := str[len(str)-r.Len():]
	// Read the pre-release, if present.
	if len(remaining) > 0 && remaining[0] == '-' {
		p := strings.IndexRune(remaining, '+')
		if p == -1 {
			p = len(remaining)
		}
		v.preRelease = remaining[1:p]
		remaining = remaining[p:]
	}
	// Read the metadata, if present.
	if len(remaining) > 0 {
		if remaining[0] != '+' {
			panic(fmt.Sprintf("invalid version '%s' passed the regex", str))
		}
		v.metadata = remaining[1:]
	}
	return &v, nil
}

// MustParse is like Parse but panics on any error. Recommended as an
// initializer for global values.
func MustParse(str string) *Version {
	v, err := Parse(str)
	if err != nil {
		panic(err)
	}
	return v
}

func cmpVal(a, b int32) int {
	if a > b {
		return +1
	}
	if a < b {
		return -1
	}
	return 0
}

// Compare returns -1, 0, or +1 indicating the relative ordering of versions.
func (v *Version) Compare(w *Version) int {
	if v := cmpVal(v.major, w.major); v != 0 {
		return v
	}
	if v := cmpVal(v.minor, w.minor); v != 0 {
		return v
	}
	if v := cmpVal(v.patch, w.patch); v != 0 {
		return v
	}
	if v.preRelease != w.preRelease {
		if v.preRelease == "" && w.preRelease != "" {
			// 1.0.0 is greater than 1.0.0-alpha.
			return 1
		}
		if v.preRelease != "" && w.preRelease == "" {
			// 1.0.0-alpha is less than 1.0.0.
			return -1
		}
		// 1.0.0-alpha is less than 1.0.0-beta.
		if v.preRelease < w.preRelease {
			return -1
		}
		// 1.0.0-beta is greater than 1.0.0-alpha.
		return 1
	}
	return 0
}

// AtLeast returns true if v >= w.
func (v *Version) AtLeast(w *Version) bool {
	return v.Compare(w) > 0
}
