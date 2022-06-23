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

package protoversion

import (
	"strconv"
	"strings"
)

var _ PackageVersion = &packageVersion{}

type packageVersion struct {
	major          int
	stabilityLevel StabilityLevel
	minor          int
	patch          int
	suffix         string
}

func newPackageVersionForPackage(pkg string) (*packageVersion, bool) {
	if pkg == "" {
		return nil, false
	}
	parts := strings.Split(pkg, ".")
	if len(parts) < 2 {
		return nil, false
	}
	lastPart := parts[len(parts)-1]
	// must at least contain 'v' and a number
	if len(lastPart) < 2 {
		return nil, false
	}
	if lastPart[0] != 'v' {
		return nil, false
	}

	// v1beta1 -> 1beta1
	// v1testfoo -> 1testfoo
	// v1p1alpha1 -> p1alpha1
	version := lastPart[1:]

	if strings.Contains(version, "test") {
		// 1testfoo -> [1, foo]
		split := strings.SplitN(version, "test", 2)
		if len(split) != 2 {
			return nil, false
		}
		major, ok := positiveNumber(split[0])
		if !ok {
			return nil, false
		}
		return newPackageVersion(major, StabilityLevelTest, 0, 0, split[1]), true
	}

	var stabilityLevel StabilityLevel
	containsAlpha := strings.Contains(version, "alpha")
	containsBeta := strings.Contains(version, "beta")
	switch {
	case !containsAlpha && !containsBeta:
		stabilityLevel = StabilityLevelStable
	case containsAlpha && !containsBeta:
		stabilityLevel = StabilityLevelAlpha
	case !containsAlpha && containsBeta:
		stabilityLevel = StabilityLevelBeta
	case containsAlpha && containsBeta:
		return nil, false
	}
	if stabilityLevel != StabilityLevelStable {
		// 1alpha1 -> [1, 1]
		// 1p1alpha1 ->[1p1, 1]
		// 1alpha -> [1, ""]
		split := strings.SplitN(version, stabilityLevel.String(), 2)
		if len(split) != 2 {
			return nil, false
		}
		minor := 0
		var ok bool
		if split[1] != "" {
			minor, ok = positiveNumber(split[1])
			if !ok {
				return nil, false
			}
		}
		major, patch, ok := getAlphaBetaMajorPatch(split[0])
		if !ok {
			return nil, false
		}
		return newPackageVersion(major, stabilityLevel, minor, patch, ""), true
	}

	// no suffix that is valid, make sure we just have a number
	major, ok := positiveNumber(version)
	if !ok {
		return nil, false
	}
	return newPackageVersion(major, StabilityLevelStable, 0, 0, ""), true
}

func newPackageVersion(
	major int,
	stabilityLevel StabilityLevel,
	minor int,
	patch int,
	suffix string,
) *packageVersion {
	return &packageVersion{
		major:          major,
		stabilityLevel: stabilityLevel,
		minor:          minor,
		patch:          patch,
		suffix:         suffix,
	}
}

func (p *packageVersion) Major() int {
	return p.major
}

func (p *packageVersion) StabilityLevel() StabilityLevel {
	return p.stabilityLevel
}

func (p *packageVersion) Minor() int {
	return p.minor
}

func (p *packageVersion) Patch() int {
	return p.patch
}

func (p *packageVersion) Suffix() string {
	return p.suffix
}

func (p *packageVersion) String() string {
	var builder strings.Builder
	builder.WriteRune('v')
	builder.WriteString(strconv.Itoa(p.major))
	if p.patch > 0 {
		builder.WriteRune('p')
		builder.WriteString(strconv.Itoa(p.patch))
	}
	builder.WriteString(p.stabilityLevel.String())
	if p.minor > 0 {
		builder.WriteString(strconv.Itoa(p.minor))
	}
	if p.suffix != "" {
		builder.WriteString(p.suffix)
	}
	return builder.String()
}

func (p *packageVersion) isPackageVersion() {}

func getAlphaBetaMajorPatch(remainder string) (int, int, bool) {
	if strings.Contains(remainder, "p") {
		// 1p1 -> [1, 1]
		patchSplit := strings.SplitN(remainder, "p", 2)
		if len(patchSplit) != 2 {
			return 0, 0, false
		}
		major, ok := positiveNumber(patchSplit[0])
		if !ok {
			return 0, 0, false
		}
		patch, ok := positiveNumber(patchSplit[1])
		if !ok {
			return 0, 0, false
		}
		return major, patch, true
	}
	// no patch, make sure just a number
	major, ok := positiveNumber(remainder)
	if !ok {
		return 0, 0, false
	}
	return major, 0, true
}

func positiveNumber(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, false
	}
	if value < 1 {
		return 0, false
	}
	return int(value), true
}
