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
	"fmt"
	"strconv"
)

const (
	// StabilityLevelStable is stable.
	StabilityLevelStable StabilityLevel = iota + 1
	// StabilityLevelAlpha is alpha stability.
	StabilityLevelAlpha
	// StabilityLevelBeta is beta stability.
	StabilityLevelBeta
	// StabilityLevelTest is test stability.
	StabilityLevelTest
)

var (
	stabilityLevelToString = map[StabilityLevel]string{
		StabilityLevelStable: "",
		StabilityLevelAlpha:  "alpha",
		StabilityLevelBeta:   "beta",
		StabilityLevelTest:   "test",
	}
)

// StabilityLevel is the stability level.
type StabilityLevel int

// String implements fmt.Stringer.
func (s StabilityLevel) String() string {
	value, ok := stabilityLevelToString[s]
	if ok {
		return value
	}
	return strconv.Itoa(int(s))
}

// PackageVersion is a package version.
//
// A package has a version if the last component is a version of the form
// v\d+, v\d+test.*, v\d+(alpha|beta)\d*, or v\d+p\d+(alpha|beta)\d*
// where numbers are >=1.
//
// See https://cloud.google.com/apis/design/versioning#channel-based_versioning
// See https://cloud.google.com/apis/design/versioning#release-based_versioning
type PackageVersion interface {
	fmt.Stringer

	// Required.
	// Will always be >=1.
	Major() int
	// Required.
	StabilityLevel() StabilityLevel
	// Optional.
	// Only potentially set if the stability level is alpha or beta.
	// Will always be >=1.
	Minor() int
	// Optional.
	// Only potentially set if the stability level is alpha or beta.
	// Will always be >=1.
	Patch() int
	// Optional.
	// Only potentially set if the stability level is test.
	Suffix() string

	isPackageVersion()
}

// NewPackageVersionForPackage returns the PackageVersion for the package.
//
// Returns false if the package has no package version per the specifications.
func NewPackageVersionForPackage(pkg string) (PackageVersion, bool) {
	return newPackageVersionForPackage(pkg)
}
