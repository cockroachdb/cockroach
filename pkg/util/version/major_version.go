// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import (
	"cmp"
	"fmt"
	"regexp"
	"strconv"

	"github.com/cockroachdb/errors"
)

type MajorVersion struct {
	Year, Ordinal int
}

func ParseMajorVersion(versionStr string) (MajorVersion, error) {
	majorVersionRE := regexp.MustCompile(`^v(0|[1-9][0-9]*)\.([1-9][0-9]*)$`)
	if !majorVersionRE.MatchString(versionStr) {
		return MajorVersion{}, errors.Newf("not a valid CRDB major version: %s", versionStr)
	}
	groups := majorVersionRE.FindStringSubmatch(versionStr)
	year, _ := strconv.Atoi(groups[1])
	ordinal, _ := strconv.Atoi(groups[2])
	return MajorVersion{year, ordinal}, nil
}

func MustParseMajorVersion(versionStr string) MajorVersion {
	majorVersion, err := ParseMajorVersion(versionStr)
	if err != nil {
		panic(err)
	}
	return majorVersion
}

func (m MajorVersion) Compare(o MajorVersion) int {
	if r := cmp.Compare(m.Year, o.Year); r != 0 {
		return r
	}
	return cmp.Compare(m.Ordinal, o.Ordinal)
}

func (m MajorVersion) Equals(o MajorVersion) bool {
	return m.Compare(o) == 0
}

func (m MajorVersion) LessThan(o MajorVersion) bool {
	return m.Compare(o) < 0
}

func (m MajorVersion) AtLeast(o MajorVersion) bool {
	return m.Compare(o) >= 0
}

func (m MajorVersion) Empty() bool {
	return m.Compare(MajorVersion{}) == 0
}

func (m MajorVersion) String() string {
	return fmt.Sprintf("v%d.%d", m.Year, m.Ordinal)
}
