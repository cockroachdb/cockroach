// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// devOffset is the value by which all versions (including the minimum version
// from which upgrades are supported) are offset in a dev binary.
//
// Refer to
// https://github.com/cockroachdb/cockroach/blob/master/pkg/clusterversion/cockroach_versions.go#L665
// for more documentation.
const devOffset = 1000000

// Less compares two Versions.
func (v Version) Less(otherV Version) bool {
	if v.Major < otherV.Major {
		return true
	} else if v.Major > otherV.Major {
		return false
	}
	if v.Minor < otherV.Minor {
		return true
	} else if v.Minor > otherV.Minor {
		return false
	}
	if v.Patch < otherV.Patch {
		return true
	} else if v.Patch > otherV.Patch {
		return false
	}
	if v.Internal < otherV.Internal {
		return true
	} else if v.Internal > otherV.Internal {
		return false
	}
	return false
}

// LessEq returns whether the receiver is less than or equal to the parameter.
func (v Version) LessEq(otherV Version) bool {
	return v.Equal(otherV) || v.Less(otherV)
}

// String implements the fmt.Stringer interface.
func (v Version) String() string { return redact.StringWithoutMarkers(v) }

// SafeFormat implements the redact.SafeFormatter interface.
func (v Version) SafeFormat(p redact.SafePrinter, _ rune) {
	if v.Internal == 0 {
		p.Printf("%d.%d", v.Major, v.Minor)
		return
	}
	p.Printf("%d.%d-%d", v.Major, v.Minor, v.Internal)
}

// VersionWithDevOffset returns v with a devOffset.
// Refer to devOffset for more documentation.
func (v Version) VersionWithDevOffset() Version {
	retV := v
	retV.Major += devOffset
	return retV
}

// VersionWithoutDevOffset returns v without a devOffset.
// Refer to devOffset for more documentation.
func (v Version) VersionWithoutDevOffset() (Version, error) {
	if v.Major < devOffset {
		return Version{}, errors.Newf("version %s is less than the dev offset")
	}
	retV := v
	retV.Major -= devOffset
	return retV, nil
}

// PrettyPrint returns the value in a format that makes it apparent whether or
// not it is a fence version.
func (v Version) PrettyPrint() string {
	// If we're a version greater than v20.2 and have an odd internal version,
	// we're a fence version. See fenceVersionFor in pkg/upgrade to understand
	// what these are.
	fenceVersion := !v.LessEq(Version{Major: 20, Minor: 2}) && (v.Internal%2) == 1
	if !fenceVersion {
		return v.String()
	}
	return fmt.Sprintf("%v(fence)", v)
}

// ParseVersion parses a Version from a string of the form
// "<major>.<minor>-<internal>" where the "-<internal>" is optional. We don't
// use the Patch component, so it is always zero.
func ParseVersion(s string) (Version, error) {
	var c Version
	dotParts := strings.Split(s, ".")

	if len(dotParts) != 2 {
		return Version{}, errors.Errorf("invalid version %s", s)
	}

	parts := append(dotParts[:1], strings.Split(dotParts[1], "-")...)
	if len(parts) == 2 {
		parts = append(parts, "0")
	}

	if len(parts) != 3 {
		return c, errors.Errorf("invalid version %s", s)
	}

	ints := make([]int64, len(parts))
	for i := range parts {
		var err error
		if ints[i], err = strconv.ParseInt(parts[i], 10, 32); err != nil {
			return c, errors.Wrapf(err, "invalid version %s", s)
		}
	}

	c.Major = int32(ints[0])
	c.Minor = int32(ints[1])
	c.Internal = int32(ints[2])

	return c, nil
}

// MustParseVersion calls ParseVersion and panics on error.
func MustParseVersion(s string) Version {
	v, err := ParseVersion(s)
	if err != nil {
		panic(err)
	}
	return v
}
