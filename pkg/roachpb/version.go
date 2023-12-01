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

// AtLeast returns true if the receiver is greater than or equal to the parameter.
func (v Version) AtLeast(otherV Version) bool {
	return !v.Less(otherV)
}

// String implements the fmt.Stringer interface.
func (v Version) String() string { return redact.StringWithoutMarkers(v) }

// SafeFormat implements the redact.SafeFormatter interface.
func (v Version) SafeFormat(p redact.SafePrinter, _ rune) {
	if v.IsFinal() {
		p.Printf("%d.%d", v.Major, v.Minor)
		return
	}
	p.Printf("%d.%d-%d", v.Major, v.Minor, v.Internal)
}

// IsFinal returns true if this is a final version (as opposed to a transitional
// internal version during upgrade).
//
// A version is final iff Internal = 0.
func (v Version) IsFinal() bool {
	return v.Internal == 0
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

// ReleaseSeries is just the major.minor part of a Version.
type ReleaseSeries struct {
	Major int32
	Minor int32
}

func (s ReleaseSeries) String() string {
	return fmt.Sprintf("%d.%d", s.Major, s.Minor)
}

// Successor returns the next release series, if known. This is only guaranteed
// to work for versions from the minimum supported series up to the previous
// series.
func (s ReleaseSeries) Successor() (_ ReleaseSeries, ok bool) {
	res, ok := successorSeries[s]
	return res, ok
}

// successorSeries stores the successor for each series. We are only concerned
// with versions within our compatibility window, but there is no harm in
// populating more if they are known.
//
// When this map is updated, the expected result in TestReleaseSeriesSuccessor
// needs to be updated. Also note that clusterversion tests ensure that this map
// contains all necessary versions.
var successorSeries = map[ReleaseSeries]ReleaseSeries{
	{20, 1}: {20, 2},
	{20, 2}: {21, 1},
	{21, 1}: {21, 2},
	{21, 2}: {22, 1},
	{22, 1}: {22, 2},
	{22, 2}: {23, 1},
	{23, 1}: {23, 2},
	{23, 2}: {24, 1},
}

// ReleaseSeries obtains the release series for the given version. Specifically:
//   - if the version is final (Internal=0), the ReleaseSeries has the same major/minor.
//   - if the version is a transitional version during upgrade (e.g. v23.1-8),
//     the result is the next final version (e.g. v23.1).
//
// For non-final versions (which indicate an update to the next series), this
// requires knowledge of the next series; unknown non-final versions will return
// ok=false.
func (v Version) ReleaseSeries() (_ ReleaseSeries, ok bool) {
	base := ReleaseSeries{v.Major, v.Minor}
	if v.IsFinal() {
		return base, true
	}
	res, ok := base.Successor()
	return res, ok
}
