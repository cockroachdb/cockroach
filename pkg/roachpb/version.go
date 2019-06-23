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

	"github.com/pkg/errors"
)

// CanBump computes whether an update from version v to version o is possible.
// The main constraint is that either nothing changes; or the major version is
// bumped by one (and the new minor is zero); or the major version remains
// constant and the minor version is bumped by one. The unstable version may
// change without restrictions.
func (v Version) CanBump(o Version) bool {
	if o.Less(v) {
		return false
	}
	if o.Patch != 0 {
		// Reminder that we don't use Patch versions at all. It's just a
		// placeholder.
		return false
	}

	if o.Major == v.Major {
		return o.Minor <= v.Minor+1
	}
	if o.Major <= 2 {
		// The semver era, 1.0 to 2.1
		return o.Major == v.Major+1 && o.Minor == 0
	} else if v.Major == 2 && o.Major == 19 {
		// Transitioning from 2.1 to 19.1
		return v.Minor == 1 && o.Minor == 1
	}
	// The calver era, 19.1 and onwards.
	return o.Major == v.Major+1 && o.Minor == 1
}

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
	if v.Unstable < otherV.Unstable {
		return true
	} else if v.Unstable > otherV.Unstable {
		return false
	}
	return false
}

func (v Version) String() string {
	if v.Unstable == 0 {
		return fmt.Sprintf("%d.%d", v.Major, v.Minor)
	}
	return fmt.Sprintf("%d.%d-%d", v.Major, v.Minor, v.Unstable)
}

// ParseVersion parses a Version from a string of the form
// "<major>.<minor>-<unstable>" where the "-<unstable>" is optional. We don't
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
			return c, errors.Errorf("invalid version %s: %s", s, err)
		}
	}

	c.Major = int32(ints[0])
	c.Minor = int32(ints[1])
	c.Unstable = int32(ints[2])

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
