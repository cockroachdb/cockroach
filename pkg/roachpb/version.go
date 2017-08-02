// Copyright 2017 The Cockroach Authors.
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

package roachpb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// CanBump computes whether an update from version o to version n is possible.
// The main constraint is that major and minor can't change at the same time,
// and that they can change by at most one. We are more flexible with the
// Unstable component, which may change by more than one (but also has to be
// changed in isolation; this could be relaxed if necessary).
func (v Version) CanBump(o Version) bool {
	if o.Less(v) {
		return false
	}
	unstableN := o.Unstable - v.Unstable
	if unstableN > 1 {
		unstableN = 1
	}

	var dist int32
	for _, d := range []int32{
		o.Major - v.Major,
		o.Minor - v.Minor,
		o.Patch - v.Patch,
		unstableN,
	} {
		if d > 0 {
			dist += d
		}
	}
	return dist <= 1
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
