// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

// String implements the fmt.Stringer interface. The result is of the form
// "23.2" for final versions and "23.2-upgrading-to-24.1-step-004" for
// transitional internal versions during upgrade.
func (v Version) String() string { return redact.StringWithoutMarkers(v) }

// VersionMajorDevOffset is an offset we apply to major version numbers during
// development; see clusterversion.DevOffset for more information.
const VersionMajorDevOffset = 1_000_000

// SafeFormat implements the redact.SafeFormatter interface.
func (v Version) SafeFormat(p redact.SafePrinter, _ rune) {
	if v.IsFinal() {
		p.Printf("%d.%d", v.Major, v.Minor)
		return
	}
	// NB: Internal may be -1. This is the case for all fence versions for final
	// versions of a release. Handle it specially to avoid printing the -1, which
	// is confusable with the `-` separator.
	if v.Internal < 0 {
		if buildutil.CrdbTestBuild && v.Internal != -1 {
			panic(errors.Newf("%s should not have Internal less than -1", v))
		}
		p.Printf("%d.%d-upgrading-final-step", v.Major, v.Minor)
		return
	}
	// If the version is offset, remove the offset and add it back to the result. We want
	// 1000023.1-upgrading-to-1000023.2-step-002, not 1000023.1-upgrading-to-23.2-step-002.
	noOffsetVersion := v
	if v.Major > VersionMajorDevOffset {
		noOffsetVersion.Major -= VersionMajorDevOffset
	}
	if s, ok := noOffsetVersion.ReleaseSeries(); ok {
		if v.Major > VersionMajorDevOffset {
			s.Major += VersionMajorDevOffset
		}
		p.Printf("%d.%d-upgrading-to-%d.%d-step-%03d", v.Major, v.Minor, s.Major, s.Minor, v.Internal)
	} else {
		// This shouldn't happen in practice.
		p.Printf("%d.%d-upgrading-step-%03d", v.Major, v.Minor, v.Internal)
	}
}

// IsFinal returns true if this is a final version (as opposed to a transitional
// internal version during upgrade).
//
// A version is final iff Internal = 0.
func (v Version) IsFinal() bool {
	return v.Internal == 0
}

// IsFence returns true if this is a fence version.
//
// A version is a fence version iff Internal is odd.
func (v Version) IsFence() bool {
	// NB: Internal may be -1. This is the case for all fence versions for final
	// versions of a release.
	return v.Internal%2 != 0
}

// PrettyPrint returns the value in a format that makes it apparent whether or
// not it is a fence version.
func (v Version) PrettyPrint() redact.RedactableString {
	if !v.IsFence() {
		return redact.Sprintf("%v", v)
	}
	return redact.Sprintf("%v(fence)", v)
}

// FenceVersion is the fence version -- the internal immediately prior -- for
// the given version.
//
// Fence versions allow the upgrades infrastructure to safely step through
// consecutive cluster versions in the presence of Nodes (running any binary
// version) being added to the cluster. See the upgrademanager package for
// intended usage.
//
// Fence versions (and the upgrades infrastructure entirely) were introduced in
// the 21.1 release cycle. In the same release cycle, we introduced the
// invariant that new user-defined versions (users being crdb engineers) must
// always have even-numbered Internal versions, thus reserving the odd numbers
// to slot in fence versions for each cluster version. See top-level
// documentation in the clusterversion package for more details.
func (v Version) FenceVersion() Version {
	if v.IsFence() {
		panic(errors.Newf("%s already is a fence version", v))
	}
	// NB: Internal may be -1 after this. This is the case for all final versions
	// for a release.
	fenceV := v
	fenceV.Internal--
	return fenceV
}

var (
	verPattern = regexp.MustCompile(
		`^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)(|-upgrading-final-step|(-|-upgrading(|-to-[0-9]+.[0-9]+)-step-)(?P<internal>[-0-9]+))$`,
	)
	verPatternMajorIdx    = verPattern.SubexpIndex("major")
	verPatternMinorIdx    = verPattern.SubexpIndex("minor")
	verPatternInternalIdx = verPattern.SubexpIndex("internal")
)

// ParseVersion parses a Version from a string of one of the forms:
//   - "<major>.<minor>"
//   - "<major>.<minor>-upgrading-to-<nextmajor>.<nextminor>-step-<internal>"
//   - "<major>.<minor>-<internal>" (older version of the above)
//
// We don't use the Patch component, so it is always zero.
func ParseVersion(s string) (Version, error) {
	matches := verPattern.FindStringSubmatch(s)
	if matches == nil {
		return Version{}, errors.Errorf("invalid version %s", s)
	}

	var err error
	toInt := func(s string) int32 {
		if err != nil || s == "" {
			return 0
		}
		var n int64
		n, err = strconv.ParseInt(s, 10, 32)
		return int32(n)
	}
	v := Version{
		Major: toInt(matches[verPatternMajorIdx]),
		Minor: toInt(matches[verPatternMinorIdx]),
	}
	// NB: Internal is -1 for all fence versions for final versions of a release.
	if strings.Contains(s, "-upgrading-final-step") {
		v.Internal = -1
	} else {
		v.Internal = toInt(matches[verPatternInternalIdx])
	}
	if err != nil {
		return Version{}, errors.Wrapf(err, "invalid version %s", s)
	}
	return v, nil
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
	{24, 1}: {24, 2},
	{24, 2}: {24, 3},
	{24, 3}: {25, 1},
	{25, 1}: {25, 2},
}

// ReleaseSeries obtains the release series for the given version. Specifically:
//   - if the version is final (Internal=0), the ReleaseSeries has the same major/minor.
//   - if the version is a transitional version during upgrade (e.g. v23.1-8),
//     the result is the next final version (e.g. v23.2).
//   - if the internal version is -1 (which is the case for the fence
//     version of a final version), the result has the same major/minor.
//
// For non-final versions (which indicate an update to the next series), this
// requires knowledge of the next series; unknown non-final versions will return
// ok=false.
//
// Note that if the version has the clusterversion.DevOffset applied, the
// resulting release series will have it too.
func (v Version) ReleaseSeries() (s ReleaseSeries, ok bool) {
	base := ReleaseSeries{v.Major, v.Minor}
	if v.IsFinal() {
		return base, true
	}
	// NB: Internal may be -1. This is the case for all fence versions for final
	// versions of a release.
	if v.Internal < 0 {
		if buildutil.CrdbTestBuild && v.Internal != -1 {
			panic(errors.Newf("%s should not have Internal less than -1", v))
		}
		return base, true
	}
	s, ok = base.Successor()
	return s, ok
}
