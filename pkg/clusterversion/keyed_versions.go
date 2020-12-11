// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversion

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// keyedVersion associates a key to a version.
type keyedVersion struct {
	Key Key
	roachpb.Version
}

// keyedVersions is a container for managing the versions of CockroachDB.
type keyedVersions []keyedVersion

// MustByKey asserts that the version specified by this key exists, and returns it.
func (kv keyedVersions) MustByKey(k Key) roachpb.Version {
	key := int(k)
	if key >= len(kv) || key < 0 {
		log.Fatalf(context.Background(), "version with key %d does not exist, have:\n%s",
			key, pretty.Sprint(kv))
	}
	return kv[key].Version
}

// Validate makes sure that the keyedVersions are sorted chronologically, that
// their keys correspond to their position in the list, and that no obsolete
// versions (i.e. known to always be active) are present.
//
// For versions introduced in v21.1 and beyond, the internal versions must be
// even.
func (kv keyedVersions) Validate() error {
	type majorMinor struct {
		major, minor int32
		vs           []keyedVersion
	}
	// byRelease maps major.minor to a slice of versions that were first
	// released right after major.minor. For example, a version 20.1-12 would
	// first be released in 20.2, and would be slotted under 20.1. We'll need
	// this to determine which versions are always active with a binary built
	// from the current SHA.
	var byRelease []majorMinor
	for i, namedVersion := range kv {
		if int(namedVersion.Key) != i {
			return errors.Errorf("version %s should have key %d but has %d",
				namedVersion, i, namedVersion.Key)
		}
		if i > 0 {
			prev := kv[i-1]
			if !prev.Version.Less(namedVersion.Version) {
				return errors.Errorf("version %s must be larger than %s", namedVersion, prev)
			}
		}
		mami := majorMinor{major: namedVersion.Major, minor: namedVersion.Minor}
		n := len(byRelease)
		if n == 0 || byRelease[n-1].major != mami.major || byRelease[n-1].minor != mami.minor {
			// Add new entry to the slice.
			byRelease = append(byRelease, mami)
			n++
		}
		// Add to existing entry.
		byRelease[n-1].vs = append(byRelease[n-1].vs, namedVersion)
	}

	// Iterate through all versions known to be active. For example, if
	//
	//   byRelease = ["19.1", "19.2", "20.1", "20.2"]
	//
	// then we know that the current release cycle is 21.1, so mixed version
	// clusters are running at least 20.2, so anything slotted under 20.1 (like
	// 20.1-12) and 19.2 is always-on. To avoid interfering with backports, we're
	// a bit more lenient and allow one more release cycle until validation fails.
	// In the above example, we would tolerate 20.1-x but not 19.2-x.
	// Currently we're actually a few versions behind in enforcing a ban on old
	// versions/migrations. See #47447.
	if n := len(byRelease) - 5; n >= 0 {
		var buf strings.Builder
		for i, mami := range byRelease[:n+1] {
			s := "next release"
			if i+1 < len(byRelease)-1 {
				nextMM := byRelease[i+1]
				s = fmt.Sprintf("%d.%d", nextMM.major, nextMM.minor)
			}
			for _, nv := range mami.vs {
				fmt.Fprintf(&buf, "introduced in %s: %s\n", s, nv.Key)
			}
		}
		mostRecentRelease := byRelease[len(byRelease)-1]
		return errors.Errorf(
			"found versions that are always active because %d.%d is already "+
				"released; these should be removed:\n%s",
			mostRecentRelease.minor, mostRecentRelease.major,
			buf.String(),
		)
	}

	// Check to see that for versions introduced in v21.1 and beyond, the
	// internal versions are always even. The odd versions are used for internal
	// book-keeping.
	for _, release := range byRelease {
		// v21.1 versions (20.2-x) will be slotted under 20.2. Ignore all other
		// releases.
		if release.major < 20 {
			continue
		}
		if release.major == 20 && release.minor == 1 {
			continue
		}

		for _, v := range release.vs {
			if (v.Internal % 2) != 0 {
				return errors.Errorf("found version %s with odd-numbered internal version (%s);"+
					" versions introduced in 21.1+ must have even-numbered internal versions", v.Key, v.Version)
			}
		}
	}

	return nil
}
