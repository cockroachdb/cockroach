// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"context"

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

func (kv keyedVersions) LastVersion() roachpb.Version {
	return kv[len(kv)-1].Version
}
