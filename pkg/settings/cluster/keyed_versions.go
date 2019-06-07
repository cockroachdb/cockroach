// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// keyedVersion associates a key to a version.
type keyedVersion struct {
	Key VersionKey
	roachpb.Version
}

// keyedVersions is a container for managing the versions of CockroachDB.
type keyedVersions []keyedVersion

// MustByKey asserts that the version specified by this key exists, and returns it.
func (kv keyedVersions) MustByKey(k VersionKey) roachpb.Version {
	key := int(k)
	if key >= len(kv) || key < 0 {
		log.Fatalf(context.Background(), "version with key %d does not exist, have:\n%s",
			key, pretty.Sprint(kv))
	}
	return kv[key].Version
}

// Validated makes sure that the keyedVersions are sorted chronologically, that
// their keys correspond to their position in the list, and that no obsolete
// versions (i.e. known to always be active) are present.
func (kv keyedVersions) Validate() error {
	type majorMinor struct {
		major, minor int32
		vs           []keyedVersion
	}
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

	if n := len(byRelease) - 3; n >= 0 {
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
	return nil
}
