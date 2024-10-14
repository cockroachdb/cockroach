// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterversion

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
)

// devOffsetKeyStart is the key at or above which we apply the DevOffset major
// version adjustment.
//
// If this is a dev branch, we offset every version +1M major versions into the
// future. This means a cluster that runs the migrations in a dev build, while
// they are still in flux, will persist this offset version, and thus cannot
// then "upgrade" to the released build, as its non-offset versions would then
// be a downgrade, which is blocked.
//
// By default, when offsetting versions in a dev binary, we offset *all of
// them*, which includes the minimum version from which upgrades are supported.
// This means a dev binary cannot join, resume or upgrade a release version
// cluster, which is by design as it avoids unintentionally but irreversibly
// upgrading a cluster to dev versions. Opting in to such an upgrade is possible
// however via setting COCKROACH_UPGRADE_TO_DEV_VERSION. Doing so skips
// offsetting the earliest version this binary supports, meaning it will support
// an upgrade from as low as that released version that then advances into the
// dev-numbered versions.
//
// COCKROACH_UPGRADE_TO_DEV_VERSION must be used very carefully - it can
// effectively cause *downgrades* of the logical version! For example, on a
// cluster that is on released version 3 with minimum supported version 2, a dev
// binary containing versions 1, 2, 3, and 4 started with this flag would
// renumber only 2-4 to be +1M. It would then step from 3 "up" to 1000002 -
// which conceptually is actually back down to 2 - then back to 1000003, then on
// to 1000004, etc.
//
// Version offsetting is controlled by the developmentBranch constant, but can
// be overridden with env vars:
//   - COCKROACH_TESTING_FORCE_RELEASE_BRANCH=1 disables version offsetting
//     unconditionally; it is used for certain upgrade tests that are trying to
//     simulate a "real" upgrade.
//   - COCKROACH_FORCE_DEV_VERSION=1 enables version offsetting unconditionally;
//     it is useful if we want to run a final release binary in a cluster that
//     was already created with a dev version.
var devOffsetKeyStart = func() Key {
	forceDev := envutil.EnvOrDefaultBool("COCKROACH_FORCE_DEV_VERSION", false)
	forceRelease := envutil.EnvOrDefaultBool("COCKROACH_TESTING_FORCE_RELEASE_BRANCH", false)
	if forceDev && forceRelease {
		panic(errors.AssertionFailedf("cannot set both COCKROACH_FORCE_DEV_VERSION and COCKROACH_TESTING_FORCE_RELEASE_BRANCH"))
	}
	isDev := (DevelopmentBranch || forceDev) && !forceRelease
	if !isDev {
		// No dev offsets.
		return numKeys + 1
	}
	// If COCKROACH_UPGRADE_TO_DEV_VERSION is set, we allow updating from the
	// minimum supported release, so we only apply the dev offset to subsequent
	// versions.
	allowUpgradeToDev := envutil.EnvOrDefaultBool("COCKROACH_UPGRADE_TO_DEV_VERSION", false)
	if allowUpgradeToDev {
		return MinSupported + 1
	}
	// Apply the dev offset to all versions (except VBootstrap versions, which
	// don't matter for offsetting logic).
	return VBootstrapMax + 1
}()

// DevOffset is the offset applied to major versions into the future if this is
// a dev branch.
const DevOffset = roachpb.VersionMajorDevOffset

// maybeApplyDevOffset applies DevOffset to the major version, if appropriate.
func maybeApplyDevOffset(key Key, v roachpb.Version) roachpb.Version {
	if key >= devOffsetKeyStart {
		v.Major += DevOffset
	}
	return v
}

// RemoveDevOffset removes DevOffset from the given version, if it was applied.
func RemoveDevOffset(v roachpb.Version) roachpb.Version {
	if v.Major > DevOffset {
		v.Major -= DevOffset
	}
	return v
}
