// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clusterversion defines the interfaces to interact with cluster/binary
// versions in order accommodate backward incompatible behaviors. It handles the
// feature gates and so must maintain a fairly lightweight set of dependencies.
// The migration sub-package will handle advancing a cluster from one version to
// a later one.
//
// Ideally, every code change in a database would be backward compatible, but
// this is not always possible. Some features, fixes, or cleanups need to
// introduce a backward incompatibility and others are dramatically simplified by
// it. This package provides a way to do this safely with (hopefully) minimal
// disruption. It works as follows:
//
// - Each node in the cluster is running a binary that was released at some
//   version ("binary version"). We allow for rolling upgrades, so two nodes in
//   the cluster may be running different binary versions. All nodes in a given
//   cluster must be within 1 major release of each other (i.e. to upgrade two
//   major releases, the cluster must first be rolled onto X+1 and then to X+2).
// - Separate from the build versions of the binaries, the cluster itself has a
//   logical "active cluster version", the version all the binaries are
//   currently operating at. This is used for two related things: first as a
//   promise from the user that they'll never downgrade any nodes in the cluster
//   to a binary below some "minimum supported version", and second, to unlock
//   features that are not backwards compatible (which is now safe given that
//   the old binary will never be used).
// - Each binary can operate within a "range of supported versions". When a
// 	 cluster is initialized, the binary doing the initialization uses the upper
//	 end of its supported range as the initial "active cluster version". Each
//	 node that joins this cluster then must be compatible with this cluster
//	 version.
package clusterversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(irfansharif): Should Initialize and SetBeforeChange be a part of the
// Handle interface? For SetBeforeChange at least, the callback is captured in
// the Handle implementation. Given that Initialize uses recorded state from a
// settings.Values (which is also a part of the Handle implementation), it seems
// appropriate. On the other hand, Handle.Initialize does not make it
// sufficiently clear that what's being initialized is the global cluster
// version setting, despite being done through a stand alone Handle.

// Initialize initializes the global cluster version. Before this method has
// been called, usage of the cluster version (through Handle) is illegal and
// leads to a fatal error.
func Initialize(ctx context.Context, ver roachpb.Version, sv *settings.Values) error {
	return version.initialize(ctx, ver, sv)
}

// SetBeforeChange registers a callback to be called before the global cluster
// version is updated. The new cluster version will only become "visible" after
// the callback has returned.
//
// The callback can be set at most once.
func SetBeforeChange(
	ctx context.Context, sv *settings.Values, cb func(context.Context, ClusterVersion),
) {
	version.setBeforeChange(ctx, cb, sv)
}

// Handle is a read-only view to the active cluster version and this binary's
// version details.
type Handle interface {
	// ActiveVersion returns the cluster's current active version: the minimum
	// cluster version the caller may assume is in effect.
	//
	// ActiveVersion fatals if the cluster version setting has not been
	// initialized (through `Initialize()`).
	ActiveVersion(context.Context) ClusterVersion

	// ActiveVersionOrEmpty is like ActiveVersion, but returns an empty version
	// if the active version was not initialized.
	ActiveVersionOrEmpty(context.Context) ClusterVersion

	// IsActive returns true if the features of the supplied version key are
	// active at the running version. In other words, if a particular version
	// `v` returns true from this method, it means that you're guaranteed that
	// all of the nodes in the cluster have running binaries that are at least
	// as new as `v`, and that those nodes will never be downgraded to a binary
	// with a version less than `v`.
	//
	// If this returns true then all nodes in the cluster will eventually see
	// this version. However, this is not atomic because versions are gossiped.
	// Because of this, nodes should not be gating proper handling of remotely
	// initiated requests that their binary knows how to handle on this state.
	// The following example shows why this is important:
	//
	//  The cluster restarts into the new version and the operator issues a SET
	//  VERSION, but node1 learns of the bump 10 seconds before node2, so during
	//  that window node1 might be receiving "old" requests that it itself
	//  wouldn't issue any more. Similarly, node2 might be receiving "new"
	//  requests that its binary must necessarily be able to handle (because the
	//  SET VERSION was successful) but that it itself wouldn't issue yet.
	//
	// This is still a useful method to have as node1, in the example above, can
	// use this information to know when it's safe to start issuing "new"
	// outbound requests. When receiving these "new" inbound requests, despite
	// not seeing the latest active version, node2 is aware that the sending
	// node has, and it will too, eventually.
	IsActive(context.Context, VersionKey) bool

	// BinaryVersion returns the build version of this binary.
	BinaryVersion() roachpb.Version

	// BinaryMinSupportedVersion returns the earliest binary version that can
	// interoperate with this binary.
	BinaryMinSupportedVersion() roachpb.Version
}

// handleImpl is a concrete implementation of Handle. It mostly relegates to the
// underlying cluster version setting, though provides a way for callers to
// override the binary and minimum supported versions. It also stores the
// callback that can be attached on cluster version change.
type handleImpl struct {
	// sv captures the mutable state associated with usage of the otherwise
	// immutable cluster version setting.
	sv *settings.Values

	// Each handler stores its own view of the binary and minimum supported
	// version. Tests can use `MakeVersionHandleWithOverride` to specify
	// versions other than the baked in ones, but by default
	// (`MakeVersionHandle`) they are initialized with this binary's build
	// and minimum supported versions.
	binaryVersion             roachpb.Version
	binaryMinSupportedVersion roachpb.Version

	// beforeClusterVersionChangeMu captures the callback that can be attached
	// to the cluster version setting via SetBeforeChange.
	beforeClusterVersionChangeMu struct {
		syncutil.Mutex
		// Callback to be called when the cluster version is about to be updated.
		cb func(ctx context.Context, newVersion ClusterVersion)
	}
}

var _ Handle = (*handleImpl)(nil)

// MakeVersionHandle returns a Handle that has its binary and minimum
// supported versions initialized to this binary's build and it's minimum
// supported versions respectively.
func MakeVersionHandle(sv *settings.Values) Handle {
	return MakeVersionHandleWithOverride(sv, binaryVersion, binaryMinSupportedVersion)
}

// MakeVersionHandleWithOverride returns a Handle that has its
// binary and minimum supported versions initialized to the provided versions.
//
// It's typically used in tests that want to override the default binary and
// minimum supported versions.
func MakeVersionHandleWithOverride(
	sv *settings.Values, binaryVersion, binaryMinSupportedVersion roachpb.Version,
) Handle {
	return &handleImpl{
		sv: sv,

		binaryVersion:             binaryVersion,
		binaryMinSupportedVersion: binaryMinSupportedVersion,
	}
}
func (v *handleImpl) ActiveVersion(ctx context.Context) ClusterVersion {
	return version.activeVersion(ctx, v.sv)
}

func (v *handleImpl) ActiveVersionOrEmpty(ctx context.Context) ClusterVersion {
	return version.activeVersionOrEmpty(ctx, v.sv)
}

func (v *handleImpl) IsActive(ctx context.Context, key VersionKey) bool {
	return version.isActive(ctx, v.sv, key)
}

func (v *handleImpl) BinaryVersion() roachpb.Version {
	return v.binaryVersion
}

func (v *handleImpl) BinaryMinSupportedVersion() roachpb.Version {
	return v.binaryMinSupportedVersion
}

// IsActiveVersion returns true if the features of the supplied version are
// active at the running version.
func (cv ClusterVersion) IsActiveVersion(v roachpb.Version) bool {
	return !cv.Less(v)
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (cv ClusterVersion) IsActive(versionKey VersionKey) bool {
	v := VersionByKey(versionKey)
	return cv.IsActiveVersion(v)
}

func (cv ClusterVersion) String() string {
	return cv.Version.String()
}
