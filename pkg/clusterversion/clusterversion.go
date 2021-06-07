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
// The migration sub-package handles advancing a cluster from one version to
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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/redact"
)

// Initialize initializes the global cluster version. Before this method has
// been called, usage of the cluster version (through Handle) is illegal and
// leads to a fatal error.
//
// Initialization of the cluster version is tightly coupled with the setting of
// the active cluster version (`Handle.SetActiveVersion` below). Look towards
// there for additional commentary.
func Initialize(ctx context.Context, ver roachpb.Version, sv *settings.Values) error {
	return version.initialize(ctx, ver, sv)
}

// Handle is the interface through which callers access the active cluster
// version and this binary's version details.
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
	// this version. However, this is not atomic because version gates (for a
	// given version) are pushed through to each node concurrently. Because of
	// this, nodes should not be gating proper handling of remotely initiated
	// requests that their binary knows how to handle on this state. The
	// following example shows why this is important:
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
	IsActive(context.Context, Key) bool

	// BinaryVersion returns the build version of this binary.
	BinaryVersion() roachpb.Version

	// BinaryMinSupportedVersion returns the earliest binary version that can
	// interoperate with this binary.
	BinaryMinSupportedVersion() roachpb.Version

	// SetActiveVersion lets the caller set the given cluster version as the
	// currently active one. When a new active version is set, all subsequent
	// calls to `ActiveVersion`, `IsActive`, etc. will reflect as much. The
	// ClusterVersion supplied here is one retrieved from other node.
	//
	// This has a very specific intended usage pattern, and is probably only
	// appropriate for usage within the BumpClusterVersion RPC and during server
	// initialization.
	//
	// NB: It's important to note that this method is tightly coupled to cluster
	// version initialization (through `Initialize` above) and the version
	// persisted to disk. Specifically the following invariant must hold true:
	//
	//  If a version vX is active on a given server, upon restart, the version
	//  that is immediately active must be >= vX (in practice it'll almost
	//  always be vX).
	//
	// This is currently achieved by always durably persisting the target
	// cluster version to the store local keys.StoreClusterVersionKey() before
	// setting it to be active. This persisted version is also consulted during
	// node restarts when initializing the cluster version, as seen by this
	// node.
	SetActiveVersion(context.Context, ClusterVersion) error
}

// handleImpl is a concrete implementation of Handle. It mostly relegates to the
// underlying cluster version setting, though provides a way for callers to
// override the binary and minimum supported versions (for tests usually).
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

// ActiveVersion implements the Handle interface.
func (v *handleImpl) ActiveVersion(ctx context.Context) ClusterVersion {
	return version.activeVersion(ctx, v.sv)
}

// ActiveVersionOrEmpty implements the Handle interface.
func (v *handleImpl) ActiveVersionOrEmpty(ctx context.Context) ClusterVersion {
	return version.activeVersionOrEmpty(ctx, v.sv)
}

// SetActiveVersion implements the Handle interface.
func (v *handleImpl) SetActiveVersion(ctx context.Context, cv ClusterVersion) error {
	// We only perform binary version validation here. SetActiveVersion is only
	// called on cluster versions received from other nodes (where `SET CLUSTER
	// SETTING version` was originally called). The stricter form of validation
	// happens there. SetActiveVersion is simply the cluster version bump that
	// follows from it.
	if err := version.validateBinaryVersions(cv.Version, v.sv); err != nil {
		return err
	}

	encoded, err := protoutil.Marshal(&cv)
	if err != nil {
		return err
	}

	version.SetInternal(ctx, v.sv, encoded)
	return nil
}

// IsActive implements the Handle interface.
func (v *handleImpl) IsActive(ctx context.Context, key Key) bool {
	return version.isActive(ctx, v.sv, key)
}

// BinaryVersion implements the Handle interface.
func (v *handleImpl) BinaryVersion() roachpb.Version {
	return v.binaryVersion
}

// BinaryMinSupportedVersion implements the Handle interface.
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
func (cv ClusterVersion) IsActive(versionKey Key) bool {
	v := ByKey(versionKey)
	return cv.IsActiveVersion(v)
}

func (cv ClusterVersion) String() string {
	return redact.StringWithoutMarkers(cv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (cv ClusterVersion) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(cv.Version)
}

// PrettyPrint returns the value in a format that makes it apparent whether or
// not it is a fence version.
func (cv ClusterVersion) PrettyPrint() string {
	// If we're a version greater than v20.2 and have an odd internal version,
	// we're a fence version. See fenceVersionFor in pkg/migration to understand
	// what these are.
	fenceVersion := !cv.Version.LessEq(roachpb.Version{Major: 20, Minor: 2}) && (cv.Internal%2) == 1
	if !fenceVersion {
		return cv.String()
	}
	return redact.Sprintf("%s%s", cv.String(), "(fence)").StripMarkers()
}

// ClusterVersionImpl implements the settings.ClusterVersionImpl interface.
func (cv ClusterVersion) ClusterVersionImpl() {}

var _ settings.ClusterVersionImpl = ClusterVersion{}

// EncodingFromVersionStr is a shorthand to generate an encoded cluster version
// from a version string.
func EncodingFromVersionStr(v string) ([]byte, error) {
	newV, err := roachpb.ParseVersion(v)
	if err != nil {
		return nil, err
	}
	newCV := ClusterVersion{Version: newV}
	return protoutil.Marshal(&newCV)
}
