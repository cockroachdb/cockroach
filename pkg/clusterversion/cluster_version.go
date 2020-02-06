// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clusterversion is a logical version that allows for backward
// incompatible behaviors.
//
// Ideally, every code change in a database would be backward compatible, but
// this is not always possible. Some features, fixes, or cleanups need to
// introduce a backward incompatiblity and others are dramatically simplified by
// it. This package provides a way to do this safely with (hopefully) minimal
// disruption. It works as follows:
//
// - Each node in the cluster is running a binary that was released as some
//   version. We allow for rolling upgrades, so two nodes in the cluster may be
//   running different binary versions. All nodes in a given cluster must be
//   within 1 major release of each other (i.e. to upgrade two major releases,
//   the cluster must first be rolled onto X+1 and then onto X+2).
// - Separate from the build versions of the binaries, the cluster itself has a
//   logical "cluster version". This is used for two related things: first as a
//   promise from the user that they'll never downgrade any nodes in the cluster
//   to a binary below some version and second to unlock features that are
//   backward compatible (which is now safe given that the old binary will never
//   be used).
// - Each binary can operation within a range of some cluster versions. When a
//   cluster is initializes, the binary doing the initialization uses the upper
//   end of its supported range as the initial cluster version. Each node that
//   joins this cluster then must be compatible with this cluster version.
//
// This package handles the feature gates and so must maintain a fairly
// lightweight set of dependencies. The migration sub-package handles advancing
// a cluster from one version to a later one.
package clusterversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// IsActiveVersion returns true if the features of the supplied version are active at the running
// version.
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

// Handle is a read-only view
type Handle interface {
	ActiveVersion(context.Context) ClusterVersion
	ActiveVersionOrEmpty(context.Context) ClusterVersion
	IsActive(context.Context, VersionKey) bool
	BinaryVersion() roachpb.Version
	BinaryMinSupportedVersion() roachpb.Version
}
