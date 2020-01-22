// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package fflag WIP
package fflag

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Handle is a read-only view of the cluster's currently supported features.
// This is based both on the binaries present in the cluster as well as the
// backward-incompatible migrations that have been run.
type Handle struct {
	mu struct {
		syncutil.Mutex
		version ClusterVersion
	}
}

// GetHandle returns a read-only handle for querying which features are enabled.
// The argument must be the BootVersion field from a ConnectResponse proto
// returned by connect.Connect.
//
// WIP how does this work for cluster init? separate method in this package?
func GetHandle(bootVersion ClusterVersion) *Handle {
	h := &Handle{}
	h.mu.version = bootVersion
	return h
}

// IsActive returns true if the features of the supplied version are active at
// the running version. This may change over time so the results of calling this
// method should not be cached.
func (h *Handle) IsActive(feature VersionKey) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	panic(`WIP`)
}

// Forward migrates this cluster to activate the target features.
func (h *Handle) Forward(ctx context.Context, target ClusterVersion) error {
	panic(`WIP`)
}

// VersionKey is cluster.VersionKey but moved here.
type VersionKey = int

// Feature constants, the same as the VersionKey ones in cluster.
const (
	Version19_2                            VersionKey = 0
	VersionStart20_1                       VersionKey = 1
	VersionNoNewPreemptiveSnapshotsStarted VersionKey = 2
	VersionNoPreemptiveSnapshotsOnDisk     VersionKey = 3
	VersionFillInTableDescriptor           VersionKey = 4
)

// keyedVersion is the same thing as cluster.keyedVersion.
type keyedVersion struct {
	Key VersionKey
	roachpb.Version
}

// versionsSingleton is the same thing as cluster.versionsSingleton.
var versionsSingleton = keyedVersions{
	{
		// Version19_2 is CockroachDB v19.2. It's used for all v19.2.x patch releases.
		Key:     Version19_2,
		Version: roachpb.Version{Major: 19, Minor: 2},
	},
	{
		// VersionStart20_1 demarcates work towards CockroachDB v20.1.
		Key:     VersionStart20_1,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 1},
	},
	{
		Key:     VersionNoNewPreemptiveSnapshotsStarted,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 2},
	},
	{
		Key:     VersionNoPreemptiveSnapshotsOnDisk,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 3},
	},
	{
		Key:     VersionFillInTableDescriptor,
		Version: roachpb.Version{Major: 19, Minor: 2, Unstable: 4},
	},
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

// VersionByKey returns the roachpb.Version for a given key.
// It is a fatal error to use an invalid key.
func VersionByKey(key VersionKey) roachpb.Version {
	return versionsSingleton.MustByKey(key)
}
