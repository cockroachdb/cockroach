// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/pebble/vfs"
)

// BasaltStoreScratchDir returns the store-level scratch directory that contains
// all scratch files. With basaltfs flat-dirs, per-range scratch subdirectories
// are flattened to prefixed files in this directory.
// Path format: s<storeID>/scratch
func BasaltStoreScratchDir(basaltFS vfs.FS, storeID roachpb.StoreID) string {
	return basaltFS.PathJoin(fmt.Sprintf("s%d", storeID), "scratch")
}

// BasaltStoreRangesDir returns the store-level ranges directory that contains
// all range-shared engine data files. With basaltfs flat-dirs, per-range
// subdirectories are flattened to prefixed files in this directory.
// Path format: s<storeID>/ranges
func BasaltStoreRangesDir(basaltFS vfs.FS, storeID roachpb.StoreID) string {
	return basaltFS.PathJoin(fmt.Sprintf("s%d", storeID), "ranges")
}

// BasaltDir returns the Basalt data directory path for a range-shared engine.
// Path format: s<storeID>/ranges/r<rangeID>:<replicaID>
//
// With basaltfs flat-dirs configured for "ranges", this path is not a real
// directory. Instead, basaltfs flattens it so that files like
// s<storeID>/ranges/r<rangeID>:<replicaID>/MANIFEST-066 are stored as
// s<storeID>/ranges/r<rangeID>:<replicaID>.MANIFEST-066.
// On vfs.NewMem() (tests), the path creates a real subdirectory.
func BasaltDir(
	basaltFS vfs.FS, storeID roachpb.StoreID, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID,
) string {
	return basaltFS.PathJoin(
		BasaltStoreRangesDir(basaltFS, storeID),
		fmt.Sprintf("r%d:%d", rangeID, replicaID),
	)
}

// BasaltScratchDir returns the Basalt scratch directory path for a
// range-shared engine.
// Path format: s<storeID>/scratch/r<rangeID>:<replicaID>
func BasaltScratchDir(
	basaltFS vfs.FS, storeID roachpb.StoreID, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID,
) string {
	return basaltFS.PathJoin(
		BasaltStoreScratchDir(basaltFS, storeID),
		fmt.Sprintf("r%d:%d", rangeID, replicaID),
	)
}
