// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// PartitionState enumerates the possible states in the state machine that
// governs building and maintaining a K-means tree in the C-SPANN index.
type PartitionState int

// ParsePartitionState returns the given string as a PartitionState. If the
// string is not recognized, it returns MissingState.
func ParsePartitionState(s string) PartitionState {
	switch s {
	case "Ready":
		return ReadyState
	case "Splitting":
		return SplittingState
	case "Merging":
		return MergingState
	case "Updating":
		return UpdatingState
	case "DrainingForSplit":
		return DrainingForSplitState
	case "DrainingForMerge":
		return DrainingForMergeState
	case "AddingLevel":
		return AddingLevelState
	case "RemovingLevel":
		return RemovingLevelState
	}
	return MissingState
}

// AllowAddOrRemove returns true if vectors can be added or removed to/from the
// partition in this state.
func (s PartitionState) AllowAddOrRemove() bool {
	switch s {
	case ReadyState, SplittingState, MergingState, UpdatingState,
		AddingLevelState, RemovingLevelState:
		return true
	}
	return false
}

// String returns the state formatted as a string.
func (s PartitionState) String() string {
	switch s {
	case MissingState:
		return "Missing"
	case ReadyState:
		return "Ready"
	case SplittingState:
		return "Splitting"
	case MergingState:
		return "Merging"
	case UpdatingState:
		return "Updating"
	case DrainingForSplitState:
		return "DrainingForSplit"
	case DrainingForMergeState:
		return "DrainingForMerge"
	case AddingLevelState:
		return "AddingLevel"
	case RemovingLevelState:
		return "RemovingLevel"
	}
	return "Unknown"
}

const (
	// MissingState indicates that the partition does not exist, either because
	// it was deleted or because it never existed in the first place.
	MissingState PartitionState = iota
	// ReadyState indicates that the partition can be searched, can have vectors
	// added or removed, and can be split or merged.
	ReadyState
	// SplittingState indicates that empty sub-partitions are being created to
	// receive vectors from a split of this partition. Searches, inserts, and
	// deletes are still allowed, but not merges.
	SplittingState
	// MergingState indicates that vectors in the paritition are about to be moved
	// into other partitions at the same level. Searches, inserts, and deletes are
	// still allowed, but not splits.
	MergingState
	// UpdatingState indicates that the partition is a target of an ongoing split
	// or merge operation. Searches, inserts, and deletes are allowed, but not
	// splits or merges.
	UpdatingState
	// DrainingForSplitState indicates that the partition is actively moving
	// vectors to target split sub-partitions. Searches are allowed, but not
	// inserts, deletes, splits, or merges.
	DrainingForSplitState
	// DrainingForMergeState indicates that the partition is actively moving
	// vectors into other partitions at the same level (or deleting vectors if
	// this is the root partition). Searches are allowed, but not inserts,
	// deletes, splits, or merges.
	DrainingForMergeState
	// AddingLevelState indicates that a root partition has been drained after a
	// split and has had its level increased by one. What remains is to add the
	// target split sub-partitions to the partition. Searches, inserts, and
	// deletes are allowed, but not splits or merges.
	AddingLevelState
	// RemovingLevelState indicates that a root partition has been drained as
	// part of merging in the last remaining child and has had its level
	// decreased by one. What remains is to move vectors from the child into the
	// root. Searches, inserts, and deletes are allowed, but not splits or merges.
	RemovingLevelState
)

// PartitionStateDetails records information about the current state of a
// partition. It is embedded within PartitionMetadata.
type PartitionStateDetails struct {
	// State is the current state of the partition state machine. See the above
	// state machine diagram for details.
	State PartitionState
	// Target1 is set differently depending on the state:
	// - SplittingState, DrainingForSplitState, AddingLevelState: key of a new
	//   partition which will contain a subset of the vectors in the splitting
	//   partition.
	// - DrainingForMergeState (non-root): key of an existing partition at the
	//   same level where vectors can be inserted if no better partition has been
	//   found; this guarantees that inserts always find a target partition that
	//   allows inserts, even when racing with other operations.
	// - DrainingForMergeState (root): key of the last remaining child partition
	//   that will be merged into the root partition.
	Target1 PartitionKey
	// Target2 is set the same way as Target1 for these states:
	// SplittingState, DrainingForSplitState, AddingLevelState.
	Target2 PartitionKey
	// Source is set differently depending on the state:
	// - UpdatingState: key of the partition that is currently moving vectors into
	//   this partition as part of a split or merge. One of the source partition's
	//   targets points to this partition.
	// - RemovingLevelState: key of the last remaining child partition that will
	//   be merged into the root partition.
	Source PartitionKey
	// Timestamp is the time of the last state transition for the partition.
	Timestamp time.Time
}

// MakeReadyDetails constructs state for a Ready partition.
func MakeReadyDetails() PartitionStateDetails {
	return PartitionStateDetails{
		State:     ReadyState,
		Timestamp: timeutil.Now(),
	}
}

// MakeSplittingDetails constructs state for a Splitting partition, including
// the two target sub-partitions to which vectors are copied.
func MakeSplittingDetails(target1, target2 PartitionKey) PartitionStateDetails {
	return PartitionStateDetails{
		State:     SplittingState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: timeutil.Now(),
	}
}

// MakeDrainingForSplitDetails constructs state for a Splitting partition,
// including the two target sub-partitions to which vectors are copied.
func MakeDrainingForSplitDetails(target1, target2 PartitionKey) PartitionStateDetails {
	return PartitionStateDetails{
		State:     DrainingForSplitState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: timeutil.Now(),
	}
}

// MakeUpdatingDetails constructs state for an Updating partition, including the
// source partition from which vectors are being copied.
func MakeUpdatingDetails(source PartitionKey) PartitionStateDetails {
	return PartitionStateDetails{
		State:     UpdatingState,
		Source:    source,
		Timestamp: timeutil.Now(),
	}
}

// MakeAddingLevelDetails constructs state for an AddingLevel partition,
// including the target partitions which will become the new children of the
// root partition.
func MakeAddingLevelDetails(target1, target2 PartitionKey) PartitionStateDetails {
	return PartitionStateDetails{
		State:     AddingLevelState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: timeutil.Now(),
	}
}

// stalledOpTimeout specifies how long a partition can remain in a non-ready
// state before other fixup workers conclude a fixup has stalled (e.g. because
// the original worker crashed) and attempt to assist. If this is set too high,
// then a fixup can get stuck for too long. If it is set too low, then multiple
// workers can assist at the same time, resulting in duplicate work.
// TODO(andyk): Consider making this more dynamic, e.g. with
// livenesspb.NodeVitalityInterface. Also, consider making a cluster setting.
var stalledOpTimeout = 100 * time.Millisecond

// MaybeSplitStalled returns true if this partition has been in a splitting
// state for longer than the timeout, possibly indicating the fixup is stalled.
func (psd *PartitionStateDetails) MaybeSplitStalled() bool {
	if psd.State == SplittingState || psd.State == DrainingForSplitState {
		return timeutil.Since(psd.Timestamp) > stalledOpTimeout
	}
	return false
}

// MaybeMergeStalled returns true if this partition has been in a merging
// state for longer than the timeout, possibly indicating the fixup is stalled.
func (psd *PartitionStateDetails) MaybeMergeStalled() bool {
	if psd.State == MergingState || psd.State == DrainingForMergeState {
		return timeutil.Since(psd.Timestamp) > stalledOpTimeout
	}
	return false
}

// String returns the partition state details formatted as a string in this
// format:
//
//	Ready
//	Splitting:2,3
//	Updating:4
func (psd *PartitionStateDetails) String() string {
	var buf bytes.Buffer
	buf.WriteString(psd.State.String())
	if psd.Target1 != InvalidKey {
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(int(psd.Target1)))
		if psd.Target2 != InvalidKey {
			buf.WriteByte(',')
			buf.WriteString(strconv.Itoa(int(psd.Target2)))
		}
	} else if psd.Source != InvalidKey {
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(int(psd.Source)))
	}
	return buf.String()
}

// Level specifies a level in the K-means tree. Levels are numbered from leaf to
// root, in ascending order, with the leaf level always equal to one.
type Level uint32

const (
	// InvalidLevel is the default (invalid) value for a K-means tree level.
	InvalidLevel Level = 0
	// LeafLevel is the well-known identifier for the K-means leaf level.
	LeafLevel Level = 1
	// SecondLevel is the well-known identifier for the level above the leaf
	// level.
	SecondLevel Level = 2
)

// PartitionMetadata contains metadata about the partition that is stored
// alongside the data.
type PartitionMetadata struct {
	// Level is the level of the partition in the K-means tree.
	Level Level
	// Centroid is the centroid for vectors in the partition. It is calculated
	// once when the partition is created and never changes, even if additional
	// vectors are added later.
	Centroid vector.T
	// StateDetails records details about the partition's current state, used to
	// track progress during splits and merges.
	StateDetails PartitionStateDetails
}

// metadataEquals returns true if the two metadata structs are the same.
func (md *PartitionMetadata) Equal(other *PartitionMetadata) bool {
	if md.Level != other.Level {
		return false
	}
	if md.StateDetails != other.StateDetails {
		return false
	}

	// Fast compare centroids for pointer equality; fall back to slow compare.
	if md.Centroid != nil && other.Centroid != nil {
		if &md.Centroid[0] == &other.Centroid[0] {
			return true
		}
	}
	return slices.Equal(md.Centroid, other.Centroid)
}
