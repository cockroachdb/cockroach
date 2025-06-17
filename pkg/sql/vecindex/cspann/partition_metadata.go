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
	case "DeletingForSplit":
		return DeletingForSplitState
	case "AddingLevel":
		return AddingLevelState
	case "RemovingLevel":
		return RemovingLevelState
	}
	return MissingState
}

// AllowAdd returns true if vectors can be added to the partition in this state.
func (s PartitionState) AllowAdd() bool {
	switch s {
	case ReadyState, SplittingState, MergingState, UpdatingState,
		AddingLevelState, RemovingLevelState:
		return true
	}
	return false
}

// CanSkipRemove returns true if there's no need to remove vectors from the
// partition because the partition is being drained or is about to be deleted.
func (s PartitionState) CanSkipRemove() bool {
	return !s.AllowAdd()
}

// AllowRemove returns true if vectors can be removed from the partition in this
// state.
func (s PartitionState) AllowRemove() bool {
	return s != MissingState
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
	case DeletingForSplitState:
		return "DeletingForSplitState"
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
	// inserts, splits, or merges.
	DrainingForSplitState
	// DrainingForMergeState indicates that the partition is actively moving
	// vectors into other partitions at the same level (or deleting vectors if
	// this is the root partition). Searches are allowed, but not inserts, splits,
	// or merges.
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
	// DeletingForSplitState indicates that a non-root splitting partition is
	// about to be removed from its tree and deleted. Searches are allowed, but
	// not inserts, splits, or merges.
	DeletingForSplitState
)

// PartitionStateDetails records information about the current state of a
// partition. It is embedded within PartitionMetadata.
type PartitionStateDetails struct {
	// State is the current state of the partition state machine. See the above
	// state machine diagram for details.
	State PartitionState
	// Target1 is set differently depending on the state:
	// - SplittingState, DrainingForSplitState, DeletingForSplitState,
	//   AddingLevelState: key of a new partition which will contain a subset of
	//   the vectors in the splitting partition.
	// - DrainingForMergeState (non-root): key of an existing partition at the
	//   same level where vectors can be inserted if no better partition has been
	//   found; this guarantees that inserts always find a target partition that
	//   allows inserts, even when racing with other operations.
	// - DrainingForMergeState (root): key of the last remaining child partition
	//   that will be merged into the root partition.
	Target1 PartitionKey
	// Target2 is set the same way as Target1 for these states:
	// SplittingState, DrainingForSplitState, DeletingForSplitState,
	// AddingLevelState.
	Target2 PartitionKey
	// Source is set differently depending on the state:
	// - UpdatingState: key of the partition that is currently moving vectors into
	//   this partition as part of a split or merge. One of the source partition's
	//   targets points to this partition.
	// - RemovingLevelState: key of the last remaining child partition that will
	//   be merged into the root partition.
	Source PartitionKey
	// Timestamp is the time of the last state transition for the partition.
	// NOTE: We use NowNoMono to get the current time because the monotonic clock
	// reading is not round-tripped through the encoding/decoding functions, since
	// it's not useful to store.
	Timestamp time.Time
}

// MakeReady sets Ready partition state details.
func (psd *PartitionStateDetails) MakeReady() {
	*psd = PartitionStateDetails{
		State:     ReadyState,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeSplitting sets Splitting partition state details, including the two
// target sub-partitions to which vectors are copied.
func (psd *PartitionStateDetails) MakeSplitting(target1, target2 PartitionKey) {
	*psd = PartitionStateDetails{
		State:     SplittingState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeDrainingForSplit sets DrainingForSplit partition state details, including
// the two target sub-partitions to which vectors are copied.
func (psd *PartitionStateDetails) MakeDrainingForSplit(target1, target2 PartitionKey) {
	*psd = PartitionStateDetails{
		State:     DrainingForSplitState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeDrainingForMerge sets DrainingForMerge partition state details, including
// the target sub-partition to which vectors can be copied.
func (psd *PartitionStateDetails) MakeDrainingForMerge(target PartitionKey) {
	*psd = PartitionStateDetails{
		State:     DrainingForMergeState,
		Target1:   target,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeUpdating sets Updating partition state details, including the source
// partition from which vectors are being copied.
func (psd *PartitionStateDetails) MakeUpdating(source PartitionKey) {
	*psd = PartitionStateDetails{
		State:     UpdatingState,
		Source:    source,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeAddingLevel sets AddingLevel partition state details, including the
// target partitions which will become the new children of the root partition.
func (psd *PartitionStateDetails) MakeAddingLevel(target1, target2 PartitionKey) {
	*psd = PartitionStateDetails{
		State:     AddingLevelState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// MakeDeletingForSplit sets DeletingForSplit partition state details.
func (psd *PartitionStateDetails) MakeDeletingForSplit(target1, target2 PartitionKey) {
	*psd = PartitionStateDetails{
		State:     DeletingForSplitState,
		Target1:   target1,
		Target2:   target2,
		Timestamp: getHigherTimestamp(psd.Timestamp),
	}
}

// getHigherTimestamp takes an existing timestamp (can be zero) and returns a
// time that's greater than the existing timestamp. It prefers to use the
// current time, but if that is not greater, it simply adds one nanosecond to
// the existing time.
func getHigherTimestamp(existing time.Time) time.Time {
	// Use NowNoMono since it is round-trippable. However, it only retains
	// millisecond precision on MacOS, so rapid calls can return the same value.
	ts := timeutil.NowNoMono()
	if ts.After(existing) {
		return ts
	}
	return existing.Add(time.Nanosecond)
}

// DefaultStalledOpTimeout specifies how long a split/merge operation can remain
// in the same state before another worker may attempt to assist.
var DefaultStalledOpTimeout = 100 * time.Millisecond

// MaybeSplitStalled returns true if this partition has been in a splitting
// state for longer than the timeout, possibly indicating the fixup is stalled.
func (psd *PartitionStateDetails) MaybeSplitStalled(stalledOpTimeout time.Duration) bool {
	switch psd.State {
	case SplittingState, DrainingForSplitState, DeletingForSplitState, AddingLevelState:
		return timeutil.Since(psd.Timestamp) > stalledOpTimeout
	}
	return false
}

// MaybeMergeStalled returns true if this partition has been in a merging
// state for longer than the timeout, possibly indicating the fixup is stalled.
func (psd *PartitionStateDetails) MaybeMergeStalled(stalledOpTimeout time.Duration) bool {
	switch psd.State {
	case MergingState, DrainingForMergeState, RemovingLevelState:
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
	if psd.State == ReadyState {
		// Short-circuit the common case.
		return "Ready"
	}

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

// HasSameTimestamp returns true if this metadata's timestamp is the same as the
// other's timestamp.
func (md *PartitionMetadata) HasSameTimestamp(other *PartitionMetadata) bool {
	return md.StateDetails.Timestamp.Equal(other.StateDetails.Timestamp)
}

// MakeReadyPartitionMetadata returns metadata for the given level and centroid,
// in the Ready state.
func MakeReadyPartitionMetadata(level Level, centroid vector.T) PartitionMetadata {
	metadata := PartitionMetadata{Level: level, Centroid: centroid}
	metadata.StateDetails.MakeReady()
	return metadata
}
