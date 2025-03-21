// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// PartitionKey uniquely identifies a partition. Keys should never be reused as
// partitions are created and deleted.
type PartitionKey uint64

const (
	// InvalidKey is the default (invalid) value for a partition key.
	InvalidKey PartitionKey = 0
	// RootKey is the well-known identifier for the root partition. All non-root
	// partition keys are greater than this.
	RootKey PartitionKey = 1
)

// TreeKey identifies a particular K-means tree among the forest of trees that
// make up a C-SPANN index. This enables partitioning of the index.
type TreeKey []byte

// KeyBytes refers to the unique row in the primary index that contains the
// indexed vector. It typically contains part or all of the primary key for the
// row.
type KeyBytes []byte

// ValueBytes are opaque bytes that are stored alongside the quantized vector
// and returned by searches. Depending on the store, this could be empty, or it
// could contain information associated with the vector, such as STORING
// columns.
type ValueBytes []byte

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

// PartitionState enumerates the possible states in the state machine that
// governs building and maintaining a K-means tree in the C-SPANN index.
type PartitionState int

// Here is the state transition diagram for partition states. There are a few
// possible flows:
// TODO(andyk): Separate each flow into its own diagram.
//
//	New Root      : Missing => Ready
//	Splitting     : Ready => Splitting => DrainingForSplit => Missing
//	Splitting Root: Ready => Splitting => DrainingForSplit => AddingLevel => Missing
//	Split Target  : Missing => Updating => Ready
//	Merging       : Ready => Merging => DrainingForMerge => Missing
//	Merge Target  : Ready => Updating => Ready
//	Merging Root  : Ready => DrainingForMerge => RemovingLevel => Ready
//
// .            +-------------------+
// +----------->|      Missing      |<---------------------+
// |            +----+---------+----+                      |
// |                 |         |                           |
// |          +------v-----+   |                           |
// |          |  Updating  |   |                           |
// |          +--+------^--+   |                           |
// |             |      |      |                           |
// |         +---v------+------v-----------+ if Root       |
// |         |            Ready            |-------+       |
// |         +---+---------------------+---+       |       |
// |             |                     |           |       |
// |     +-------v--------+    +-------v--------+  |       |
// |     |    Splitting   |    |    Merging     |  |       |
// |     +-------+--------+    +-------+--------+  |       |
// |             |                     |           |       |
// |     +-------v--------+    +-------v-----------v-+     |
// +-----|DrainingForSplit|    |  DrainingForMerge   |-----+
// |     +-------+--------+    +-------+-------------+
// |             | if Root             | if Root
// |     +-------v--------+    +-------v--------+
// +-----|  AddingLevel   |    | RemovingLevel  |----> Ready
// .     +----------------+    +----------------+
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

// Partition contains a set of quantized vectors that are clustered around a
// centroid in some level of the K-means tree. Each vector is associated with a
// primary key if this is a leaf partition, or a child partition key if this is
// a branch/root partition.
type Partition struct {
	metadata     PartitionMetadata
	quantizer    quantize.Quantizer
	quantizedSet quantize.QuantizedVectorSet
	childKeys    []ChildKey
	valueBytes   []ValueBytes
}

// NewPartition constructs a new partition.
func NewPartition(
	metadata PartitionMetadata,
	quantizer quantize.Quantizer,
	quantizedSet quantize.QuantizedVectorSet,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
) *Partition {
	return &Partition{
		metadata:     metadata,
		quantizer:    quantizer,
		quantizedSet: quantizedSet,
		childKeys:    childKeys,
		valueBytes:   valueBytes,
	}
}

// Clone makes a deep copy of this partition. Changes to the original or clone
// do not affect the other.
func (p *Partition) Clone() *Partition {
	return &Partition{
		metadata:     p.metadata,
		quantizer:    p.quantizer,
		quantizedSet: p.quantizedSet.Clone(),
		childKeys:    slices.Clone(p.childKeys),
		valueBytes:   slices.Clone(p.valueBytes),
	}
}

// Metadata returns metadata for the partition.
func (p *Partition) Metadata() *PartitionMetadata {
	return &p.metadata
}

// Count is the number of quantized vectors in the partition.
func (p *Partition) Count() int {
	return len(p.childKeys)
}

// Level is the level of this partition in the K-means tree. The leaf level
// always has the well-known value of one.
func (p *Partition) Level() Level {
	return p.metadata.Level
}

// Quantizer is the quantizer used to quantize vectors in this partition.
func (p *Partition) Quantizer() quantize.Quantizer {
	return p.quantizer
}

// QuantizedSet contains the quantized vectors in this partition.
func (p *Partition) QuantizedSet() quantize.QuantizedVectorSet {
	return p.quantizedSet
}

// Centroid is the full-sized centroid vector for this partition.
// NOTE: The centroid is immutable and therefore this method is thread-safe.
func (p *Partition) Centroid() vector.T {
	return p.quantizedSet.GetCentroid()
}

// ChildKeys point to the location of the full-size vectors that are quantized
// in this partition. If this is a leaf partition, then these are primary keys
// that point to rows in the primary index. If this is a branch/root partition,
// then these are the keys of child partitions.
func (p *Partition) ChildKeys() []ChildKey {
	return p.childKeys
}

// ValueBytes are opaque bytes stored alongside the quantized vectors and
// returned by searches. Depending on the store, this could be empty, or it
// could contain information associated with the vector, such as STORING
// columns.
func (p *Partition) ValueBytes() []ValueBytes {
	return p.valueBytes
}

// Search estimates the set of data vectors that are nearest to the given query
// vector and returns them in the given search set. Search also returns this
// partition's level in the K-means tree and the count of quantized vectors in
// the partition.
func (p *Partition) Search(
	w *workspace.T, partitionKey PartitionKey, queryVector vector.T, searchSet *SearchSet,
) (level Level, count int) {
	count = p.Count()
	tempFloats := w.AllocFloats(count * 2)
	defer w.FreeFloats(tempFloats)

	// Estimate distances of the data vectors from the query vector.
	tempSquaredDistances := tempFloats[:count]
	tempErrorBounds := tempFloats[count : count*2]
	p.quantizer.EstimateSquaredDistances(
		w, p.quantizedSet, queryVector, tempSquaredDistances, tempErrorBounds)
	centroidDistances := p.quantizedSet.GetCentroidDistances()

	// Add candidates to the search set, which is responsible for retaining the
	// top-k results.
	for i := range tempSquaredDistances {
		searchSet.result = SearchResult{
			QuerySquaredDistance: tempSquaredDistances[i],
			ErrorBound:           tempErrorBounds[i],
			CentroidDistance:     centroidDistances[i],
			ParentPartitionKey:   partitionKey,
			ChildKey:             p.childKeys[i],
			ValueBytes:           p.valueBytes[i],
		}
		searchSet.Add(&searchSet.result)
	}

	return p.Level(), count
}

// Add quantizes the given vector as part of this partition. If a vector with
// the same key is already in the partition, update its value if "overwrite" is
// true, else no-op. Return true if no duplicate was found and a new vector was
// added to the partition.
func (p *Partition) Add(
	w *workspace.T, vec vector.T, childKey ChildKey, valueBytes ValueBytes, overwrite bool,
) bool {
	offset := p.Find(childKey)
	if offset != -1 {
		if overwrite {
			// Remove the vector from the partition and re-add it below.
			p.ReplaceWithLast(offset)
		} else {
			// Skip the add.
			return false
		}
	}

	vectorSet := vec.AsSet()
	p.quantizer.QuantizeInSet(w, p.quantizedSet, vectorSet)
	p.childKeys = append(p.childKeys, childKey)
	p.valueBytes = append(p.valueBytes, valueBytes)

	return offset == -1
}

// AddSet quantizes the given set of vectors as part of this partition. If a
// vector with the same key is already in the partition, its value is
// overwritten if "overwrite" is true, else it is not added.
//
// NOTE: AddSet assumes that there are no duplicate keys in the input set.
func (p *Partition) AddSet(
	w *workspace.T, vectors vector.Set, childKeys []ChildKey, valueBytes []ValueBytes, overwrite bool,
) {
	if p.Count() > 0 {
		// Check for duplicates.
		for i := range vectors.Count {
			p.Add(w, vectors.At(i), childKeys[i], valueBytes[i], overwrite)
		}
		return
	}

	// No duplicates possible here, so add all vectors to the partition.
	p.quantizer.QuantizeInSet(w, p.quantizedSet, vectors)
	p.childKeys = append(p.childKeys, childKeys...)
	p.valueBytes = append(p.valueBytes, valueBytes...)
}

// ReplaceWithLast removes the quantized vector at the given offset from the
// partition, replacing it with the last quantized vector in the partition. The
// modified partition has one less element and the last quantized vector's
// position changes.
func (p *Partition) ReplaceWithLast(offset int) {
	p.quantizedSet.ReplaceWithLast(offset)
	newCount := len(p.childKeys) - 1
	p.childKeys[offset] = p.childKeys[newCount]
	p.childKeys[newCount] = ChildKey{} // for GC
	p.childKeys = p.childKeys[:newCount]
	p.valueBytes[offset] = p.valueBytes[newCount]
	p.valueBytes[newCount] = nil // for GC
	p.valueBytes = p.valueBytes[:newCount]
}

// ReplaceWithLastByKey calls ReplaceWithLast with the offset of the given child
// key in the partition. If no matching child key can be found, it returns
// false.
func (p *Partition) ReplaceWithLastByKey(childKey ChildKey) bool {
	offset := p.Find(childKey)
	if offset == -1 {
		return false
	}
	p.ReplaceWithLast(offset)
	return true
}

// Find returns the offset of the given child key in the partition, or -1 if the
// key cannot be found.
func (p *Partition) Find(childKey ChildKey) int {
	for i := range p.childKeys {
		if childKey.Equal(p.childKeys[i]) {
			return i
		}
	}
	return -1
}

// CreateEmptyPartition returns an empty partition for the given quantizer and
// level.
func CreateEmptyPartition(quantizer quantize.Quantizer, metadata PartitionMetadata) *Partition {
	quantizedSet := quantizer.NewQuantizedVectorSet(0, metadata.Centroid)
	return NewPartition(metadata, quantizer, quantizedSet, []ChildKey(nil), []ValueBytes(nil))
}
