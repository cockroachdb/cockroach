// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"slices"

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

// Init initializes an existing partition's data, overwriting any existing data.
func (p *Partition) Init(
	metadata PartitionMetadata,
	quantizer quantize.Quantizer,
	quantizedSet quantize.QuantizedVectorSet,
	childKeys []ChildKey,
	valueBytes []ValueBytes,
) {
	*p = Partition{
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
// vector and returns them in the given search set. Search also returns the
// count of quantized vectors in the partition.
func (p *Partition) Search(
	w *workspace.T, partitionKey PartitionKey, queryVector vector.T, searchSet *SearchSet,
) int {
	count := p.Count()
	tempFloats := w.AllocFloats(count * 2)
	defer w.FreeFloats(tempFloats)

	// Estimate distances of the data vectors from the query vector.
	tempDistances := tempFloats[:count]
	tempErrorBounds := tempFloats[count : count*2]
	p.quantizer.EstimateDistances(
		w, p.quantizedSet, queryVector, tempDistances, tempErrorBounds)

	// Add candidates to the search set, which is responsible for retaining the
	// top-k results.
	for i := range tempDistances {
		searchSet.tempResult = SearchResult{
			QueryDistance:      tempDistances[i],
			ErrorBound:         tempErrorBounds[i],
			ParentPartitionKey: partitionKey,
			ChildKey:           p.childKeys[i],
			ValueBytes:         p.valueBytes[i],
		}
		searchSet.Add(&searchSet.tempResult)
	}

	return count
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
// overwritten if "overwrite" is true, else it is not added. If at least one
// vector was added to the set, then AddSet returns true.
//
// NOTE: AddSet assumes that there are no duplicate keys in the input set.
func (p *Partition) AddSet(
	w *workspace.T, vectors vector.Set, childKeys []ChildKey, valueBytes []ValueBytes, overwrite bool,
) bool {
	if p.Count() > 0 {
		// Check for duplicates.
		added := false
		for i := range vectors.Count {
			added = p.Add(w, vectors.At(i), childKeys[i], valueBytes[i], overwrite) || added
		}
		return added
	}

	// No duplicates possible here, so add all vectors to the partition.
	p.quantizer.QuantizeInSet(w, p.quantizedSet, vectors)
	p.childKeys = append(p.childKeys, childKeys...)
	p.valueBytes = append(p.valueBytes, valueBytes...)
	return len(childKeys) > 0
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

// Clear removes all vectors from the partition and returns the number of
// vectors that were cleared. The centroid stays the same.
func (p *Partition) Clear() int {
	count := len(p.childKeys)
	p.quantizedSet.Clear(p.quantizedSet.GetCentroid())
	clear(p.childKeys)
	p.childKeys = p.childKeys[:0]
	clear(p.valueBytes)
	p.valueBytes = p.valueBytes[:0]
	return count
}

// CreateEmptyPartition returns an empty partition for the given quantizer and
// level.
func CreateEmptyPartition(quantizer quantize.Quantizer, metadata PartitionMetadata) *Partition {
	quantizedSet := quantizer.NewQuantizedVectorSet(0, metadata.Centroid)
	return NewPartition(metadata, quantizer, quantizedSet, []ChildKey(nil), []ValueBytes(nil))
}
