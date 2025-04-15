// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// VectorWithKey contains an original, full-size vector and its referencing key.
type VectorWithKey struct {
	// Key is a partition key (for an interior partition) or a primary key (for
	// a leaf partition). If a partition key, the vector is the centroid of the
	// partition. Otherwise, the key references the primary key of the row in the
	// table that contains the vector.
	Key ChildKey
	// Vector is the original, full-size vector that the key references.
	Vector vector.T
}

// PartitionToSearch contains information about a partition to be searched by
// the SearchPartitions method.
type PartitionToSearch struct {
	// Key is the key of the partition to search.
	Key PartitionKey
	// ExcludeLeafVectors indicates that the search should not return vectors if
	// this turns out to be a leaf partition. When inserting a new vector into
	// the index, it is never necessary to scan leaf vectors. However, until the
	// partition metadata is scanned, it is not known whether a root partition is
	// a leaf partition.
	ExcludeLeafVectors bool
	// Level returns the partition's level in the K-means tree.
	Level Level
	// StateDetails returns the latest state information from the partition
	// metadata. This is used by fixup workers to detect interference from other
	// agents that are updating the partition.
	StateDetails PartitionStateDetails
	// Count is set to the number of vectors in the searched partition. This is
	// an output value (i.e. it's set by SearchPartitions).
	Count int
}

// Store encapsulates the component that's actually storing the vectors, whether
// that's in a CRDB cluster for production or in memory for testing and
// benchmarking. Callers can use Store to start and commit transactions against
// the store that update its structure and contents.
//
// Store implementations must be thread-safe. There should typically be only one
// Store instance in the process for each index.
type Store interface {
	// RunTransaction invokes the given function in the scope of a new
	// transaction. If the function returns an error, the transaction is aborted,
	// else it is committed.
	RunTransaction(ctx context.Context, fn func(txn Txn) error) error

	// MakePartitionKey allocates a new partition key that is guaranteed to be
	// globally unique.
	MakePartitionKey() PartitionKey

	// EstimatePartitionCount returns the approximate number of vectors in the
	// given partition. The estimate can be based on a (bounded) stale copy of
	// the partition. It returns 0 if the partition does not exist.
	EstimatePartitionCount(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey,
	) (int, error)

	// MergeStats merges recently gathered stats for this process with global
	// stats if "skipMerge" is false. "stats" is updated with the latest global
	// stats.
	MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error

	// TryCreateEmptyPartition constructs a new partition containing no vectors,
	// having the specified key and metadata. It returns a ConditionFailedError
	// if the partition already exists.
	TryCreateEmptyPartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, metadata PartitionMetadata,
	) error

	// TryDeletePartition deletes the specified partition. It returns
	// ErrPartitionNotFound if the partition does not exist.
	TryDeletePartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey,
	) error

	// TryGetPartition returns the requested partition, if it exists, else it
	// returns ErrPartitionNotFound.
	TryGetPartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey,
	) (*Partition, error)

	// TryGetPartitionMetadata returns just the metadata of the requested
	// partition, if it exists, else it returns ErrPartitionNotFound. This is
	// more efficient than loading the entire partition when only metadata is
	// needed.
	TryGetPartitionMetadata(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey,
	) (PartitionMetadata, error)

	// TryUpdatePartitionMetadata updates the partition's metadata only if it's
	// equal to the expected value, else it returns a ConditionFailedError. If
	// the partition does not exist, it returns ErrPartitionNotFound.
	TryUpdatePartitionMetadata(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey PartitionKey,
		metadata PartitionMetadata,
		expected PartitionMetadata,
	) error

	// TryAddToPartition adds the given vectors (and associated keys/values) to
	// the specified partition. If a vector's key already exists in
	// the partition, the vector is not added.
	//
	// Before performing any action, TryAddToPartition checks the partition's
	// metadata and returns a ConditionFailedError if it is not the same as the
	// expected metadata. If the partition does not exist, it returns
	// ErrPartitionNotFound.
	//
	// NOTE: Individual adds do not need to share a transaction. If an error
	// occurs in the middle of the operation, vectors that have already been added
	// are not guaranteed to roll back.
	TryAddToPartition(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey PartitionKey,
		vectors vector.Set,
		childKeys []ChildKey,
		valueBytes []ValueBytes,
		expected PartitionMetadata,
	) (added bool, err error)

	// TryRemoveFromPartition removes vectors from the given partition by their
	// child keys. If a key is not present in the partition, it is a no-op. If
	// an attempt is made to remove all vectors in a non-leaf partition, then
	// ErrRemoveNotAllowed is returned.
	//
	// Before performing any action, TryRemoveFromPartition checks the partition's
	// metadata and returns a ConditionFailedError if it is not the same as the
	// expected metadata. If the partition does not exist, it returns
	// ErrPartitionNotFound.
	//
	// NOTE: Individual removes do not need to share a transaction. If an error
	// occurs in the middle of the operation, vectors that have already been
	// removed are not guaranteed to roll back.
	TryRemoveFromPartition(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey PartitionKey,
		childKeys []ChildKey,
		expected PartitionMetadata,
	) (removed bool, err error)

	// TryClearPartition removes all vectors in the specified partition and
	// returns the number of vectors that were cleared. It returns
	// ErrPartitionNotFound if the partition does not exist.
	//
	// Before performing any action, TryClearPartition checks the partition's
	// metadata and returns a ConditionFailedError if it is not the same as the
	// expected metadata. If the partition does not exist, it returns
	// ErrPartitionNotFound.
	TryClearPartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, expected PartitionMetadata,
	) (count int, err error)
}

// Txn enables callers to make changes to the stored index in a transactional
// context. Changes might be directly committed to the store or simply buffered
// up for later commit. Changes might be committed as part of a larger
// transaction that includes non-vector index changes as well.
//
// The interface is carefully designed to allow batching of important operations
// like searching so that the search could be conducted at remote nodes, close
// to the data.
//
// Txn implementations are not thread-safe.
type Txn interface {
	// GetPartition returns the partition identified by the given key, or
	// ErrPartitionNotFound if the key cannot be found. The returned partition's
	// memory is owned by the caller - it can be modified as needed.
	// TODO(andyk): Remove this method once we switch to non-transactional
	// splits/merges.
	GetPartition(ctx context.Context, treeKey TreeKey, partitionKey PartitionKey) (*Partition, error)

	// SetRootPartition makes the given partition the root partition in the store.
	// If the root partition already exists, it is replaced, else it is newly
	// inserted into the store.
	// TODO(andyk): Remove this method once we switch to non-transactional
	// splits/merges.
	SetRootPartition(ctx context.Context, treeKey TreeKey, partition *Partition) error

	// InsertPartition inserts the given partition into the store and returns a
	// new key that identifies it.
	// TODO(andyk): Remove this method once we switch to non-transactional
	// splits/merges.
	InsertPartition(ctx context.Context, treeKey TreeKey, partition *Partition) (PartitionKey, error)

	// DeletePartition deletes the partition with the given key from the store,
	// or returns ErrPartitionNotFound if the key cannot be found.
	// TODO(andyk): Remove this method once we switch to non-transactional
	// splits/merges.
	DeletePartition(ctx context.Context, treeKey TreeKey, partitionKey PartitionKey) error

	// GetPartitionMetadata returns metadata for the given partition, including
	// its size, its centroid, and its level in the K-means tree. If "forUpdate"
	// is true, fetching the metadata is part of a mutation operation; the store
	// can perform any needed locking in this case.
	//
	// GetPartitionMetadata returns ConditionFailedError if "forUpdate" is true
	// and the partition is in a state that does not allow updates. It returns
	// ErrPartitionNotFound if the partition cannot be found, or
	// ErrRestartOperation if the caller should retry the operation that triggered
	// this call.
	GetPartitionMetadata(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, forUpdate bool,
	) (PartitionMetadata, error)

	// AddToPartition adds the given vector and its associated child key and value
	// bytes to the partition with the given key. If the vector already exists, it
	// is overwritten with the new key.
	//
	// AddToPartition returns ConditionalFailedError if the partition is in a
	// state that does not allow adds. It returns ErrPartitionNotFound if the
	// partition cannot be found, or ErrRestartOperation if the caller should
	// retry the insert operation that triggered this call.
	AddToPartition(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey PartitionKey,
		level Level,
		vec vector.T,
		childKey ChildKey,
		valueBytes ValueBytes,
	) error

	// RemoveFromPartition removes the given vector and its associated child key
	// from the partition with the given key. If the key is not present in the
	// partition, it is a no-op.
	//
	// RemoveFromPartition returns ConditionalFailedError if the partition is in
	// a state that does not allow removes. It returns ErrPartitionNotFound if the
	// partition cannot be found, or ErrRestartOperation if the caller should
	// retry the delete operation that triggered this call.
	RemoveFromPartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, level Level, childKey ChildKey,
	) error

	// SearchPartitions finds vectors that are closest to the given query vector.
	// Only partitions referenced by the "toSearch" list will be returned, and all
	// of them must be at the same level of the tree. SearchPartitions returns
	// found vectors in the search set, along with the level of the K-means tree
	// that was searched. It will also update the "Count" field of each "toSearch"
	// partition with the number of quantized vectors in that searched partition.
	// This is used to determine if a partition needs to be split or merged.
	//
	// If a partition cannot be found, SearchPartitions returns InvalidLevel,
	// MissingState, and Count=0 for it. SearchPartitions returns
	// ErrRestartOperation if the caller should retry the search operation that
	// triggered this call.
	SearchPartitions(
		ctx context.Context,
		treeKey TreeKey,
		toSearch []PartitionToSearch,
		queryVector vector.T,
		searchSet *SearchSet,
	) error

	// GetFullVectors fetches the original full-size vectors that are referenced
	// by the given child keys and stores them in "refs". If a vector has been
	// deleted, then its corresponding reference will be set to nil. If a
	// partition cannot be found, GetFullVectors returns ErrPartitionNotFound.
	//
	// NOTE: The caller takes ownership of any vector memory returned in "refs".
	// The Store implementation should not try to use it after returning it.
	//
	// TODO(andyk): what if the row exists but the vector column is NULL? Right
	// now, this whole library expects vectors passed to it to be non-nil and have
	// the same number of dims. We should look into how pgvector handles NULL
	// values - could we just treat them as if they were missing, for example?
	GetFullVectors(ctx context.Context, treeKey TreeKey, refs []VectorWithKey) error
}
