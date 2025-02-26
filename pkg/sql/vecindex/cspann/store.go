// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// ErrPartitionNotFound is returned by the store when the requested partition
// cannot be found because it has been deleted by another agent.
var ErrPartitionNotFound = errors.New("partition not found")

// ErrRestartOperation is returned by the store when it needs the last index
// operation (e.g. search or insert) to be restarted. This should only be
// returned in situations where restarting the operation is guaranteed to make
// progress, in order to avoid an infinite loop. An example is a stale root
// partition, where a restarted operation can make progress after the cache has
// been refreshed.
var ErrRestartOperation = errors.New("conflict detected, restart operation")

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

// PartitionMetadata includes the size of the partition, as well as information
// from the metadata record.
type PartitionMetadata struct {
	// Level is the level of the partition in the K-means tree.
	Level Level
	// Centroid is the centroid for vectors in the partition. It is calculated
	// once when the partition is created and never changes, even if additional
	// vectors are added later.
	Centroid vector.T
	// Count is the number of vectors in the partition.
	Count int
}

// Store encapsulates the component that’s actually storing the vectors, whether
// that’s in a CRDB cluster for production or in memory for testing and
// benchmarking. Callers can use Store to start and commit transactions against
// the store that update its structure and contents.
//
// Store implementations must be thread-safe. There should typically be only one
// Store instance in the process for each index.
type Store interface {
	// BeginTransaction creates a new transaction that can be used to read and
	// write the store in a transactional context.
	BeginTransaction(ctx context.Context) (Txn, error)

	// CommitTransaction commits a transaction previously started by a call to
	// Begin.
	CommitTransaction(ctx context.Context, txn Txn) error

	// AbortTransaction aborts a transaction previously started by a call to
	// Begin.
	AbortTransaction(ctx context.Context, txn Txn) error

	// MergeStats merges recently gathered stats for this process with global
	// stats if "skipMerge" is false. "stats" is updated with the latest global
	// stats.
	MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error
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
	// ErrPartitionNotFound if the key cannot be found. The returned partition
	// can be modified by the caller in the scope of the transaction with a
	// guarantee it won't be changed by other agents.
	GetPartition(ctx context.Context, treeKey TreeKey, partitionKey PartitionKey) (*Partition, error)

	// SetRootPartition makes the given partition the root partition in the store.
	// If the root partition already exists, it is replaced, else it is newly
	// inserted into the store.
	SetRootPartition(ctx context.Context, treeKey TreeKey, partition *Partition) error

	// InsertPartition inserts the given partition into the store and returns a
	// new key that identifies it.
	InsertPartition(ctx context.Context, treeKey TreeKey, partition *Partition) (PartitionKey, error)

	// DeletePartition deletes the partition with the given key from the store,
	// or returns ErrPartitionNotFound if the key cannot be found.
	DeletePartition(ctx context.Context, treeKey TreeKey, partitionKey PartitionKey) error

	// GetPartitionMetadata returns metadata for the given partition, including
	// its size, its centroid, and its level in the K-means tree. If "forUpdate"
	// is true, fetching the metadata is part of a mutation operation; the store
	// can perform any needed locking in this case. GetPartitionMetadata returns
	// ErrPartitionNotFound if the partition cannot be found, or
	// ErrRestartOperation if the caller should retry the operation that triggered
	// this call.
	GetPartitionMetadata(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, forUpdate bool,
	) (PartitionMetadata, error)

	// AddToPartition adds the given vector and its associated child key and value
	// bytes to the partition with the given key. If the vector already exists, it
	// is overwritten with the new key. AddToPartition returns the partition's
	// metadata, reflecting its size after the add operation. It returns
	// ErrPartitionNotFound if the partition cannot be found, or
	// ErrRestartOperation if the caller should retry the insert operation that
	// triggered this call.
	AddToPartition(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey PartitionKey,
		vec vector.T,
		childKey ChildKey,
		valueBytes ValueBytes,
	) (PartitionMetadata, error)

	// RemoveFromPartition removes the given vector and its associated child key
	// from the partition with the given key. If the key is not present in the
	// partition, it is a no-op. RemoveFromPartition returns the partition's
	// metadata, reflecting its size after the remove operation. It returns
	// ErrPartitionNotFound if the partition cannot be found, or
	// ErrRestartOperation if the caller should retry the delete operation that
	// triggered this call.
	RemoveFromPartition(
		ctx context.Context, treeKey TreeKey, partitionKey PartitionKey, childKey ChildKey,
	) (PartitionMetadata, error)

	// SearchPartitions finds vectors that are closest to the given query vector.
	// Only partitions with the given keys are searched, and all of them must be
	// at the same level of the tree. SearchPartitions returns found vectors in
	// the search set, along with the level of the K-means tree that was searched.
	//
	// The caller is responsible for allocating the "partitionCounts" slice with
	// length equal to the number of partitions to search. SearchPartitions will
	// update the slice with the number of quantized vectors in the searched
	// partitions. This is used to determine if a partition needs to be split
	// or merged.
	//
	// If one or more partitions cannot be found, SearchPartitions returns
	// ErrPartitionNotFound, or ErrRestartOperation if the caller should retry
	// the search operation that triggered this call.
	SearchPartitions(
		ctx context.Context,
		treeKey TreeKey,
		partitionKey []PartitionKey,
		queryVector vector.T,
		searchSet *SearchSet,
		partitionCounts []int,
	) (level Level, err error)

	// GetFullVectors fetches the original full-size vectors that are referenced
	// by the given child keys and stores them in "refs". If a vector has been
	// deleted, then its corresponding reference will be set to nil. If a
	// partition cannot be found, GetFullVectors returns ErrPartitionNotFound.
	//
	// TODO(andyk): what if the row exists but the vector column is NULL? Right
	// now, this whole library expects vectors passed to it to be non-nil and have
	// the same number of dims. We should look into how pgvector handles NULL
	// values - could we just treat them as if they were missing, for example?
	GetFullVectors(ctx context.Context, treeKey TreeKey, refs []VectorWithKey) error
}
