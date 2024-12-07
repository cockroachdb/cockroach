// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

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

// VectorWithKey contains a original, full-size vector and its referencing key.
type VectorWithKey struct {
	// Key is a partition key (for an interior partition) or a primary key (for
	// a leaf partition). If a partition key, the vector is the centroid of the
	// partition. Otherwise, the key references the primary key of the row in the
	// table that contains the vector.
	Key ChildKey
	// Vector is the original, full-size vector that the key references.
	Vector vector.T
}

// Txn represents the transaction in which store operations run. In production,
// this would be kv.Txn, but in testing this may be inMemoryTxn.
type Txn interface{}

// Store encapsulates the component that’s actually storing the vectors, whether
// that’s in a CRDB cluster for production or in memory for testing and
// benchmarking. The interface is carefully designed to allow batching of
// important operations like searching so that the search could be conducted at
// the remote node, close to the data.
//
// Store implementations must be thread-safe. There should typically be only one
// Store instance in the process for each index.
type Store interface {
	// BeginTransaction starts a new transaction in the store to be used for
	// background fixups like split or merge.
	BeginTransaction(ctx context.Context) (Txn, error)

	// CommitTransaction commits a transaction previously started by a call to
	// BeginTransaction.
	CommitTransaction(ctx context.Context, txn Txn) error

	// AbortTransaction aborts a transaction previously started by a call to
	// BeginTransaction.
	AbortTransaction(ctx context.Context, txn Txn) error

	// GetPartition returns the partition identified by the given key, or
	// ErrPartitionNotFound if the key cannot be found. The returned partition
	// can be modified by the caller in the scope of the transaction.
	GetPartition(ctx context.Context, txn Txn, partitionKey PartitionKey) (*Partition, error)

	// SetRootPartition makes the given partition the root partition in the store.
	// If the root partition already exists, it is replaced, else it is newly
	// inserted into the store.
	SetRootPartition(ctx context.Context, txn Txn, partition *Partition) error

	// InsertPartition inserts the given partition into the store and returns a
	// new key that identifies it.
	InsertPartition(ctx context.Context, txn Txn, partition *Partition) (PartitionKey, error)

	// DeletePartition deletes the partition with the given key from the store,
	// or returns ErrPartitionNotFound if the key cannot be found.
	DeletePartition(ctx context.Context, txn Txn, partitionKey PartitionKey) error

	// AddToPartition adds the given vector and its associated child key to the
	// partition with the given key. It returns the count of quantized vectors in
	// the partition, or ErrPartitionNotFound if the partition cannot be found.
	AddToPartition(
		ctx context.Context, txn Txn, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
	) (int, error)

	// RemoveFromPartition removes the given vector and its associated child key
	// from the partition with the given key. It returns the count of quantized
	// vectors in the partition or ErrPartitionNotFound if the partition cannot
	// be found.
	RemoveFromPartition(
		ctx context.Context, txn Txn, partitionKey PartitionKey, childKey ChildKey,
	) (int, error)

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
	// ErrPartitionNotFound.
	SearchPartitions(
		ctx context.Context,
		txn Txn,
		partitionKey []PartitionKey,
		queryVector vector.T,
		searchSet *SearchSet,
		partitionCounts []int,
	) (level Level, err error)

	// GetFullVectors fetches the original full-size vectors that are referenced
	// by the given child keys and stores them in "refs". If a vector has been
	// deleted, then its corresponding reference will be set to nil. If a
	// partition cannot be found, GetFullVectors returns ErrPartitionNotFound.
	GetFullVectors(ctx context.Context, txn Txn, refs []VectorWithKey) error

	// MergeStats merges recently gathered stats for this process with global
	// stats if "skipMerge" is false. "stats" is updated with the latest global
	// stats.
	MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error
}
