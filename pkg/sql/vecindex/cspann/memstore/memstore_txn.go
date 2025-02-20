// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/veclib"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// memTxn tracks the transaction's state.
type memTxn struct {
	// workspace allocates temporary memory.
	workspace *veclib.Workspace

	// store references the in-memory store instance that created this
	// transaction.
	store *Store

	// id is the unique identifier for this transaction. It is immutable and can
	// be accessed on any goroutine without a lock.
	id uint64

	// The following fields should only be accessed on the same goroutine that
	// created the transaction.

	// current is the logical clock time of the transaction. This can be "stepped"
	// during the transaction's lifetime so that it can "see" later iterations of
	// the root partition.
	current uint64

	// updated is true if any in-memory state has been updated during the
	// lifetime of the transaction.
	updated bool

	// unbalanced, if non-nil, records a non-leaf partition that had all of its
	// vectors removed during the transaction. If, by the end of the transaction,
	// the partition is still empty, the store will panic, since this violates
	// the constraint that the K-means tree is always fully balanced.
	unbalanced *memPartition

	// ownedLocks contains all exclusive partition locks that have been obtained
	// during the transaction. These will be released at the end of the
	// transaction.
	ownedLocks []*memLock

	// The following fields can only be accessed after the store mutex has been
	// acquired (i.e. Store.mu).
	muStore struct {
		// ended is set to true once the transaction has ended. It is consulted
		// to determine when it's safe to garbage collect deleted partitions.
		ended bool
	}
}

// GetPartition implements the Txn interface.
func (tx *memTxn) GetPartition(
	ctx context.Context, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	// GetPartition is only called by split and merge operations, so acquire the
	// exclusive structure lock so that only one operation at a time can modify
	// the tree structure.
	tx.store.structureLock.Acquire(tx.id)
	tx.ownedLocks = append(tx.ownedLocks, &tx.store.structureLock)

	// Acquire exclusive lock on the requested partition for the duration of the
	// transaction.
	inMemPartition, err := tx.store.getPartition(partitionKey)
	if err != nil {
		return nil, err
	}
	inMemPartition.lock.Acquire(tx.id)
	tx.ownedLocks = append(tx.ownedLocks, &inMemPartition.lock.memLock)

	// Return an error if the partition has been deleted.
	if !inMemPartition.isVisibleLocked(tx.current) {
		return nil, cspann.ErrPartitionNotFound
	}

	// Make a deep copy of the partition, since modifications shouldn't impact
	// the store's copy.
	return inMemPartition.lock.partition.Clone(), nil
}

// SetRootPartition implements the Txn interface.
func (tx *memTxn) SetRootPartition(ctx context.Context, partition *cspann.Partition) error {
	if !tx.store.structureLock.IsAcquiredBy(tx.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", tx.id))
	}

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	existing, ok := tx.store.mu.partitions[cspann.RootKey]
	if !ok {
		panic(errors.AssertionFailedf("the root partition cannot be found"))
	}

	// Existing root partition should have been locked by the transaction.
	if !existing.lock.IsAcquiredBy(tx.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", tx.id))
	}

	tx.store.reportPartitionSizeLocked(partition.Count())

	// Grow or shrink CVStats slice if a new level is being added or removed.
	expectedLevels := int(partition.Level() - 1)
	if expectedLevels > len(tx.store.mu.stats.CVStats) {
		tx.store.mu.stats.CVStats =
			slices.Grow(tx.store.mu.stats.CVStats, expectedLevels-len(tx.store.mu.stats.CVStats))
	}
	tx.store.mu.stats.CVStats = tx.store.mu.stats.CVStats[:expectedLevels]

	// Update the root partition's creation time to indicate to callers that it
	// was replaced.
	existing.lock.partition = partition
	existing.lock.created = tx.store.tickLocked()

	tx.store.updatedStructureLocked(tx)
	return nil
}

// InsertPartition implements the Txn interface.
func (tx *memTxn) InsertPartition(
	ctx context.Context, partition *cspann.Partition,
) (cspann.PartitionKey, error) {
	if !tx.store.structureLock.IsAcquiredBy(tx.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", tx.id))
	}

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	// Assign key to new partition.
	partitionKey := tx.store.mu.nextKey
	tx.store.mu.nextKey++

	// Insert new partition.
	memPart := &memPartition{key: partitionKey}
	memPart.lock.partition = partition
	memPart.lock.created = tx.store.tickLocked()
	tx.store.mu.partitions[partitionKey] = memPart

	// Update stats.
	tx.store.mu.stats.NumPartitions++
	tx.store.reportPartitionSizeLocked(partition.Count())

	tx.store.updatedStructureLocked(tx)
	return partitionKey, nil
}

// DeletePartition implements the Txn interface.
func (tx *memTxn) DeletePartition(ctx context.Context, partitionKey cspann.PartitionKey) error {
	if !tx.store.structureLock.IsAcquiredBy(tx.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", tx.id))
	}

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	if partitionKey == cspann.RootKey {
		panic(errors.AssertionFailedf("cannot delete the root partition"))
	}

	inMemPartition, ok := tx.store.mu.partitions[partitionKey]
	if !ok {
		return cspann.ErrPartitionNotFound
	}

	// Existing root partition should have been locked by the transaction.
	if !inMemPartition.lock.IsAcquiredBy(tx.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", tx.id))
	}

	// Mark partition as deleted.
	if inMemPartition.lock.deleted {
		panic(errors.AssertionFailedf("partition %d is already deleted", partitionKey))
	}
	inMemPartition.lock.deleted = true

	// Add the partition to the pending list so that it will only be garbage
	// collected once all older transactions have ended.
	tx.store.mu.pending.PushBack(pendingItem{deletedPartition: inMemPartition})

	tx.store.mu.stats.NumPartitions--

	tx.store.updatedStructureLocked(tx)
	return nil
}

// GetPartitionMetadata implements the Txn interface.
func (tx *memTxn) GetPartitionMetadata(
	ctx context.Context, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	inMemPartition, err := tx.store.getPartition(partitionKey)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Acquire shared lock on the partition and get its metadata.
	inMemPartition.lock.AcquireShared(tx.id)
	defer inMemPartition.lock.ReleaseShared()
	return inMemPartition.lock.partition.Metadata(), nil
}

// AddToPartition implements the Txn interface.
func (tx *memTxn) AddToPartition(
	ctx context.Context,
	partitionKey cspann.PartitionKey,
	vec vector.T,
	childKey cspann.ChildKey,
	valueBytes cspann.ValueBytes,
) (cspann.PartitionMetadata, error) {
	inMemPartition, err := tx.store.getPartition(partitionKey)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Acquire exclusive lock on the partition.
	inMemPartition.lock.Acquire(tx.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(tx.current) {
		// Push forward transaction's current time so that the restarted operation
		// will see the root partition if it was replaced.
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.current = tx.store.tickLocked()
		return cspann.PartitionMetadata{}, cspann.ErrRestartOperation
	}

	// Add the vector to the partition.
	partition := inMemPartition.lock.partition
	if partition.Add(tx.workspace, vec, childKey, valueBytes) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.store.reportPartitionSizeLocked(partition.Count())
	}

	tx.updated = true
	return partition.Metadata(), nil
}

// RemoveFromPartition implements the Txn interface.
func (tx *memTxn) RemoveFromPartition(
	ctx context.Context, partitionKey cspann.PartitionKey, childKey cspann.ChildKey,
) (cspann.PartitionMetadata, error) {
	inMemPartition, err := tx.store.getPartition(partitionKey)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}

	// Acquire exclusive lock on the partition.
	inMemPartition.lock.Acquire(tx.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(tx.current) {
		// Push forward transaction's current time so that the restarted operation
		// will see the root partition if it was replaced.
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.current = tx.store.tickLocked()
		return cspann.PartitionMetadata{}, cspann.ErrRestartOperation
	}

	// Remove vector from the partition.
	partition := inMemPartition.lock.partition
	if partition.ReplaceWithLastByKey(childKey) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.store.reportPartitionSizeLocked(partition.Count())
	}

	if partition.Count() == 0 && partition.Level() > cspann.LeafLevel {
		// A non-leaf partition has zero vectors. If this is still true at the
		// end of the transaction, the K-means tree will be unbalanced, which
		// violates a key constraint.
		tx.unbalanced = inMemPartition
	}

	tx.updated = true
	return partition.Metadata(), nil
}

// SearchPartitions implements the Txn interface.
func (tx *memTxn) SearchPartitions(
	ctx context.Context,
	partitionKeys []cspann.PartitionKey,
	queryVector vector.T,
	searchSet *cspann.SearchSet,
	partitionCounts []int,
) (level cspann.Level, err error) {
	for i := 0; i < len(partitionKeys); i++ {
		inMemPartition, err := tx.store.getPartition(partitionKeys[i])
		if err != nil {
			return 0, err
		}

		// Acquire shared lock on partition and search it. Note that we don't need
		// to check if the partition has been deleted, since the transaction that
		// deleted it must be concurrent with this transaction (or else the
		// deleted partition would not have been found by this transaction).
		func() {
			inMemPartition.lock.AcquireShared(tx.id)
			defer inMemPartition.lock.ReleaseShared()

			searchLevel, partitionCount := inMemPartition.lock.partition.Search(
				tx.workspace, partitionKeys[i], queryVector, searchSet)
			if i == 0 {
				level = searchLevel
			} else if level != searchLevel {
				// Callers should only search for partitions at the same level.
				panic(errors.AssertionFailedf(
					"caller already searched a partition at level %d, cannot search at level %d",
					level, searchLevel))
			}
			partitionCounts[i] = partitionCount
		}()
	}

	return level, nil
}

// GetFullVectors implements the Txn interface.
func (tx *memTxn) GetFullVectors(ctx context.Context, refs []cspann.VectorWithKey) error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	for i := 0; i < len(refs); i++ {
		ref := &refs[i]
		if ref.Key.PartitionKey != cspann.InvalidKey {
			// Get the partition's centroid.
			inMemPartition, ok := tx.store.mu.partitions[ref.Key.PartitionKey]
			if !ok {
				panic(errors.AssertionFailedf("partition %d does not exist", ref.Key.PartitionKey))
			}

			// Don't need to acquire lock to call the Centroid method, since it
			// is immutable and thread-safe.
			ref.Vector = inMemPartition.lock.partition.Centroid()
		} else {
			vector, ok := tx.store.mu.vectors[string(refs[i].Key.KeyBytes)]
			if ok {
				ref.Vector = vector
			} else {
				ref.Vector = nil
			}
		}
	}

	return nil
}
