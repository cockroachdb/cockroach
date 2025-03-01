// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// memTxn tracks the transaction's state.
type memTxn struct {
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

	// workspace is used to stack-allocate temporary memory.
	workspace workspace.T

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
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	// GetPartition is only called by split and merge operations, so acquire the
	// exclusive structure lock so that only one operation at a time can modify
	// the tree structure.
	tx.acquireStructureLock()

	// Acquire exclusive lock on the requested partition for the duration of the
	// transaction.
	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition has not yet been created, so return empty partition.
			return cspann.CreateEmptyPartition(tx.store.rootQuantizer, cspann.LeafLevel), nil
		}
		return nil, err
	}
	tx.ownedLocks = append(tx.ownedLocks, &memPart.lock.memLock)

	// Make a deep copy of the partition, since modifications shouldn't impact
	// the store's copy.
	return memPart.lock.partition.Clone(), nil
}

// SetRootPartition implements the Txn interface.
func (tx *memTxn) SetRootPartition(
	ctx context.Context, treeKey cspann.TreeKey, partition *cspann.Partition,
) error {
	// Acquire the structure lock before replacing the root partition.
	tx.acquireStructureLock()

	// Acquire exclusive lock on the root partition to replace.
	memPart, err := tx.lockPartition(treeKey, cspann.RootKey, true /* isExclusive */)
	if err != nil {
		return err
	}
	defer memPart.lock.Release()

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

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
	memPart.lock.partition = partition
	memPart.lock.created = tx.store.tickLocked()

	tx.store.updatedStructureLocked(tx)
	return nil
}

// InsertPartition implements the Txn interface.
func (tx *memTxn) InsertPartition(
	ctx context.Context, treeKey cspann.TreeKey, partition *cspann.Partition,
) (cspann.PartitionKey, error) {
	// Acquire the structure lock before inserting a new partition.
	tx.acquireStructureLock()

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	// Assign key to new partition.
	partitionKey := tx.store.mu.nextKey
	tx.store.mu.nextKey++

	// Insert new partition.
	tx.store.insertPartitionLocked(treeKey, partitionKey, partition)

	// Update stats.
	tx.store.mu.stats.NumPartitions++
	tx.store.reportPartitionSizeLocked(partition.Count())

	tx.store.updatedStructureLocked(tx)
	return partitionKey, nil
}

// DeletePartition implements the Txn interface.
func (tx *memTxn) DeletePartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) error {
	// Acquire the structure lock before deleting a partition.
	tx.acquireStructureLock()

	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		// If partition doesn't exist or isn't visible, it's a no-op.
		if errors.Is(err, cspann.ErrPartitionNotFound) || errors.Is(err, cspann.ErrRestartOperation) {
			return nil
		}
		return err
	}
	defer memPart.lock.Release()

	// Mark partition as deleted.
	if memPart.lock.deleted {
		panic(errors.AssertionFailedf("partition %d is already deleted", partitionKey))
	}
	memPart.lock.deleted = true

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	// Add the partition to the pending list so that it will only be garbage
	// collected once all older transactions have ended.
	tx.store.mu.pending.PushBack(pendingItem{deletedPartition: memPart})

	tx.store.mu.stats.NumPartitions--

	tx.store.updatedStructureLocked(tx)
	return nil
}

// GetPartitionMetadata implements the Txn interface.
func (tx *memTxn) GetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	// Acquire shared lock on the partition in order to get its metadata.
	memPart, err := tx.lockPartition(treeKey, partitionKey, false /* IsExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition has not yet been created, so create empty metadata.
			return cspann.PartitionMetadata{
				Level:    cspann.LeafLevel,
				Centroid: make(vector.T, tx.store.rootQuantizer.GetDims()),
			}, nil
		}
		return cspann.PartitionMetadata{}, err
	}
	defer memPart.lock.ReleaseShared()

	return memPart.lock.partition.Metadata(), nil
}

// AddToPartition implements the Txn interface.
func (tx *memTxn) AddToPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	vec vector.T,
	childKey cspann.ChildKey,
	valueBytes cspann.ValueBytes,
) (cspann.PartitionMetadata, error) {
	// Acquire exclusive lock on the partition in order to add a vector.
	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition did not exist, so ensure it's created now.
			memPart, err = tx.ensureLockedRootPartition(treeKey)
		}
		if err != nil {
			return cspann.PartitionMetadata{}, err
		}
	}
	defer memPart.lock.Release()

	// Add the vector to the partition.
	partition := memPart.lock.partition
	if partition.Add(&tx.workspace, vec, childKey, valueBytes) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.store.reportPartitionSizeLocked(partition.Count())
	}

	tx.updated = true
	return partition.Metadata(), nil
}

// RemoveFromPartition implements the Txn interface.
func (tx *memTxn) RemoveFromPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	childKey cspann.ChildKey,
) (cspann.PartitionMetadata, error) {
	// Acquire exclusive lock on the partition in order to remove a vector.
	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		return cspann.PartitionMetadata{}, err
	}
	defer memPart.lock.Release()

	// Remove vector from the partition.
	partition := memPart.lock.partition
	if partition.ReplaceWithLastByKey(childKey) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.store.reportPartitionSizeLocked(partition.Count())
	}

	if partition.Count() == 0 && partition.Level() > cspann.LeafLevel {
		// A non-leaf partition has zero vectors. If this is still true at the
		// end of the transaction, the K-means tree will be unbalanced, which
		// violates a key constraint.
		tx.unbalanced = memPart
	}

	tx.updated = true
	return partition.Metadata(), nil
}

// SearchPartitions implements the Txn interface.
func (tx *memTxn) SearchPartitions(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKeys []cspann.PartitionKey,
	queryVector vector.T,
	searchSet *cspann.SearchSet,
	partitionCounts []int,
) (level cspann.Level, err error) {
	for i := 0; i < len(partitionKeys); i++ {
		var searchLevel cspann.Level
		var partitionCount int

		memPart, ok := tx.store.getPartition(treeKey, partitionKeys[i])
		if !ok {
			if partitionKeys[i] == cspann.RootKey {
				// Root partition has not yet been created, so it must be empty.
				searchLevel = cspann.LeafLevel
			} else {
				return cspann.InvalidLevel, cspann.ErrPartitionNotFound
			}
		} else {
			// Acquire shared lock on partition and search it. Note that we don't
			// need to check if the partition has been deleted, since the transaction
			// that deleted it must be concurrent with this transaction (or else the
			// deleted partition would not have been found by this transaction).
			func() {
				memPart.lock.AcquireShared(tx.id)
				defer memPart.lock.ReleaseShared()

				searchLevel, partitionCount = memPart.lock.partition.Search(
					&tx.workspace, partitionKeys[i], queryVector, searchSet)
			}()
		}

		if i == 0 {
			level = searchLevel
		} else if level != searchLevel {
			// Callers should only search for partitions at the same level.
			panic(errors.AssertionFailedf(
				"caller already searched a partition at level %d, cannot search at level %d",
				level, searchLevel))
		}
		partitionCounts[i] = partitionCount
	}

	return level, nil
}

// GetFullVectors implements the Txn interface.
func (tx *memTxn) GetFullVectors(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	for i := 0; i < len(refs); i++ {
		ref := &refs[i]
		if ref.Key.PartitionKey != cspann.InvalidKey {
			// Get the partition's centroid.
			memPart, ok := tx.store.getPartitionLocked(treeKey, ref.Key.PartitionKey)
			if !ok {
				return cspann.ErrPartitionNotFound
			}

			// Don't need to acquire lock to call the Centroid method, since it
			// is immutable and thread-safe.
			ref.Vector = memPart.lock.partition.Centroid()
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

// ensureLockedRootPartition lazily creates a root partition for the specified
// K-means tree, and returns it with an exclusive lock already acquired on it.
// NOTE: This is only intended for use with AddToPartition and/or other methods
// that need an exclusive lock.
func (tx *memTxn) ensureLockedRootPartition(treeKey cspann.TreeKey) (*memPartition, error) {
	// Acquire the structure lock in order to create the root partition.
	tx.store.structureLock.Acquire(tx.id)
	tx.ownedLocks = append(tx.ownedLocks, &tx.store.structureLock)

	// Check for race condition where another thread already created the root.
	memPart, err := tx.lockPartition(treeKey, cspann.RootKey, true /* isExclusive */)
	if err != nil {
		if !errors.Is(err, cspann.ErrPartitionNotFound) {
			return nil, err
		}

		// Partition not found, so create it.
	} else {
		// Root was already created, return it.
		return memPart, err
	}

	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	root := cspann.CreateEmptyPartition(tx.store.rootQuantizer, cspann.LeafLevel)
	memPart = tx.store.insertPartitionLocked(treeKey, cspann.RootKey, root)
	memPart.lock.Acquire(tx.id)

	tx.store.updatedStructureLocked(tx)
	return memPart, nil
}

// acquireStructureLock acquires an exclusive lock that's needed by operations
// that want to modify the structure of a tree, e.g. inserting or deleting a
// partition. The lock is held for the duration of the transaction.
func (tx *memTxn) acquireStructureLock() {
	if tx.store.structureLock.IsAcquiredBy(tx.id) {
		// Transaction has already acquired the structure lock.
		return
	}
	tx.store.structureLock.Acquire(tx.id)
	tx.ownedLocks = append(tx.ownedLocks, &tx.store.structureLock)
}

// lockPartition acquires a shared or exclusive lock of the given partition.
// If the partition does not exist, it returns ErrPartitionNotFound. If the
// partition was deleted by a concurrent transaction, it pushes the
// transaction's forward and returns ErrRestartOperation.
func (tx *memTxn) lockPartition(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, isExclusive bool,
) (*memPartition, error) {
	memPart, ok := tx.store.getPartition(treeKey, partitionKey)
	if !ok {
		return nil, cspann.ErrPartitionNotFound
	}

	if isExclusive {
		memPart.lock.Acquire(tx.id)
	} else {
		memPart.lock.AcquireShared(tx.id)
	}

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !memPart.isVisibleLocked(tx.current) {
		// Release the partition lock.
		if isExclusive {
			memPart.lock.Release()
		} else {
			memPart.lock.ReleaseShared()
		}

		// Push forward transaction's current time so that the restarted operation
		// will see the root partition if it was replaced.
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		tx.current = tx.store.tickLocked()
		return nil, cspann.ErrRestartOperation
	}

	return memPart, nil
}
