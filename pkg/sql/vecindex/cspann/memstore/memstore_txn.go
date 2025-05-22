// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"

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

// GetPartitionMetadata implements the Txn interface.
func (tx *memTxn) GetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, forUpdate bool,
) (cspann.PartitionMetadata, error) {
	// Acquire shared lock on the partition in order to get its metadata.
	memPart, err := tx.lockPartition(treeKey, partitionKey, forUpdate /* IsExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition has not yet been created, so create empty metadata.
			return tx.store.makeEmptyRootMetadata(), nil
		}
		return cspann.PartitionMetadata{}, err
	}

	if forUpdate {
		// Hold exclusive lock for the remainder of the transaction.
		// TODO(andyk): This creates the potential for deadlocks. Right now, this
		// is only called in the SearchForInsert case, which we don't use with the
		// the memstore (outside of tests). To fix this, update lockPartition to
		// return ErrRestartOperation if another txn already has the exclusive lock.
		tx.ownedLocks = append(tx.ownedLocks, &memPart.lock.memLock)
	} else {
		// No need to hold the lock beyond this call.
		defer memPart.lock.ReleaseShared()
	}

	// Do not allow updates to the partition if the state doesn't allow it.
	metadata := memPart.lock.partition.Metadata()
	if forUpdate && !metadata.StateDetails.State.AllowAddOrRemove() {
		err = cspann.NewConditionFailedError(*metadata)
		return cspann.PartitionMetadata{}, errors.Wrapf(err,
			"getting metadata for partition %d (state=%s)",
			partitionKey, metadata.StateDetails.State.String())
	}

	return *metadata, nil
}

// AddToPartition implements the Txn interface.
func (tx *memTxn) AddToPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	level cspann.Level,
	vec vector.T,
	childKey cspann.ChildKey,
	valueBytes cspann.ValueBytes,
) error {
	// Acquire exclusive lock on the partition in order to add a vector.
	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition did not exist, so lazily create it now.
			memPart = tx.ensureLockedRootPartition(treeKey)
		} else {
			return errors.Wrapf(err, "adding to partition %d (level=%d)", partitionKey, level)
		}
	}
	defer memPart.lock.Release()

	// Do not allow vectors to be added to the partition if the state doesn't
	// allow it.
	partition := memPart.lock.partition
	state := partition.Metadata().StateDetails.State
	if !state.AllowAddOrRemove() {
		return errors.Wrapf(cspann.NewConditionFailedError(*partition.Metadata()),
			"adding to partition %d (state=%s)", partitionKey, state.String())
	}

	// Add the vector to the partition.
	if level != partition.Level() {
		return errors.Wrapf(cspann.ErrRestartOperation,
			"partition %d level %d does not match expected level %d",
			partitionKey, partition.Level(), level)
	}

	if partition.Add(&tx.workspace, vec, childKey, valueBytes, true /* overwrite */) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		memPart.count.Add(1)
	}

	tx.updated = true
	return nil
}

// RemoveFromPartition implements the Txn interface.
func (tx *memTxn) RemoveFromPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	level cspann.Level,
	childKey cspann.ChildKey,
) error {
	// Acquire exclusive lock on the partition in order to remove a vector.
	memPart, err := tx.lockPartition(treeKey, partitionKey, true /* isExclusive */)
	if err != nil {
		if partitionKey == cspann.RootKey && errors.Is(err, cspann.ErrPartitionNotFound) {
			// Root partition did not exist, so removal is no-op.
			return nil
		}
		return err
	}
	defer memPart.lock.Release()

	// Do not allow vectors to be removed from the partition if the state doesn't
	// allow it.
	partition := memPart.lock.partition
	state := partition.Metadata().StateDetails.State
	if !state.AllowAddOrRemove() {
		return errors.Wrapf(cspann.NewConditionFailedError(*partition.Metadata()),
			"removing from partition %d (state=%s)", partitionKey, state.String())
	}

	// Remove vector from the partition.
	if level != partition.Level() {
		return errors.Wrapf(cspann.ErrRestartOperation,
			"removing from partition %d (expected: %d, actual: %d)",
			partitionKey, level, partition.Level())
	}

	if partition.ReplaceWithLastByKey(childKey) {
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		memPart.count.Add(-1)
	}

	tx.updated = true
	return nil
}

// SearchPartitions implements the Txn interface.
func (tx *memTxn) SearchPartitions(
	ctx context.Context,
	treeKey cspann.TreeKey,
	toSearch []cspann.PartitionToSearch,
	queryVector vector.T,
	searchSet *cspann.SearchSet,
) error {
	for i := range toSearch {
		memPart, ok := tx.store.getPartition(treeKey, toSearch[i].Key)
		if !ok {
			if toSearch[i].Key == cspann.RootKey {
				// Root partition has not yet been created, so it must be empty.
				toSearch[i].Level = cspann.LeafLevel
				toSearch[i].StateDetails.MakeReady()
				toSearch[i].Count = 0
			} else {
				// Partition does not exist, so return InvalidLevel, MissingState
				// and Count=0.
				toSearch[i].Level = cspann.InvalidLevel
				toSearch[i].StateDetails = cspann.PartitionStateDetails{}
				toSearch[i].Count = 0
			}
		} else {
			// Acquire shared lock on partition and search it. Note that we don't
			// need to check if the partition has been deleted, since the transaction
			// that deleted it must be concurrent with this transaction (or else the
			// deleted partition would not have been found by this transaction).
			func() {
				memPart.lock.AcquireShared(tx.id)
				defer memPart.lock.ReleaseShared()

				partition := memPart.lock.partition
				toSearch[i].Level = partition.Level()
				toSearch[i].StateDetails = partition.Metadata().StateDetails
				toSearch[i].Count = partition.Search(&tx.workspace, toSearch[i].Key, queryVector, searchSet)
			}()
		}
	}

	return nil
}

// GetFullVectors implements the Txn interface.
func (tx *memTxn) GetFullVectors(
	ctx context.Context, treeKey cspann.TreeKey, refs []cspann.VectorWithKey,
) error {
	if len(refs) == 0 {
		return nil
	}

	// All vectors must be at the same level of the tree.
	if refs[0].Key.PartitionKey != cspann.InvalidKey {
		// Get partition centroids.
		for i := range len(refs) {
			func(vectorWithKey *cspann.VectorWithKey) {
				// Lock the partition to read its data.
				memPart := tx.store.lockPartition(
					treeKey, vectorWithKey.Key.PartitionKey, uniqueOwner, false /* isExclusive */)
				if memPart != nil {
					defer memPart.lock.ReleaseShared()
					vectorWithKey.Vector = memPart.lock.partition.Centroid()
				} else {
					vectorWithKey.Vector = nil
				}
			}(&refs[i])
		}
	} else {
		// Get inserted vectors.
		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()

		for i := range len(refs) {
			vectorWithKey := &refs[i]
			vector, ok := tx.store.mu.vectors[string(vectorWithKey.Key.KeyBytes)]
			if ok {
				vectorWithKey.Vector = vector
			} else {
				vectorWithKey.Vector = nil
			}
		}
	}

	return nil
}

// ensureLockedRootPartition lazily creates a root partition for the specified
// K-means tree, and returns it with an exclusive lock already acquired on it.
// NOTE: This is only intended for use with AddToPartition and/or other methods
// that need an exclusive lock.
func (tx *memTxn) ensureLockedRootPartition(treeKey cspann.TreeKey) *memPartition {
	metadata := tx.store.makeEmptyRootMetadata()
	memPart, _ := tx.store.tryCreateEmptyPartition(treeKey, cspann.RootKey, metadata)

	// Acquire an exclusive lock on the partition.
	memPart.lock.Acquire(tx.id)
	return memPart
}

// lockPartition acquires a shared or exclusive lock of the given partition. If
// the partition does not exist, it returns ErrPartitionNotFound. Unlike
// Store.lockPartition, this method takes the timestamp of the transaction into
// account. For example, if the partition has been deleted before the
// transaction's creation time, it returns ErrPartitionNotFound. Or if the root
// partition's level has been updated since the transaction's creation time, it
// returns ErrRestartOperation.
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

	// If the root partition's level was updated after the transaction was
	// started, or if the partition was deleted after the transaction was started,
	// then treat this as a concurrency conflict and instruct the caller to
	// restart the operation at a later time.
	if tx.current < memPart.lock.created || tx.current < memPart.lock.deleted {
		// Release the partition lock.
		if isExclusive {
			memPart.lock.Release()
		} else {
			memPart.lock.ReleaseShared()
		}

		tx.store.mu.Lock()
		defer tx.store.mu.Unlock()
		prevCurrent := tx.current
		tx.current = tx.store.tickLocked()
		return nil, errors.Wrapf(cspann.ErrRestartOperation,
			"root partition (created=%d) is not visible to txn %d (current=%d)",
			memPart.lock.created, tx.id, prevCurrent)
	}

	// If the partition has been deleted, and the operation wasn't restarted
	// above, then return ErrPartitionNotFound.
	if memPart.lock.deleted != 0 {
		if isExclusive {
			memPart.lock.Release()
		} else {
			memPart.lock.ReleaseShared()
		}

		return nil, errors.Wrapf(cspann.ErrPartitionNotFound,
			"partition %d (created=%d, deleted=%d) has been deleted, txn %d (current=%d)",
			partitionKey, memPart.lock.created, memPart.lock.deleted, tx.id, tx.current)
	}

	return memPart, nil
}
