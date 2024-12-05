// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/container/list"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Threading and Locking
// =====================
// 1. Only one split or merge runs at a time. This is ensured using a store-wide
//    structure modification lock.
// 2. Each partition can be locked separately using single writer, multiple
//    readers locking. For example, one caller at a time can be adding a vector
//    to a partition, but multiple callers can concurrently search it.
// 3. Only split or merge operations hold more than one lock at a time. Because
//    only one runs at a time, this prevents deadlocks.
// 4. All other operations such as adding or removing vectors from a partition,
//    or searching a partition, acquire a single partition lock on a single
//    partition, do their work, and then release the lock.
// 5. When a partition is deleted, it is not removed from the store until all
//    transactions created before the deletion have ended. Instead, it is marked
//    as deleted, and operations can decide what to do when they come across
//    a deleted partition. For example, insert operations restart and search
//    operations simply ignore the mark. Keeping deleted partitions in the store
//    ensures that the K-means tree is always structurally complete; while some
//    of its partitions may be marked as deleted, there are never "dangling"
//    partition references that would trigger "partition not found" errors.
// 6. To ensure that searches of deleted partitions do not miss vectors, store
//    operations should not remove vectors during the deletion process. In
//    effect, the deleted partition should retain a static snapshot of the
//    vectors it had at the time of its deletion.

// storeStatsAlpha specifies the ratio of new values to existing values in EMA
// calculations.
const storeStatsAlpha = 0.05

// inMemoryTxn tracks the transaction's state.
type inMemoryTxn struct {
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
	unbalanced *inMemoryPartition

	// ownedLocks contains all exclusive partition locks that have been obtained
	// during the transaction. These will be released at the end of the
	// transaction.
	ownedLocks []*inMemoryLock

	// The following fields can only be accessed after the store mutex has been
	// acquired (i.e. InMemoryStore.mu).
	muStore struct {
		// ended is set to true once the transaction has ended. It is consulted
		// to determine when it's safe to garbage collect deleted partitions.
		ended bool
	}
}

// inMemoryPartition wraps the partition data structure in order to add more
// information, such as its key, creation time, and whether it is deleted.
type inMemoryPartition struct {
	// key is the unique identifier for the partition. It is immutable and can
	// be accessed without a lock.
	key PartitionKey

	// lock is a read-write lock that callers must acquire before accessing any
	// of the fields.
	lock struct {
		inMemoryLock

		// partition is the wrapped partition.
		partition *Partition
		// created is the logical clock time at which the partition was created.
		created uint64
		// deleted is true if the partition has been deleted.
		deleted bool
	}
}

// isVisibleLocked returns true if the partition is visible to a transaction at
// the given logical clock time. Once a partition has been deleted, it is not
// considered visible to any transaction, regardless of its current time. In
// addition, if a transaction's current time is before the root partition's
// creation time, then the partition is not visible. This latter check is
// necessary because the root partition is modified in-place, unlike other
// partitions which are never reused after their deletion.
func (p *inMemoryPartition) isVisibleLocked(current uint64) bool {
	return !p.lock.deleted && (p.key != RootKey || current > p.lock.created)
}

// pendingItem tracks currently active transactions as well as partitions that
// have been deleted. Deleted partitions cannot be garbage collected until all
// transactions that pre-date the deletion have ended.
// NOTE: Either activeTxn or deletedPartition must be defined, but not both.
type pendingItem struct {
	activeTxn        inMemoryTxn
	deletedPartition *inMemoryPartition
}

// InMemoryStore implements the Store interface over in-memory partitions and
// vectors. This is only used for testing and benchmarking. As such, it is
// packed with assertions and extra validation intended to catch bugs.
type InMemoryStore struct {
	dims int
	seed int64

	// structureLock must be acquired by transactions that intend to modify the
	// structure of the tree, e.g. splitting or merging a partition. This ensures
	// that only one split or merge can be running at any given time, so that
	// deadlocks are not possible.
	structureLock inMemoryLock

	mu struct {
		syncutil.Mutex

		// partitions stores all partitions in the store.
		partitions map[PartitionKey]*inMemoryPartition
		// vectors stores all original, full-sized vectors in the store, indexed
		// by primary key (bytes converted to a string).
		vectors map[string]vector.T
		// clock tracks the logical clock time. It is incremented each time it's
		// accessed in order to order events relative to one another.
		clock uint64
		// nextKey is the next unused unique partition key.
		nextKey PartitionKey
		// stats tracks the global index stats that are periodically merged with
		// local stats cached by instances of the vector index.
		stats IndexStats
		// pending is a list of active transactions and deleted partitions that
		// are ordered by creation time and deletion time, respectively. This is
		// used to ensure that a deleted partition is not garbage collected until
		// there are no longer any active transactions that might reference it.
		pending list.List[pendingItem]
	}
}

var _ Store = (*InMemoryStore)(nil)

// NewInMemoryStore constructs a new in-memory store for vectors of the given
// size. The seed determines how vectors are transformed as part of
// quantization and must be preserved if the store is saved to disk or loaded
// from disk.
func NewInMemoryStore(dims int, seed int64) *InMemoryStore {
	st := &InMemoryStore{
		dims: dims,
		seed: seed,
	}
	st.mu.partitions = make(map[PartitionKey]*inMemoryPartition)
	st.mu.nextKey = RootKey + 1

	// Create empty root partition.
	var empty vector.Set
	quantizer := quantize.NewUnQuantizer(dims)
	quantizedSet := quantizer.Quantize(context.Background(), &empty)
	inMemPartition := &inMemoryPartition{
		key: RootKey,
	}
	inMemPartition.lock.partition = &Partition{
		quantizer:    quantizer,
		quantizedSet: quantizedSet,
		level:        LeafLevel,
	}
	inMemPartition.lock.created = 1
	st.mu.partitions[RootKey] = inMemPartition

	st.mu.clock = 2
	st.mu.vectors = make(map[string]vector.T)
	st.mu.stats.NumPartitions = 1
	st.mu.pending.Init()
	return st
}

// BeginTransaction implements the Store interface.
func (s *InMemoryStore) BeginTransaction(ctx context.Context) (Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create new transaction with unique id set to the current logical clock
	// tick and insert the transaction into the pending list.
	current := s.tickLocked()
	txn := inMemoryTxn{id: current, current: current}
	elem := s.mu.pending.PushBack(pendingItem{activeTxn: txn})
	return &elem.Value.activeTxn, nil
}

// CommitTransaction implements the Store interface.
func (s *InMemoryStore) CommitTransaction(ctx context.Context, txn Txn) error {
	// Release any exclusive partition locks held by the transaction.
	inMemTxn := txn.(*inMemoryTxn)
	for i := range inMemTxn.ownedLocks {
		inMemTxn.ownedLocks[i].Release()
	}

	// Panic if the K-means tree contains an empty non-leaf partition after the
	// transaction ends, as this violates the constraint that the K-means tree
	// is always full balanced. This is helpful in testing.
	if inMemTxn.unbalanced != nil {
		// Need to acquire partition lock before inspecting the partition.
		func() {
			inMemTxn.unbalanced.lock.AcquireShared(inMemTxn.id)
			defer inMemTxn.unbalanced.lock.ReleaseShared()

			partition := inMemTxn.unbalanced.lock.partition
			if partition.Count() == 0 && partition.Level() > LeafLevel {
				if inMemTxn.unbalanced.isVisibleLocked(inMemTxn.current) {
					panic(errors.AssertionFailedf(
						"K-means tree is unbalanced, with empty non-leaf partition %d",
						inMemTxn.unbalanced.key))
				}
			}
		}()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark transaction as ended.
	inMemTxn.muStore.ended = true

	// Iterate over pending actions:
	//   1. Remove any ended transactions that are at the front of the pending
	//      list (i.e. that have been pending for longest). Transactions are
	//      always removed in FIFO order, even if they actually ended in a
	//      different order.
	//   2. Garbage collect any deleted partitions that are at the front of the
	//      pending list.
	for s.mu.pending.Len() > 0 {
		elem := s.mu.pending.Front()
		if elem.Value.activeTxn.id != 0 {
			if !elem.Value.activeTxn.muStore.ended {
				// Oldest transaction is still active, no nothing more to do.
				break
			}
		} else {
			// Garbage collect the deleted partition.
			delete(s.mu.partitions, elem.Value.deletedPartition.key)
		}

		// Remove the action from the pending list.
		s.mu.pending.Remove(elem)
	}

	return nil
}

// AbortTransaction implements the Store interface.
func (s *InMemoryStore) AbortTransaction(ctx context.Context, txn Txn) error {
	inMemTxn := txn.(*inMemoryTxn)
	if inMemTxn.updated {
		// AbortTransaction is only trivially supported by the in-memory store.
		panic(errors.AssertionFailedf(
			"in-memory transaction cannot be aborted because state has already been updated"))
	}
	return s.CommitTransaction(ctx, txn)
}

// GetPartition implements the Store interface.
func (s *InMemoryStore) GetPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey,
) (*Partition, error) {
	// GetPartition is only called by split and merge operations, so acquire the
	// exclusive structure lock so that only one operation at a time can modify
	// the tree structure.
	inMemTxn := txn.(*inMemoryTxn)
	s.structureLock.Acquire(inMemTxn.id)
	inMemTxn.ownedLocks = append(inMemTxn.ownedLocks, &s.structureLock)

	// Acquire exclusive lock on the requested partition for the duration of the
	// transaction.
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return nil, err
	}
	inMemPartition.lock.Acquire(inMemTxn.id)
	inMemTxn.ownedLocks = append(inMemTxn.ownedLocks, &inMemPartition.lock.inMemoryLock)

	// Return an error if the partition has been deleted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		return nil, ErrPartitionNotFound
	}

	// Make a deep copy of the partition, since modifications shouldn't impact
	// the store's copy.
	return inMemPartition.lock.partition.Clone(), nil
}

// SetRootPartition implements the Store interface.
func (s *InMemoryStore) SetRootPartition(ctx context.Context, txn Txn, partition *Partition) error {
	inMemTxn := txn.(*inMemoryTxn)
	if !s.structureLock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", inMemTxn.id))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.mu.partitions[RootKey]
	if !ok {
		panic(errors.AssertionFailedf("the root partition cannot be found"))
	}

	// Existing root partition should have been locked by the transaction.
	if !existing.lock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", inMemTxn.id))
	}

	s.reportPartitionSizeLocked(partition.Count())

	// Grow or shrink CVStats slice if a new level is being added or removed.
	expectedLevels := int(partition.Level() - 1)
	if expectedLevels > len(s.mu.stats.CVStats) {
		s.mu.stats.CVStats = slices.Grow(s.mu.stats.CVStats, expectedLevels-len(s.mu.stats.CVStats))
	}
	s.mu.stats.CVStats = s.mu.stats.CVStats[:expectedLevels]

	// Update the root partition's creation time to indicate to callers that it
	// was replaced.
	existing.lock.partition = partition
	existing.lock.created = s.tickLocked()

	s.updatedStructureLocked(inMemTxn)
	return nil
}

// InsertPartition implements the Store interface.
func (s *InMemoryStore) InsertPartition(
	ctx context.Context, txn Txn, partition *Partition,
) (PartitionKey, error) {
	inMemTxn := txn.(*inMemoryTxn)
	if !s.structureLock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", inMemTxn.id))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Assign key to new partition.
	partitionKey := s.mu.nextKey
	s.mu.nextKey++

	// Insert new partition.
	inMemPartition := &inMemoryPartition{key: partitionKey}
	inMemPartition.lock.partition = partition
	inMemPartition.lock.created = s.tickLocked()
	s.mu.partitions[partitionKey] = inMemPartition

	// Update stats.
	s.mu.stats.NumPartitions++
	s.reportPartitionSizeLocked(partition.Count())

	s.updatedStructureLocked(inMemTxn)
	return partitionKey, nil
}

// DeletePartition implements the Store interface.
func (s *InMemoryStore) DeletePartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey,
) error {
	inMemTxn := txn.(*inMemoryTxn)
	if !s.structureLock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire structure lock", inMemTxn.id))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if partitionKey == RootKey {
		panic(errors.AssertionFailedf("cannot delete the root partition"))
	}

	inMemPartition, ok := s.mu.partitions[partitionKey]
	if !ok {
		return ErrPartitionNotFound
	}

	// Existing root partition should have been locked by the transaction.
	if !inMemPartition.lock.IsAcquiredBy(inMemTxn.id) {
		panic(errors.AssertionFailedf("txn %d did not acquire root partition lock", inMemTxn.id))
	}

	// Mark partition as deleted.
	if inMemPartition.lock.deleted {
		panic(errors.AssertionFailedf("partition %d is already deleted", partitionKey))
	}
	inMemPartition.lock.deleted = true

	// Add the partition to the pending list so that it will only be garbage
	// collected once all older transactions have ended.
	s.mu.pending.PushBack(pendingItem{deletedPartition: inMemPartition})

	s.mu.stats.NumPartitions--

	s.updatedStructureLocked(inMemTxn)
	return nil
}

// AddToPartition implements the Store interface.
func (s *InMemoryStore) AddToPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
) (int, error) {
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return 0, err
	}

	// Acquire exclusive lock on the partition.
	inMemTxn := txn.(*inMemoryTxn)
	inMemPartition.lock.Acquire(inMemTxn.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		// Push forward transaction's current time so that the restarted operation
		// will see the root partition if it was replaced.
		s.mu.Lock()
		defer s.mu.Unlock()
		inMemTxn.current = s.tickLocked()
		return 0, ErrRestartOperation
	}

	// Add the vector to the partition.
	partition := inMemPartition.lock.partition
	if partition.Add(ctx, vector, childKey) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.reportPartitionSizeLocked(partition.Count())
	}

	inMemTxn.updated = true
	return partition.Count(), nil
}

// RemoveFromPartition implements the Store interface.
func (s *InMemoryStore) RemoveFromPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	inMemPartition, err := s.getPartition(partitionKey)
	if err != nil {
		return 0, err
	}

	// Acquire exclusive lock on the partition.
	inMemTxn := txn.(*inMemoryTxn)
	inMemPartition.lock.Acquire(inMemTxn.id)
	defer inMemPartition.lock.Release()

	// If the partition is deleted, then this transaction conflicted with another
	// transaction and needs to be restarted.
	if !inMemPartition.isVisibleLocked(inMemTxn.current) {
		// Push forward transaction's current time so that the restarted operation
		// will see the root partition if it was replaced.
		s.mu.Lock()
		defer s.mu.Unlock()
		inMemTxn.current = s.tickLocked()
		return 0, ErrRestartOperation
	}

	// Remove vector from the partition.
	partition := inMemPartition.lock.partition
	if partition.ReplaceWithLastByKey(childKey) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.reportPartitionSizeLocked(partition.Count())
	}

	if partition.Count() == 0 && partition.Level() > LeafLevel {
		// A non-leaf partition has zero vectors. If this is still true at the
		// end of the transaction, the K-means tree will be unbalanced, which
		// violates a key constraint.
		txn.(*inMemoryTxn).unbalanced = inMemPartition
	}

	inMemTxn.updated = true
	return partition.Count(), nil
}

// SearchPartitions implements the Store interface.
func (s *InMemoryStore) SearchPartitions(
	ctx context.Context,
	txn Txn,
	partitionKeys []PartitionKey,
	queryVector vector.T,
	searchSet *SearchSet,
	partitionCounts []int,
) (level Level, err error) {
	inMemTxn := txn.(*inMemoryTxn)

	for i := 0; i < len(partitionKeys); i++ {
		inMemPartition, err := s.getPartition(partitionKeys[i])
		if err != nil {
			return 0, err
		}

		// Acquire shared lock on partition and search it. Note that we don't need
		// to check if the partition has been deleted, since the transaction that
		// deleted it must be concurrent with this transaction (or else the
		// deleted partition would not have been found by this transaction).
		func() {
			inMemPartition.lock.AcquireShared(inMemTxn.id)
			defer inMemPartition.lock.ReleaseShared()

			searchLevel, partitionCount := inMemPartition.lock.partition.Search(
				ctx, partitionKeys[i], queryVector, searchSet)
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

// GetFullVectors implements the Store interface.
func (s *InMemoryStore) GetFullVectors(ctx context.Context, txn Txn, refs []VectorWithKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < len(refs); i++ {
		ref := &refs[i]
		if ref.Key.PartitionKey != InvalidKey {
			// Get the partition's centroid.
			inMemPartition, ok := s.mu.partitions[ref.Key.PartitionKey]
			if !ok {
				panic(errors.AssertionFailedf("partition %d does not exist", ref.Key.PartitionKey))
			}

			// Don't need to acquire lock to call the Centroid method, since it
			// is immutable and thread-safe.
			ref.Vector = inMemPartition.lock.partition.Centroid()
		} else {
			vector, ok := s.mu.vectors[string(refs[i].Key.PrimaryKey)]
			if ok {
				ref.Vector = vector
			} else {
				ref.Vector = nil
			}
		}
	}

	return nil
}

// MergeStats implements the Store interface.
func (s *InMemoryStore) MergeStats(ctx context.Context, stats *IndexStats, skipMerge bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !skipMerge {
		// Merge CVStats.
		for i := range stats.CVStats {
			if i >= len(s.mu.stats.CVStats) {
				// More levels of incoming stats than the in-memory store has.
				break
			}

			// Calculate exponentially weighted moving averages of the mean and
			// variance samples that are reported by calling agents.
			sample := &stats.CVStats[i]
			cvstats := &s.mu.stats.CVStats[i]
			if cvstats.Mean == 0 {
				cvstats.Mean = sample.Mean
			} else {
				cvstats.Mean = storeStatsAlpha*sample.Mean + (1-storeStatsAlpha)*cvstats.Mean
				cvstats.Variance = storeStatsAlpha*sample.Variance + (1-storeStatsAlpha)*cvstats.Variance
			}
		}
	}

	// Update incoming stats with global stats.
	stats.NumPartitions = s.mu.stats.NumPartitions
	stats.VectorsPerPartition = s.mu.stats.VectorsPerPartition

	extra := len(s.mu.stats.CVStats) - len(stats.CVStats)
	if extra > 0 {
		stats.CVStats = slices.Grow(stats.CVStats, extra)
	}
	stats.CVStats = stats.CVStats[:len(s.mu.stats.CVStats)]
	copy(stats.CVStats, s.mu.stats.CVStats)

	return nil
}

// InsertVector inserts a new full-size vector into the in-memory store,
// associated with the given primary key. This mimics inserting a vector into
// the primary index, and is used during testing and benchmarking.
func (s *InMemoryStore) InsertVector(txn Txn, key PrimaryKey, vector vector.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.vectors[string(key)] = vector

	inMemTxn := txn.(*inMemoryTxn)
	inMemTxn.updated = true
}

// DeleteVector deletes the vector associated with the given primary key from
// the in-memory store. This mimics deleting a vector from the primary index,
// and is used during testing and benchmarking.
func (s *InMemoryStore) DeleteVector(txn Txn, key PrimaryKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.vectors, string(key))

	inMemTxn := txn.(*inMemoryTxn)
	inMemTxn.updated = true
}

// GetVector returns a single vector from the store, by its primary key. This
// is used for testing.
func (s *InMemoryStore) GetVector(key PrimaryKey) vector.T {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mu.vectors[string(key)]
}

// GetAllVectors returns all vectors that have been added to the store as key
// and vector pairs. This is used for testing.
func (s *InMemoryStore) GetAllVectors() []VectorWithKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	refs := make([]VectorWithKey, 0, len(s.mu.vectors))
	for key, vec := range s.mu.vectors {
		refs = append(refs, VectorWithKey{Key: ChildKey{PrimaryKey: PrimaryKey(key)}, Vector: vec})
	}
	return refs
}

// MarshalBinary saves the in-memory store as a bytes. This allows the store to
// be saved and later loaded without needing to rebuild it from scratch.
func (s *InMemoryStore) MarshalBinary() (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.pending.Len() > 0 {
		panic(errors.AssertionFailedf("cannot save store if there are pending transactions"))
	}

	storeProto := StoreProto{
		Dims:       s.dims,
		Seed:       s.seed,
		Partitions: make([]PartitionProto, 0, len(s.mu.partitions)),
		NextKey:    s.mu.nextKey,
		Vectors:    make([]VectorProto, 0, len(s.mu.vectors)),
		Stats:      s.mu.stats,
	}

	// Remap partitions to protobufs.
	for partitionKey, inMemPartition := range s.mu.partitions {
		func() {
			inMemPartition.lock.AcquireShared(0)
			defer inMemPartition.lock.ReleaseShared()

			partition := inMemPartition.lock.partition
			partitionProto := PartitionProto{
				PartitionKey: partitionKey,
				ChildKeys:    partition.ChildKeys(),
				Level:        partition.Level(),
			}

			rabitq, ok := partition.quantizedSet.(*quantize.RaBitQuantizedVectorSet)
			if ok {
				partitionProto.RaBitQ = rabitq
			} else {
				partitionProto.UnQuantized = partition.quantizedSet.(*quantize.UnQuantizedVectorSet)
			}

			storeProto.Partitions = append(storeProto.Partitions, partitionProto)
		}()
	}

	// Remap vectors to protobufs.
	for key, vector := range s.mu.vectors {
		vectorWithKey := VectorProto{PrimaryKey: []byte(key), Vector: vector}
		storeProto.Vectors = append(storeProto.Vectors, vectorWithKey)
	}

	// Serialize the protobuf as bytes.
	return protoutil.Marshal(&storeProto)
}

// LoadInMemoryStore loads the in-memory store from bytes that were previously
// saved by MarshalBinary.
func LoadInMemoryStore(data []byte) (*InMemoryStore, error) {
	// Unmarshal bytes into a protobuf.
	var storeProto StoreProto
	if err := protoutil.Unmarshal(data, &storeProto); err != nil {
		return nil, err
	}

	// Construct the InMemoryStore object.
	inMemStore := &InMemoryStore{
		dims: storeProto.Dims,
		seed: storeProto.Seed,
	}
	inMemStore.mu.clock = 2
	inMemStore.mu.partitions = make(map[PartitionKey]*inMemoryPartition, len(storeProto.Partitions))
	inMemStore.mu.nextKey = storeProto.NextKey
	inMemStore.mu.vectors = make(map[string]vector.T, len(storeProto.Vectors))
	inMemStore.mu.stats = storeProto.Stats
	inMemStore.mu.pending.Init()

	raBitQuantizer := quantize.NewRaBitQuantizer(storeProto.Dims, storeProto.Seed)
	unquantizer := quantize.NewUnQuantizer(storeProto.Dims)

	// Construct the Partition objects.
	for i := range storeProto.Partitions {
		partitionProto := &storeProto.Partitions[i]
		var inMemPartition = &inMemoryPartition{
			key: partitionProto.PartitionKey,
		}
		inMemPartition.lock.created = 1

		if partitionProto.RaBitQ != nil {
			inMemPartition.lock.partition = &Partition{
				quantizer:    raBitQuantizer,
				quantizedSet: partitionProto.RaBitQ,
			}
		} else {
			inMemPartition.lock.partition = &Partition{
				quantizer:    unquantizer,
				quantizedSet: partitionProto.UnQuantized,
			}
		}

		partition := inMemPartition.lock.partition
		partition.childKeys = partitionProto.ChildKeys
		partition.level = partitionProto.Level

		inMemStore.mu.partitions[partitionProto.PartitionKey] = inMemPartition
	}

	// Insert vectors into the in-memory store.
	for i := range storeProto.Vectors {
		vectorProto := storeProto.Vectors[i]
		inMemStore.mu.vectors[string(vectorProto.PrimaryKey)] = vectorProto.Vector
	}

	return inMemStore, nil
}

// getPartition returns a partition by its key, or ErrPartitionNotFound if no
// such partition exists.
func (s *InMemoryStore) getPartition(partitionKey PartitionKey) (*inMemoryPartition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partition, ok := s.mu.partitions[partitionKey]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return partition, nil
}

// tickLocked returns the current value of the logical clock and advances its
// time.
// NOTE: Callers must have locked the s.mu mutex.
func (s *InMemoryStore) tickLocked() uint64 {
	val := s.mu.clock
	s.mu.clock++
	return val
}

// updatedStructureLocked marks the transaction as having updated the K-means
// tree. It also pushes the transactions current time forward so that it can
// always observe its changes.
// NOTE: Callers must have locked the s.mu mutex.
func (s *InMemoryStore) updatedStructureLocked(inMemTxn *inMemoryTxn) {
	inMemTxn.updated = true
	inMemTxn.current = s.tickLocked()
}

// reportPartitionSizeLocked updates the vectors per partition statistic. It is
// called with the count of vectors in a partition when a partition is inserted
// or updated.
// NOTE: Callers must have locked the s.mu mutex.
func (s *InMemoryStore) reportPartitionSizeLocked(count int) {
	if s.mu.stats.VectorsPerPartition == 0 {
		// Use first value if this is the first update.
		s.mu.stats.VectorsPerPartition = float64(count)
	} else {
		// Calculate exponential moving average.
		s.mu.stats.VectorsPerPartition = (1 - storeStatsAlpha) * s.mu.stats.VectorsPerPartition
		s.mu.stats.VectorsPerPartition += storeStatsAlpha * float64(count)
	}
}
