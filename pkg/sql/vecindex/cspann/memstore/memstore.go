// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"
	"encoding/binary"
	"slices"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
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

// TreeID identifies a K-means tree in a vector index. The set of partitions in
// a particular tree are independent from those in any other tree in the index.
type TreeID uint64

// ToTreeKey converts a tree ID to a cspann.TreeKey. If the ID is zero, it is
// converted to nil (the default tree).
func ToTreeKey(treeID TreeID) cspann.TreeKey {
	if treeID == 0 {
		return nil
	}

	// Pre-allocate exactly 8 bytes for uint64
	key := make(cspann.TreeKey, 8)
	binary.BigEndian.PutUint64(key, uint64(treeID))
	return key
}

// FromTreeKey converts a cspann.TreeKey to a tree ID. A nil tree key gets
// converted to zero.
func FromTreeKey(treeKey cspann.TreeKey) TreeID {
	if treeKey == nil {
		return 0
	}
	return TreeID(binary.BigEndian.Uint64(treeKey))
}

// qualifiedPartitionKey uniquely identifies a partition within a K-means tree
// in vector index. Because every tree uses the same well-known partition key
// for its root partition, the tree ID is needed in order to disambiguate.
type qualifiedPartitionKey struct {
	treeID       TreeID
	partitionKey cspann.PartitionKey
}

// makeQualifiedPartitionKey constructs a new qualified partition key.
func makeQualifiedPartitionKey(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) qualifiedPartitionKey {
	return qualifiedPartitionKey{treeID: FromTreeKey(treeKey), partitionKey: partitionKey}
}

// memPartition wraps the partition data structure in order to add more
// information, such as its key, creation time, and whether it is deleted.
type memPartition struct {
	// key is the unique identifier for the partition. It is immutable and can
	// be accessed without a lock.
	key qualifiedPartitionKey
	// count tracks the number of vectors in this partition. It is an atomic so
	// that it can be read outside the scope of the lock.
	count atomic.Int64

	// lock is a read-write lock that callers must acquire before accessing any
	// of the fields.
	lock struct {
		memLock

		// partition is the wrapped partition.
		partition *cspann.Partition
		// created is the logical clock time at which the partition was created.
		created uint64
		// deleted is the logical clock time at which the partition was deleted.
		deleted uint64
	}
}

// pendingItem tracks currently active transactions as well as partitions that
// have been deleted. Deleted partitions cannot be garbage collected until all
// transactions that pre-date the deletion have ended.
// NOTE: Either activeTxn or deletedPartition must be defined, but not both.
type pendingItem struct {
	activeTxn        memTxn
	deletedPartition *memPartition
}

// Store implements the Store interface over in-memory partitions and
// vectors. This is only used for testing and benchmarking. As such, it is
// packed with assertions and extra validation intended to catch bugs.
type Store struct {
	dims          int
	seed          int64
	rootQuantizer quantize.Quantizer
	quantizer     quantize.Quantizer

	mu struct {
		syncutil.Mutex

		// partitions stores all partitions in the store.
		partitions map[qualifiedPartitionKey]*memPartition
		// vectors stores all original, full-sized vectors in the store, indexed
		// by primary key (bytes converted to a string).
		vectors map[string]vector.T
		// clock tracks the logical clock time. It is incremented each time it's
		// accessed in order to order events relative to one another.
		clock uint64
		// nextKey is the next unused unique partition key.
		nextKey cspann.PartitionKey
		// stats tracks the global index stats that are periodically merged with
		// local stats cached by instances of the vector index.
		stats cspann.IndexStats
		// pending is a list of active transactions and deleted partitions that
		// are ordered by creation time and deletion time, respectively. This is
		// used to ensure that a deleted partition is not garbage collected until
		// there are no longer any active transactions that might reference it.
		pending list.List[pendingItem]
		// emptyVec is a zero-valued vector, used when root centroid does not
		// exist.
		emptyVec vector.T
	}
}

var _ cspann.Store = (*Store)(nil)

// New constructs a new in-memory store that uses the given quantizer for
// non-root partitions. The seed determines how vectors are transformed as part
// of quantization and must be preserved if the store is saved to disk or loaded
// from disk.
func New(quantizer quantize.Quantizer, seed int64) *Store {
	st := &Store{
		dims:          quantizer.GetDims(),
		seed:          seed,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetDims(), quantizer.GetDistanceMetric()),
		quantizer:     quantizer,
	}

	st.mu.partitions = make(map[qualifiedPartitionKey]*memPartition)
	st.mu.vectors = make(map[string]vector.T)
	st.mu.clock = 2
	st.mu.nextKey = cspann.RootKey + 1
	st.mu.pending.Init()
	return st
}

// Dims returns the number of dimensions in stored vectors.
func (s *Store) Dims() int {
	return s.dims
}

// RunTransaction implements the Store interface.
func (s *Store) RunTransaction(ctx context.Context, fn func(txn cspann.Txn) error) (err error) {
	var txn cspann.Txn
	txn, err = s.beginTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = s.commitTransaction(txn)
		}
		if err != nil {
			err = errors.CombineErrors(err, s.abortTransaction(txn))
		}
	}()

	return fn(txn)
}

// beginTransaction starts a new memstore transaction.
func (s *Store) beginTransaction() (cspann.Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create new transaction with unique id set to the current logical clock
	// tick and insert the transaction into the pending list.
	current := s.tickLocked()
	txn := memTxn{id: current, current: current, store: s}
	elem := s.mu.pending.PushBack(pendingItem{activeTxn: txn})
	return &elem.Value.activeTxn, nil
}

// commitTransaction commits a running memstore transaction.
func (s *Store) commitTransaction(txn cspann.Txn) error {
	// Release any exclusive partition locks held by the transaction.
	tx := txn.(*memTxn)
	for i := range tx.ownedLocks {
		tx.ownedLocks[i].Release()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark transaction as ended.
	tx.muStore.ended = true

	s.processPendingActionsLocked()

	return nil
}

// abortTransaction implements the Store interface.
func (s *Store) abortTransaction(txn cspann.Txn) error {
	tx := txn.(*memTxn)
	if tx.updated {
		// Abort is only trivially supported by the in-memory store.
		panic(errors.AssertionFailedf(
			"in-memory transaction cannot be aborted because state has already been updated"))
	}
	return s.commitTransaction(txn)
}

// MakePartitionKey implements the Store interface.
func (s *Store) MakePartitionKey() cspann.PartitionKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitionKey := s.mu.nextKey
	s.mu.nextKey++

	return partitionKey
}

// EstimatePartitionCount implements the Store interface.
func (s *Store) EstimatePartitionCount(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (int, error) {
	memPart, ok := s.getPartition(treeKey, partitionKey)
	if !ok {
		// Partition does not exist, so return 0.
		return 0, nil
	}
	return int(memPart.count.Load()), nil
}

// MergeStats implements the Store interface.
func (s *Store) MergeStats(ctx context.Context, stats *cspann.IndexStats, skipMerge bool) error {
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

	extra := len(s.mu.stats.CVStats) - len(stats.CVStats)
	if extra > 0 {
		stats.CVStats = slices.Grow(stats.CVStats, extra)
	}
	stats.CVStats = stats.CVStats[:len(s.mu.stats.CVStats)]
	copy(stats.CVStats, s.mu.stats.CVStats)

	return nil
}

// TryCreateEmptyPartition implements the Store interface.
func (s *Store) TryCreateEmptyPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	metadata cspann.PartitionMetadata,
) error {
	memPart, ok := s.tryCreateEmptyPartition(treeKey, partitionKey, metadata)
	if ok {
		return nil
	}

	// Partition already exists, so return a ConditionFailedError.
	memPart.lock.AcquireShared(uniqueOwner)
	defer memPart.lock.ReleaseShared()
	return cspann.NewConditionFailedError(*memPart.lock.partition.Metadata())
}

// TryDeletePartition implements the Store interface.
func (s *Store) TryDeletePartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) error {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, true /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return cspann.ErrPartitionNotFound
	}
	defer memPart.lock.Release()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark partition as deleted.
	if memPart.lock.deleted != 0 {
		panic(errors.AssertionFailedf("partition %d is already deleted", partitionKey))
	}
	memPart.lock.deleted = s.tickLocked()

	// Add the partition to the pending list so that it will only be garbage
	// collected once all older transactions have ended.
	s.mu.pending.PushBack(pendingItem{deletedPartition: memPart})
	s.processPendingActionsLocked()

	s.mu.stats.NumPartitions--

	return nil
}

// TryGetPartition implements the Store interface.
func (s *Store) TryGetPartition(
	ctx context.Context, treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (*cspann.Partition, error) {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, false /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return nil, cspann.ErrPartitionNotFound
	}
	defer memPart.lock.ReleaseShared()

	// Make a deep copy of the partition, since modifications shouldn't impact
	// the store's copy.
	return memPart.lock.partition.Clone(), nil
}

// TryGetPartitionMetadata implements the Store interface.
func (s *Store) TryGetPartitionMetadata(
	ctx context.Context, treeKey cspann.TreeKey, toGet []cspann.PartitionMetadataToGet,
) error {
	for i := range toGet {
		item := &toGet[i]

		func() {
			memPart := s.lockPartition(treeKey, item.Key, uniqueOwner, false /* isExclusive */)
			if memPart == nil {
				// Partition does not exist, so map it to Missing.
				item.Metadata = cspann.PartitionMetadata{}
				return
			}
			defer memPart.lock.ReleaseShared()

			// Return a copy of the metadata.
			item.Metadata = *memPart.lock.partition.Metadata()
		}()
	}

	return nil
}

// TryUpdatePartitionMetadata implements the Store interface.
func (s *Store) TryUpdatePartitionMetadata(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	metadata cspann.PartitionMetadata,
	expected cspann.PartitionMetadata,
) error {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, true /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return cspann.ErrPartitionNotFound
	}
	defer memPart.lock.Release()

	// Check precondition.
	existing := memPart.lock.partition.Metadata()
	if !existing.Equal(&expected) {
		return cspann.NewConditionFailedError(*existing)
	}

	// Check whether new level is being added or removed.
	if partitionKey == cspann.RootKey && metadata.Level != existing.Level {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Treat adding new level as if it were a new root partition.
		memPart.lock.created = s.tickLocked()

		// Grow or shrink CVStats slice. Stats are for non-leaf levels, so subtract
		// one to get the new slice length.
		expectedLevels := int(metadata.Level - 1)
		if expectedLevels > len(s.mu.stats.CVStats) {
			s.mu.stats.CVStats =
				slices.Grow(s.mu.stats.CVStats, expectedLevels-len(s.mu.stats.CVStats))
		}
		s.mu.stats.CVStats = s.mu.stats.CVStats[:expectedLevels]
	}

	// Update the partition's metadata.
	*existing = metadata

	return nil
}

// TryAddToPartition implements the Store interface.
func (s *Store) TryAddToPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	vectors vector.Set,
	childKeys []cspann.ChildKey,
	valueBytes []cspann.ValueBytes,
	expected cspann.PartitionMetadata,
) (added bool, err error) {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, true /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return false, cspann.ErrPartitionNotFound
	}
	defer memPart.lock.Release()

	// Check precondition.
	partition := memPart.lock.partition
	existing := partition.Metadata()
	if !existing.Equal(&expected) {
		return false, cspann.NewConditionFailedError(*existing)
	}
	if !existing.StateDetails.State.AllowAdd() {
		return false, errors.AssertionFailedf(
			"cannot add to partition in state %s that disallows adds", existing.StateDetails.State)
	}

	// Add the vectors to the partition. Ignore any duplicate vectors.
	// TODO(andyk): Figure out how to give Store flexible scratch space.
	var w workspace.T
	added = partition.AddSet(&w, vectors, childKeys, valueBytes, false /* overwrite */)
	memPart.count.Store(int64(partition.Count()))

	return added, nil
}

// TryRemoveFromPartition implements the Store interface.
func (s *Store) TryRemoveFromPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	childKeys []cspann.ChildKey,
	expected cspann.PartitionMetadata,
) (removed bool, err error) {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, true /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return false, cspann.ErrPartitionNotFound
	}
	defer memPart.lock.Release()

	// Check precondition.
	partition := memPart.lock.partition
	existing := partition.Metadata()
	if !existing.Equal(&expected) {
		return false, cspann.NewConditionFailedError(*existing)
	}

	// Remove vectors from the partition.
	for i := range childKeys {
		removed = partition.ReplaceWithLastByKey(childKeys[i]) || removed
	}

	// Update partition count.
	memPart.count.Store(int64(partition.Count()))

	return removed, err
}

// TryClearPartition implements the Store interface.
func (s *Store) TryClearPartition(
	ctx context.Context,
	treeKey cspann.TreeKey,
	partitionKey cspann.PartitionKey,
	expected cspann.PartitionMetadata,
) (count int, err error) {
	memPart := s.lockPartition(treeKey, partitionKey, uniqueOwner, true /* isExclusive */)
	if memPart == nil {
		// Partition does not exist.
		return 0, cspann.ErrPartitionNotFound
	}
	defer memPart.lock.Release()

	// Check precondition.
	partition := memPart.lock.partition
	existing := partition.Metadata()
	if !existing.Equal(&expected) {
		return 0, cspann.NewConditionFailedError(*existing)
	}
	if existing.StateDetails.State.AllowAdd() {
		return 0, errors.AssertionFailedf(
			"cannot clear partition in state %s that allows adds", existing.StateDetails.State)
	}

	// Remove vectors from the partition and update partition count.
	count = partition.Clear()
	memPart.count.Store(0)

	return count, nil
}

// EnsureUniquePartitionKey checks that the given partition key is not being
// used yet and also ensures it won't be given out in the future. This is used
// for testing.
func (s *Store) EnsureUniquePartitionKey(treeKey cspann.TreeKey, partitionKey cspann.PartitionKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.getPartitionLocked(treeKey, partitionKey); ok {
		panic(errors.AssertionFailedf("partition key %d is already being used", partitionKey))
	}

	if partitionKey <= s.mu.nextKey {
		s.mu.nextKey = partitionKey + 1
	}
}

// InsertVector inserts a new full-size vector into the in-memory store,
// associated with the given tree key and primary key. This mimics inserting a
// vector into the primary index, and is used during testing and benchmarking.
func (s *Store) InsertVector(key cspann.KeyBytes, vec vector.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.vectors[string(key)] = vec
}

// DeleteVector deletes the vector associated with the given primary key from
// the in-memory store. This mimics deleting a vector from the primary index,
// and is used during testing and benchmarking.
func (s *Store) DeleteVector(key cspann.KeyBytes) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.vectors, string(key))
}

// GetVector returns a single vector from the store, by its primary key. This
// is used for testing.
func (s *Store) GetVector(key cspann.KeyBytes) vector.T {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mu.vectors[string(key)]
}

// GetAllVectors returns all vectors that have been added to the store as key
// and vector pairs. This is used for testing.
func (s *Store) GetAllVectors() []cspann.VectorWithKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	refs := make([]cspann.VectorWithKey, 0, len(s.mu.vectors))
	for key, vec := range s.mu.vectors {
		refs = append(refs, cspann.VectorWithKey{
			Key: cspann.ChildKey{KeyBytes: cspann.KeyBytes(key)}, Vector: vec})
	}
	return refs
}

// MarshalBinary saves the in-memory store as a bytes. This allows the store to
// be saved and later loaded without needing to rebuild it from scratch.
func (s *Store) MarshalBinary() (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.pending.Len() > 0 {
		panic(errors.AssertionFailedf("cannot save store if there are pending transactions"))
	}

	storeProto := StoreProto{
		Dims:           s.dims,
		Seed:           s.seed,
		DistanceMetric: s.quantizer.GetDistanceMetric(),
		Partitions:     make([]PartitionProto, 0, len(s.mu.partitions)),
		NextKey:        s.mu.nextKey,
		Vectors:        make([]VectorProto, 0, len(s.mu.vectors)),
		Stats:          s.mu.stats,
	}

	// Remap partitions to protobufs.
	for qkey, memPart := range s.mu.partitions {
		err = func() error {
			memPart.lock.AcquireShared(uniqueOwner)
			defer memPart.lock.ReleaseShared()

			partition := memPart.lock.partition
			metadata := partition.Metadata()
			partitionProto := PartitionProto{
				TreeId:       qkey.treeID,
				PartitionKey: qkey.partitionKey,
				Metadata: PartitionMetadataProto{
					Level:   partition.Level(),
					State:   metadata.StateDetails.State,
					Target1: metadata.StateDetails.Target1,
					Target2: metadata.StateDetails.Target2,
					Source:  metadata.StateDetails.Source,
				},
				ChildKeys:  partition.ChildKeys(),
				ValueBytes: partition.ValueBytes(),
			}

			rabitq, ok := partition.QuantizedSet().(*quantize.RaBitQuantizedVectorSet)
			if ok {
				partitionProto.RaBitQ = rabitq
			} else {
				partitionProto.UnQuantized = partition.QuantizedSet().(*quantize.UnQuantizedVectorSet)
			}

			storeProto.Partitions = append(storeProto.Partitions, partitionProto)

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	// Remap vectors to protobufs.
	for key, vector := range s.mu.vectors {
		vectorWithKey := VectorProto{KeyBytes: []byte(key), Vector: vector}
		storeProto.Vectors = append(storeProto.Vectors, vectorWithKey)
	}

	// Serialize the protobuf as bytes.
	return protoutil.Marshal(&storeProto)
}

// Load loads the in-memory store from bytes that were previously saved by
// MarshalBinary.
func Load(data []byte) (*Store, error) {
	// Unmarshal bytes into a protobuf.
	var storeProto StoreProto
	if err := protoutil.Unmarshal(data, &storeProto); err != nil {
		return nil, err
	}

	raBitQuantizer := quantize.NewRaBitQuantizer(
		storeProto.Dims, storeProto.Seed, storeProto.DistanceMetric)
	unquantizer := quantize.NewUnQuantizer(storeProto.Dims, storeProto.DistanceMetric)

	// Construct the InMemoryStore object.
	inMemStore := &Store{
		dims:          storeProto.Dims,
		seed:          storeProto.Seed,
		rootQuantizer: unquantizer,
		quantizer:     raBitQuantizer,
	}
	inMemStore.mu.clock = 2
	inMemStore.mu.partitions = make(map[qualifiedPartitionKey]*memPartition, len(storeProto.Partitions))
	inMemStore.mu.nextKey = storeProto.NextKey
	inMemStore.mu.vectors = make(map[string]vector.T, len(storeProto.Vectors))
	inMemStore.mu.stats = storeProto.Stats
	inMemStore.mu.pending.Init()

	// Construct the Partition objects.
	for i := range storeProto.Partitions {
		partitionProto := &storeProto.Partitions[i]
		var memPart = &memPartition{
			key: qualifiedPartitionKey{
				treeID:       partitionProto.TreeId,
				partitionKey: partitionProto.PartitionKey,
			},
		}
		memPart.lock.created = 1

		var quantizer quantize.Quantizer
		var quantizedSet quantize.QuantizedVectorSet
		var centroid vector.T
		if partitionProto.RaBitQ != nil {
			quantizer = raBitQuantizer
			quantizedSet = partitionProto.RaBitQ
			centroid = partitionProto.RaBitQ.Centroid
		} else {
			quantizer = unquantizer
			quantizedSet = partitionProto.UnQuantized
			centroid = make(vector.T, unquantizer.GetDims())
		}

		metadata := cspann.PartitionMetadata{
			Level:    partitionProto.Metadata.Level,
			Centroid: centroid,
			StateDetails: cspann.PartitionStateDetails{
				State:   partitionProto.Metadata.State,
				Target1: partitionProto.Metadata.Target1,
				Target2: partitionProto.Metadata.Target2,
				Source:  partitionProto.Metadata.Source,
			},
		}
		memPart.lock.partition = cspann.NewPartition(metadata, quantizer, quantizedSet,
			partitionProto.ChildKeys, partitionProto.ValueBytes)

		inMemStore.mu.partitions[memPart.key] = memPart
	}

	// Insert vectors into the in-memory store.
	for i := range storeProto.Vectors {
		vectorProto := storeProto.Vectors[i]
		inMemStore.mu.vectors[string(vectorProto.KeyBytes)] = vectorProto.Vector
	}

	return inMemStore, nil
}

// insertPartitionLocked adds the given partition to the set of partitions
// managed by the store.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) insertPartitionLocked(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, partition *cspann.Partition,
) *memPartition {
	qkey := makeQualifiedPartitionKey(treeKey, partitionKey)
	memPart := &memPartition{key: qkey}
	memPart.lock.partition = partition
	memPart.lock.created = s.tickLocked()
	memPart.count.Store(int64(partition.Count()))
	s.mu.partitions[qkey] = memPart
	return memPart
}

// tickLocked returns the current value of the logical clock and advances its
// time.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) tickLocked() uint64 {
	val := s.mu.clock
	s.mu.clock++
	return val
}

// getPartition returns a partition by its key, or ErrPartitionNotFound if no
// such partition exists.
func (s *Store) getPartition(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (memPart *memPartition, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getPartitionLocked(treeKey, partitionKey)
}

// getPartitionLocked returns a partition by its key, or ErrPartitionNotFound if
// no such partition exists.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) getPartitionLocked(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey,
) (memPart *memPartition, ok bool) {
	memPart, ok = s.mu.partitions[makeQualifiedPartitionKey(treeKey, partitionKey)]
	return memPart, ok
}

// lockPartition looks up the given partition by its key and then acquires an
// exclusive lock on it if "isExclusive" is true, else a shared lock. If the
// partition does not exist, it returns nil.
func (s *Store) lockPartition(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, owner uint64, isExclusive bool,
) *memPartition {
	memPart, ok := s.getPartition(treeKey, partitionKey)
	if !ok {
		return nil
	}

	if isExclusive {
		memPart.lock.Acquire(owner)
	} else {
		memPart.lock.AcquireShared(owner)
	}

	// If the partition is deleted, then release the lock and return nil.
	if memPart.lock.deleted != 0 {
		if isExclusive {
			memPart.lock.Release()
		} else {
			memPart.lock.ReleaseShared()
		}
		return nil
	}

	return memPart
}

// makeEmptyRootMetadata returns the partition metadata for an empty root
// partition.
func (s *Store) makeEmptyRootMetadata() cspann.PartitionMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.makeEmptyRootMetadataLocked()
}

// makeEmptyRootMetadataLocked returns the partition metadata for an empty root
// partition.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) makeEmptyRootMetadataLocked() cspann.PartitionMetadata {
	if s.mu.emptyVec == nil {
		s.mu.emptyVec = make(vector.T, s.dims)
	}
	return cspann.MakeReadyPartitionMetadata(cspann.LeafLevel, s.mu.emptyVec)
}

// tryCreateEmptyPartition creates a empty partition with the given metadata and
// inserts it into the tree if there is no existing partition with the same key.
// If there is an existing partition, it returns that and ok=false; otherwise,
// it returns the newly created partition and ok=true.
func (s *Store) tryCreateEmptyPartition(
	treeKey cspann.TreeKey, partitionKey cspann.PartitionKey, metadata cspann.PartitionMetadata,
) (memPart *memPartition, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for race condition where another thread already created the
	// partition.
	memPart, ok = s.getPartitionLocked(treeKey, partitionKey)
	if ok {
		return memPart, false
	}

	// Create the new empty partition.
	quantizer := s.quantizer
	if partitionKey == cspann.RootKey {
		quantizer = s.rootQuantizer
	}
	partition := cspann.CreateEmptyPartition(quantizer, metadata)
	memPart = s.insertPartitionLocked(treeKey, partitionKey, partition)

	// Update stats.
	s.mu.stats.NumPartitions++

	return memPart, true
}

// processPendingActionsLocked iterates over pending actions to:
//  1. Remove any ended transactions that are at the front of the pending
//     list (i.e. that have been pending for longest). Transactions are
//     always removed in FIFO order, even if they actually ended in a
//     different order.
//  2. Garbage collect any deleted partitions that are at the front of the
//     pending list.
//
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) processPendingActionsLocked() {
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
}
