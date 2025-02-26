// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memstore

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
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

	// lock is a read-write lock that callers must acquire before accessing any
	// of the fields.
	lock struct {
		memLock

		// partition is the wrapped partition.
		partition *cspann.Partition
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
// NOTE: Callers must acquire p.lock before calling this.
func (p *memPartition) isVisibleLocked(current uint64) bool {
	return !p.lock.deleted && (p.key.partitionKey != cspann.RootKey || current > p.lock.created)
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

	// structureLock must be acquired by transactions that intend to modify the
	// structure of a tree, e.g. splitting or merging a partition. This ensures
	// that only one split or merge can be running at any given time, so that
	// deadlocks are not possible.
	structureLock memLock

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
	}
}

var _ cspann.Store = (*Store)(nil)

// New constructs a new in-memory store for vectors of the given
// size. The seed determines how vectors are transformed as part of
// quantization and must be preserved if the store is saved to disk or loaded
// from disk.
func New(dims int, seed int64) *Store {
	st := &Store{
		dims:          dims,
		seed:          seed,
		rootQuantizer: quantize.NewUnQuantizer(dims),
	}

	st.mu.partitions = make(map[qualifiedPartitionKey]*memPartition)
	st.mu.vectors = make(map[string]vector.T)
	st.mu.clock = 2
	st.mu.nextKey = cspann.RootKey + 1
	st.mu.stats.NumPartitions = 1
	st.mu.pending.Init()
	return st
}

// Dims returns the number of dimensions in stored vectors.
func (s *Store) Dims() int {
	return s.dims
}

// BeginTransactionBegin implements the Store interface.
func (s *Store) BeginTransaction(ctx context.Context) (cspann.Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create new transaction with unique id set to the current logical clock
	// tick and insert the transaction into the pending list.
	current := s.tickLocked()
	txn := memTxn{id: current, current: current, store: s}
	elem := s.mu.pending.PushBack(pendingItem{activeTxn: txn})
	return &elem.Value.activeTxn, nil
}

// CommitTransaction implements the Store interface.
func (s *Store) CommitTransaction(ctx context.Context, txn cspann.Txn) error {
	// Release any exclusive partition locks held by the transaction.
	tx := txn.(*memTxn)
	for i := range tx.ownedLocks {
		tx.ownedLocks[i].Release()
	}

	// Panic if the K-means tree contains an empty non-leaf partition after the
	// transaction ends, as this violates the constraint that the K-means tree
	// is always full balanced. This is helpful in testing.
	if tx.unbalanced != nil {
		// Need to acquire partition lock before inspecting the partition.
		func() {
			tx.unbalanced.lock.AcquireShared(tx.id)
			defer tx.unbalanced.lock.ReleaseShared()

			partition := tx.unbalanced.lock.partition
			if partition.Count() == 0 && partition.Level() > cspann.LeafLevel {
				if tx.unbalanced.isVisibleLocked(tx.current) {
					panic(errors.AssertionFailedf(
						"K-means tree is unbalanced, with empty non-leaf partition %d",
						tx.unbalanced.key))
				}
			}
		}()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark transaction as ended.
	tx.muStore.ended = true

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
func (s *Store) AbortTransaction(ctx context.Context, txn cspann.Txn) error {
	tx := txn.(*memTxn)
	if tx.updated {
		// Abort is only trivially supported by the in-memory store.
		panic(errors.AssertionFailedf(
			"in-memory transaction cannot be aborted because state has already been updated"))
	}
	return s.CommitTransaction(ctx, txn)
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
		Config:     vecpb.Config{Dims: int32(s.dims), Seed: s.seed},
		Partitions: make([]PartitionProto, 0, len(s.mu.partitions)),
		NextKey:    s.mu.nextKey,
		Vectors:    make([]VectorProto, 0, len(s.mu.vectors)),
		Stats:      s.mu.stats,
	}

	// Remap partitions to protobufs.
	for qkey, memPart := range s.mu.partitions {
		func() {
			memPart.lock.AcquireShared(0)
			defer memPart.lock.ReleaseShared()

			partition := memPart.lock.partition
			partitionProto := PartitionProto{
				TreeId:       qkey.treeID,
				PartitionKey: qkey.partitionKey,
				ChildKeys:    partition.ChildKeys(),
				ValueBytes:   partition.ValueBytes(),
				Level:        partition.Level(),
			}

			rabitq, ok := partition.QuantizedSet().(*quantize.RaBitQuantizedVectorSet)
			if ok {
				partitionProto.RaBitQ = rabitq
			} else {
				partitionProto.UnQuantized = partition.QuantizedSet().(*quantize.UnQuantizedVectorSet)
			}

			storeProto.Partitions = append(storeProto.Partitions, partitionProto)
		}()
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

	// Construct the InMemoryStore object.
	inMemStore := &Store{
		dims: int(storeProto.Config.Dims),
		seed: storeProto.Config.Seed,
	}
	inMemStore.mu.clock = 2
	inMemStore.mu.partitions = make(map[qualifiedPartitionKey]*memPartition, len(storeProto.Partitions))
	inMemStore.mu.nextKey = storeProto.NextKey
	inMemStore.mu.vectors = make(map[string]vector.T, len(storeProto.Vectors))
	inMemStore.mu.stats = storeProto.Stats
	inMemStore.mu.pending.Init()

	raBitQuantizer := quantize.NewRaBitQuantizer(int(storeProto.Config.Dims), storeProto.Config.Seed)
	unquantizer := quantize.NewUnQuantizer(int(storeProto.Config.Dims))

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
		if partitionProto.RaBitQ != nil {
			quantizer = raBitQuantizer
			quantizedSet = partitionProto.RaBitQ
		} else {
			quantizer = unquantizer
			quantizedSet = partitionProto.UnQuantized
		}

		memPart.lock.partition = cspann.NewPartition(quantizer, quantizedSet,
			partitionProto.ChildKeys, partitionProto.ValueBytes, partitionProto.Level)

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

// updatedStructureLocked marks the transaction as having updated the K-means
// tree. It also pushes the transactions current time forward so that it can
// always observe its changes.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) updatedStructureLocked(tx *memTxn) {
	tx.updated = true
	tx.current = s.tickLocked()
}

// reportPartitionSizeLocked updates the vectors per partition statistic. It is
// called with the count of vectors in a partition when a partition is inserted
// or updated.
// NOTE: Callers must have locked the s.mu mutex.
func (s *Store) reportPartitionSizeLocked(count int) {
	if s.mu.stats.VectorsPerPartition == 0 {
		// Use first value if this is the first update.
		s.mu.stats.VectorsPerPartition = float64(count)
	} else {
		// Calculate exponential moving average.
		s.mu.stats.VectorsPerPartition = (1 - storeStatsAlpha) * s.mu.stats.VectorsPerPartition
		s.mu.stats.VectorsPerPartition += storeStatsAlpha * float64(count)
	}
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
