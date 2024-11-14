// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// storeStatsAlpha specifies the ratio of new values to existing values in EMA
// calculations.
const storeStatsAlpha = 0.1

// lockType specifies the type of lock that transactions have acquired.
type lockType int

const (
	noLock lockType = iota
	// dataLock is a shared lock that's acquired when reading, inserting, or
	// deleting vectors in the in-memory store. These operations can proceed in
	// parallel.
	dataLock
	// partitionLock is an exclusive lock that's acquired when reading, inserting,
	// or deleting partitions in the in-memory store. When this lock is acquired,
	// all other data and partition transactions will be forced to wait.
	partitionLock
)

// inMemoryTxn tracks the transaction's state.
type inMemoryTxn struct {
	// lock is the kind of lock that's been acquired by the transaction.
	lock lockType
	// updated is true if any in-memory state has been updated.
	updated bool
}

// InMemoryStore implements the Store interface over in-memory partitions and
// vectors. This is only used for testing and benchmarking.
type InMemoryStore struct {
	dims int
	seed int64

	txnLock syncutil.RWMutex
	mu      struct {
		syncutil.Mutex
		index   map[PartitionKey]*Partition
		nextKey PartitionKey
		vectors map[string]vector.T
		stats   IndexStats
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
	st.mu.index = make(map[PartitionKey]*Partition)
	st.mu.nextKey = RootKey + 1
	st.mu.vectors = make(map[string]vector.T)
	return st
}

// BeginTransaction implements the Store interface.
func (s *InMemoryStore) BeginTransaction(ctx context.Context) (Txn, error) {
	return &inMemoryTxn{}, nil
}

// CommitTransaction implements the Store interface.
func (s *InMemoryStore) CommitTransaction(ctx context.Context, txn Txn) error {
	inMemTxn := txn.(*inMemoryTxn)
	switch inMemTxn.lock {
	case dataLock:
		s.txnLock.RUnlock()
	case partitionLock:
		s.txnLock.Unlock()
	}
	inMemTxn.lock = noLock
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
	s.acquireTxnLock(txn, partitionLock, false /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	partition, ok := s.mu.index[partitionKey]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return partition, nil
}

// SetRootPartition implements the Store interface.
func (s *InMemoryStore) SetRootPartition(ctx context.Context, txn Txn, partition *Partition) error {
	s.acquireTxnLock(txn, partitionLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.mu.index[RootKey]
	if !ok {
		s.mu.stats.NumPartitions++
	}

	// Grow or shrink CVStats slice if a new level is being added or removed.
	expectedLevels := int(partition.Level() - 1)
	if expectedLevels > len(s.mu.stats.CVStats) {
		s.mu.stats.CVStats = slices.Grow(s.mu.stats.CVStats, expectedLevels-len(s.mu.stats.CVStats))
	}
	s.mu.stats.CVStats = s.mu.stats.CVStats[:expectedLevels]

	s.mu.index[RootKey] = partition
	return nil
}

// InsertPartition implements the Store interface.
func (s *InMemoryStore) InsertPartition(
	ctx context.Context, txn Txn, partition *Partition,
) (PartitionKey, error) {
	s.acquireTxnLock(txn, partitionLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	partitionKey := s.mu.nextKey
	s.mu.nextKey++
	s.mu.index[partitionKey] = partition
	s.mu.stats.NumPartitions++
	return partitionKey, nil
}

// DeletePartition implements the Store interface.
func (s *InMemoryStore) DeletePartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey,
) error {
	s.acquireTxnLock(txn, partitionLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.mu.index[partitionKey]
	if !ok {
		return ErrPartitionNotFound
	}
	delete(s.mu.index, partitionKey)
	s.mu.stats.NumPartitions--
	return nil
}

// AddToPartition implements the Store interface.
func (s *InMemoryStore) AddToPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
) (int, error) {
	s.acquireTxnLock(txn, dataLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	partition, ok := s.mu.index[partitionKey]
	if !ok {
		return 0, ErrPartitionNotFound
	}
	if partition.Add(ctx, vector, childKey) {
		return partition.Count(), nil
	}
	return -1, nil
}

// RemoveFromPartition implements the Store interface.
func (s *InMemoryStore) RemoveFromPartition(
	ctx context.Context, txn Txn, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	s.acquireTxnLock(txn, dataLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	partition, ok := s.mu.index[partitionKey]
	if !ok {
		return 0, ErrPartitionNotFound
	}
	if partition.ReplaceWithLastByKey(childKey) {
		return partition.Count(), nil
	}
	return -1, nil
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
	s.acquireTxnLock(txn, dataLock, false /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < len(partitionKeys); i++ {
		partition, ok := s.mu.index[partitionKeys[i]]
		if !ok {
			return 0, ErrPartitionNotFound
		}
		searchLevel, partitionCount := partition.Search(ctx, partitionKeys[i], queryVector, searchSet)
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

// GetFullVectors implements the Store interface.
func (s *InMemoryStore) GetFullVectors(ctx context.Context, txn Txn, refs []VectorWithKey) error {
	s.acquireTxnLock(txn, dataLock, false /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < len(refs); i++ {
		ref := &refs[i]
		if ref.Key.PartitionKey != InvalidKey {
			// Return the partition's centroid.
			partition, ok := s.mu.index[ref.Key.PartitionKey]
			if !ok {
				return ErrPartitionNotFound
			}
			ref.Vector = partition.Centroid()
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
		// Merge VectorsPerPartition.
		if s.mu.stats.VectorsPerPartition == 0 {
			// Use first value if this is the first update.
			s.mu.stats.VectorsPerPartition = stats.VectorsPerPartition
		} else {
			s.mu.stats.VectorsPerPartition = (1 - storeStatsAlpha) * s.mu.stats.VectorsPerPartition
			s.mu.stats.VectorsPerPartition += stats.VectorsPerPartition * storeStatsAlpha
		}

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
				cvstats.Mean = sample.Mean*storeStatsAlpha + (1-storeStatsAlpha)*cvstats.Mean
				cvstats.Variance = sample.Variance*storeStatsAlpha + (1-storeStatsAlpha)*cvstats.Variance
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
	s.acquireTxnLock(txn, dataLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.vectors[string(key)] = vector
}

// DeleteVector deletes the vector associated with the given primary key from
// the in-memory store. This mimics deleting a vector from the primary index,
// and is used during testing and benchmarking.
func (s *InMemoryStore) DeleteVector(txn Txn, key PrimaryKey) {
	s.acquireTxnLock(txn, dataLock, true /* updating */)

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.mu.vectors, string(key))
}

// MarshalBinary saves the in-memory store as a bytes. This allows the store to
// be saved and later loaded without needing to rebuild it from scratch.
func (s *InMemoryStore) MarshalBinary() (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storeProto := StoreProto{
		Dims:       s.dims,
		Seed:       s.seed,
		Partitions: make([]PartitionProto, 0, len(s.mu.index)),
		NextKey:    s.mu.nextKey,
		Vectors:    make([]VectorProto, 0, len(s.mu.vectors)),
		Stats:      s.mu.stats,
	}

	// Remap partitions to protobufs.
	for partitionKey, partition := range s.mu.index {
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
	}

	// Remap vectors to protobufs.
	for key, vector := range s.mu.vectors {
		vectorWithKey := VectorProto{PrimaryKey: []byte(key), Vector: vector}
		storeProto.Vectors = append(storeProto.Vectors, vectorWithKey)
	}

	// Serialize the protobuf as bytes.
	return protoutil.Marshal(&storeProto)
}

// UnmarshalBinary loads the in-memory store from bytes that were previously
// saved by MarshalBinary.
func (s *InMemoryStore) UnmarshalBinary(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unmarshal bytes into a protobuf.
	var storeProto StoreProto
	if err := protoutil.Unmarshal(data, &storeProto); err != nil {
		return err
	}

	// Construct the InMemoryStore object.
	s.seed = storeProto.Seed
	s.mu.index = make(map[PartitionKey]*Partition, len(storeProto.Partitions))
	s.mu.nextKey = storeProto.NextKey
	s.mu.vectors = make(map[string]vector.T, len(storeProto.Vectors))
	s.mu.stats = storeProto.Stats

	raBitQuantizer := quantize.NewRaBitQuantizer(storeProto.Dims, storeProto.Seed)
	unquantizer := quantize.NewUnQuantizer(storeProto.Dims)

	// Construct the Partition objects.
	for i := range storeProto.Partitions {
		partitionProto := &storeProto.Partitions[i]
		var partition = Partition{
			childKeys: partitionProto.ChildKeys,
			level:     partitionProto.Level,
		}
		if partitionProto.RaBitQ != nil {
			partition.quantizer = raBitQuantizer
			partition.quantizedSet = partitionProto.RaBitQ
		} else {
			partition.quantizer = unquantizer
			partition.quantizedSet = partitionProto.UnQuantized
		}
		s.mu.index[partitionProto.PartitionKey] = &partition
	}

	// Insert vectors into the in-memory store.
	for i := range storeProto.Vectors {
		vectorProto := storeProto.Vectors[i]
		s.mu.vectors[string(vectorProto.PrimaryKey)] = vectorProto.Vector
	}

	return nil
}

// acquireTxnLock acquires a data or partition lock within the scope of the
// given transaction. It is an error to attempt to acquire a partition lock if
// the transaction already holds a data lock.
func (s *InMemoryStore) acquireTxnLock(txn Txn, lock lockType, updating bool) {
	inMemTxn := txn.(*inMemoryTxn)
	if inMemTxn.lock == noLock {
		switch lock {
		case dataLock:
			s.txnLock.RLock()
		case partitionLock:
			s.txnLock.Lock()
		}
		inMemTxn.lock = lock
	} else if inMemTxn.lock == dataLock && lock == partitionLock { // nolint:deferunlockcheck
		panic(errors.AssertionFailedf("cannot upgrade from data to partition lock"))
	}
	inMemTxn.updated = updating
}
