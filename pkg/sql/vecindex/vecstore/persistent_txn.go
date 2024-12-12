// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/quantize"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// The PersistentStoreTxn provides a context to make transactional changes to a
// vector index. Calling methods here will use the wrapped KV Txn to update the
// vector index's internal data. Committing changes is the responsibility of the
// caller.
type PersistentStoreTxn struct {
	kv    *kv.Txn
	store *PersistentStore

	keyBuffer    roachpb.Key // We cache the key prefix for the index to avoid allocations
	keyPrefixLen int
}

var _ Txn = (*PersistentStoreTxn)(nil)

func NewPersistentStoreTxn(store *PersistentStore, kv *kv.Txn) *PersistentStoreTxn {
	psTxn := PersistentStoreTxn{
		kv:    kv,
		store: store,
	}
	psTxn.keyBuffer = make(roachpb.Key, len(store.prefix))
	copy(psTxn.keyBuffer, store.prefix)
	psTxn.keyPrefixLen = len(store.prefix)

	return &psTxn
}

// GetPartition() is part of the vecstore.Txn interface. Read the partition
// indicated by `partitionKey` and build a Partition data structure, which is
// returned.
func (psTxn *PersistentStoreTxn) GetPartition(
	ctx context.Context, partitionKey PartitionKey,
) (*Partition, error) {
	b := psTxn.kv.NewBatch()

	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	b.Scan(startKey, endKey)
	err := psTxn.kv.Run(ctx, b)
	if err != nil {
		return nil, err
	}
	if len(b.Results[0].Rows) == 0 {
		return nil, errors.Errorf("Partition %v not found", partitionKey)
	}
	// Partition metadata is stored in /Prefix/PartitionID, with vector data following in /Prefix/PartitionID/ChildKey
	level, centroid, err := DecodePartitionMetadata(b.Results[0].Rows[0].ValueBytes())
	if err != nil {
		return nil, err
	}

	// Build a decoder function for deserializing encoded vectors. We also set the
	// centroid of the empty vector set here because the centroid is stored in the
	// partition metadata but is part of the data that all implementations
	// QuantizedVectorSet need.
	vectorSet := psTxn.store.quantizer.Quantize(ctx, &vector.Set{})
	var decoder func(encodedVector []byte) error
	switch vs := vectorSet.(type) {
	case (*quantize.UnQuantizedVectorSet):
		decoder = func(encodedVector []byte) error {
			return DecodeUnquantizedVectorToSet(encodedVector, vectorSet.(*quantize.UnQuantizedVectorSet))
		}
		vs.Centroid = centroid
	case (*quantize.RaBitQuantizedVectorSet):
		decoder = func(encodedVector []byte) error {
			return DecodeRaBitQVectorToSet(encodedVector, vectorSet.(*quantize.RaBitQuantizedVectorSet))
		}
		vs.Centroid = centroid
	default:
		panic(fmt.Sprintf("unknown vector set type %T", vectorSet))
	}

	vectorEntries := b.Results[0].Rows[1:]
	childKeys := make([]ChildKey, len(vectorEntries))
	for i, entry := range vectorEntries {
		childKey, err := DecodeChildKey(entry.Key[len(startKey):], level)
		if err != nil {
			return nil, err
		}
		childKeys[i] = childKey

		if err = decoder(entry.ValueBytes()); err != nil {
			return nil, err
		}
	}

	return NewPartition(psTxn.store.quantizer, vectorSet, childKeys, level), nil
}

// Insert a partition with the given partition key into the store. Insertion will
// fail if the partition ID already exists.
func (psTxn *PersistentStoreTxn) insertPartition(
	ctx context.Context, partitionKey PartitionKey, partition *Partition, putter func(b *kv.Batch, k roachpb.Key, v []byte),
) error {
	b := psTxn.kv.NewBatch()

	key := psTxn.encodePartitionKey(partitionKey)
	meta, err := EncodePartitionMetadata(partition.Level(), partition.quantizedSet.GetCentroid())
	if err != nil {
		return err
	}
	putter(b, key, meta)

	centroidDistances := partition.quantizedSet.GetCentroidDistances()

	// Build an encoder function that is agnostic to the quantization method used.
	var encoder func([]byte, int) ([]byte, error)
	switch qs := partition.quantizedSet.(type) {
	case (*quantize.UnQuantizedVectorSet):
		encoder = func(appendTo []byte, idx int) ([]byte, error) {
			return EncodeUnquantizedVector(appendTo, centroidDistances[idx], qs.Vectors.At(idx))
		}
	case (*quantize.RaBitQuantizedVectorSet):
		encoder = func(appendTo []byte, idx int) ([]byte, error) {
			return EncodeRaBitQVector(appendTo, qs.CodeCounts[idx], centroidDistances[idx], qs.DotProducts[idx], qs.Codes.At(idx)), nil
		}
	default:
		panic(fmt.Sprintf("unknown partition quantizedSet type: %T", qs))
	}

	// Store the length of the partition's metadata key so that we can truncate back
	// to that length instead of building a new key from scratch.
	childKeys := partition.ChildKeys()
	for i := 0; i < partition.quantizedSet.GetCount(); i++ {
		// The child key gets appended to 'key' here.
		// TODO(mw5h): preallocate memory for keys and encoded vectors.
		k := make(roachpb.Key, len(key))
		copy(k, key)
		k = EncodeChildKey(k, childKeys[i])
		encodedVector, err := encoder([]byte{}, i)
		if err != nil {
			return err
		}
		putter(b, k, encodedVector)
	}

	return psTxn.kv.Run(ctx, b)
}

func (psTxn *PersistentStoreTxn) SetRootPartition(
	ctx context.Context, partition *Partition,
) error {
	// TODO(mw5h): How does DeleteRange interact with keys inserted in the same
	// batch? Could this be batched with the insertPartition? Docs say operations
	// happen out of order, but maybe there's something to keep a single batch from
	// self-interfering?
	psTxn.DeletePartition(ctx, RootKey)
	return psTxn.insertPartition(ctx, RootKey, partition,
		func(b *kv.Batch, k roachpb.Key, v []byte) {
			b.Put(k, v)
		})
}

func (psTxn *PersistentStoreTxn) InsertPartition(
	ctx context.Context, partition *Partition,
) (PartitionKey, error) {
	partitionID := PartitionKey(builtins.GenerateUniqueInt(builtins.ProcessUniqueID(psTxn.store.db.Context().NodeID.SQLInstanceID())))
	return partitionID, psTxn.insertPartition(ctx, partitionID, partition,
		func(b *kv.Batch, k roachpb.Key, v []byte) {
			b.CPutAllowingIfNotExists(k, v, nil /* expValue */)
		})
}

func (psTxn *PersistentStoreTxn) DeletePartition(
	ctx context.Context, partitionKey PartitionKey,
) error {
	b := psTxn.kv.NewBatch()

	startKey := psTxn.encodePartitionKey(partitionKey)
	endKey := startKey.PrefixEnd()

	b.DelRange(startKey, endKey, false /* returnKeys */)
	return psTxn.kv.Run(ctx, b)
}

func (psTxn *PersistentStoreTxn) AddToPartition(
	ctx context.Context, partitionKey PartitionKey, vector vector.T, childKey ChildKey,
) (int, error) {
	panic("AddToPartition() unimplemented")
}

func (psTxn *PersistentStoreTxn) RemoveFromPartition(
	ctx context.Context, partitionKey PartitionKey, childKey ChildKey,
) (int, error) {
	panic("RemoveFromPartition() unimplemented")
}

func (psTxn *PersistentStoreTxn) SearchPartitions(
	ctx context.Context,
	partitionKey []PartitionKey,
	queryVector vector.T,
	searchSet *SearchSet,
	partitionCounts []int,
) (Level, error) {
	panic("SearchPartitions() unimplemented")
}

func (psTxn *PersistentStoreTxn) GetFullVectors(ctx context.Context, refs []VectorWithKey) error {
	panic("GetFullVectors() unimplemented")
}

// encodePartitionKey() takes a partition key and creates a KV key to read that
// partition's metadata. Vector data can be read by scanning from the metadata to
// the next partition's metadata.
func (psTxn *PersistentStoreTxn) encodePartitionKey(partitionKey PartitionKey) roachpb.Key {
	psTxn.keyBuffer = psTxn.keyBuffer[:psTxn.keyPrefixLen]
	psTxn.keyBuffer = encoding.EncodeUvarintAscending(psTxn.keyBuffer, uint64(partitionKey))
	return psTxn.keyBuffer
}
