// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VectorIndexUpdateHelper keeps track of the information needed to encode put
// and del operations on vector indexes.
//
// Forward and inverted indexes produce the index encoding directly from the
// value itself. Vector indexes, however, require a vector search to determine
// which partition "owns" the vector. The partition is not stored as a table
// column, and an indexed vector can move to a new partition in the background
// via index maintenance operations.
//
// For an index put operation, VectorIndexUpdateHelper stores the key of the
// partition to be updated for each vector index on the table, as well as the
// quantized vector. For an index del, it stores only the partition key. It can
// handle both put and del operations simultaneously (e.g. for an UPDATE).
type VectorIndexUpdateHelper struct {
	// PutPartitionKeys indicates the target vector index partitions for an index
	// put operation. Null values indicate a no-op.
	PutPartitionKeys map[descpb.IndexID]tree.Datum
	// PutQuantizedVecs contains the quantized and encoded vectors that will be
	// used as the values for the index put operations. An entry should only be
	// null if the corresponding partition key is null.
	PutQuantizedVecs map[descpb.IndexID]tree.Datum
	// PutPartitionKeys indicates the target vector index partitions for an index
	// del operation. Null values indicate a no-op.
	DelPartitionKeys map[descpb.IndexID]tree.Datum
}

func (vm *VectorIndexUpdateHelper) GetPut() rowenc.VectorIndexEncodingHelper {
	return rowenc.VectorIndexEncodingHelper{
		PartitionKeys: vm.PutPartitionKeys,
		QuantizedVecs: vm.PutQuantizedVecs,
	}
}

func (vm *VectorIndexUpdateHelper) GetDel() rowenc.VectorIndexEncodingHelper {
	return rowenc.VectorIndexEncodingHelper{PartitionKeys: vm.DelPartitionKeys}
}

// InitForPut initializes a VectorIndexUpdateHelper to track vector index
// partitions and quantized vectors for use in encoding index puts. It can be
// used in conjunction with InitForDel to handle both put and del operations.
func (vm *VectorIndexUpdateHelper) InitForPut(
	putPartitionKeys, putQuantizedVecs tree.Datums, tabDesc catalog.TableDescriptor,
) {
	// Clear the Put maps to remove entries from previous iterations.
	clear(vm.PutPartitionKeys)
	clear(vm.PutQuantizedVecs)
	vm.initImpl(putPartitionKeys, putQuantizedVecs, nil /* delPartitionKeys */, tabDesc)
}

// InitForDel initializes a VectorIndexUpdateHelper to track vector index
// partitions for use in encoding index deletes. It can be used in conjunction
// with InitForPut to handle both put and del operations.
func (vm *VectorIndexUpdateHelper) InitForDel(
	delPartitionKeys tree.Datums, tabDesc catalog.TableDescriptor,
) {
	// Clear the Del map to remove entries from previous iterations.
	clear(vm.DelPartitionKeys)
	vm.initImpl(nil /* putPartitionKeys */, nil /* putQuantizedVecs */, delPartitionKeys, tabDesc)
}

func (vm *VectorIndexUpdateHelper) initImpl(
	putPartitionKeys, putQuantizedVecs, delPartitionKeys tree.Datums, tabDesc catalog.TableDescriptor,
) {
	valIdx := 0
	for _, idx := range tabDesc.VectorIndexes() {
		// Retrieve the partition key value, if it exists.
		if valIdx < len(putPartitionKeys) {
			if vm.PutPartitionKeys == nil {
				vm.PutPartitionKeys = make(map[descpb.IndexID]tree.Datum)
			}
			if putPartitionKeys[valIdx] != tree.DNull {
				vm.PutPartitionKeys[idx.GetID()] = putPartitionKeys[valIdx]
			}
		}

		// Retrieve the quantized vector, if it exists.
		if valIdx < len(putQuantizedVecs) {
			if vm.PutQuantizedVecs == nil {
				vm.PutQuantizedVecs = make(map[descpb.IndexID]tree.Datum)
			}
			if putQuantizedVecs[valIdx] != tree.DNull {
				vm.PutQuantizedVecs[idx.GetID()] = putQuantizedVecs[valIdx]
			}
		}

		// Retrieve the partition key value for delete, if it exists.
		if valIdx < len(delPartitionKeys) {
			if vm.DelPartitionKeys == nil {
				vm.DelPartitionKeys = make(map[descpb.IndexID]tree.Datum)
			}
			if delPartitionKeys[valIdx] != tree.DNull {
				vm.DelPartitionKeys[idx.GetID()] = delPartitionKeys[valIdx]
			}
		}

		valIdx++
	}
}
