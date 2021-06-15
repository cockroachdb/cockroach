// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	Codec keys.SQLCodec

	TableDesc catalog.TableDescriptor
	// Secondary indexes.
	Indexes      []catalog.Index
	indexEntries []rowenc.IndexEntry

	// Computed during initialization for pretty-printing.
	primIndexValDirs []encoding.Direction
	secIndexValDirs  [][]encoding.Direction

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexKeyCols   catalog.TableColSet
	primaryIndexValueCols catalog.TableColSet
	sortedColumnFamilies  map[descpb.FamilyID][]descpb.ColumnID
}

func newRowHelper(
	codec keys.SQLCodec, desc catalog.TableDescriptor, indexes []catalog.Index,
) rowHelper {
	rh := rowHelper{Codec: codec, TableDesc: desc, Indexes: indexes}

	// Pre-compute the encoding directions of the index key values for
	// pretty-printing in traces.
	rh.primIndexValDirs = catalogkeys.IndexKeyValDirs(rh.TableDesc.GetPrimaryIndex())

	rh.secIndexValDirs = make([][]encoding.Direction, len(rh.Indexes))
	for i := range rh.Indexes {
		rh.secIndexValDirs[i] = catalogkeys.IndexKeyValDirs(rh.Indexes[i])
	}

	return rh
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes. includeEmpty details whether the results should
// include empty secondary index k/v pairs.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (primaryIndexKey []byte, secondaryIndexEntries []rowenc.IndexEntry, err error) {
	primaryIndexKey, err = rh.encodePrimaryIndex(colIDtoRowIndex, values)
	if err != nil {
		return nil, nil, err
	}
	secondaryIndexEntries, err = rh.encodeSecondaryIndexes(colIDtoRowIndex, values, ignoreIndexes, includeEmpty)
	if err != nil {
		return nil, nil, err
	}
	return primaryIndexKey, secondaryIndexEntries, nil
}

// encodePrimaryIndex encodes the primary index key.
func (rh *rowHelper) encodePrimaryIndex(
	colIDtoRowIndex catalog.TableColMap, values []tree.Datum,
) (primaryIndexKey []byte, err error) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(rh.Codec, rh.TableDesc,
			rh.TableDesc.GetPrimaryIndexID())
	}
	primaryIndexKey, _, err = rowenc.EncodeIndexKey(
		rh.TableDesc, rh.TableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, rh.primaryIndexKeyPrefix)
	return primaryIndexKey, err
}

// encodeSecondaryIndexes encodes the secondary index keys based on a row's
// values.
//
// The secondaryIndexEntries are only valid until the next call to encodeIndexes
// or encodeSecondaryIndexes, when they are overwritten.
//
// This function will not encode index entries for any index with an ID in
// ignoreIndexes.
//
// includeEmpty details whether the results should include empty secondary index
// k/v pairs.
func (rh *rowHelper) encodeSecondaryIndexes(
	colIDtoRowIndex catalog.TableColMap,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (secondaryIndexEntries []rowenc.IndexEntry, err error) {
	if cap(rh.indexEntries) < len(rh.Indexes) {
		rh.indexEntries = make([]rowenc.IndexEntry, 0, len(rh.Indexes))
	}

	rh.indexEntries = rh.indexEntries[:0]

	for i := range rh.Indexes {
		index := rh.Indexes[i]
		if !ignoreIndexes.Contains(int(index.GetID())) {
			entries, err := rowenc.EncodeSecondaryIndex(rh.Codec, rh.TableDesc, index, colIDtoRowIndex, values, includeEmpty)
			if err != nil {
				return nil, err
			}
			rh.indexEntries = append(rh.indexEntries, entries...)
		}
	}

	return rh.indexEntries, nil
}

// skipColumnNotInPrimaryIndexValue returns true if the value at column colID
// does not need to be encoded, either because it is already part of the primary
// key, or because it is not part of the primary index altogether. Composite
// datums are considered too, so a composite datum in a PK will return false.
func (rh *rowHelper) skipColumnNotInPrimaryIndexValue(
	colID descpb.ColumnID, value tree.Datum,
) (bool, error) {
	if rh.primaryIndexKeyCols.Empty() {
		rh.primaryIndexKeyCols = rh.TableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
		rh.primaryIndexValueCols = rh.TableDesc.GetPrimaryIndex().CollectPrimaryStoredColumnIDs()
	}
	if !rh.primaryIndexKeyCols.Contains(colID) {
		return !rh.primaryIndexValueCols.Contains(colID), nil
	}
	if cdatum, ok := value.(tree.CompositeDatum); ok {
		// Composite columns are encoded in both the key and the value.
		return !cdatum.IsComposite(), nil
	}
	// Skip primary key columns as their values are encoded in the key of
	// each family. Family 0 is guaranteed to exist and acts as a
	// sentinel.
	return true, nil
}

func (rh *rowHelper) sortedColumnFamily(famID descpb.FamilyID) ([]descpb.ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[descpb.FamilyID][]descpb.ColumnID, rh.TableDesc.NumFamilies())

		_ = rh.TableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			colIDs := append([]descpb.ColumnID{}, family.ColumnIDs...)
			sort.Sort(descpb.ColumnIDs(colIDs))
			rh.sortedColumnFamilies[family.ID] = colIDs
			return nil
		})
	}
	colIDs, ok := rh.sortedColumnFamilies[famID]
	return colIDs, ok
}
