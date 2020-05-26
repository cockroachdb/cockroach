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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// rowHelper has the common methods for table row manipulations.
type rowHelper struct {
	Codec keys.SQLCodec

	TableDesc *sqlbase.ImmutableTableDescriptor
	// Secondary indexes.
	Indexes      []sqlbase.IndexDescriptor
	indexEntries []sqlbase.IndexEntry

	// Computed during initialization for pretty-printing.
	primIndexValDirs []encoding.Direction
	secIndexValDirs  [][]encoding.Direction

	// Computed and cached.
	primaryIndexKeyPrefix []byte
	primaryIndexCols      map[sqlbase.ColumnID]struct{}
	sortedColumnFamilies  map[sqlbase.FamilyID][]sqlbase.ColumnID
}

func newRowHelper(
	codec keys.SQLCodec, desc *sqlbase.ImmutableTableDescriptor, indexes []sqlbase.IndexDescriptor,
) rowHelper {
	rh := rowHelper{Codec: codec, TableDesc: desc, Indexes: indexes}

	// Pre-compute the encoding directions of the index key values for
	// pretty-printing in traces.
	rh.primIndexValDirs = sqlbase.IndexKeyValDirs(&rh.TableDesc.PrimaryIndex)

	rh.secIndexValDirs = make([][]encoding.Direction, len(rh.Indexes))
	for i := range rh.Indexes {
		rh.secIndexValDirs[i] = sqlbase.IndexKeyValDirs(&rh.Indexes[i])
	}

	return rh
}

// encodeIndexes encodes the primary and secondary index keys. The
// secondaryIndexEntries are only valid until the next call to encodeIndexes or
// encodeSecondaryIndexes. includeEmpty details whether the results should
// include empty secondary index k/v pairs.
func (rh *rowHelper) encodeIndexes(
	colIDtoRowIndex map[sqlbase.ColumnID]int,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (primaryIndexKey []byte, secondaryIndexEntries []sqlbase.IndexEntry, err error) {
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
	colIDtoRowIndex map[sqlbase.ColumnID]int, values []tree.Datum,
) (primaryIndexKey []byte, err error) {
	if rh.primaryIndexKeyPrefix == nil {
		rh.primaryIndexKeyPrefix = sqlbase.MakeIndexKeyPrefix(rh.Codec, rh.TableDesc.TableDesc(),
			rh.TableDesc.PrimaryIndex.ID)
	}
	primaryIndexKey, _, err = sqlbase.EncodeIndexKey(
		rh.TableDesc.TableDesc(), &rh.TableDesc.PrimaryIndex, colIDtoRowIndex, values, rh.primaryIndexKeyPrefix)
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
	colIDtoRowIndex map[sqlbase.ColumnID]int,
	values []tree.Datum,
	ignoreIndexes util.FastIntSet,
	includeEmpty bool,
) (secondaryIndexEntries []sqlbase.IndexEntry, err error) {
	if cap(rh.indexEntries) < len(rh.Indexes) {
		rh.indexEntries = make([]sqlbase.IndexEntry, 0, len(rh.Indexes))
	}

	rh.indexEntries = rh.indexEntries[:0]

	for i := range rh.Indexes {
		index := &rh.Indexes[i]
		if !ignoreIndexes.Contains(int(index.ID)) {
			entries, err := sqlbase.EncodeSecondaryIndex(rh.Codec, rh.TableDesc.TableDesc(), index, colIDtoRowIndex, values, includeEmpty)
			if err != nil {
				return nil, err
			}
			rh.indexEntries = append(rh.indexEntries, entries...)
		}
	}

	return rh.indexEntries, nil
}

// skipColumnInPK returns true if the value at column colID does not need
// to be encoded because it is already part of the primary key. Composite
// datums are considered too, so a composite datum in a PK will return false.
// TODO(dan): This logic is common and being moved into TableDescriptor (see
// #6233). Once it is, use the shared one.
func (rh *rowHelper) skipColumnInPK(
	colID sqlbase.ColumnID, family sqlbase.FamilyID, value tree.Datum,
) (bool, error) {
	if rh.primaryIndexCols == nil {
		rh.primaryIndexCols = make(map[sqlbase.ColumnID]struct{})
		for _, colID := range rh.TableDesc.PrimaryIndex.ColumnIDs {
			rh.primaryIndexCols[colID] = struct{}{}
		}
	}
	if _, ok := rh.primaryIndexCols[colID]; !ok {
		return false, nil
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

func (rh *rowHelper) sortedColumnFamily(famID sqlbase.FamilyID) ([]sqlbase.ColumnID, bool) {
	if rh.sortedColumnFamilies == nil {
		rh.sortedColumnFamilies = make(map[sqlbase.FamilyID][]sqlbase.ColumnID, len(rh.TableDesc.Families))
		for i := range rh.TableDesc.Families {
			family := &rh.TableDesc.Families[i]
			colIDs := append([]sqlbase.ColumnID(nil), family.ColumnIDs...)
			sort.Sort(sqlbase.ColumnIDs(colIDs))
			rh.sortedColumnFamilies[family.ID] = colIDs
		}
	}
	colIDs, ok := rh.sortedColumnFamilies[famID]
	return colIDs, ok
}
