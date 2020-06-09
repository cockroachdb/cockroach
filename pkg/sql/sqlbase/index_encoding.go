// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

// This file contains facilities to encode primary and secondary
// indexes on SQL tables.

// MakeIndexKeyPrefix returns the key prefix used for the index's data. If you
// need the corresponding Span, prefer desc.IndexSpan(indexID) or
// desc.PrimaryIndexSpan().
func MakeIndexKeyPrefix(codec keys.SQLCodec, desc *TableDescriptor, indexID IndexID) []byte {
	if i, err := desc.FindIndexByID(indexID); err == nil && len(i.Interleave.Ancestors) > 0 {
		ancestor := &i.Interleave.Ancestors[0]
		return codec.IndexPrefix(uint32(ancestor.TableID), uint32(ancestor.IndexID))
	}
	return codec.IndexPrefix(uint32(desc.ID), uint32(indexID))
}

// EncodeIndexKey creates a key by concatenating keyPrefix with the
// encodings of the columns in the index, and returns the key and
// whether any of the encoded values were NULLs.
//
// If a table or index is interleaved, `encoding.interleavedSentinel`
// is used in place of the family id (a varint) to signal the next
// component of the key.  An example of one level of interleaving (a
// parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
//
// Note that ExtraColumnIDs are not encoded, so the result isn't always a
// full index key.
func EncodeIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	return EncodePartialIndexKey(
		tableDesc,
		index,
		len(index.ColumnIDs), /* encode all columns */
		colMap,
		values,
		keyPrefix,
	)
}

// EncodePartialIndexSpan creates the minimal key span for the key specified by the
// given table, index, and values, with the same method as
// EncodePartialIndexKey.
func EncodePartialIndexSpan(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	numCols int,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (span roachpb.Span, containsNull bool, err error) {
	var key roachpb.Key
	var endKey roachpb.Key
	key, containsNull, err = EncodePartialIndexKey(tableDesc, index, numCols, colMap, values, keyPrefix)
	if err != nil {
		return span, containsNull, err
	}
	if numCols == len(index.ColumnIDs) {
		// If all values in the input index were specified, append an interleave
		// marker instead of PrefixEnding the key, to avoid including any child
		// interleaves of the input key.
		endKey = encoding.EncodeInterleavedSentinel(key)
	} else {
		endKey = key.PrefixEnd()
	}
	return roachpb.Span{Key: key, EndKey: endKey}, containsNull, nil
}

// EncodePartialIndexKey encodes a partial index key; only the first numCols of
// the index key columns are encoded. The index key columns are
//  - index.ColumnIDs for unique indexes, and
//  - append(index.ColumnIDs, index.ExtraColumnIDs) for non-unique indexes.
func EncodePartialIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	numCols int,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	var colIDs, extraColIDs []ColumnID
	if numCols <= len(index.ColumnIDs) {
		colIDs = index.ColumnIDs[:numCols]
	} else {
		if index.Unique || numCols > len(index.ColumnIDs)+len(index.ExtraColumnIDs) {
			return nil, false, errors.Errorf("encoding too many columns (%d)", numCols)
		}
		colIDs = index.ColumnIDs
		extraColIDs = index.ExtraColumnIDs[:numCols-len(index.ColumnIDs)]
	}

	// We know we will append to the key which will cause the capacity to grow so
	// make it bigger from the get-go.
	// Add twice the key prefix as an initial guess.
	// Add 3 bytes for every ancestor: table,index id + interleave sentinel.
	// Add 2 bytes for every column value. An underestimate for all but low integers.
	key = make([]byte, len(keyPrefix), 2*len(keyPrefix)+3*len(index.Interleave.Ancestors)+2*len(values))
	copy(key, keyPrefix)

	dirs := directions(index.ColumnDirections)

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = EncodePartialTableIDIndexID(key, ancestor.TableID, ancestor.IndexID)
			}

			partial := false
			length := int(ancestor.SharedPrefixLen)
			if length > len(colIDs) {
				length = len(colIDs)
				partial = true
			}
			var n bool
			key, n, err = EncodeColumns(colIDs[:length], dirs[:length], colMap, values, key)
			if err != nil {
				return nil, false, err
			}
			containsNull = containsNull || n
			if partial {
				// Early stop. Note that if we had exactly SharedPrefixLen columns
				// remaining, we want to append the next tableID/indexID pair because
				// that results in a more specific key.
				return key, containsNull, nil
			}
			colIDs, dirs = colIDs[length:], dirs[length:]
			// Each ancestor is separated by an interleaved
			// sentinel (0xfe).
			key = encoding.EncodeInterleavedSentinel(key)
		}

		key = EncodePartialTableIDIndexID(key, tableDesc.ID, index.ID)
	}

	var n bool
	key, n, err = EncodeColumns(colIDs, dirs, colMap, values, key)
	if err != nil {
		return nil, false, err
	}
	containsNull = containsNull || n

	key, n, err = EncodeColumns(extraColIDs, nil /* directions */, colMap, values, key)
	if err != nil {
		return nil, false, err
	}
	containsNull = containsNull || n
	return key, containsNull, nil
}

type directions []IndexDescriptor_Direction

func (d directions) get(i int) (encoding.Direction, error) {
	if i < len(d) {
		return d[i].ToEncodingDirection()
	}
	return encoding.Ascending, nil
}

// MakeSpanFromEncDatums creates a minimal index key span on the input
// values. A minimal index key span is a span that includes the fewest possible
// keys after the start key generated by the input values.
//
// The start key is generated by concatenating keyPrefix with the encodings of
// the given EncDatum values. The values, types, and dirs parameters should be
// specified in the same order as the index key columns and may be a prefix.
//
// If a table or index is interleaved, `encoding.interleavedSentinel` is used
// in place of the family id (a varint) to signal the next component of the
// key.  An example of one level of interleaving (a parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
func MakeSpanFromEncDatums(
	values EncDatumRow,
	types []*types.T,
	dirs []IndexDescriptor_Direction,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	alloc *DatumAlloc,
	keyPrefix []byte,
) (_ roachpb.Span, containsNull bool, _ error) {
	startKey, complete, containsNull, err := makeKeyFromEncDatums(values, types, dirs, tableDesc, index, alloc, keyPrefix)
	if err != nil {
		return roachpb.Span{}, false, err
	}

	var endKey roachpb.Key
	if complete && index.Unique {
		// If all values in the input index were specified and the input index is
		// unique, indicating that it might have child interleaves, append an
		// interleave marker instead of PrefixEnding the key, to avoid including
		// any child interleaves of the input key.
		//
		// Note that currently only primary indexes can contain interleaved
		// tables or indexes, so this condition is broader than necessary in
		// case one day we permit interleaving into arbitrary unique indexes.
		// Note also that we could precisely only emit an interleaved sentinel
		// if this index does in fact have interleaves - we choose not to do
		// that to make testing simpler and traces and spans more consistent.
		endKey = encoding.EncodeInterleavedSentinel(startKey)
	} else {
		endKey = startKey.PrefixEnd()
	}
	return roachpb.Span{Key: startKey, EndKey: endKey}, containsNull, nil
}

// NeededColumnFamilyIDs returns the minimal set of column families required to
// retrieve neededCols for the specified table and index. The returned FamilyIDs
// are in sorted order.
func NeededColumnFamilyIDs(
	neededCols util.FastIntSet, table *TableDescriptor, index *IndexDescriptor,
) []FamilyID {
	if len(table.Families) == 1 {
		return []FamilyID{table.Families[0].ID}
	}

	// Build some necessary data structures for column metadata.
	columns := table.ColumnsWithMutations(true)
	colIdxMap := table.ColumnIdxMapWithMutations(true)
	var indexedCols util.FastIntSet
	var compositeCols util.FastIntSet
	var extraCols util.FastIntSet
	for _, columnID := range index.ColumnIDs {
		columnOrdinal := colIdxMap[columnID]
		indexedCols.Add(columnOrdinal)
	}
	for _, columnID := range index.CompositeColumnIDs {
		columnOrdinal := colIdxMap[columnID]
		compositeCols.Add(columnOrdinal)
	}
	for _, columnID := range index.ExtraColumnIDs {
		columnOrdinal := colIdxMap[columnID]
		extraCols.Add(columnOrdinal)
	}

	// The column family with ID 0 is special because it always has a KV entry.
	// Other column families will omit a value if all their columns are null, so
	// we may need to retrieve family 0 to use as a sentinel for distinguishing
	// between null values and the absence of a row. Also, secondary indexes store
	// values here for composite and "extra" columns. ("Extra" means primary key
	// columns which are not indexed.)
	var family0 *ColumnFamilyDescriptor
	hasSecondaryEncoding := index.GetEncodingType(table.PrimaryIndex.ID) == SecondaryIndexEncoding

	// First iterate over the needed columns and look for a few special cases:
	// columns which can be decoded from the key and columns whose value is stored
	// in family 0.
	family0Needed := false
	nc := neededCols.Copy()
	neededCols.ForEach(func(columnOrdinal int) {
		if indexedCols.Contains(columnOrdinal) && !compositeCols.Contains(columnOrdinal) {
			// We can decode this column from the index key, so no particular family
			// is needed.
			nc.Remove(columnOrdinal)
		}
		if hasSecondaryEncoding && (compositeCols.Contains(columnOrdinal) ||
			extraCols.Contains(columnOrdinal)) {
			// Secondary indexes store composite and "extra" column values in family
			// 0.
			family0Needed = true
			nc.Remove(columnOrdinal)
		}
	})

	// Iterate over the column families to find which ones contain needed columns.
	// We also keep track of whether all of the needed families' columns are
	// nullable, since this means we need column family 0 as a sentinel, even if
	// none of its columns are needed.
	var neededFamilyIDs []FamilyID
	allFamiliesNullable := true
	for i := range table.Families {
		family := &table.Families[i]
		needed := false
		nullable := true
		if family.ID == 0 {
			// Set column family 0 aside in case we need it as a sentinel.
			family0 = family
			if family0Needed {
				needed = true
			}
			nullable = false
		}
		for _, columnID := range family.ColumnIDs {
			if needed && !nullable {
				// Nothing left to check.
				break
			}
			columnOrdinal := colIdxMap[columnID]
			if nc.Contains(columnOrdinal) {
				needed = true
			}
			if !columns[columnOrdinal].Nullable && (!indexedCols.Contains(columnOrdinal) ||
				compositeCols.Contains(columnOrdinal) && !hasSecondaryEncoding) {
				// The column is non-nullable and cannot be decoded from a different
				// family, so this column family must have a KV entry for every row.
				nullable = false
			}
		}
		if needed {
			neededFamilyIDs = append(neededFamilyIDs, family.ID)
			if !nullable {
				allFamiliesNullable = false
			}
		}
	}
	if family0 == nil {
		panic("column family 0 not found")
	}

	// If all the needed families are nullable, we also need family 0 as a
	// sentinel. Note that this is only the case if family 0 was not already added
	// to neededFamilyIDs.
	if allFamiliesNullable {
		// Prepend family 0.
		neededFamilyIDs = append(neededFamilyIDs, 0)
		copy(neededFamilyIDs[1:], neededFamilyIDs)
		neededFamilyIDs[0] = family0.ID
	}

	return neededFamilyIDs
}

// SplitSpanIntoSeparateFamilies splits a span representing a single row point
// lookup into separate disjoint spans that request only the particular column
// families from neededFamilies instead of requesting all the families. It is up
// to the client to ensure the requested span represents a single row lookup and
// that the span splitting is appropriate (see CanSplitSpanIntoSeparateFamilies).
//
// The function accepts a slice of spans to append to.
func SplitSpanIntoSeparateFamilies(
	appendTo roachpb.Spans, span roachpb.Span, neededFamilies []FamilyID,
) roachpb.Spans {
	span.Key = span.Key[:len(span.Key):len(span.Key)] // avoid mutation and aliasing
	for i, familyID := range neededFamilies {
		var famSpan roachpb.Span
		famSpan.Key = keys.MakeFamilyKey(span.Key, uint32(familyID))
		famSpan.EndKey = famSpan.Key.PrefixEnd()
		if i > 0 && familyID == neededFamilies[i-1]+1 {
			// This column family is adjacent to the previous one. We can merge
			// the two spans into one.
			appendTo[len(appendTo)-1].EndKey = famSpan.EndKey
		} else {
			appendTo = append(appendTo, famSpan)
		}
	}
	return appendTo
}

// makeKeyFromEncDatums creates an index key by concatenating keyPrefix with the
// encodings of the given EncDatum values. The values, types, and dirs
// parameters should be specified in the same order as the index key columns and
// may be a prefix. The complete return value is true if the resultant key
// fully constrains the index.
//
// If a table or index is interleaved, `encoding.interleavedSentinel` is used
// in place of the family id (a varint) to signal the next component of the
// key.  An example of one level of interleaving (a parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
func makeKeyFromEncDatums(
	values EncDatumRow,
	types []*types.T,
	dirs []IndexDescriptor_Direction,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	alloc *DatumAlloc,
	keyPrefix []byte,
) (_ roachpb.Key, complete bool, containsNull bool, _ error) {
	// Values may be a prefix of the index columns.
	if len(values) > len(dirs) {
		return nil, false, false, errors.Errorf("%d values, %d directions", len(values), len(dirs))
	}
	if len(values) != len(types) {
		return nil, false, false, errors.Errorf("%d values, %d types", len(values), len(types))
	}
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key := make(roachpb.Key, len(keyPrefix), len(keyPrefix)*2)
	copy(key, keyPrefix)

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = EncodePartialTableIDIndexID(key, ancestor.TableID, ancestor.IndexID)
			}

			partial := false
			length := int(ancestor.SharedPrefixLen)
			if length > len(types) {
				length = len(types)
				partial = true
			}
			var (
				err error
				n   bool
			)
			key, n, err = appendEncDatumsToKey(key, types[:length], values[:length], dirs[:length], alloc)
			if err != nil {
				return nil, false, false, err
			}
			containsNull = containsNull || n
			if partial {
				// Early stop - the number of desired columns was fewer than the number
				// left in the current interleave.
				return key, false, false, nil
			}
			types, values, dirs = types[length:], values[length:], dirs[length:]

			// Each ancestor is separated by an interleaved
			// sentinel (0xfe).
			key = encoding.EncodeInterleavedSentinel(key)
		}

		key = EncodePartialTableIDIndexID(key, tableDesc.ID, index.ID)
	}
	var (
		err error
		n   bool
	)
	key, n, err = appendEncDatumsToKey(key, types, values, dirs, alloc)
	if err != nil {
		return key, false, false, err
	}
	containsNull = containsNull || n
	return key, len(types) == len(index.ColumnIDs), containsNull, err
}

// findColumnValue returns the value corresponding to the column. If
// the column isn't present return a NULL value.
func findColumnValue(column ColumnID, colMap map[ColumnID]int, values []tree.Datum) tree.Datum {
	if i, ok := colMap[column]; ok {
		// TODO(pmattis): Need to convert the values[i] value to the type
		// expected by the column.
		return values[i]
	}
	return tree.DNull
}

// appendEncDatumsToKey concatenates the encoded representations of
// the datums at the end of the given roachpb.Key.
func appendEncDatumsToKey(
	key roachpb.Key,
	types []*types.T,
	values EncDatumRow,
	dirs []IndexDescriptor_Direction,
	alloc *DatumAlloc,
) (_ roachpb.Key, containsNull bool, _ error) {
	for i, val := range values {
		encoding := DatumEncoding_ASCENDING_KEY
		if dirs[i] == IndexDescriptor_DESC {
			encoding = DatumEncoding_DESCENDING_KEY
		}
		if val.IsNull() {
			containsNull = true
		}
		var err error
		key, err = val.Encode(types[i], alloc, encoding, key)
		if err != nil {
			return nil, false, err
		}
	}
	return key, containsNull, nil
}

// EncodePartialTableIDIndexID encodes a table id followed by an index id to an
// existing key. The key must already contain a tenant id.
func EncodePartialTableIDIndexID(key []byte, tableID ID, indexID IndexID) []byte {
	return keys.MakeTableIDIndexID(key, uint32(tableID), uint32(indexID))
}

// DecodePartialTableIDIndexID decodes a table id followed by an index id. The
// input key must already have its tenant id removed.
func DecodePartialTableIDIndexID(key []byte) ([]byte, ID, IndexID, error) {
	key, tableID, indexID, err := keys.DecodeTableIDIndexID(key)
	return key, ID(tableID), IndexID(indexID), err
}

// DecodeIndexKeyPrefix decodes the prefix of an index key and returns the
// index id and a slice for the rest of the key.
//
// Don't use this function in the scan "hot path".
func DecodeIndexKeyPrefix(
	codec keys.SQLCodec, desc *TableDescriptor, key []byte,
) (indexID IndexID, remaining []byte, err error) {
	key, err = codec.StripTenantPrefix(key)
	if err != nil {
		return 0, nil, err
	}

	// TODO(dan): This whole operation is n^2 because of the interleaves
	// bookkeeping. We could improve it to n with a prefix tree of components.

	interleaves := append([]IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...)

	for component := 0; ; component++ {
		var tableID ID
		key, tableID, indexID, err = DecodePartialTableIDIndexID(key)
		if err != nil {
			return 0, nil, err
		}
		if tableID == desc.ID {
			// Once desc's table id has been decoded, there can be no more
			// interleaves.
			break
		}

		for i := len(interleaves) - 1; i >= 0; i-- {
			if len(interleaves[i].Interleave.Ancestors) <= component ||
				interleaves[i].Interleave.Ancestors[component].TableID != tableID ||
				interleaves[i].Interleave.Ancestors[component].IndexID != indexID {

				// This component, and thus this interleave, doesn't match what was
				// decoded, remove it.
				copy(interleaves[i:], interleaves[i+1:])
				interleaves = interleaves[:len(interleaves)-1]
			}
		}
		// The decoded key doesn't many any known interleaves
		if len(interleaves) == 0 {
			return 0, nil, errors.Errorf("no known interleaves for key")
		}

		// Anything left has the same SharedPrefixLen at index `component`, so just
		// use the first one.
		for i := uint32(0); i < interleaves[0].Interleave.Ancestors[component].SharedPrefixLen; i++ {
			l, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, err
			}
			key = key[l:]
		}

		// Consume the interleaved sentinel.
		var ok bool
		key, ok = encoding.DecodeIfInterleavedSentinel(key)
		if !ok {
			return 0, nil, errors.Errorf("invalid interleave key")
		}
	}

	return indexID, key, err
}

// DecodeIndexKey decodes the values that are a part of the specified index
// key (setting vals).
//
// The remaining bytes in the index key are returned which will either be an
// encoded column ID for the primary key index, the primary key suffix for
// non-unique secondary indexes or unique secondary indexes containing NULL or
// empty. If the given descriptor does not match the key, false is returned with
// no error.
func DecodeIndexKey(
	codec keys.SQLCodec,
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []*types.T,
	vals []EncDatum,
	colDirs []IndexDescriptor_Direction,
	key []byte,
) (remainingKey []byte, matches bool, foundNull bool, _ error) {
	key, err := codec.StripTenantPrefix(key)
	if err != nil {
		return nil, false, false, err
	}
	key, _, _, err = DecodePartialTableIDIndexID(key)
	if err != nil {
		return nil, false, false, err
	}
	return DecodeIndexKeyWithoutTableIDIndexIDPrefix(desc, index, types, vals, colDirs, key)
}

// DecodeIndexKeyWithoutTableIDIndexIDPrefix is the same as DecodeIndexKey,
// except it expects its index key is missing in its tenant id and first table
// id / index id key prefix.
func DecodeIndexKeyWithoutTableIDIndexIDPrefix(
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []*types.T,
	vals []EncDatum,
	colDirs []IndexDescriptor_Direction,
	key []byte,
) (remainingKey []byte, matches bool, foundNull bool, _ error) {
	var decodedTableID ID
	var decodedIndexID IndexID
	var err error

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// Our input key had its first table id / index id chopped off, so
			// don't try to decode those for the first ancestor.
			if i != 0 {
				key, decodedTableID, decodedIndexID, err = DecodePartialTableIDIndexID(key)
				if err != nil {
					return nil, false, false, err
				}
				if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
					return nil, false, false, nil
				}
			}

			length := int(ancestor.SharedPrefixLen)
			var isNull bool
			key, isNull, err = DecodeKeyVals(types[:length], vals[:length], colDirs[:length], key)
			if err != nil {
				return nil, false, false, err
			}
			types, vals, colDirs = types[length:], vals[length:], colDirs[length:]
			foundNull = foundNull || isNull

			// Consume the interleaved sentinel.
			var ok bool
			key, ok = encoding.DecodeIfInterleavedSentinel(key)
			if !ok {
				return nil, false, false, nil
			}
		}

		key, decodedTableID, decodedIndexID, err = DecodePartialTableIDIndexID(key)
		if err != nil {
			return nil, false, false, err
		}
		if decodedTableID != desc.ID || decodedIndexID != index.ID {
			return nil, false, false, nil
		}
	}

	var isNull bool
	key, isNull, err = DecodeKeyVals(types, vals, colDirs, key)
	if err != nil {
		return nil, false, false, err
	}
	foundNull = foundNull || isNull

	// We're expecting a column family id next (a varint). If
	// interleavedSentinel is actually next, then this key is for a child
	// table.
	if _, ok := encoding.DecodeIfInterleavedSentinel(key); ok {
		return nil, false, false, nil
	}

	return key, true, foundNull, nil
}

// DecodeKeyVals decodes the values that are part of the key. The decoded
// values are stored in the vals. If this slice is nil, the direction
// used will default to encoding.Ascending.
// DecodeKeyVals returns whether or not NULL was encountered in the key.
func DecodeKeyVals(
	types []*types.T, vals []EncDatum, directions []IndexDescriptor_Direction, key []byte,
) ([]byte, bool, error) {
	if directions != nil && len(directions) != len(vals) {
		return nil, false, errors.Errorf("encoding directions doesn't parallel vals: %d vs %d.",
			len(directions), len(vals))
	}
	foundNull := false
	for j := range vals {
		enc := DatumEncoding_ASCENDING_KEY
		if directions != nil && (directions[j] == IndexDescriptor_DESC) {
			enc = DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(types[j], enc, key)
		if err != nil {
			return nil, false, err
		}
		if vals[j].IsNull() {
			foundNull = true
		}
	}
	return key, foundNull, nil
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *DatumAlloc, codec keys.SQLCodec, tableDesc *TableDescriptor, entry kv.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(codec, tableDesc, entry.Key)
	if err != nil {
		return nil, err
	}
	if indexID == tableDesc.PrimaryIndex.ID {
		return entry.Key, nil
	}

	index, err := tableDesc.FindIndexByID(indexID)
	if err != nil {
		return nil, err
	}

	// Extract the values for index.ColumnIDs.
	indexTypes, err := GetColumnTypes(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, len(index.ColumnIDs))
	dirs := index.ColumnDirections
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(dan): In the interleaved index case, we parse the key twice; once to
		// find the index id so we can look up the descriptor, and once to extract
		// the values. Only parse once.
		var ok bool
		_, ok, _, err = DecodeIndexKey(codec, tableDesc, index, indexTypes, values, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, _, err = DecodeKeyVals(indexTypes, values, dirs, key)
		if err != nil {
			return nil, err
		}
	}

	// Extract the values for index.ExtraColumnIDs
	extraTypes, err := GetColumnTypes(tableDesc, index.ExtraColumnIDs)
	if err != nil {
		return nil, err
	}
	extraValues := make([]EncDatum, len(index.ExtraColumnIDs))
	dirs = make([]IndexDescriptor_Direction, len(index.ExtraColumnIDs))
	for i := range index.ExtraColumnIDs {
		// Implicit columns are always encoded Ascending.
		dirs[i] = IndexDescriptor_ASC
	}
	extraKey := key
	if index.Unique {
		extraKey, err = entry.Value.GetBytes()
		if err != nil {
			return nil, err
		}
	}
	_, _, err = DecodeKeyVals(extraTypes, extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	colMap := make(map[ColumnID]int)
	for i, columnID := range index.ColumnIDs {
		colMap[columnID] = i
	}
	for i, columnID := range index.ExtraColumnIDs {
		colMap[columnID] = i + len(index.ColumnIDs)
	}
	indexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, tableDesc.PrimaryIndex.ID)

	decodedValues := make([]tree.Datum, len(values)+len(extraValues))
	for i, value := range values {
		err := value.EnsureDecoded(indexTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}
	for i, value := range extraValues {
		err := value.EnsureDecoded(extraTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[len(values)+i] = value.Datum
	}
	indexKey, _, err := EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}

// IndexEntry represents an encoded key/value for an index entry.
type IndexEntry struct {
	Key   roachpb.Key
	Value roachpb.Value
	// Only used for forward indexes.
	Family FamilyID
}

// valueEncodedColumn represents a composite or stored column of a secondary
// index.
type valueEncodedColumn struct {
	id          ColumnID
	isComposite bool
}

// byID implements sort.Interface for []valueEncodedColumn based on the id
// field.
type byID []valueEncodedColumn

func (a byID) Len() int           { return len(a) }
func (a byID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byID) Less(i, j int) bool { return a[i].id < a[j].id }

// EncodeInvertedIndexKeys creates a list of inverted index keys by
// concatenating keyPrefix with the encodings of the column in the
// index.
func EncodeInvertedIndexKeys(
	index *IndexDescriptor, colMap map[ColumnID]int, values []tree.Datum, keyPrefix []byte,
) (key [][]byte, err error) {
	if len(index.ColumnIDs) > 1 {
		return nil, errors.AssertionFailedf("trying to apply inverted index to more than one column")
	}

	var val tree.Datum
	if i, ok := colMap[index.ColumnIDs[0]]; ok {
		val = values[i]
	} else {
		val = tree.DNull
	}
	if !geoindex.IsEmptyConfig(&index.GeoConfig) {
		return EncodeGeoInvertedIndexTableKeys(val, keyPrefix, index)
	}
	return EncodeInvertedIndexTableKeys(val, keyPrefix)
}

// EncodeInvertedIndexTableKeys produces one inverted index key per element in
// the input datum, which should be a container (either JSON or Array). For
// JSON, "element" means unique path through the document. Each output key is
// prefixed by inKey, and is guaranteed to be lexicographically sortable, but
// not guaranteed to be round-trippable during decoding. If the input Datum
// is (SQL) NULL, no inverted index keys will be produced, because inverted
// indexes cannot and do not need to satisfy the predicate col IS NULL.
func EncodeInvertedIndexTableKeys(val tree.Datum, inKey []byte) (key [][]byte, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	datum := tree.UnwrapDatum(nil, val)
	switch val.ResolvedType().Family() {
	case types.JsonFamily:
		return json.EncodeInvertedIndexKeys(inKey, val.(*tree.DJSON).JSON)
	case types.ArrayFamily:
		return encodeArrayInvertedIndexTableKeys(val.(*tree.DArray), inKey)
	}
	return nil, errors.AssertionFailedf("trying to apply inverted index to unsupported type %s", datum.ResolvedType())
}

// encodeArrayInvertedIndexTableKeys returns a list of inverted index keys for
// the given input array, one per entry in the array. The input inKey is
// prefixed to all returned keys.
// N.B.: This won't return any keys for
func encodeArrayInvertedIndexTableKeys(val *tree.DArray, inKey []byte) (key [][]byte, err error) {
	outKeys := make([][]byte, 0, len(val.Array))
	for i := range val.Array {
		d := val.Array[i]
		if d == tree.DNull {
			// We don't need to make keys for NULL, since in SQL:
			// SELECT ARRAY[1, NULL, 2] @> ARRAY[NULL]
			// returns false.
			continue
		}
		outKey := make([]byte, len(inKey))
		copy(outKey, inKey)
		newKey, err := EncodeTableKey(outKey, d, encoding.Ascending)
		if err != nil {
			return nil, err
		}
		outKeys = append(outKeys, newKey)
	}
	outKeys = unique.UniquifyByteSlices(outKeys)
	return outKeys, nil
}

// EncodeGeoInvertedIndexTableKeys is the equivalent of EncodeInvertedIndexTableKeys
// for Geography and Geometry.
func EncodeGeoInvertedIndexTableKeys(
	val tree.Datum, inKey []byte, index *IndexDescriptor,
) (key [][]byte, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	switch val.ResolvedType().Family() {
	case types.GeographyFamily:
		index := geoindex.NewS2GeographyIndex(*index.GeoConfig.S2Geography)
		intKeys, err := index.InvertedIndexKeys(context.TODO(), val.(*tree.DGeography).Geography)
		if err != nil {
			return nil, err
		}
		return encodeGeoKeys(inKey, intKeys)
	case types.GeometryFamily:
		index := geoindex.NewS2GeometryIndex(*index.GeoConfig.S2Geometry)
		intKeys, err := index.InvertedIndexKeys(context.TODO(), val.(*tree.DGeometry).Geometry)
		if err != nil {
			return nil, err
		}
		return encodeGeoKeys(inKey, intKeys)
	default:
		return nil, errors.Errorf("internal error: unexpected type: %s", val.ResolvedType().Family())
	}
}

func encodeGeoKeys(inKey []byte, geoKeys []geoindex.Key) (keys [][]byte, err error) {
	keys = make([][]byte, 0, len(geoKeys))
	for _, k := range geoKeys {
		outKey := make([]byte, len(inKey))
		copy(outKey, inKey)
		d := (tree.DInt)(k)
		newKey, err := EncodeTableKey(outKey, &d, encoding.Ascending)
		if err != nil {
			return nil, err
		}
		keys = append(keys, newKey)
	}
	return keys, nil
}

// EncodePrimaryIndex constructs a list of k/v pairs for a
// row encoded as a primary index. This function mirrors the encoding
// logic in prepareInsertOrUpdateBatch in pkg/sql/row/writer.go.
// It is somewhat duplicated here due to the different arguments
// that prepareOrInsertUpdateBatch needs and uses to generate
// the k/v's for the row it inserts. includeEmpty controls
// whether or not k/v's with empty values should be returned.
// It returns indexEntries in family sorted order.
func EncodePrimaryIndex(
	codec keys.SQLCodec,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	includeEmpty bool,
) ([]IndexEntry, error) {
	keyPrefix := MakeIndexKeyPrefix(codec, tableDesc, index.ID)
	indexKey, _, err := EncodeIndexKey(tableDesc, index, colMap, values, keyPrefix)
	if err != nil {
		return nil, err
	}
	// This information should be precomputed on the table descriptor.
	indexedColumns := map[ColumnID]struct{}{}
	for _, colID := range index.ColumnIDs {
		indexedColumns[colID] = struct{}{}
	}
	var entryValue []byte
	indexEntries := make([]IndexEntry, 0, len(tableDesc.Families))
	var columnsToEncode []valueEncodedColumn

	for i := range tableDesc.Families {
		var err error
		family := &tableDesc.Families[i]
		if i > 0 {
			indexKey = indexKey[:len(indexKey):len(indexKey)]
			entryValue = entryValue[:0]
			columnsToEncode = columnsToEncode[:0]
		}
		familyKey := keys.MakeFamilyKey(indexKey, uint32(family.ID))
		// The decoders expect that column family 0 is encoded with a TUPLE value tag, so we
		// don't want to use the untagged value encoding.
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
			datum := findColumnValue(family.DefaultColumnID, colMap, values)
			// We want to include this column if its value is non-null or
			// we were requested to include all of the columns.
			if datum != tree.DNull || includeEmpty {
				col, err := tableDesc.FindColumnByID(family.DefaultColumnID)
				if err != nil {
					return nil, err
				}
				value, err := MarshalColumnValue(col, datum)
				if err != nil {
					return nil, err
				}
				indexEntries = append(indexEntries, IndexEntry{Key: familyKey, Value: value, Family: family.ID})
			}
			continue
		}

		for _, colID := range family.ColumnIDs {
			if _, ok := indexedColumns[colID]; !ok {
				columnsToEncode = append(columnsToEncode, valueEncodedColumn{id: colID})
				continue
			}
			if cdatum, ok := values[colMap[colID]].(tree.CompositeDatum); ok {
				if cdatum.IsComposite() {
					columnsToEncode = append(columnsToEncode, valueEncodedColumn{id: colID, isComposite: true})
					continue
				}
			}
		}
		sort.Sort(byID(columnsToEncode))
		entryValue, err = writeColumnValues(entryValue, colMap, values, columnsToEncode)
		if err != nil {
			return nil, err
		}
		if family.ID != 0 && len(entryValue) == 0 && !includeEmpty {
			continue
		}
		entry := IndexEntry{Key: familyKey, Family: family.ID}
		entry.Value.SetTuple(entryValue)
		indexEntries = append(indexEntries, entry)
	}
	return indexEntries, nil
}

// EncodeSecondaryIndex encodes key/values for a secondary
// index. colMap maps ColumnIDs to indices in `values`. This returns a
// slice of IndexEntry. includeEmpty controls whether or not
// EncodeSecondaryIndex should return k/v's that contain
// empty values. For forward indexes the returned list of
// index entries is in family sorted order.
func EncodeSecondaryIndex(
	codec keys.SQLCodec,
	tableDesc *TableDescriptor,
	secondaryIndex *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	includeEmpty bool,
) ([]IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, secondaryIndex.ID)

	// Use the primary key encoding for covering indexes.
	if secondaryIndex.GetEncodingType(tableDesc.PrimaryIndex.ID) == PrimaryIndexEncoding {
		return EncodePrimaryIndex(codec, tableDesc, secondaryIndex, colMap, values, includeEmpty)
	}

	var containsNull = false
	var secondaryKeys [][]byte
	var err error
	if secondaryIndex.Type == IndexDescriptor_INVERTED {
		secondaryKeys, err = EncodeInvertedIndexKeys(secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
	} else {
		var secondaryIndexKey []byte
		secondaryIndexKey, containsNull, err = EncodeIndexKey(
			tableDesc, secondaryIndex, colMap, values, secondaryIndexKeyPrefix)

		secondaryKeys = [][]byte{secondaryIndexKey}
	}
	if err != nil {
		return []IndexEntry{}, err
	}

	// Add the extra columns - they are encoded in ascending order which is done
	// by passing nil for the encoding directions.
	extraKey, _, err := EncodeColumns(secondaryIndex.ExtraColumnIDs, nil,
		colMap, values, nil)
	if err != nil {
		return []IndexEntry{}, err
	}

	// entries is the resulting array that we will return. We allocate upfront at least
	// len(secondaryKeys) positions to avoid allocations from appending.
	entries := make([]IndexEntry, 0, len(secondaryKeys))
	for _, key := range secondaryKeys {
		if !secondaryIndex.Unique || containsNull {
			// If the index is not unique or it contains a NULL value, append
			// extraKey to the key in order to make it unique.
			key = append(key, extraKey...)
		}

		if len(tableDesc.Families) == 1 ||
			secondaryIndex.Type == IndexDescriptor_INVERTED ||
			secondaryIndex.Version == BaseIndexFormatVersion {
			// We do all computation that affects indexes with families in a separate code path to avoid performance
			// regression for tables without column families.
			entry, err := encodeSecondaryIndexNoFamilies(secondaryIndex, colMap, key, values, extraKey)
			if err != nil {
				return []IndexEntry{}, err
			}
			entries = append(entries, entry)
		} else {
			// This is only executed once as len(secondaryKeys) = 1 for non inverted secondary indexes.
			// Create a mapping of family ID to stored columns.
			// TODO (rohany): we want to share this information across calls to EncodeSecondaryIndex --
			//  its not easy to do this right now. It would be nice if the index descriptor or table descriptor
			//  had this information computed/cached for us.
			familyToColumns := make(map[FamilyID][]valueEncodedColumn)
			addToFamilyColMap := func(id FamilyID, column valueEncodedColumn) {
				if _, ok := familyToColumns[id]; !ok {
					familyToColumns[id] = []valueEncodedColumn{}
				}
				familyToColumns[id] = append(familyToColumns[id], column)
			}
			// Ensure that column family 0 always generates a k/v pair.
			familyToColumns[0] = []valueEncodedColumn{}
			// All composite columns are stored in family 0.
			for _, id := range secondaryIndex.CompositeColumnIDs {
				addToFamilyColMap(0, valueEncodedColumn{id: id, isComposite: true})
			}
			for _, family := range tableDesc.Families {
				for _, id := range secondaryIndex.StoreColumnIDs {
					for _, col := range family.ColumnIDs {
						if id == col {
							addToFamilyColMap(family.ID, valueEncodedColumn{id: id, isComposite: false})
						}
					}
				}
			}
			entries, err = encodeSecondaryIndexWithFamilies(
				familyToColumns, secondaryIndex, colMap, key, values, extraKey, entries, includeEmpty)
			if err != nil {
				return []IndexEntry{}, err
			}
		}
	}
	return entries, nil
}

// encodeSecondaryIndexWithFamilies generates a k/v pair for
// each family/column pair in familyMap. The row parameter will be
// modified by the function, so copy it before using. includeEmpty
// controls whether or not k/v's with empty values will be returned.
// The returned indexEntries are in family sorted order.
func encodeSecondaryIndexWithFamilies(
	familyMap map[FamilyID][]valueEncodedColumn,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	key []byte,
	row []tree.Datum,
	extraKeyCols []byte,
	results []IndexEntry,
	includeEmpty bool,
) ([]IndexEntry, error) {
	var (
		value []byte
		err   error
	)
	origKeyLen := len(key)
	// TODO (rohany): is there a natural way of caching this information as well?
	// We have to iterate over the map in sorted family order. Other parts of the code
	// depend on a per-call consistent order of keys generated.
	familyIDs := make([]int, 0, len(familyMap))
	for familyID := range familyMap {
		familyIDs = append(familyIDs, int(familyID))
	}
	sort.Ints(familyIDs)
	for _, familyID := range familyIDs {
		storedColsInFam := familyMap[FamilyID(familyID)]
		// Ensure that future appends to key will cause a copy and not overwrite
		// existing key values.
		key = key[:origKeyLen:origKeyLen]

		// If we aren't storing any columns in this family and we are not the first family,
		// skip onto the next family. We need to write family 0 no matter what to ensure
		// that each row has at least one entry in the DB.
		if len(storedColsInFam) == 0 && familyID != 0 {
			continue
		}

		sort.Sort(byID(storedColsInFam))

		key = keys.MakeFamilyKey(key, uint32(familyID))
		if index.Unique && familyID == 0 {
			// Note that a unique secondary index that contains a NULL column value
			// will have extraKey appended to the key and stored in the value. We
			// require extraKey to be appended to the key in order to make the key
			// unique. We could potentially get rid of the duplication here but at
			// the expense of complicating scanNode when dealing with unique
			// secondary indexes.
			value = extraKeyCols
		} else {
			// The zero value for an index-value is a 0-length bytes value.
			value = []byte{}
		}

		value, err = writeColumnValues(value, colMap, row, storedColsInFam)
		if err != nil {
			return []IndexEntry{}, err
		}
		entry := IndexEntry{Key: key, Family: FamilyID(familyID)}
		// If we aren't looking at family 0 and don't have a value,
		// don't include an entry for this k/v.
		if familyID != 0 && len(value) == 0 && !includeEmpty {
			continue
		}
		// If we are looking at family 0, encode the data as BYTES, as it might
		// include encoded primary key columns. For other families, use the
		// tuple encoding for the value.
		if familyID == 0 {
			entry.Value.SetBytes(value)
		} else {
			entry.Value.SetTuple(value)
		}
		results = append(results, entry)
	}
	return results, nil
}

// encodeSecondaryIndexNoFamilies takes a mostly constructed
// secondary index key (without the family/sentinel at
// the end), and appends the 0 family sentinel to it, and
// constructs the value portion of the index. This function
// performs the index encoding version before column
// families were introduced onto secondary indexes.
func encodeSecondaryIndexNoFamilies(
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	key []byte,
	row []tree.Datum,
	extraKeyCols []byte,
) (IndexEntry, error) {
	var (
		value []byte
		err   error
	)
	// If we aren't encoding index keys with families, all index keys use the sentinel family 0.
	key = keys.MakeFamilyKey(key, 0)
	if index.Unique {
		// Note that a unique secondary index that contains a NULL column value
		// will have extraKey appended to the key and stored in the value. We
		// require extraKey to be appended to the key in order to make the key
		// unique. We could potentially get rid of the duplication here but at
		// the expense of complicating scanNode when dealing with unique
		// secondary indexes.
		value = append(value, extraKeyCols...)
	} else {
		// The zero value for an index-value is a 0-length bytes value.
		value = []byte{}
	}
	var cols []valueEncodedColumn
	// Since we aren't encoding data with families, we just encode all stored and composite columns in the value.
	for _, id := range index.StoreColumnIDs {
		cols = append(cols, valueEncodedColumn{id: id, isComposite: false})
	}
	for _, id := range index.CompositeColumnIDs {
		// Inverted indexes on a composite type (i.e. an array of composite types)
		// should not add the indexed column to the value.
		if index.Type == IndexDescriptor_INVERTED && id == index.ColumnIDs[0] {
			continue
		}
		cols = append(cols, valueEncodedColumn{id: id, isComposite: true})
	}
	sort.Sort(byID(cols))
	value, err = writeColumnValues(value, colMap, row, cols)
	if err != nil {
		return IndexEntry{}, err
	}
	entry := IndexEntry{Key: key, Family: 0}
	entry.Value.SetBytes(value)
	return entry, nil
}

// writeColumnValues writes the value encoded versions of the desired columns from the input
// row of datums into the value byte slice.
func writeColumnValues(
	value []byte, colMap map[ColumnID]int, row []tree.Datum, columns []valueEncodedColumn,
) ([]byte, error) {
	var lastColID ColumnID
	for _, col := range columns {
		val := findColumnValue(col.id, colMap, row)
		if val == tree.DNull || (col.isComposite && !val.(tree.CompositeDatum).IsComposite()) {
			continue
		}
		if lastColID > col.id {
			panic(fmt.Errorf("cannot write column id %d after %d", col.id, lastColID))
		}
		colIDDiff := col.id - lastColID
		lastColID = col.id
		var err error
		value, err = EncodeTableValue(value, colIDDiff, val, nil)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps ColumnIDs to indices in `values`. secondaryIndexEntries is the return
// value (passed as a parameter so the caller can reuse between rows) and is
// expected to be the same length as indexes.
func EncodeSecondaryIndexes(
	codec keys.SQLCodec,
	tableDesc *TableDescriptor,
	indexes []IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	secondaryIndexEntries []IndexEntry,
	includeEmpty bool,
) ([]IndexEntry, error) {
	if len(secondaryIndexEntries) > 0 {
		panic("Length of secondaryIndexEntries was non-zero")
	}
	for i := range indexes {
		entries, err := EncodeSecondaryIndex(codec, tableDesc, &indexes[i], colMap, values, includeEmpty)
		if err != nil {
			return secondaryIndexEntries, err
		}
		// Normally, each index will have exactly one entry. However, inverted
		// indexes can have 0 or >1 entries, as well as secondary indexes which
		// store columns from multiple column families.
		secondaryIndexEntries = append(secondaryIndexEntries, entries...)
	}
	return secondaryIndexEntries, nil
}

// IndexKeyEquivSignature parses an index key if and only if the index
// key belongs to a table where its equivalence signature and all its
// interleave ancestors' signatures can be found in
// validEquivSignatures.
//
// Its validEquivSignatures argument is a map containing equivalence
// signatures of valid ancestors of the desired table and of the
// desired table itself.
//
// IndexKeyEquivSignature returns whether or not the index key
// satisfies the above condition, the value mapped to by the desired
// table (could be a table index), and the rest of the key that's not
// part of the signature.
//
// It also requires two []byte buffers: one for the signature
// (signatureBuf) and one for the rest of the key (keyRestBuf).
//
// The equivalence signature defines the equivalence classes for the
// signature of potentially interleaved tables. For example, the
// equivalence signatures for the following interleaved indexes:
//
//    <parent@primary>
//    <child@secondary>
//
// and index keys
//    <parent index key>:   /<parent table id>/<parent index id>/<val 1>/<val 2>
//    <child index key>:    /<parent table id>/<parent index id>/<val 1>/<val 2>/#/<child table id>/child index id>/<val 3>/<val 4>
//
// correspond to the equivalence signatures
//    <parent@primary>:     /<parent table id>/<parent index id>
//    <child@secondary>:    /<parent table id>/<parent index id>/#/<child table id>/<child index id>
//
// Equivalence signatures allow us to associate an index key with its
// table without having to invoke DecodeIndexKey multiple times.
//
// IndexKeyEquivSignature will return false if the a table's
// ancestor's signature or the table's signature (table which the
// index key belongs to) is not mapped in validEquivSignatures.
//
// For example, suppose the given key is
//
//    /<t2 table id>/<t2 index id>/<val t2>/#/<t3 table id>/<t3 table id>/<val t3>
//
// and validEquivSignatures contains
//
//    /<t1 table id>/t1 index id>
//    /<t1 table id>/t1 index id>/#/<t4 table id>/<t4 index id>
//
// IndexKeyEquivSignature will short-circuit and return false once
//
//    /<t2 table id>/<t2 index id>
//
// is processed since t2's signature is not specified in validEquivSignatures.
func IndexKeyEquivSignature(
	key []byte, validEquivSignatures map[string]int, signatureBuf []byte, restBuf []byte,
) (tableIdx int, restResult []byte, success bool, err error) {
	signatureBuf = signatureBuf[:0]
	restResult = restBuf[:0]
	for {
		// Well-formed key is guaranteed to to have 2 varints for every
		// ancestor: the TableID and IndexID.
		// We extract these out and add them to our buffer.
		for i := 0; i < 2; i++ {
			idLen, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, false, err
			}
			signatureBuf = append(signatureBuf, key[:idLen]...)
			key = key[idLen:]
		}

		// The current signature (either an ancestor table's or the key's)
		// is not one of the validEquivSignatures.
		// We can short-circuit and return false.
		recentTableIdx, found := validEquivSignatures[string(signatureBuf)]
		if !found {
			return 0, nil, false, nil
		}

		var isSentinel bool
		// Peek and discard encoded index values.
		for {
			key, isSentinel = encoding.DecodeIfInterleavedSentinel(key)
			// We stop once the key is empty or if we encounter a
			// sentinel for the next TableID-IndexID pair.
			if len(key) == 0 || isSentinel {
				break
			}
			len, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, false, err
			}
			// Append any other bytes (column values initially,
			// then family ID and timestamp) to return.
			restResult = append(restResult, key[:len]...)
			key = key[len:]
		}

		if !isSentinel {
			// The key has been fully decomposed and is valid up to
			// this point.
			// Return the most recent table index from
			// validEquivSignatures.
			return recentTableIdx, restResult, true, nil
		}
		// If there was a sentinel, we know there are more
		// descendant(s).
		// We insert an interleave sentinel and continue extracting the
		// next descendant's IDs.
		signatureBuf = encoding.EncodeInterleavedSentinel(signatureBuf)
	}
}

// TableEquivSignatures returns the equivalence signatures for each interleave
// ancestor and itself. See IndexKeyEquivSignature for more info.
func TableEquivSignatures(
	desc *TableDescriptor, index *IndexDescriptor,
) (signatures [][]byte, err error) {
	// signatures contains the slice reference to the signature of every
	// ancestor of the current table-index.
	// The last slice reference is the given table-index's signature.
	signatures = make([][]byte, len(index.Interleave.Ancestors)+1)
	// fullSignature is the backing byte slice for each individual signature
	// as it buffers each block of table and index IDs.
	// We eagerly allocate 4 bytes for each of the two IDs per ancestor
	// (which can fit Uvarint IDs up to 2^17-1 without another allocation),
	// 1 byte for each interleave sentinel, and 4 bytes each for the given
	// table's and index's ID.
	fullSignature := make([]byte, 0, len(index.Interleave.Ancestors)*9+8)

	// Encode the table's ancestors' TableIDs and IndexIDs.
	for i, ancestor := range index.Interleave.Ancestors {
		fullSignature = EncodePartialTableIDIndexID(fullSignature, ancestor.TableID, ancestor.IndexID)
		// Create a reference up to this point for the ancestor's
		// signature.
		signatures[i] = fullSignature
		// Append Interleave sentinel after every ancestor.
		fullSignature = encoding.EncodeInterleavedSentinel(fullSignature)
	}

	// Encode the table's table and index IDs.
	fullSignature = EncodePartialTableIDIndexID(fullSignature, desc.ID, index.ID)
	// Create a reference for the given table's signature as the last
	// element of signatures.
	signatures[len(signatures)-1] = fullSignature

	return signatures, nil
}

// maxKeyTokens returns the maximum number of key tokens in an index's key,
// including the table ID, index ID, and index column values (including extra
// columns that may be stored in the key).
// It requires knowledge of whether the key will or might contain a NULL value:
// if uncertain, pass in true to 'overestimate' the maxKeyTokens.
//
// In general, a key belonging to an interleaved index grandchild is encoded as:
//
//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>/#/table/index/<grandchild-pk1>/.../<grandchild-pkZ>
//
// The part of the key with respect to the grandchild index would be
// the entire key since there are no grand-grandchild table/index IDs or
// <grandgrandchild-pk>. The maximal prefix of the key that belongs to child is
//
//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>
//
// and the maximal prefix of the key that belongs to parent is
//
//    /table/index/<parent-pk1>/.../<parent-pkX>
//
// This returns the maximum number of <tokens> in this prefix.
func maxKeyTokens(index *IndexDescriptor, containsNull bool) int {
	nTables := len(index.Interleave.Ancestors) + 1
	nKeyCols := len(index.ColumnIDs)

	// Non-unique secondary indexes or unique secondary indexes with a NULL
	// value have additional columns in the key that may appear in a span
	// (e.g. primary key columns not part of the index).
	// See EncodeSecondaryIndex.
	if !index.Unique || containsNull {
		nKeyCols += len(index.ExtraColumnIDs)
	}

	// To illustrate how we compute max # of key tokens, take the
	// key in the example above and let the respective index be child.
	// We'd like to return the number of bytes in
	//
	//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>
	// For each table-index, there is
	//    1. table ID
	//    2. index ID
	//    3. interleave sentinel
	// or 3 * nTables.
	// Each <parent-pkX> must be a part of the index's columns (nKeys).
	// Finally, we do not want to include the interleave sentinel for the
	// current index (-1).
	return 3*nTables + nKeyCols - 1
}

// AdjustStartKeyForInterleave adjusts the start key to skip unnecessary
// interleaved sections.
//
// For example, if child is interleaved into parent, a typical parent
// span might look like
//    /1 - /3
// and a typical child span might look like
//    /1/#/2 - /2/#/5
// Suppose the parent span is
//    /1/#/2 - /3
// where the start key is a child's index key. Notice that the first parent
// key read actually starts at /2 since all the parent keys with the prefix
// /1 come before the child key /1/#/2 (and is not read in the span).
// We can thus push forward the start key from /1/#/2 to /2. If the start key
// was /1, we cannot push this forwards since that is the first key we want
// to read.
func AdjustStartKeyForInterleave(
	codec keys.SQLCodec, index *IndexDescriptor, start roachpb.Key,
) (roachpb.Key, error) {
	// Remove the tenant prefix before decomposing.
	strippedStart, err := codec.StripTenantPrefix(start)
	if err != nil {
		return roachpb.Key{}, err
	}

	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(strippedStart)
	if err != nil {
		return roachpb.Key{}, err
	}
	nIndexTokens := maxKeyTokens(index, containsNull)

	// This is either the index's own key or one of its ancestor's key.
	// Nothing to do.
	if len(keyTokens) <= nIndexTokens {
		return start, nil
	}

	// len(keyTokens) > nIndexTokens, so this must be a child key.
	// Transform /1/#/2 --> /2.
	firstNTokenLen := 0
	for _, token := range keyTokens[:nIndexTokens] {
		firstNTokenLen += len(token)
	}

	return start[:firstNTokenLen].PrefixEnd(), nil
}

// AdjustEndKeyForInterleave returns an exclusive end key. It does two things:
//    - determines the end key based on the prior: inclusive vs exclusive
//    - adjusts the end key to skip unnecessary interleaved sections
//
// For example, the parent span composed from the filter PK >= 1 and PK < 3 is
//    /1 - /3
// This reads all keys up to the first parent key for PK = 3. If parent had
// interleaved tables and keys, it would unnecessarily scan over interleaved
// rows under PK2 (e.g. /2/#/5).
// We can instead "tighten" or adjust the end key from /3 to /2/#.
// DO NOT pass in any keys that have been invoked with PrefixEnd: this may
// cause issues when trying to decode the key tokens.
// AdjustEndKeyForInterleave is idempotent upon successive invocation(s).
func AdjustEndKeyForInterleave(
	codec keys.SQLCodec,
	table *TableDescriptor,
	index *IndexDescriptor,
	end roachpb.Key,
	inclusive bool,
) (roachpb.Key, error) {
	if index.Type == IndexDescriptor_INVERTED {
		return end.PrefixEnd(), nil
	}

	// Remove the tenant prefix before decomposing.
	strippedEnd, err := codec.StripTenantPrefix(end)
	if err != nil {
		return roachpb.Key{}, err
	}

	// To illustrate, suppose we have the interleaved hierarchy
	//    parent
	//	child
	//	  grandchild
	// Suppose our target index is child.
	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(strippedEnd)
	if err != nil {
		return roachpb.Key{}, err
	}
	nIndexTokens := maxKeyTokens(index, containsNull)

	// Sibling/nibling keys: it is possible for this key to be part
	// of a sibling tree in the interleaved hierarchy, especially after
	// partitioning on range split keys.
	// As such, a sibling may be interpretted as an ancestor (if the sibling
	// has fewer key-encoded columns) or a descendant (if the sibling has
	// more key-encoded columns). Similarly for niblings.
	// This is fine because if the sibling is sorted before or after the
	// current index (child in our example), it is not possible for us to
	// adjust the sibling key such that we add or remove child (the current
	// index's) rows from our span.

	if index.ID != table.PrimaryIndex.ID || len(keyTokens) < nIndexTokens {
		// Case 1: secondary index, parent key or partial child key:
		// Secondary indexes cannot have interleaved rows.
		// We cannot adjust or tighten parent keys with respect to a
		// child index.
		// Partial child keys e.g. /1/#/1 vs /1/#/1/2 cannot have
		// interleaved rows.
		// Nothing to do besides making the end key exclusive if it was
		// initially inclusive.
		if inclusive {
			end = end.PrefixEnd()
		}
		return end, nil
	}

	if len(keyTokens) == nIndexTokens {
		// Case 2: child key

		lastToken := keyTokens[len(keyTokens)-1]
		_, isNotNullDesc := encoding.DecodeIfNotNullDescending(lastToken)
		// If this is the child's key and the last value in the key is
		// NotNullDesc, then it does not need (read: shouldn't) to be
		// tightened.
		// For example, the query with IS NOT NULL may generate
		// the end key
		//    /1/#/NOTNULLDESC
		if isNotNullDesc {
			if inclusive {
				end = end.PrefixEnd()
			}
			return end, nil
		}

		// We only want to UndoPrefixEnd if the end key passed is not
		// inclusive initially.
		if !inclusive {
			lastType := encoding.PeekType(lastToken)
			if lastType == encoding.Bytes || lastType == encoding.BytesDesc || lastType == encoding.Decimal {
				// If the last value is of type Decimals or
				// Bytes then this is more difficult since the
				// escape term is the last value.
				// TODO(richardwu): Figure out how to go back 1
				// logical bytes/decimal value.
				return end, nil
			}

			// We first iterate back to the previous key value
			//    /1/#/1 --> /1/#/0
			undoPrefixEnd, ok := encoding.UndoPrefixEnd(end)
			if !ok {
				return end, nil
			}
			end = undoPrefixEnd
		}

		// /1/#/0 --> /1/#/0/#
		return encoding.EncodeInterleavedSentinel(end), nil
	}

	// len(keyTokens) > nIndexTokens
	// Case 3: tightened child, sibling/nibling, or grandchild key

	// Case 3a: tightened child key
	// This could from a previous invocation of AdjustEndKeyForInterleave.
	// For example, if during index selection the key for child was
	// tightened
	//	/1/#/2 --> /1/#/1/#
	// We don't really want to tighten on '#' again.
	if _, isSentinel := encoding.DecodeIfInterleavedSentinel(keyTokens[nIndexTokens]); isSentinel && len(keyTokens)-1 == nIndexTokens {
		if inclusive {
			end = end.PrefixEnd()
		}
		return end, nil
	}

	// Case 3b/c: sibling/nibling or grandchild key
	// Ideally, we want to form
	//    /1/#/2/#/3 --> /1/#/2/#
	// We truncate up to and including the interleave sentinel (or next
	// sibling/nibling column value) after the last index key token.
	firstNTokenLen := 0
	for _, token := range keyTokens[:nIndexTokens] {
		firstNTokenLen += len(token)
	}

	return end[:firstNTokenLen+1], nil
}
