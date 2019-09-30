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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// This file contains facilities to encode primary and secondary
// indexes on SQL tables.

// MakeIndexKeyPrefix returns the key prefix used for the index's data. If you
// need the corresponding Span, prefer desc.IndexSpan(indexID) or
// desc.PrimaryIndexSpan().
func MakeIndexKeyPrefix(desc *TableDescriptor, indexID IndexID) []byte {
	var key []byte
	if i, err := desc.FindIndexByID(indexID); err == nil && len(i.Interleave.Ancestors) > 0 {
		key = encoding.EncodeUvarintAscending(key, uint64(i.Interleave.Ancestors[0].TableID))
		key = encoding.EncodeUvarintAscending(key, uint64(i.Interleave.Ancestors[0].IndexID))
		return key
	}
	key = encoding.EncodeUvarintAscending(key, uint64(desc.ID))
	key = encoding.EncodeUvarintAscending(key, uint64(indexID))
	return key
}

// EncodeIndexSpan creates the minimal key span for the key specified by the
// given table, index, and values, with the same method as EncodeIndexKey.
func EncodeIndexSpan(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (span roachpb.Span, containsNull bool, err error) {
	var key roachpb.Key
	key, containsNull, err = EncodeIndexKey(tableDesc, index, colMap, values, keyPrefix)
	if err != nil {
		return span, containsNull, err
	}
	return roachpb.Span{
		Key:    key,
		EndKey: encoding.EncodeInterleavedSentinel(key),
	}, containsNull, nil
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
// index.ColumnIDs are encoded.
func EncodePartialIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	numCols int,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	colIDs := index.ColumnIDs[:numCols]
	// We know we will append to the key which will cause the capacity to grow so
	// make it bigger from the get-go.
	// Add twice the key prefix as an initial guess.
	// Add 3 bytes for every ancestor: table,index id + interleave sentinel.
	// Add 2 bytes for every column value. An underestimate for all but low integers.
	key = make([]byte, len(keyPrefix), 2*len(keyPrefix)+3*len(index.Interleave.Ancestors)+2*len(values))
	copy(key, keyPrefix)
	dirs := directions(index.ColumnDirections)[:numCols]

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
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
				return key, containsNull, err
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

		key = encoding.EncodeUvarintAscending(key, uint64(tableDesc.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	}

	var n bool
	key, n, err = EncodeColumns(colIDs, dirs, colMap, values, key)
	containsNull = containsNull || n
	return key, containsNull, err
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
	keyPrefix []byte,
	values EncDatumRow,
	types []types.T,
	dirs []IndexDescriptor_Direction,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	alloc *DatumAlloc,
) (roachpb.Span, error) {
	startKey, complete, err := makeKeyFromEncDatums(keyPrefix, values, types, dirs, tableDesc, index, alloc)
	if err != nil {
		return roachpb.Span{}, err
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
	return roachpb.Span{Key: startKey, EndKey: endKey}, nil
}

// NeededColumnFamilyIDs returns a slice of FamilyIDs which contain
// the families needed to load a set of neededCols
func NeededColumnFamilyIDs(
	colIdxMap map[ColumnID]int, families []ColumnFamilyDescriptor, neededCols util.FastIntSet,
) []FamilyID {
	// Column family 0 is always included so we can distinguish null rows from
	// absent rows.
	needed := []FamilyID{0}
	for i := range families {
		family := &families[i]
		if family.ID == 0 {
			// Already added above.
			continue
		}
		for _, columnID := range family.ColumnIDs {
			columnOrdinal := colIdxMap[columnID]
			if neededCols.Contains(columnOrdinal) {
				needed = append(needed, family.ID)
				break
			}
		}
	}

	// TODO(solon): There is a further optimization possible here: if there is at
	// least one non-nullable column in the needed column families, we can
	// potentially omit the primary family, since the primary keys are encoded
	// in all families. (Note that composite datums are an exception.)

	return needed
}

// SplitSpanIntoSeparateFamilies can only be used to split a span representing
// a single row point lookup into separate spans that request particular
// families from neededFamilies instead of requesting all the families.
// It is up to the client to verify whether the requested span
// represents a single row lookup, and when the span splitting is appropriate.
func SplitSpanIntoSeparateFamilies(span roachpb.Span, neededFamilies []FamilyID) roachpb.Spans {
	var resultSpans roachpb.Spans
	for i, familyID := range neededFamilies {
		var tempSpan roachpb.Span
		tempSpan.Key = make(roachpb.Key, len(span.Key))
		copy(tempSpan.Key, span.Key)
		tempSpan.Key = keys.MakeFamilyKey(tempSpan.Key, uint32(familyID))
		tempSpan.EndKey = tempSpan.Key.PrefixEnd()
		if i > 0 && familyID == neededFamilies[i-1]+1 {
			// This column family is adjacent to the previous one. We can merge
			// the two spans into one.
			resultSpans[len(resultSpans)-1].EndKey = tempSpan.EndKey
		} else {
			resultSpans = append(resultSpans, tempSpan)
		}
	}
	return resultSpans
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
	keyPrefix []byte,
	values EncDatumRow,
	types []types.T,
	dirs []IndexDescriptor_Direction,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	alloc *DatumAlloc,
) (_ roachpb.Key, complete bool, _ error) {
	// Values may be a prefix of the index columns.
	if len(values) > len(dirs) {
		return nil, false, errors.Errorf("%d values, %d directions", len(values), len(dirs))
	}
	if len(values) != len(types) {
		return nil, false, errors.Errorf("%d values, %d types", len(values), len(types))
	}
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key := make(roachpb.Key, len(keyPrefix), len(keyPrefix)*2)
	copy(key, keyPrefix)

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
			}

			partial := false
			length := int(ancestor.SharedPrefixLen)
			if length > len(types) {
				length = len(types)
				partial = true
			}
			var err error
			key, err = appendEncDatumsToKey(key, types[:length], values[:length], dirs[:length], alloc)
			if err != nil {
				return nil, false, err
			}
			if partial {
				// Early stop - the number of desired columns was fewer than the number
				// left in the current interleave.
				return key, false, nil
			}
			types, values, dirs = types[length:], values[length:], dirs[length:]

			// Each ancestor is separated by an interleaved
			// sentinel (0xfe).
			key = encoding.EncodeInterleavedSentinel(key)
		}

		key = encoding.EncodeUvarintAscending(key, uint64(tableDesc.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	}
	var err error
	key, err = appendEncDatumsToKey(key, types, values, dirs, alloc)
	if err != nil {
		return key, false, err
	}
	return key, len(types) == len(index.ColumnIDs), err
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
	types []types.T,
	values EncDatumRow,
	dirs []IndexDescriptor_Direction,
	alloc *DatumAlloc,
) (roachpb.Key, error) {
	for i, val := range values {
		encoding := DatumEncoding_ASCENDING_KEY
		if dirs[i] == IndexDescriptor_DESC {
			encoding = DatumEncoding_DESCENDING_KEY
		}
		var err error
		key, err = val.Encode(&types[i], alloc, encoding, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// EncodeTableIDIndexID encodes a table id followed by an index id.
func EncodeTableIDIndexID(key []byte, tableID ID, indexID IndexID) []byte {
	key = encoding.EncodeUvarintAscending(key, uint64(tableID))
	key = encoding.EncodeUvarintAscending(key, uint64(indexID))
	return key
}

// DecodeTableIDIndexID decodes a table id followed by an index id.
func DecodeTableIDIndexID(key []byte) ([]byte, ID, IndexID, error) {
	var tableID uint64
	var indexID uint64
	var err error

	key, tableID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}
	key, indexID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}

	return key, ID(tableID), IndexID(indexID), nil
}

// DecodeIndexKeyPrefix decodes the prefix of an index key and returns the
// index id and a slice for the rest of the key.
//
// Don't use this function in the scan "hot path".
func DecodeIndexKeyPrefix(
	desc *TableDescriptor, key []byte,
) (indexID IndexID, remaining []byte, err error) {
	// TODO(dan): This whole operation is n^2 because of the interleaves
	// bookkeeping. We could improve it to n with a prefix tree of components.

	interleaves := append([]IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...)

	for component := 0; ; component++ {
		var tableID ID
		key, tableID, indexID, err = DecodeTableIDIndexID(key)
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
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []types.T,
	vals []EncDatum,
	colDirs []IndexDescriptor_Direction,
	key []byte,
) (remainingKey []byte, matches bool, _ error) {
	key, _, _, err := DecodeTableIDIndexID(key)
	if err != nil {
		return nil, false, err
	}
	return DecodeIndexKeyWithoutTableIDIndexIDPrefix(desc, index, types, vals, colDirs, key)
}

// DecodeIndexKeyWithoutTableIDIndexIDPrefix is the same as DecodeIndexKey,
// except it expects its index key is missing its first table id / index id
// key prefix.
func DecodeIndexKeyWithoutTableIDIndexIDPrefix(
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []types.T,
	vals []EncDatum,
	colDirs []IndexDescriptor_Direction,
	key []byte,
) (remainingKey []byte, matches bool, _ error) {
	var decodedTableID ID
	var decodedIndexID IndexID
	var err error

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// Our input key had its first table id / index id chopped off, so
			// don't try to decode those for the first ancestor.
			if i != 0 {
				key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
				if err != nil {
					return nil, false, err
				}
				if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
					return nil, false, nil
				}
			}

			length := int(ancestor.SharedPrefixLen)
			key, err = DecodeKeyVals(types[:length], vals[:length], colDirs[:length], key)
			if err != nil {
				return nil, false, err
			}
			types, vals, colDirs = types[length:], vals[length:], colDirs[length:]

			// Consume the interleaved sentinel.
			var ok bool
			key, ok = encoding.DecodeIfInterleavedSentinel(key)
			if !ok {
				return nil, false, nil
			}
		}

		key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
		if err != nil {
			return nil, false, err
		}
		if decodedTableID != desc.ID || decodedIndexID != index.ID {
			return nil, false, nil
		}
	}

	key, err = DecodeKeyVals(types, vals, colDirs, key)
	if err != nil {
		return nil, false, err
	}

	// We're expecting a column family id next (a varint). If
	// interleavedSentinel is actually next, then this key is for a child
	// table.
	if _, ok := encoding.DecodeIfInterleavedSentinel(key); ok {
		return nil, false, nil
	}

	return key, true, nil
}

// DecodeKeyVals decodes the values that are part of the key. The decoded
// values are stored in the vals. If this slice is nil, the direction
// used will default to encoding.Ascending.
func DecodeKeyVals(
	types []types.T, vals []EncDatum, directions []IndexDescriptor_Direction, key []byte,
) ([]byte, error) {
	if directions != nil && len(directions) != len(vals) {
		return nil, errors.Errorf("encoding directions doesn't parallel vals: %d vs %d.",
			len(directions), len(vals))
	}
	for j := range vals {
		enc := DatumEncoding_ASCENDING_KEY
		if directions != nil && (directions[j] == IndexDescriptor_DESC) {
			enc = DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(&types[j], enc, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *DatumAlloc, tableDesc *TableDescriptor, entry client.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(tableDesc, entry.Key)
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
		_, ok, err = DecodeIndexKey(tableDesc, index, indexTypes, values, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, err = DecodeKeyVals(indexTypes, values, dirs, key)
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
	_, err = DecodeKeyVals(extraTypes, extraValues, dirs, extraKey)
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
	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)

	decodedValues := make([]tree.Datum, len(values)+len(extraValues))
	for i, value := range values {
		err := value.EnsureDecoded(&indexTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}
	for i, value := range extraValues {
		err := value.EnsureDecoded(&extraTypes[i], a)
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
// index. Returns the key and whether any of the encoded values were
// NULLs.
func EncodeInvertedIndexKeys(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
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

	return EncodeInvertedIndexTableKeys(val, keyPrefix)
}

// EncodeInvertedIndexTableKeys encodes the paths in a JSON `val` and
// concatenates it with `inKey`and returns a list of buffers per
// path. The encoded values is guaranteed to be lexicographically
// sortable, but not guaranteed to be round-trippable during decoding.
func EncodeInvertedIndexTableKeys(val tree.Datum, inKey []byte) (key [][]byte, err error) {
	if val == tree.DNull {
		return [][]byte{encoding.EncodeNullAscending(inKey)}, nil
	}
	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DJSON:
		return json.EncodeInvertedIndexKeys(inKey, (t.JSON))
	}
	return nil, errors.AssertionFailedf("trying to apply inverted index to non JSON type")
}

// EncodeSecondaryIndex encodes key/values for a secondary
// index. colMap maps ColumnIDs to indices in `values`. This returns a
// slice of IndexEntry. Forward indexes will return one value, while
// inverted indices can return multiple values.
func EncodeSecondaryIndex(
	tableDesc *TableDescriptor,
	secondaryIndex *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
) ([]IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc, secondaryIndex.ID)

	var containsNull = false
	var secondaryKeys [][]byte
	var err error
	if secondaryIndex.Type == IndexDescriptor_INVERTED {
		secondaryKeys, err = EncodeInvertedIndexKeys(tableDesc, secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
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

	var entries = make([]IndexEntry, len(secondaryKeys))
	for i, key := range secondaryKeys {
		entry := IndexEntry{Key: key}

		if !secondaryIndex.Unique || containsNull {
			// If the index is not unique or it contains a NULL value, append
			// extraKey to the key in order to make it unique.
			entry.Key = append(entry.Key, extraKey...)
		}

		// Index keys are considered "sentinel" keys in that they do not have a
		// column ID suffix.
		entry.Key = keys.MakeFamilyKey(entry.Key, 0)

		var entryValue []byte
		if secondaryIndex.Unique {
			// Note that a unique secondary index that contains a NULL column value
			// will have extraKey appended to the key and stored in the value. We
			// require extraKey to be appended to the key in order to make the key
			// unique. We could potentially get rid of the duplication here but at
			// the expense of complicating scanNode when dealing with unique
			// secondary indexes.
			entryValue = extraKey
		} else {
			// The zero value for an index-key is a 0-length bytes value.
			entryValue = []byte{}
		}

		var cols []valueEncodedColumn
		for _, id := range secondaryIndex.StoreColumnIDs {
			cols = append(cols, valueEncodedColumn{id: id, isComposite: false})
		}
		for _, id := range secondaryIndex.CompositeColumnIDs {
			cols = append(cols, valueEncodedColumn{id: id, isComposite: true})
		}
		sort.Sort(byID(cols))

		var lastColID ColumnID
		// Composite columns have their contents at the end of the value.
		for _, col := range cols {
			val := findColumnValue(col.id, colMap, values)
			if val == tree.DNull || (col.isComposite && !val.(tree.CompositeDatum).IsComposite()) {
				continue
			}
			if lastColID > col.id {
				panic(fmt.Errorf("cannot write column id %d after %d", col.id, lastColID))
			}
			colIDDiff := col.id - lastColID
			lastColID = col.id
			entryValue, err = EncodeTableValue(entryValue, colIDDiff, val, nil)
			if err != nil {
				return []IndexEntry{}, err
			}
		}
		entry.Value.SetBytes(entryValue)
		entries[i] = entry
	}

	return entries, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps ColumnIDs to indices in `values`. secondaryIndexEntries is the return
// value (passed as a parameter so the caller can reuse between rows) and is
// expected to be the same length as indexes.
func EncodeSecondaryIndexes(
	tableDesc *TableDescriptor,
	indexes []IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	secondaryIndexEntries []IndexEntry,
) ([]IndexEntry, error) {
	if len(secondaryIndexEntries) != len(indexes) {
		panic("Length of secondaryIndexEntries is not equal to the number of indexes.")
	}
	for i := range indexes {
		entries, err := EncodeSecondaryIndex(tableDesc, &indexes[i], colMap, values)
		if err != nil {
			return secondaryIndexEntries, err
		}
		secondaryIndexEntries[i] = entries[0]

		// This is specifically for inverted indexes which can have more than one entry
		// associated with them.
		if len(entries) > 1 {
			secondaryIndexEntries = append(secondaryIndexEntries, entries[1:]...)
		}
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
		fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(ancestor.TableID))
		fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(ancestor.IndexID))
		// Create a reference up to this point for the ancestor's
		// signature.
		signatures[i] = fullSignature
		// Append Interleave sentinel after every ancestor.
		fullSignature = encoding.EncodeInterleavedSentinel(fullSignature)
	}

	// Encode the table's table and index IDs.
	fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(desc.ID))
	fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(index.ID))
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
func AdjustStartKeyForInterleave(index *IndexDescriptor, start roachpb.Key) (roachpb.Key, error) {
	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(start)
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
	table *TableDescriptor, index *IndexDescriptor, end roachpb.Key, inclusive bool,
) (roachpb.Key, error) {
	if index.Type == IndexDescriptor_INVERTED {
		return end.PrefixEnd(), nil
	}

	// To illustrate, suppose we have the interleaved hierarchy
	//    parent
	//	child
	//	  grandchild
	// Suppose our target index is child.
	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(end)
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
