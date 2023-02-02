// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"context"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/rowencpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

// This file contains facilities to encode primary and secondary
// indexes on SQL tables.

// MakeIndexKeyPrefix returns the key prefix used for the index's data. If you
// need the corresponding Span, prefer desc.IndexSpan(indexID) or
// desc.PrimaryIndexSpan().
func MakeIndexKeyPrefix(codec keys.SQLCodec, tableID descpb.ID, indexID descpb.IndexID) []byte {
	return codec.IndexPrefix(uint32(tableID), uint32(indexID))
}

// EncodeIndexKey creates a key by concatenating keyPrefix with the encodings of
// the index key columns, and returns the key and whether any of the encoded
// values were NULLs.
//
// Note that key suffix columns are not encoded, so the result isn't always a
// full index key.
func EncodeIndexKey(
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	colMap catalog.TableColMap,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	keyAndSuffixCols := tableDesc.IndexFetchSpecKeyAndSuffixColumns(index)
	keyCols := keyAndSuffixCols[:index.NumKeyColumns()]
	key, containsNull, err = EncodePartialIndexKey(
		keyCols,
		colMap,
		values,
		keyPrefix,
	)
	if err != nil {
		return nil, false, err
	}
	return key, containsNull, err
}

// EncodePartialIndexSpan creates the minimal key span for the key specified by the
// given table, index, and values, with the same method as
// EncodePartialIndexKey.
func EncodePartialIndexSpan(
	keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	colMap catalog.TableColMap,
	values []tree.Datum,
	keyPrefix []byte,
) (span roachpb.Span, containsNull bool, err error) {
	var key roachpb.Key
	key, containsNull, err = EncodePartialIndexKey(keyCols, colMap, values, keyPrefix)
	if err != nil {
		return span, false, err
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, containsNull, nil
}

// EncodePartialIndexKey encodes a partial index key; only the given key (or key
// suffix) columns are encoded; these can be a prefix of the index key columns.
// Does not directly append to keyPrefix.
func EncodePartialIndexKey(
	keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	colMap catalog.TableColMap,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, _ error) {
	// We know we will append to the key which will cause the capacity to grow so
	// make it bigger from the get-go.
	// Add the length of the key prefix as an initial guess.
	// Add 2 bytes for every column value. An underestimate for all but low integers.
	key = growKey(keyPrefix, len(keyPrefix)+2*len(values))

	for i := range keyCols {
		keyCol := &keyCols[i]
		val := findColumnValue(keyCol.ColumnID, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := catalogkeys.IndexColumnEncodingDirection(keyCol.Direction)
		if err != nil {
			return nil, false, err
		}

		if key, err = keyside.Encode(key, val, dir); err != nil {
			return nil, false, err
		}
	}
	return key, containsNull, nil
}

type directions []catenumpb.IndexColumn_Direction

func (d directions) get(i int) (encoding.Direction, error) {
	if i < len(d) {
		return catalogkeys.IndexColumnEncodingDirection(d[i])
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
func MakeSpanFromEncDatums(
	values EncDatumRow,
	keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	alloc *tree.DatumAlloc,
	keyPrefix []byte,
) (_ roachpb.Span, containsNull bool, _ error) {
	startKey, containsNull, err := MakeKeyFromEncDatums(values, keyCols, alloc, keyPrefix)
	if err != nil {
		return roachpb.Span{}, false, err
	}
	return roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}, containsNull, nil
}

// NeededColumnFamilyIDs returns the minimal set of column families required to
// retrieve neededCols for the specified table and index. The returned descpb.FamilyIDs
// are in sorted order.
func NeededColumnFamilyIDs(
	neededColOrdinals intsets.Fast, table catalog.TableDescriptor, index catalog.Index,
) []descpb.FamilyID {
	if table.NumFamilies() == 1 {
		return []descpb.FamilyID{table.GetFamilies()[0].ID}
	}

	// Build some necessary data structures for column metadata.
	columns := table.DeletableColumns()
	colIdxMap := catalog.ColumnIDToOrdinalMap(columns)
	var indexedCols intsets.Fast
	var compositeCols intsets.Fast
	var extraCols intsets.Fast
	for i := 0; i < index.NumKeyColumns(); i++ {
		columnID := index.GetKeyColumnID(i)
		columnOrdinal := colIdxMap.GetDefault(columnID)
		indexedCols.Add(columnOrdinal)
	}
	for i := 0; i < index.NumCompositeColumns(); i++ {
		columnID := index.GetCompositeColumnID(i)
		columnOrdinal := colIdxMap.GetDefault(columnID)
		compositeCols.Add(columnOrdinal)
	}
	for i := 0; i < index.NumKeySuffixColumns(); i++ {
		columnID := index.GetKeySuffixColumnID(i)
		columnOrdinal := colIdxMap.GetDefault(columnID)
		extraCols.Add(columnOrdinal)
	}

	// The column family with ID 0 is special because it always has a KV entry.
	// Other column families will omit a value if all their columns are null, so
	// we may need to retrieve family 0 to use as a sentinel for distinguishing
	// between null values and the absence of a row. Also, secondary indexes store
	// values here for composite and "extra" columns. ("Extra" means primary key
	// columns which are not indexed.)
	var family0 *descpb.ColumnFamilyDescriptor
	hasSecondaryEncoding := index.GetEncodingType() == catenumpb.SecondaryIndexEncoding

	// First iterate over the needed columns and look for a few special cases:
	// * columns which can be decoded from the key and columns whose value is stored
	//   in family 0.
	// * certain system columns, like the MVCC timestamp column require all of the
	//   column families to be scanned to produce a value.
	family0Needed := false
	mvccColumnRequested := false
	nc := neededColOrdinals.Copy()
	neededColOrdinals.ForEach(func(columnOrdinal int) {
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
		// System column ordinals are larger than the number of columns.
		if columnOrdinal >= len(columns) {
			mvccColumnRequested = true
		}
	})

	// If the MVCC timestamp column was requested, then bail out.
	if mvccColumnRequested {
		families := make([]descpb.FamilyID, 0, table.NumFamilies())
		_ = table.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
			families = append(families, family.ID)
			return nil
		})
		return families
	}

	secondaryStoredColumnIDs := index.CollectSecondaryStoredColumnIDs()

	// Iterate over the column families to find which ones contain needed columns.
	// We also keep track of whether all of the needed families' columns are
	// nullable, since this means we need column family 0 as a sentinel, even if
	// none of its columns are needed.
	var neededFamilyIDs []descpb.FamilyID
	allFamiliesNullable := true
	_ = table.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
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
			columnOrdinal := colIdxMap.GetDefault(columnID)
			if nc.Contains(columnOrdinal) {
				needed = true
			}
			if !columns[columnOrdinal].IsNullable() && !indexedCols.Contains(columnOrdinal) {
				// This column is non-nullable and is not indexed, thus, if it
				// is stored in the value part of the KV entry (which is the
				// case for the primary indexes as well as when the column is
				// included in STORING clause of the secondary index), the
				// column family is non-nullable too.
				//
				// Note that for unique secondary indexes more columns might be
				// included in the value part (namely "key suffix" columns when
				// the indexed columns have a NULL value), but we choose to
				// ignore those here. This is needed for correctness, and as a
				// result we might fetch the zeroth column family when it turns
				// out to be not needed.
				if index.Primary() || secondaryStoredColumnIDs.Contains(columnID) {
					nullable = false
				}
			}
		}
		if needed {
			neededFamilyIDs = append(neededFamilyIDs, family.ID)
			allFamiliesNullable = allFamiliesNullable && nullable
		}
		return nil
	})
	if family0 == nil {
		panic(errors.AssertionFailedf("column family 0 not found"))
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

// SplitRowKeyIntoFamilySpans splits a key representing a single row point
// lookup into separate disjoint spans that request only the particular column
// families from neededFamilies instead of requesting all the families. It is up
// to the client to ensure the requested span represents a single row lookup and
// that the span splitting is appropriate (see CanSplitSpanIntoFamilySpans).
//
// The returned spans might or might not have EndKeys set. If they are for a
// single key, they will not have EndKeys set.
//
// Note that this function will still return a family-specific span even if the
// input span is for a table that has just a single column family, so that the
// caller can have a precise key to send via a GetRequest if desired.
//
// The function accepts a slice of spans to append to.
func SplitRowKeyIntoFamilySpans(
	appendTo roachpb.Spans, key roachpb.Key, neededFamilies []descpb.FamilyID,
) roachpb.Spans {
	key = key[:len(key):len(key)] // avoid mutation and aliasing
	for i, familyID := range neededFamilies {
		var famSpan roachpb.Span
		famSpan.Key = keys.MakeFamilyKey(key, uint32(familyID))
		// Don't set the EndKey yet, because a column family on its own can be
		// fetched using a GetRequest.
		if i > 0 && familyID == neededFamilies[i-1]+1 {
			// This column family is adjacent to the previous one. We can merge
			// the two spans into one.
			appendTo[len(appendTo)-1].EndKey = famSpan.Key.PrefixEnd()
		} else {
			appendTo = append(appendTo, famSpan)
		}
	}
	return appendTo
}

// MakeKeyFromEncDatums creates an index key by concatenating keyPrefix with the
// encodings of the given EncDatum values.
func MakeKeyFromEncDatums(
	values EncDatumRow,
	keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	alloc *tree.DatumAlloc,
	keyPrefix []byte,
) (_ roachpb.Key, containsNull bool, _ error) {
	// Values may be a prefix of the index columns.
	if len(values) > len(keyCols) {
		return nil, false, errors.Errorf("%d values, %d key cols", len(values), len(keyCols))
	}
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key := make(roachpb.Key, len(keyPrefix), len(keyPrefix)*2)
	copy(key, keyPrefix)

	for i, val := range values {
		encoding := catenumpb.DatumEncoding_ASCENDING_KEY
		if keyCols[i].Direction == catenumpb.IndexColumn_DESC {
			encoding = catenumpb.DatumEncoding_DESCENDING_KEY
		}
		if val.IsNull() {
			containsNull = true
		}
		var err error
		key, err = val.Encode(keyCols[i].Type, alloc, encoding, key)
		if err != nil {
			return nil, false, err
		}
	}
	return key, containsNull, nil
}

// findColumnValue returns the value corresponding to the column. If
// the column isn't present return a NULL value.
func findColumnValue(
	column descpb.ColumnID, colMap catalog.TableColMap, values []tree.Datum,
) tree.Datum {
	if i, ok := colMap.Get(column); ok {
		// TODO(pmattis): Need to convert the values[i] value to the type
		// expected by the column.
		return values[i]
	}
	return tree.DNull
}

// DecodePartialTableIDIndexID decodes a table id followed by an index id. The
// input key must already have its tenant id removed.
func DecodePartialTableIDIndexID(key []byte) ([]byte, descpb.ID, descpb.IndexID, error) {
	key, tableID, indexID, err := keys.DecodeTableIDIndexID(key)
	return key, descpb.ID(tableID), descpb.IndexID(indexID), err
}

// DecodeIndexKeyPrefix decodes the prefix of an index key and returns the
// index id and a slice for the rest of the key.
//
// Don't use this function in the scan "hot path".
func DecodeIndexKeyPrefix(
	codec keys.SQLCodec, expectedTableID descpb.ID, key []byte,
) (indexID descpb.IndexID, remaining []byte, err error) {
	key, err = codec.StripTenantPrefix(key)
	if err != nil {
		return 0, nil, err
	}
	var tableID descpb.ID
	key, tableID, indexID, err = DecodePartialTableIDIndexID(key)
	if err != nil {
		return 0, nil, err
	}
	if tableID != expectedTableID {
		return 0, nil, errors.Errorf(
			"unexpected table ID %d, expected %d instead", tableID, expectedTableID)
	}
	return indexID, key, err
}

// DecodeIndexKey decodes the values that are a part of the specified index
// key (setting vals).
//
// The remaining bytes in the index key are returned which will either be an
// encoded column ID for the primary key index, the primary key suffix for
// non-unique secondary indexes or unique secondary indexes containing NULL or
// empty.
func DecodeIndexKey(
	codec keys.SQLCodec,
	types []*types.T,
	vals []EncDatum,
	colDirs []catenumpb.IndexColumn_Direction,
	key []byte,
) (remainingKey []byte, foundNull bool, _ error) {
	key, err := codec.StripTenantPrefix(key)
	if err != nil {
		return nil, false, err
	}
	key, _, _, err = DecodePartialTableIDIndexID(key)
	if err != nil {
		return nil, false, err
	}
	remainingKey, foundNull, err = DecodeKeyVals(types, vals, colDirs, key)
	if err != nil {
		return nil, false, err
	}
	return remainingKey, foundNull, nil
}

// DecodeKeyVals decodes the values that are part of the key. The decoded
// values are stored in the vals. If this slice is nil, the direction
// used will default to encoding.Ascending.
// DecodeKeyVals returns whether or not NULL was encountered in the key.
func DecodeKeyVals(
	types []*types.T, vals []EncDatum, directions []catenumpb.IndexColumn_Direction, key []byte,
) (remainingKey []byte, foundNull bool, _ error) {
	if directions != nil && len(directions) != len(vals) {
		return nil, false, errors.Errorf("encoding directions doesn't parallel vals: %d vs %d.",
			len(directions), len(vals))
	}
	for j := range vals {
		enc := catenumpb.DatumEncoding_ASCENDING_KEY
		if directions != nil && (directions[j] == catenumpb.IndexColumn_DESC) {
			enc = catenumpb.DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(types[j], enc, key)
		if err != nil {
			return nil, false, err
		}
		foundNull = foundNull || vals[j].IsNull()
	}
	return key, foundNull, nil
}

// DecodeKeyValsUsingSpec is a variant of DecodeKeyVals which uses
// fetchpb.IndexFetchSpec_KeyColumn for column metadata.
func DecodeKeyValsUsingSpec(
	keyCols []fetchpb.IndexFetchSpec_KeyColumn, key []byte, vals []EncDatum,
) (remainingKey []byte, foundNull bool, _ error) {
	for j := range vals {
		c := keyCols[j]
		enc := catenumpb.DatumEncoding_ASCENDING_KEY
		if c.Direction == catenumpb.IndexColumn_DESC {
			enc = catenumpb.DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(c.Type, enc, key)
		if err != nil {
			return nil, false, err
		}
		foundNull = foundNull || vals[j].IsNull()
	}
	return key, foundNull, nil
}

// IndexEntry represents an encoded key/value for an index entry.
type IndexEntry struct {
	Key   roachpb.Key
	Value roachpb.Value
	// Only used for forward indexes.
	Family descpb.FamilyID
}

// valueEncodedColumn represents a composite or stored column of a secondary
// index.
type valueEncodedColumn struct {
	id          descpb.ColumnID
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
	index catalog.Index, colMap catalog.TableColMap, values []tree.Datum, keyPrefix []byte,
) (key [][]byte, err error) {
	keyPrefix, err = EncodeInvertedIndexPrefixKeys(index, colMap, values, keyPrefix)
	if err != nil {
		return nil, err
	}

	var val tree.Datum
	if i, ok := colMap.Get(index.InvertedColumnID()); ok {
		val = values[i]
	} else {
		val = tree.DNull
	}
	indexGeoConfig := index.GetGeoConfig()
	if !indexGeoConfig.IsEmpty() {
		return EncodeGeoInvertedIndexTableKeys(val, keyPrefix, indexGeoConfig)
	}
	return EncodeInvertedIndexTableKeys(val, keyPrefix, index.GetVersion())
}

// EncodeInvertedIndexPrefixKeys encodes the non-inverted prefix columns if
// the given index is a multi-column inverted index.
func EncodeInvertedIndexPrefixKeys(
	index catalog.Index, colMap catalog.TableColMap, values []tree.Datum, keyPrefix []byte,
) (_ []byte, err error) {
	numColumns := index.NumKeyColumns()

	// If the index is a multi-column inverted index, we encode the non-inverted
	// columns in the key prefix.
	if numColumns > 1 {
		// Do not encode the last column, which is the inverted column, here. It
		// is encoded below this block.
		colIDs := index.IndexDesc().KeyColumnIDs[:numColumns-1]
		dirs := directions(index.IndexDesc().KeyColumnDirections)

		// Double the size of the key to make the imminent appends more
		// efficient.
		keyPrefix = growKey(keyPrefix, len(keyPrefix))

		keyPrefix, _, err = EncodeColumns(colIDs, dirs, colMap, values, keyPrefix)
		if err != nil {
			return nil, err
		}
	}
	return keyPrefix, nil
}

// EncodeInvertedIndexTableKeys produces one inverted index key per element in
// the input datum, which should be a container (either JSON or Array). For
// JSON, "element" means unique path through the document. Each output key is
// prefixed by inKey, and is guaranteed to be lexicographically sortable, but
// not guaranteed to be round-trippable during decoding. If the input Datum
// is (SQL) NULL, no inverted index keys will be produced, because inverted
// indexes cannot and do not need to satisfy the predicate col IS NULL.
//
// This function does not return keys for empty arrays or for NULL array
// elements unless the version is at least
// descpb.EmptyArraysInInvertedIndexesVersion. (Note that this only applies
// to arrays, not JSONs. This function returns keys for all non-null JSONs
// regardless of the version.)
func EncodeInvertedIndexTableKeys(
	val tree.Datum, inKey []byte, version descpb.IndexDescriptorVersion,
) (key [][]byte, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	// TODO(yuzefovich): can val ever be a placeholder?
	datum := tree.UnwrapDOidWrapper(val)
	switch val.ResolvedType().Family() {
	case types.JsonFamily:
		// We do not need to pass the version for JSON types, since all prior
		// versions of JSON inverted indexes include keys for empty objects and
		// arrays.
		return json.EncodeInvertedIndexKeys(inKey, val.(*tree.DJSON).JSON)
	case types.ArrayFamily:
		return encodeArrayInvertedIndexTableKeys(val.(*tree.DArray), inKey, version, false /* excludeNulls */)
	case types.StringFamily:
		// TODO(jordan): Right now, this is just trigram inverted indexes. If we
		// want to support different types of inverted indexes on strings, we'll
		// need to pass in the inverted index column kind to this function.
		// We pad the keys when writing them to the index.
		// TODO(jordan): why are we doing this padding at all? Postgres does it.
		// val could be a DOidWrapper, so we need to use the unwrapped datum
		// here.
		return encodeTrigramInvertedIndexTableKeys(string(*datum.(*tree.DString)), inKey, version, true /* pad */)
	case types.TSVectorFamily:
		return tsearch.EncodeInvertedIndexKeys(inKey, val.(*tree.DTSVector).TSVector)
	}
	return nil, errors.AssertionFailedf("trying to apply inverted index to unsupported type %s", datum.ResolvedType())
}

// EncodeContainingInvertedIndexSpans returns the spans that must be scanned in
// the inverted index to evaluate a contains (@>) predicate with the given
// datum, which should be a container (either JSON or Array). These spans
// should be used to find the objects in the index that contain the given json
// or array. In other words, if we have a predicate x @> y, this function
// should use the value of y to find the spans to scan in an inverted index on
// x.
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution. See
// comments in the SpanExpression definition for details.
func EncodeContainingInvertedIndexSpans(
	ctx context.Context, evalCtx *eval.Context, val tree.Datum,
) (invertedExpr inverted.Expression, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	datum := eval.UnwrapDatum(ctx, evalCtx, val)
	switch val.ResolvedType().Family() {
	case types.JsonFamily:
		return json.EncodeContainingInvertedIndexSpans(nil /* inKey */, val.(*tree.DJSON).JSON)
	case types.ArrayFamily:
		return encodeContainingArrayInvertedIndexSpans(val.(*tree.DArray), nil /* inKey */)
	default:
		return nil, errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", datum.ResolvedType(),
		)
	}
}

// EncodeContainedInvertedIndexSpans returns the spans that must be scanned in
// the inverted index to evaluate a contained by (<@) predicate with the given
// datum, which should be a container (either an Array or JSON). These spans
// should be used to find the objects in the index that could be contained by
// the given json or array. In other words, if we have a predicate x <@ y, this
// function should use the value of y to find the spans to scan in an inverted
// index on x.
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution. The
// span expression returned will never be tight. See comments in the
// SpanExpression definition for details.
func EncodeContainedInvertedIndexSpans(
	ctx context.Context, evalCtx *eval.Context, val tree.Datum,
) (invertedExpr inverted.Expression, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	datum := eval.UnwrapDatum(ctx, evalCtx, val)
	switch val.ResolvedType().Family() {
	case types.ArrayFamily:
		return encodeContainedArrayInvertedIndexSpans(val.(*tree.DArray), nil /* inKey */)
	case types.JsonFamily:
		return json.EncodeContainedInvertedIndexSpans(nil /* inKey */, val.(*tree.DJSON).JSON)
	default:
		return nil, errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", datum.ResolvedType(),
		)
	}
}

// EncodeExistsInvertedIndexSpans returns the spans that must be scanned in
// the inverted index to evaluate an exists (?) predicate with the given
// string datum. These spans should be used to find the objects in the index
// that have the string datum as a top-level key.
//
// If val is an array, then the inverted expression is a conjunction if all is
// true, and a disjunction otherwise.
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution.
func EncodeExistsInvertedIndexSpans(
	ctx context.Context, evalCtx *eval.Context, val tree.Datum, all bool,
) (invertedExpr inverted.Expression, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	datum := eval.UnwrapDatum(ctx, evalCtx, val)
	switch val.ResolvedType().Family() {
	case types.StringFamily:
		// val could be a DOidWrapper, so we need to use the unwrapped datum
		// here.
		s := string(*datum.(*tree.DString))
		return json.EncodeExistsInvertedIndexSpans(nil /* inKey */, s)
	case types.ArrayFamily:
		if val.ResolvedType().ArrayContents().Family() != types.StringFamily {
			return nil, errors.AssertionFailedf(
				"trying to apply inverted index to unsupported type %s", datum.ResolvedType(),
			)
		}
		var expr inverted.Expression
		for _, d := range val.(*tree.DArray).Array {
			s := string(*d.(*tree.DString))
			newExpr, err := json.EncodeExistsInvertedIndexSpans(nil /* inKey */, s)
			if err != nil {
				return nil, err
			}
			if expr == nil {
				expr = newExpr
			} else if all {
				expr = inverted.And(expr, newExpr)
			} else {
				expr = inverted.Or(expr, newExpr)
			}
		}
		return expr, nil
	default:
		return nil, errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", datum.ResolvedType(),
		)
	}
}

// EncodeOverlapsInvertedIndexSpans returns the spans that must be scanned in
// the inverted index to evaluate an overlaps (&&) predicate with the given
// datum, which should be an Array. These spans should be used to find the
// objects in the index that could overlap with the given array. In other
// words, if we have a predicate x && y, this function should use the value of
// y to find the spans to scan in an inverted index on x.
//
// The spans are returned in an inverted.SpanExpression, which represents the
// set operations that must be applied on the spans read during execution. The
// span expression returned will be tight. See comments in the
// SpanExpression definition for details.
func EncodeOverlapsInvertedIndexSpans(
	ctx context.Context, evalCtx *eval.Context, val tree.Datum,
) (invertedExpr inverted.Expression, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	datum := eval.UnwrapDatum(ctx, evalCtx, val)
	switch val.ResolvedType().Family() {
	case types.ArrayFamily:
		return encodeOverlapsArrayInvertedIndexSpans(val.(*tree.DArray), nil /* inKey */)
	default:
		return nil, errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", datum.ResolvedType(),
		)
	}
}

// encodeArrayInvertedIndexTableKeys returns a list of inverted index keys for
// the given input array, one per entry in the array. The input inKey is
// prefixed to all returned keys.
//
// This function does not return keys for empty arrays or for NULL array elements
// unless the version is at least descpb.EmptyArraysInInvertedIndexesVersion.
// It also does not return keys for NULL array elements if excludeNulls is
// true. This option is used by encodeContainedArrayInvertedIndexSpans, which
// builds index spans to evaluate <@ (contained by) expressions.
func encodeArrayInvertedIndexTableKeys(
	val *tree.DArray, inKey []byte, version descpb.IndexDescriptorVersion, excludeNulls bool,
) (key [][]byte, err error) {
	if val.Array.Len() == 0 {
		if version >= descpb.EmptyArraysInInvertedIndexesVersion {
			return [][]byte{encoding.EncodeEmptyArray(inKey)}, nil
		}
	}

	outKeys := make([][]byte, 0, len(val.Array))
	for i := range val.Array {
		d := val.Array[i]
		if d == tree.DNull && (version < descpb.EmptyArraysInInvertedIndexesVersion || excludeNulls) {
			// Older versions did not include null elements, but we must include them
			// going forward since `SELECT ARRAY[NULL] @> ARRAY[]` returns true.
			continue
		}
		outKey := make([]byte, len(inKey))
		copy(outKey, inKey)
		newKey, err := keyside.Encode(outKey, d, encoding.Ascending)
		if err != nil {
			return nil, err
		}
		outKeys = append(outKeys, newKey)
	}
	outKeys = unique.UniquifyByteSlices(outKeys)
	return outKeys, nil
}

// encodeContainingArrayInvertedIndexSpans returns the spans that must be
// scanned in the inverted index to evaluate a contains (@>) predicate with
// the given array, one slice of spans per entry in the array. The input
// inKey is prefixed to all returned keys.
func encodeContainingArrayInvertedIndexSpans(
	val *tree.DArray, inKey []byte,
) (invertedExpr inverted.Expression, err error) {
	if val.Array.Len() == 0 {
		// All arrays contain the empty array. Return a SpanExpression that
		// requires a full scan of the inverted index.
		invertedExpr = inverted.ExprForSpan(
			inverted.MakeSingleValSpan(inKey), true, /* tight */
		)
		return invertedExpr, nil
	}

	if val.HasNulls {
		// If there are any nulls, return empty spans. This is needed to ensure
		// that `SELECT ARRAY[NULL, 2] @> ARRAY[NULL, 2]` is false.
		return &inverted.SpanExpression{Tight: true, Unique: true}, nil
	}

	keys, err := encodeArrayInvertedIndexTableKeys(val, inKey, descpb.LatestIndexDescriptorVersion, false /* excludeNulls */)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		spanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(key), true, /* tight */
		)
		spanExpr.Unique = true
		if invertedExpr == nil {
			invertedExpr = spanExpr
		} else {
			invertedExpr = inverted.And(invertedExpr, spanExpr)
		}
	}
	return invertedExpr, nil
}

// encodeContainedArrayInvertedIndexSpans returns the spans that must be
// scanned in the inverted index to evaluate a contained by (<@) predicate with
// the given array, one slice of spans per entry in the array. The input
// inKey is prefixed to all returned keys.
func encodeContainedArrayInvertedIndexSpans(
	val *tree.DArray, inKey []byte,
) (invertedExpr inverted.Expression, err error) {
	// The empty array should always be added to the spans, since it is contained
	// by everything.
	emptyArrSpanExpr := inverted.ExprForSpan(
		inverted.MakeSingleValSpan(encoding.EncodeEmptyArray(inKey)), false, /* tight */
	)
	emptyArrSpanExpr.Unique = true

	// If the given array is empty, we return the SpanExpression.
	if val.Array.Len() == 0 {
		return emptyArrSpanExpr, nil
	}

	// We always exclude nulls from the list of keys when evaluating <@.
	// This is because an expression like ARRAY[NULL] <@ ARRAY[NULL] is false,
	// since NULL in SQL represents an unknown value.
	keys, err := encodeArrayInvertedIndexTableKeys(val, inKey, descpb.LatestIndexDescriptorVersion, true /* excludeNulls */)
	if err != nil {
		return nil, err
	}
	invertedExpr = emptyArrSpanExpr
	for _, key := range keys {
		spanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(key), false, /* tight */
		)
		invertedExpr = inverted.Or(invertedExpr, spanExpr)
	}

	// The inverted expression produced for <@ will never be tight.
	// For example, if we are evaluating if indexed column x <@ ARRAY[1], the
	// inverted expression would scan for all arrays in x that contain the
	// empty array or ARRAY[1]. The resulting arrays could contain other values
	// and would need to be passed through an additional filter. For example,
	// ARRAY[1, 2, 3] would be returned by the scan, but it should be filtered
	// out since ARRAY[1, 2, 3] <@ ARRAY[1] is false.
	invertedExpr.SetNotTight()
	return invertedExpr, nil
}

// encodeOverlapsArrayInvertedIndexSpans returns the spans that must be
// scanned in the inverted index to evaluate an overlaps (&&) predicate with
// the given array, one slice of spans per entry in the array. The input
// inKey is prefixed to all returned keys.
func encodeOverlapsArrayInvertedIndexSpans(
	val *tree.DArray, inKey []byte,
) (invertedExpr inverted.Expression, err error) {
	// If the given array is directly empty (i.e. Len == 0),
	// or contains only NULLs and thus has effective length 0,
	// we cannot generate an inverted expression.

	// TODO: This should be a contradiction which is treated as a no-op.
	if val.Array.Len() == 0 || !val.HasNonNulls {
		return inverted.NonInvertedColExpression{}, nil
	}

	// We always exclude nulls from the list of keys when evaluating &&.
	// This is because an expression like ARRAY[NULL] && ARRAY[NULL] is false,
	// since NULL in SQL represents an unknown value.
	keys, err := encodeArrayInvertedIndexTableKeys(val, inKey, descpb.PrimaryIndexWithStoredColumnsVersion, true /* excludeNulls */)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		spanExpr := inverted.ExprForSpan(
			inverted.MakeSingleValSpan(key), true, /* tight */
		)
		spanExpr.Unique = true
		if invertedExpr == nil {
			invertedExpr = spanExpr
		} else {
			invertedExpr = inverted.Or(invertedExpr, spanExpr)
		}
	}
	return invertedExpr, nil
}

// EncodeTrigramSpans returns the spans that must be scanned to look up trigrams
// present in the input string. If allMustMatch is true, the resultant inverted
// expression must match every trigram in the input. Otherwise, it will match
// any trigram in the input.
func EncodeTrigramSpans(s string, allMustMatch bool) (inverted.Expression, error) {
	// We do not pad the trigrams when allMustMatch is true. To see why, observe
	// the keys that we insert for a string "zfooz":
	//
	// "  z", " zf", "zfo", "foo", "foz", "oz "
	//
	// If we were then searching for the string %foo%, and we padded the output
	// keys as well, we'd be searching for the key "  f", which doesn't exist
	// in the index for zfooz, even though zfooz is like %foo%.
	keys, err := encodeTrigramInvertedIndexTableKeys(s, nil, /* inKey */
		descpb.LatestIndexDescriptorVersion, !allMustMatch /* pad */)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("no trigrams available to search with")
	}

	var ret inverted.Expression
	for _, key := range keys {
		spanExpr := inverted.ExprForSpan(inverted.MakeSingleValSpan(key), false /* tight */)
		if ret == nil {
			// The first trigram (and only the first trigram) is unique.
			// TODO(jordan): we *could* make this first expression tight if we knew
			// for sure that the expression is something like `LIKE '%foo%'`. In this
			// case, we're sure that the returned row will pass the predicate because
			// the LIKE operator has wildcards on either side of the trigram. But
			// this is such a marginal case that it doesn't seem worth it to plumb
			// in this special case. For all other single-trigram cases, such as
			// `LIKE '%foo'` or `= 'foo'`, we don't have a tight span.
			spanExpr.Unique = true
			ret = spanExpr
		} else {
			// As soon as we have more than one trigram to search for, we no longer
			// have a unique expression, since two separate trigrams could both
			// point at a single row. We also no longer have a tight expression,
			// because the trigrams that we're checking don't necessarily have to
			// be in the right order within the string to guarantee that just because
			// both trigrams match, the strings pass the LIKE or % test.
			if allMustMatch {
				ret = inverted.And(ret, spanExpr)
			} else {
				ret = inverted.Or(ret, spanExpr)
			}
		}
	}
	return ret, nil
}

// EncodeGeoInvertedIndexTableKeys is the equivalent of EncodeInvertedIndexTableKeys
// for Geography and Geometry.
func EncodeGeoInvertedIndexTableKeys(
	val tree.Datum, inKey []byte, indexGeoConfig geoindex.Config,
) (key [][]byte, err error) {
	if val == tree.DNull {
		return nil, nil
	}
	switch val.ResolvedType().Family() {
	case types.GeographyFamily:
		index := geoindex.NewS2GeographyIndex(*indexGeoConfig.S2Geography)
		intKeys, bbox, err := index.InvertedIndexKeys(context.TODO(), val.(*tree.DGeography).Geography)
		if err != nil {
			return nil, err
		}
		return encodeGeoKeys(encoding.EncodeGeoInvertedAscending(inKey), intKeys, bbox)
	case types.GeometryFamily:
		index := geoindex.NewS2GeometryIndex(*indexGeoConfig.S2Geometry)
		intKeys, bbox, err := index.InvertedIndexKeys(context.TODO(), val.(*tree.DGeometry).Geometry)
		if err != nil {
			return nil, err
		}
		return encodeGeoKeys(encoding.EncodeGeoInvertedAscending(inKey), intKeys, bbox)
	default:
		return nil, errors.Errorf("internal error: unexpected type: %s", val.ResolvedType().Family())
	}
}

func encodeGeoKeys(
	inKey []byte, geoKeys []geoindex.Key, bbox geopb.BoundingBox,
) (keys [][]byte, err error) {
	encodedBBox := make([]byte, 0, encoding.MaxGeoInvertedBBoxLen)
	encodedBBox = encoding.EncodeGeoInvertedBBox(encodedBBox, bbox.LoX, bbox.LoY, bbox.HiX, bbox.HiY)
	// Avoid per-key heap allocations.
	b := make([]byte, 0, len(geoKeys)*(len(inKey)+encoding.MaxVarintLen+len(encodedBBox)))
	keys = make([][]byte, len(geoKeys))
	for i, k := range geoKeys {
		prev := len(b)
		b = append(b, inKey...)
		b = encoding.EncodeUvarintAscending(b, uint64(k))
		b = append(b, encodedBBox...)
		// Set capacity so that the caller appending does not corrupt later keys.
		newKey := b[prev:len(b):len(b)]
		keys[i] = newKey
	}
	return keys, nil
}

// encodeTrigramInvertedIndexTableKeys produces the trigram index table keys for
// an input string. If pad is true, the returned table keys will include 3 extra
// trigrams produced by padding the string with 2 spaces at the front and 1 at
// the end.
func encodeTrigramInvertedIndexTableKeys(
	val string, inKey []byte, _ descpb.IndexDescriptorVersion, pad bool,
) ([][]byte, error) {
	trigrams := trigram.MakeTrigrams(val, pad)
	outKeys := make([][]byte, len(trigrams))
	for i := range trigrams {
		// Make sure to copy inKey into a new byte slice to avoid aliasing.
		inKeyLen := len(inKey)
		// Pre-size the outkey - we know we're going to encode the trigram plus 2
		// extra bytes for the prefix and terminator.
		outKey := make([]byte, inKeyLen, inKeyLen+len(trigrams[i])+2)
		copy(outKey, inKey)
		newKey := encoding.EncodeStringAscending(outKey, trigrams[i])
		outKeys[i] = newKey
	}
	return outKeys, nil
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
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	colMap catalog.TableColMap,
	values []tree.Datum,
	includeEmpty bool,
) ([]IndexEntry, error) {
	keyPrefix := MakeIndexKeyPrefix(codec, tableDesc.GetID(), index.GetID())
	indexKey, containsNull, err := EncodeIndexKey(tableDesc, index, colMap, values, keyPrefix)
	if err != nil {
		return nil, err
	}
	if containsNull {
		return nil, MakeNullPKError(tableDesc, index, colMap, values)
	}

	storedColumns := getStoredColumnsForPrimaryIndex(index, colMap)

	var entryValue []byte
	indexEntries := make([]IndexEntry, 0, tableDesc.NumFamilies())
	var columnsToEncode []valueEncodedColumn
	var called bool
	if err := tableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		if !called {
			called = true
		} else {
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
				col, err := catalog.MustFindColumnByID(tableDesc, family.DefaultColumnID)
				if err != nil {
					return err
				}
				value, err := valueside.MarshalLegacy(col.GetType(), datum)
				if err != nil {
					return err
				}
				indexEntries = append(indexEntries, IndexEntry{Key: familyKey, Value: value, Family: family.ID})
			}
			return nil
		}

		for _, colID := range family.ColumnIDs {
			if storedColumns.Contains(colID) {
				columnsToEncode = append(columnsToEncode, valueEncodedColumn{id: colID})
				continue
			}
			if cdatum, ok := values[colMap.GetDefault(colID)].(tree.CompositeDatum); ok {
				if cdatum.IsComposite() {
					columnsToEncode = append(columnsToEncode, valueEncodedColumn{id: colID, isComposite: true})
					continue
				}
			}
		}
		sort.Sort(byID(columnsToEncode))
		entryValue, err = writeColumnValues(entryValue, colMap, values, columnsToEncode)
		if err != nil {
			return err
		}
		if family.ID != 0 && len(entryValue) == 0 && !includeEmpty {
			return nil
		}
		entry := IndexEntry{Key: familyKey, Family: family.ID}
		entry.Value.SetTuple(entryValue)
		indexEntries = append(indexEntries, entry)
		return nil
	}); err != nil {
		return nil, err
	}

	if index.UseDeletePreservingEncoding() {
		if err := wrapIndexEntries(indexEntries); err != nil {
			return nil, err
		}
	}

	return indexEntries, nil
}

// getStoredColumnsForPrimaryIndex computes the set of columns stored in this
// primary index's value for encoding. Note that EncodePrimaryIndex will utilize
// this set to construct the value, but will augment this with the set of
// key columns which are composite encoded; this is just the set of columns
// stored in the primary index value which are not featured in the index key
// whatsoever.
//
// colMap is expected to include all columns in the table.
func getStoredColumnsForPrimaryIndex(
	index catalog.Index, colMap catalog.TableColMap,
) catalog.TableColSet {

	// It should be rare to never that we come across an index which is encoded
	// as a primary index but with a version older than this version.
	// Nevertheless, for safety, we assume at that version that the stored
	// columns set is not populated, and instead we defer to the colMap to
	// compute the complete set before subtracting the key columns.
	if index.GetVersion() < descpb.PrimaryIndexWithStoredColumnsVersion {
		var allColumn catalog.TableColSet
		colMap.ForEach(func(colID descpb.ColumnID, _ int) {
			allColumn.Add(colID)
		})
		return allColumn.Difference(index.CollectKeyColumnIDs())
	}

	// Note that the definition of Primary according to the catalog.Index method
	// is that the index is installed as the primary index of the table, not
	// that it has a primary index encoding. We must call the appropriate
	// method based on this distinction to get the desired set of columns.
	if !index.Primary() {
		return index.CollectSecondaryStoredColumnIDs()
	}
	return index.CollectPrimaryStoredColumnIDs()
}

// MakeNullPKError generates an error when the value for a primary key column is
// null.
func MakeNullPKError(
	table catalog.TableDescriptor,
	index catalog.Index,
	colMap catalog.TableColMap,
	values []tree.Datum,
) error {
	for _, col := range table.IndexKeyColumns(index) {
		ord, ok := colMap.Get(col.GetID())
		if !ok || values[ord] == tree.DNull {
			return sqlerrors.NewNonNullViolationError(col.GetName())
		}
	}
	return errors.AssertionFailedf("NULL value in unknown key column")
}

// EncodeSecondaryIndex encodes key/values for a secondary
// index. colMap maps descpb.ColumnIDs to indices in `values`. This returns a
// slice of IndexEntry. includeEmpty controls whether or not
// EncodeSecondaryIndex should return k/v's that contain
// empty values. For forward indexes the returned list of
// index entries is in family sorted order.
func EncodeSecondaryIndex(
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	secondaryIndex catalog.Index,
	colMap catalog.TableColMap,
	values []tree.Datum,
	includeEmpty bool,
) ([]IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc.GetID(), secondaryIndex.GetID())

	// Use the primary key encoding for covering indexes.
	if secondaryIndex.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
		return EncodePrimaryIndex(codec, tableDesc, secondaryIndex, colMap, values, includeEmpty)
	}

	var containsNull = false
	var secondaryKeys [][]byte
	var err error
	if secondaryIndex.GetType() == descpb.IndexDescriptor_INVERTED {
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
	extraKey, _, err := EncodeColumns(
		secondaryIndex.IndexDesc().KeySuffixColumnIDs,
		nil, /* directions */
		colMap,
		values,
		nil, /* keyPrefix */
	)
	if err != nil {
		return []IndexEntry{}, err
	}

	// entries is the resulting array that we will return. We allocate upfront at least
	// len(secondaryKeys) positions to avoid allocations from appending.
	entries := make([]IndexEntry, 0, len(secondaryKeys))
	for _, key := range secondaryKeys {
		if !secondaryIndex.IsUnique() || containsNull {
			// If the index is not unique or it contains a NULL value, append
			// extraKey to the key in order to make it unique.
			key = append(key, extraKey...)
		}

		if tableDesc.NumFamilies() == 1 ||
			secondaryIndex.GetType() == descpb.IndexDescriptor_INVERTED ||
			secondaryIndex.GetVersion() == descpb.BaseIndexFormatVersion {
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
			familyToColumns := make(map[descpb.FamilyID][]valueEncodedColumn)
			addToFamilyColMap := func(id descpb.FamilyID, column valueEncodedColumn) {
				if _, ok := familyToColumns[id]; !ok {
					familyToColumns[id] = []valueEncodedColumn{}
				}
				familyToColumns[id] = append(familyToColumns[id], column)
			}
			// Ensure that column family 0 always generates a k/v pair.
			familyToColumns[0] = []valueEncodedColumn{}
			// All composite columns are stored in family 0.
			for i := 0; i < secondaryIndex.NumCompositeColumns(); i++ {
				id := secondaryIndex.GetCompositeColumnID(i)
				addToFamilyColMap(0, valueEncodedColumn{id: id, isComposite: true})
			}
			_ = tableDesc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
				for i := 0; i < secondaryIndex.NumSecondaryStoredColumns(); i++ {
					id := secondaryIndex.GetStoredColumnID(i)
					for _, col := range family.ColumnIDs {
						if id == col {
							addToFamilyColMap(family.ID, valueEncodedColumn{id: id, isComposite: false})
						}
					}
				}
				return nil
			})
			entries, err = encodeSecondaryIndexWithFamilies(
				familyToColumns, secondaryIndex, colMap, key, values, extraKey, entries, includeEmpty)
			if err != nil {
				return []IndexEntry{}, err
			}
		}
	}

	if secondaryIndex.UseDeletePreservingEncoding() {
		if err := wrapIndexEntries(entries); err != nil {
			return nil, err
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
	familyMap map[descpb.FamilyID][]valueEncodedColumn,
	index catalog.Index,
	colMap catalog.TableColMap,
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
		storedColsInFam := familyMap[descpb.FamilyID(familyID)]
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
		if index.IsUnique() && familyID == 0 {
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
		entry := IndexEntry{Key: key, Family: descpb.FamilyID(familyID)}
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
	index catalog.Index,
	colMap catalog.TableColMap,
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
	if index.IsUnique() {
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
	for i := 0; i < index.NumSecondaryStoredColumns(); i++ {
		id := index.GetStoredColumnID(i)
		cols = append(cols, valueEncodedColumn{id: id, isComposite: false})
	}
	for i := 0; i < index.NumCompositeColumns(); i++ {
		id := index.GetCompositeColumnID(i)
		// Inverted indexes on a composite type (i.e. an array of composite types)
		// should not add the indexed column to the value.
		if index.GetType() == descpb.IndexDescriptor_INVERTED && id == index.GetKeyColumnID(0) {
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
	value []byte, colMap catalog.TableColMap, row []tree.Datum, columns []valueEncodedColumn,
) ([]byte, error) {
	var lastColID descpb.ColumnID
	for _, col := range columns {
		val := findColumnValue(col.id, colMap, row)
		if val == tree.DNull || (col.isComposite && !val.(tree.CompositeDatum).IsComposite()) {
			continue
		}
		colIDDelta := valueside.MakeColumnIDDelta(lastColID, col.id)
		lastColID = col.id
		var err error
		value, err = valueside.Encode(value, colIDDelta, val, nil)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps descpb.ColumnIDs to indices in `values`. secondaryIndexEntries is the return
// value (passed as a parameter so the caller can reuse between rows) and is
// expected to be the same length as indexes.
func EncodeSecondaryIndexes(
	ctx context.Context,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	colMap catalog.TableColMap,
	values []tree.Datum,
	secondaryIndexEntries []IndexEntry,
	includeEmpty bool,
	indexBoundAccount *mon.BoundAccount,
) ([]IndexEntry, int64, error) {
	var memUsedEncodingSecondaryIdxs int64
	if len(secondaryIndexEntries) > 0 {
		panic(errors.AssertionFailedf("length of secondaryIndexEntries was non-zero"))
	}

	if indexBoundAccount == nil || indexBoundAccount.Monitor() == nil {
		panic(errors.AssertionFailedf("memory monitor passed to EncodeSecondaryIndexes was nil"))
	}
	const sizeOfIndexEntry = int64(unsafe.Sizeof(IndexEntry{}))

	for i := range indexes {
		entries, err := EncodeSecondaryIndex(codec, tableDesc, indexes[i], colMap, values, includeEmpty)
		if err != nil {
			return secondaryIndexEntries, 0, err
		}
		// Normally, each index will have exactly one entry. However, inverted
		// indexes can have 0 or >1 entries, as well as secondary indexes which
		// store columns from multiple column families.
		//
		// The memory monitor has already accounted for cap(secondaryIndexEntries).
		// If the number of index entries are going to cause the
		// secondaryIndexEntries buffer to re-slice, then it will very likely double
		// in capacity. Therefore, we must account for another
		// cap(secondaryIndexEntries) in the index memory account.
		if cap(secondaryIndexEntries)-len(secondaryIndexEntries) < len(entries) {
			resliceSize := sizeOfIndexEntry * int64(cap(secondaryIndexEntries))
			if err := indexBoundAccount.Grow(ctx, resliceSize); err != nil {
				return nil, 0, errors.Wrap(err,
					"failed to re-slice index entries buffer")
			}
			memUsedEncodingSecondaryIdxs += resliceSize
		}

		// The index keys can be large and so we must account for them in the index
		// memory account.
		// In some cases eg: STORING indexes, the size of the value can also be
		// non-trivial.
		for _, index := range entries {
			if err := indexBoundAccount.Grow(ctx, int64(len(index.Key))); err != nil {
				return nil, 0, errors.Wrap(err, "failed to allocate space for index keys")
			}
			memUsedEncodingSecondaryIdxs += int64(len(index.Key))
			if err := indexBoundAccount.Grow(ctx, int64(len(index.Value.RawBytes))); err != nil {
				return nil, 0, errors.Wrap(err, "failed to allocate space for index values")
			}
			memUsedEncodingSecondaryIdxs += int64(len(index.Value.RawBytes))
		}

		secondaryIndexEntries = append(secondaryIndexEntries, entries...)
	}
	return secondaryIndexEntries, memUsedEncodingSecondaryIdxs, nil
}

// EncodeColumns is a version of EncodePartialIndexKey that takes KeyColumnIDs
// and directions explicitly. WARNING: unlike EncodePartialIndexKey,
// EncodeColumns appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []descpb.ColumnID,
	directions directions,
	colMap catalog.TableColMap,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, false, err
		}

		if key, err = keyside.Encode(key, val, dir); err != nil {
			return nil, false, err
		}
	}
	return key, containsNull, nil
}

// growKey returns a new key with  the same contents as the given key and with
// additionalCapacity more capacity.
func growKey(key []byte, additionalCapacity int) []byte {
	newKey := make([]byte, len(key), len(key)+additionalCapacity)
	copy(newKey, key)
	return newKey
}

func getIndexValueWrapperBytes(entry *IndexEntry) ([]byte, error) {
	var value []byte
	if entry.Value.IsPresent() {
		value = entry.Value.TagAndDataBytes()
	}
	tempKV := rowencpb.IndexValueWrapper{
		Value:   value,
		Deleted: false,
	}

	return protoutil.Marshal(&tempKV)
}

func wrapIndexEntries(indexEntries []IndexEntry) error {
	for i := range indexEntries {
		encodedEntry, err := getIndexValueWrapperBytes(&indexEntries[i])
		if err != nil {
			return err
		}

		indexEntries[i].Value.SetBytes(encodedEntry)
	}

	return nil
}

// DecodeWrapper decodes the bytes field of value into an instance of
// rowencpb.IndexValueWrapper.
func DecodeWrapper(value *roachpb.Value) (*rowencpb.IndexValueWrapper, error) {
	var wrapper rowencpb.IndexValueWrapper

	valueBytes, err := value.GetBytes()
	if err != nil {
		return nil, err
	}

	if err := protoutil.Unmarshal(valueBytes, &wrapper); err != nil {
		return nil, err
	}

	return &wrapper, nil
}
