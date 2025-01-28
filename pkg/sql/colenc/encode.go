// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc

import (
	"context"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

var ErrOverMemLimit = errors.New("memory limit exceeded")

// BatchEncoder encodes KV data to a row.Putter (wrapper interface around
// kv.Batch) using a coldata.Batch as the source.
type BatchEncoder struct {
	// RowHelper is a a utility class for doing mutations
	rh *row.RowHelper
	// The batch of data we want to encode.
	b coldata.Batch
	// The destination for KVs.
	p row.Putter
	// The map of columns in the input batch to column ids on the table.
	colMap catalog.TableColMap
	// Map of index id to a slice of bools that contain partial index predicates.
	partialIndexes map[descpb.IndexID][]bool
	// Column ID that might contain composite encoded values.
	compositeColumnIDs intsets.Fast
	// Cache of lastColID to support column delta encoding.
	lastColIDs []catid.ColumnID

	// Slice of keys we can reuse across each call to Prepare and between each
	// column family.
	keys []roachpb.Key
	// Slice of keys prefixes so we don't have to re-encode PK for each family.
	savedPrefixes []roachpb.Key
	// Slice of value we can reuse across each call to Prepare and between each
	// column family.
	values [][]byte
	// Extra keys are the keySuffix columns that may be included in key or value.
	extraKeys [][]byte
	// Sizes of allocations to make for big buffer.
	keyBufSize, extraKeysBufSize, valBufSize int
	keyPrefixOffsets                         []int32
	// start and end refer to the portion of the coldata.Batch we are encoding,
	// count is just end - start.
	start, end, count int
	// memoryUsageCheck is provided externally and should return ErrOverMemLimit
	// if memory usage limit has been exceeded.
	memoryUsageCheck func() error
}

func MakeEncoder(
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	sv *settings.Values,
	b coldata.Batch,
	insCols []catalog.Column,
	metrics *rowinfra.Metrics,
	partialIndexes map[descpb.IndexID][]bool,
	memoryUsageCheck func() error,
) BatchEncoder {
	rh := row.NewRowHelper(codec, desc, desc.WritableNonPrimaryIndexes(), nil /* uniqueWithTombstoneIndexes */, sv, false /*internal*/, metrics)
	rh.Init()
	colMap := row.ColIDtoRowIndexFromCols(insCols)
	return BatchEncoder{rh: &rh, b: b, colMap: colMap,
		partialIndexes: partialIndexes, memoryUsageCheck: memoryUsageCheck}
}

// PrepareBatch encodes a subset of rows from the batch to the given row.Putter.
// The maximum batch size is 64k (determined by NewMemBatchNoCols).
func (b *BatchEncoder) PrepareBatch(ctx context.Context, p row.Putter, start, end int) error {
	if err := row.CheckPrimaryKeyColumns(b.rh.TableDesc, b.colMap); err != nil {
		return err
	}
	if err := b.checkNotNullColumns(); err != nil {
		return err
	}
	b.p = p
	if start >= end {
		colexecerror.InternalError(errors.AssertionFailedf("PrepareBatch illegal arguments: start=%d,end=%d", start, end))
	}
	b.start = start
	b.end = end
	b.count = end - start
	if b.count > math.MaxUint16 {
		colexecerror.InternalError(errors.AssertionFailedf(`PrepareBatch illegel arguments: num rows larger than %d; requested %d`, math.MaxUint16, b.count))
	}
	b.init()
	if err := b.encodePK(ctx, b.rh.TableDesc.GetPrimaryIndex()); err != nil {
		return err
	}
	for _, ind := range b.rh.TableDesc.WritableNonPrimaryIndexes() {
		b.resetBuffers()
		// TODO(cucaroach): COPY doesn't need ForcePut support but the encoder
		// will need to support it eventually.
		if ind.ForcePut() {
			colexecerror.InternalError(errors.AssertionFailedf("vector encoder doesn't support ForcePut yet"))
		}
		if err := b.encodeSecondaryIndex(ctx, ind); err != nil {
			return err
		}
	}
	return nil
}

// Check that all non-nullable columns are present. The row engine does this in
// sql.enforceLocalColumnConstraints. Actual row values are checked in
// encodePK as we go.
func (b *BatchEncoder) checkNotNullColumns() error {
	for _, c := range b.rh.TableDesc.PublicColumns() {
		if !c.IsNullable() {
			if _, ok := b.colMap.Get(c.GetID()); !ok {
				return sqlerrors.NewNonNullViolationError(c.GetName())
			}
		}
	}
	return nil
}

func partitionSlices[T []byte | roachpb.Key](buffer []byte, slices []T) {
	// Initialize buffers sliced from big buffer.
	siz := len(buffer) / len(slices)
	if siz*len(slices) != len(buffer) {
		colexecerror.InternalError(errors.AssertionFailedf("buffer must be evenly divisible by num slices, %d / %d", len(buffer), len(slices)))
	}
	for i := 0; i < len(slices); i++ {
		offset := i * siz
		slices[i] = buffer[offset : offset : siz+offset]
	}
}

// Amount of space to reserve per value.
const perColBytes = 5

func (b *BatchEncoder) init() {
	if b.count > cap(b.keys) {
		b.keyBufSize = perColBytes * b.getMaxKeyCols()
		b.valBufSize = perColBytes * len(b.rh.TableDesc.PublicColumns())
		b.extraKeysBufSize = perColBytes * b.getMaxSuffixColumns()

		b.keys = make([]roachpb.Key, b.count)
		b.values = make([][]byte, b.count)
		b.extraKeys = make([][]byte, b.count)
		b.lastColIDs = make([]catid.ColumnID, b.count)

		valBuf := make([]byte, b.count*b.valBufSize)
		extraBuf := make([]byte, b.count*b.extraKeysBufSize)

		partitionSlices(valBuf, b.values)
		partitionSlices(extraBuf, b.extraKeys)

		// The slices above are re-used across each index but the key buffer data
		// has to be re-allocated (kv.Batch has references to the
		// keys) but the values are copied and allocated so we can
		// re-use across batches.
		b.resetBuffers()

		// Store the index up to the family id so we can reuse the prefixes.
		if len(b.rh.TableDesc.GetFamilies()) > 1 {
			b.keyPrefixOffsets = make([]int32, b.count)
		}
	} else {
		// If we have fewer rows shrink slices.
		b.keys = b.keys[:b.count]
		b.values = b.values[:b.count]
		b.extraKeys = b.extraKeys[:b.count]
		b.resetBuffers()
	}
}

func (b *BatchEncoder) resetBuffers() {
	// Keys are taken over by kv.Batch so we need new space.
	keyBuf := make([]byte, b.count*b.keyBufSize)
	partitionSlices(keyBuf, b.keys)

	for row := 0; row < b.count; row++ {
		// Reset these to zero length, if they grew we want to reuse.
		b.values[row] = b.values[row][:0]
		b.extraKeys[row] = b.extraKeys[row][:0]
		b.lastColIDs[row] = 0
	}

	b.savedPrefixes = nil
}

func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (b *BatchEncoder) getMaxKeyCols() int {
	max := 1
	max = intMax(max, b.rh.TableDesc.GetPrimaryIndex().NumKeyColumns())
	for _, ind := range b.rh.TableDesc.WritableNonPrimaryIndexes() {
		max = intMax(max, ind.NumKeyColumns())
	}
	return max
}

func (b *BatchEncoder) getMaxSuffixColumns() int {
	max := 1
	for _, ind := range b.rh.TableDesc.WritableNonPrimaryIndexes() {
		max = intMax(max, len(ind.IndexDesc().KeySuffixColumnIDs))
	}
	return max
}

func (b *BatchEncoder) encodePK(ctx context.Context, ind catalog.Index) error {
	desc := b.rh.TableDesc
	vecs := b.b.ColVecs()
	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	families := desc.GetFamilies()
	fetchedCols := desc.PublicColumns()

	b.setupPrefixes(ind, b.rh.PrimaryIndexKeyPrefix)
	kys := b.keys

	var nulls coldata.Nulls
	if err := b.encodeIndexKey(keyCols, &nulls); err != nil {
		return err
	}
	if nulls.MaybeHasNulls() {
		for _, k := range desc.IndexKeyColumns(ind) {
			ord, ok := b.colMap.Get(k.GetID())
			if !ok {
				return sqlerrors.NewNonNullViolationError(k.GetName())
			}
			colNulls := b.b.ColVec(ord).Nulls()
			if colNulls.MaybeHasNulls() {
				for i := b.start; i < b.end; i++ {
					if colNulls.NullAt(i) {
						return sqlerrors.NewNonNullViolationError(k.GetName())
					}
				}
			}
		}
		return errors.AssertionFailedf("NULL value in unknown key column")
	}

	for familyIndex := range families {
		family := &families[familyIndex]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := b.colMap.Get(colID); ok {
				update = true
				break
			}
		}
		// We can have an empty family.ColumnIDs in the following case:
		// * A table is created with the primary key not in family 0, and another column in family 0.
		// * The column in family 0 is dropped, leaving the 0'th family empty.
		// In this case, we must keep the empty 0'th column family in order to ensure that column family 0
		// is always encoded as the sentinel k/v for a row.
		if !update && len(family.ColumnIDs) != 0 {
			continue
		}
		familySortedColumnIDs, ok := b.rh.SortedColumnFamily(family.ID)
		if !ok {
			colexecerror.InternalError(errors.AssertionFailedf("invalid family sorted column id map"))
		}

		b.initFamily(familyIndex, int(family.ID))

		// We need to ensure that column family 0 contains extra metadata, like composite primary key values.
		// Additionally, the decoders expect that column family 0 is encoded with a TUPLE value tag, so we
		// don't want to use the untagged value encoding.
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.
			idx, ok := b.colMap.Get(family.DefaultColumnID)
			if !ok {
				// Column not being updated or inserted.
				continue
			}
			values := make([]roachpb.Value, len(b.keys))
			typ := fetchedCols[idx].GetType()
			vec := vecs[idx]
			for row := 0; row < b.count; row++ {
				// Elided partial index keys will be nil.
				if kys[row] == nil {
					continue
				}
				if skip := b.skipColumnNotInPrimaryIndexValue(family.DefaultColumnID, vec, row); skip {
					continue
				}
				marshaled, err := MarshalLegacy(typ, vec, row+b.start)
				if err != nil {
					return err
				}
				if marshaled.RawBytes == nil {
					// Tell CPutValues to ignore this KV. We use empty slice
					// instead of nil b/c nil is the partial index skip indicator.
					kys[row] = kys[row][:0]
				} else {
					// We only output non-NULL values. Non-existent column keys are
					// considered NULL during scanning and the row sentinel ensures we know
					// the row exists.
					if err := b.rh.CheckRowSize(ctx, &kys[row], marshaled.RawBytes, family.ID); err != nil {
						return err
					}
					values[row] = marshaled
				}
			}
			b.p.CPutValuesEmpty(kys, values)
			if err := b.checkMemory(); err != nil {
				return err
			}
			continue
		}

		values := b.values

		for _, colID := range familySortedColumnIDs {
			idx, ok := b.colMap.Get(colID)
			if !ok {
				// Column not being updated or inserted.
				continue
			}

			col := fetchedCols[idx]
			vec := vecs[idx]
			nulls := vec.Nulls()
			lastColIDs := b.lastColIDs
			for row := 0; row < b.count; row++ {
				if kys[row] == nil {
					continue
				}
				if nulls.NullAt(row + b.start) {
					if !col.IsNullable() {
						return sqlerrors.NewNonNullViolationError(col.GetName())
					}
					continue
				}
				if skip := b.skipColumnNotInPrimaryIndexValue(colID, vec, row); skip {
					continue
				}
				if lastColIDs[row] > colID {
					colexecerror.InternalError(errors.AssertionFailedf("cannot write column id %d after %d", colID, lastColIDs[row]))
				}
				colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], colID)
				lastColIDs[row] = colID
				var err error
				values[row], err = valuesideEncodeCol(values[row], colIDDelta, vec, row+b.start)
				if err != nil {
					return err
				}
				if err := b.rh.CheckRowSize(ctx, &kys[row], values[row], family.ID); err != nil {
					return err
				}
			}
		}

		// Skip empty kvs if we aren't family 0.
		if family.ID != 0 {
			for row := 0; row < b.count; row++ {
				if len(values[row]) == 0 {
					// Tell CPutValues to ignore this KV. We use empty slice
					// instead of nil b/c nil is the partial index skip indicator.
					kys[row] = kys[row][:0]
				}
			}
		}

		// If we have more than one family we have to copy the keys in order to
		// re-use their prefixes because the putter routines will sort
		// and mutate the kys slice.
		if len(families) > 1 && b.savedPrefixes == nil {
			b.savedPrefixes = make([]roachpb.Key, len(kys))
			copy(b.savedPrefixes, kys)
		}

		// TODO(cucaroach): For updates overwrite makes this a plain put.
		b.p.CPutTuplesEmpty(kys, values)

		if err := b.checkMemory(); err != nil {
			return err
		}
	}

	return nil
}

// encodeSecondaryIndex is the vector version of rowenc.EncodeSecondaryIndex.
func (b *BatchEncoder) encodeSecondaryIndex(ctx context.Context, ind catalog.Index) error {
	var err error
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(b.rh.Codec, b.rh.TableDesc.GetID(), ind.GetID())

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
		return b.encodePK(ctx, ind)
	}

	b.setupPrefixes(ind, secondaryIndexKeyPrefix)
	kys := b.keys

	// Encode key suffix columns and save the results in extraKeys.
	err = encodeColumns(ind.IndexDesc().KeySuffixColumnIDs, nil /*directions*/, b.colMap, b.start, b.end, b.b.ColVecs(), b.extraKeys)
	if err != nil {
		return err
	}

	// Store nulls we encounter so we can properly make the key unique below.
	var nulls coldata.Nulls
	if ind.GetType() == idxtype.INVERTED {
		// Since the inverted indexes generate multiple keys per row just handle them
		// separately.
		return b.encodeInvertedSecondaryIndex(ctx, ind, kys, b.extraKeys)
	} else {
		keyAndSuffixCols := b.rh.TableDesc.IndexFetchSpecKeyAndSuffixColumns(ind)
		keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
		if err := b.encodeIndexKey(keyCols, &nulls); err != nil {
			return err
		}
	}
	noFamilies := b.rh.TableDesc.NumFamilies() == 1 || ind.GetVersion() == descpb.BaseIndexFormatVersion
	for row := 0; row < b.count; row++ {
		// Elided partial index keys will be empty.
		if len(kys[row]) == 0 {
			continue
		}
		if !ind.IsUnique() || nulls.NullAtChecked(row+b.start) {
			if len(b.extraKeys[row]) > 0 {
				kys[row] = append(kys[row], b.extraKeys[row]...)
			}
		}
		// Record where we need to reset the key to for next family.
		if !noFamilies {
			b.keyPrefixOffsets[row] = int32(len(kys[row]))
		}
	}

	if noFamilies {
		if err := b.encodeSecondaryIndexNoFamilies(ind, kys); err != nil {
			return err
		}
	} else {
		familyToColumns := rowenc.MakeFamilyToColumnMap(ind, b.rh.TableDesc)
		if err := b.encodeSecondaryIndexWithFamilies(familyToColumns, ind, kys); err != nil {
			return err
		}
	}
	return b.checkMemory()
}

func (b *BatchEncoder) encodeSecondaryIndexNoFamilies(ind catalog.Index, kys []roachpb.Key) error {
	for row := 0; row < b.count; row++ {
		// Elided partial index keys will be empty.
		if len(kys[row]) == 0 {
			continue
		}
		// If we aren't encoding index keys with families, all index keys use the
		// sentinel family 0.
		kys[row] = keys.MakeFamilyKey(kys[row], 0)
	}
	values := b.values
	if ind.IsUnique() {
		// Note that a unique secondary index that contains a NULL column value
		// will have extraKey appended to the key and stored in the value. We
		// require extraKey to be appended to the key in order to make the key
		// unique. We could potentially get rid of the duplication here but at
		// the expense of complicating scanNode when dealing with unique
		// secondary indexes.
		for r := 0; r < b.count; r++ {
			values[r] = b.extraKeys[r]
		}
	}
	cols := rowenc.GetValueColumns(ind)
	if err := b.writeColumnValues(kys, values, ind, cols); err != nil {
		return err
	}
	b.p.CPutBytesEmpty(kys, values)
	return nil
}

func (b *BatchEncoder) encodeSecondaryIndexWithFamilies(
	familyMap map[catid.FamilyID][]rowenc.ValueEncodedColumn, ind catalog.Index, kys []roachpb.Key,
) error {
	// TODO (rohany): is there a natural way of caching this information as well?
	// We have to iterate over the map in sorted family order. Other parts of the code
	// depend on a per-call consistent order of keys generated.
	familyIDs := make([]int, 0, len(familyMap))
	for familyID := range familyMap {
		familyIDs = append(familyIDs, int(familyID))
	}
	sort.Ints(familyIDs)
	for familyIndex, familyID := range familyIDs {
		storedColsInFam := familyMap[descpb.FamilyID(familyID)]

		// If we aren't storing any columns in this family and we are not the first family,
		// skip onto the next family. We need to write family 0 no matter what to ensure
		// that each row has at least one entry in the DB.
		if len(storedColsInFam) == 0 && familyID != 0 {
			continue
		}
		sort.Sort(rowenc.ByID(storedColsInFam))
		b.initFamily(familyIndex, familyID)
		values := b.values
		if ind.IsUnique() && familyID == 0 {
			// Note that a unique secondary index that contains a NULL column value
			// will have extraKey appended to the key and stored in the value. We
			// require extraKey to be appended to the key in order to make the key
			// unique. We could potentially get rid of the duplication here but at
			// the expense of complicating scanNode when dealing with unique
			// secondary indexes.
			for r := 0; r < b.count; r++ {
				values[r] = b.extraKeys[r]
			}
		}
		if err := b.writeColumnValues(kys, values, ind, storedColsInFam); err != nil {
			return err
		}
		for row := 0; row < len(kys); row++ {
			// TODO(cucaroach): COPY doesn't need includeEmpty support but
			// updates and deletes will if/when we support them.
			if familyID != 0 && len(values[row]) == 0 /* && !includeEmpty */ {
				kys[row] = kys[row][:0]
				continue
			}
		}

		// If we have more than one family we have to copy the keys in order to
		// re-use their prefixes because the putter routines will sort
		// and mutate the kys slice.
		if len(familyIDs) > 1 && b.savedPrefixes == nil {
			b.savedPrefixes = make([]roachpb.Key, len(kys))
			copy(b.savedPrefixes, kys)
		}

		// If we are looking at family 0, encode the data as BYTES, as it might
		// include encoded primary key columns. For other families,
		// use the tuple encoding for the value.
		if familyID == 0 {
			b.p.CPutBytesEmpty(kys, values)
		} else {
			b.p.CPutTuplesEmpty(kys, values)
		}
		if err := b.checkMemory(); err != nil {
			return err
		}
	}

	return nil
}

// writeColumnValues writes the value encoded versions of the desired columns
// from the input into the value byte slice.
func (b *BatchEncoder) writeColumnValues(
	kys []roachpb.Key, values [][]byte, ind catalog.Index, cols []rowenc.ValueEncodedColumn,
) error {
	var err error
	lastColIDs := b.lastColIDs
	for _, col := range cols {
		idx, ok := b.colMap.Get(col.ColID)
		if !ok {
			// Column not being updated or inserted.
			continue
		}
		vec := b.b.ColVec(idx)
		nulls := vec.Nulls()
		for row := 0; row < b.count; row++ {
			if nulls.NullAt(row+b.start) || len(kys[row]) == 0 {
				continue
			}
			if col.IsComposite && !isComposite(vec, row+b.start) {
				continue
			}
			if lastColIDs[row] > col.ColID {
				colexecerror.InternalError(errors.AssertionFailedf("cannot write column id %d after %d", col.ColID, lastColIDs[row]))
			}
			colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], col.ColID)
			lastColIDs[row] = col.ColID
			values[row], err = valuesideEncodeCol(values[row], colIDDelta, vec, row+b.start)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeColumns is the vector version of rowenc.EncodeColumns. It is generic
// so we can use it on raw byte slices and roachpb.Key.
func encodeColumns[T []byte | roachpb.Key](
	columnIDs []descpb.ColumnID,
	directions rowenc.Directions,
	colMap catalog.TableColMap,
	start, end int,
	vecs []*coldata.Vec,
	keys []T,
) error {
	var err error
	for colIdx, id := range columnIDs {
		var vec *coldata.Vec
		i, ok := colMap.Get(id)
		if ok {
			vec = vecs[i]
		}
		dir := encoding.Ascending
		if directions != nil {
			dir, err = directions.Get(colIdx)
			if err != nil {
				return err
			}
		}
		if err = encodeKeys(keys, dir, vec, start, end); err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchEncoder) initFamily(familyIndex, familyID int) {
	kys := b.keys
	if familyIndex == 0 {
		// First family, set up keyPrefixOffsets and append family ID.
		for row := 0; row < b.count; row++ {
			// Elided partial index keys will be nil.
			if b.keys[row] == nil {
				continue
			}
			// Record where we need to reset the key to for next family.
			if b.keyPrefixOffsets != nil {
				b.keyPrefixOffsets[row] = int32(len(kys[row]))
			}
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(familyID))
		}
	} else {
		// For the rest of families set keys for new family up by copying the old
		// keys up to end of PK prefix and then appending family id.
		keyBuf := make([]byte, b.count*b.keyBufSize)
		for row := 0; row < b.count; row++ {
			// Elided partial index keys will be nil but since the putter can and
			// will sort kys we need to check savedPrefixes.
			if b.savedPrefixes[row] == nil {
				kys[row] = nil
				continue
			}
			offset := row * b.keyBufSize
			// Get a slice pointing to prefix bytes.
			prefix := b.savedPrefixes[row][:b.keyPrefixOffsets[row]]
			// Set slice to new space.
			kys[row] = keyBuf[offset : offset : b.keyBufSize+offset]
			// Append prefix.
			kys[row] = append(kys[row], prefix...)
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(familyID))
			// Reset values.
			b.values[row] = b.values[row][:0]
			// Also reset lastColIDs.
			b.lastColIDs[row] = 0
		}
	}
}

func (b *BatchEncoder) setupPrefixes(ind catalog.Index, prefix []byte) {
	// Partial index support, we will use this to leave keys empty for rows that
	// don't satisfy predicate.
	var piPreds []bool
	if b.partialIndexes != nil {
		piPreds = b.partialIndexes[ind.GetID()]
	}
	for row := 0; row < b.count; row++ {
		if piPreds != nil && !piPreds[row+b.start] {
			b.keys[row] = nil
			continue
		}
		b.keys[row] = append(b.keys[row], prefix...)
	}
}

func (b *BatchEncoder) checkMemory() error {
	return b.memoryUsageCheck()
}

func (b *BatchEncoder) skipColumnNotInPrimaryIndexValue(
	colID catid.ColumnID, vec *coldata.Vec, row int,
) bool {
	// Reuse this function but fake out the value and handle composites here.
	if skip, _ := b.rh.SkipColumnNotInPrimaryIndexValue(colID, tree.DNull); skip {
		if !b.compositeColumnIDs.Contains(int(colID)) {
			return true
		}
		return !isComposite(vec, row+b.start)
	}
	return false
}

func isComposite(vec *coldata.Vec, row int) bool {
	switch vec.CanonicalTypeFamily() {
	case types.FloatFamily:
		f := tree.DFloat(vec.Float64()[row])
		return f.IsComposite()
	case types.DecimalFamily:
		d := tree.DDecimal{Decimal: vec.Decimal()[row]}
		return d.IsComposite()
	case types.JsonFamily:
		j := tree.DJSON{JSON: vec.JSON().Get(row)}
		return j.IsComposite()
	default:
		d := vec.Datum().Get(row)
		if cdatum, ok := d.(tree.CompositeDatum); ok {
			return cdatum.IsComposite()
		}
	}
	return false
}
