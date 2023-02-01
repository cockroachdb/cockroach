// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

var ErrOverMemLimit = errors.New("memory limit exceeded")

type BatchEncoder struct {
	rh                                       *row.RowHelper
	b                                        coldata.Batch
	p                                        row.Putter
	colMap                                   catalog.TableColMap
	partialIndexes                           map[descpb.IndexID]coldata.Bools
	compositeColumnIDs                       intsets.Fast
	lastColIDs                               []catid.ColumnID
	keys                                     []roachpb.Key
	values                                   [][]byte
	extraKeys                                [][]byte
	valBuf, extraBuf                         []byte
	keyBufSize, extraKeysBufSize, valBufSize int
	keyPrefixOffsets                         []int32
	start                                    int
	rows                                     int
	mutationQuotaCheck                       func() bool
}

func MakeEncoder(
	codec keys.SQLCodec,
	desc catalog.TableDescriptor,
	sv *settings.Values,
	b coldata.Batch,
	insCols []catalog.Column,
	metrics *rowinfra.Metrics,
	partialIndexes map[descpb.IndexID]coldata.Bools,
	mutationQuotaCheck func() bool,
) BatchEncoder {
	rh := row.NewRowHelper(codec, desc, desc.WritableNonPrimaryIndexes(), sv, false /*internal*/, metrics)
	rh.Init()
	colMap := row.ColIDtoRowIndexFromCols(insCols)
	return BatchEncoder{rh: &rh, b: b, colMap: colMap,
		partialIndexes: partialIndexes, mutationQuotaCheck: mutationQuotaCheck}
}

func (b *BatchEncoder) PrepareBatch(ctx context.Context, p row.Putter, start, end int) error {
	if err := row.CheckPrimaryKeyColumns(b.rh.TableDesc, b.colMap); err != nil {
		return err
	}
	if err := b.checkNotNullColumns(); err != nil {
		return err
	}
	b.p = p
	if start >= end {
		return errors.AssertionFailedf("PrepareBatch illegal arguments: start=%d,end=%d", start, end)
	}
	b.start = start
	b.rows = end - start
	if b.rows > math.MaxUint16 {
		b.rows = math.MaxUint16
	}
	// If we have partial indexes or families we may have keys that get dropped
	// and we need to compact KV slices.
	//b.p = &sliceCompressor{p: b.p}
	b.initBuffers()
	if err := b.encodePK(ctx, b.rh.TableDesc.GetPrimaryIndex()); err != nil {
		return err
	}
	for _, ind := range b.rh.TableDesc.WritableNonPrimaryIndexes() {
		b.resetBuffers()
		// TODO(cucaroach): COPY doesn't need ForcePut support but the encoder
		// will need to support it eventually.
		if ind.ForcePut() {
			return errors.AssertionFailedf("vector encoder doesn't support ForcePut yet")
		}
		if err := b.encodeSecondaryIndex(ctx, ind); err != nil {
			return err
		}
	}
	return nil
}

// Check that all non-nullable columns are present. The row engine does this in
// sql.enforceLocalColumnConstraints.
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
	// Initialize buffers sliced from big buffer
	siz := len(buffer) / len(slices)
	for i := 0; i < len(slices); i++ {
		offset := i * siz
		slices[i] = buffer[offset : offset : siz+offset]
	}
}

func (b *BatchEncoder) initBuffers() {
	b.keyBufSize = 5 * b.getMaxKeyCols()
	b.valBufSize = 5 * len(b.rh.TableDesc.PublicColumns())
	b.extraKeysBufSize = 5 * b.getMaxSuffixColumns()

	b.keys = make([]roachpb.Key, b.rows)
	b.values = make([][]byte, b.rows)
	b.extraKeys = make([][]byte, b.rows)
	b.lastColIDs = make([]catid.ColumnID, b.rows)

	b.valBuf = make([]byte, b.rows*b.valBufSize)
	b.extraBuf = make([]byte, b.rows*b.extraKeysBufSize)

	// The slices above are re-used across each index but the key buffer data has
	// to be re-allocated (kv.Batch has references to the keys) but the
	// values are copied and allocated so we can re-use.
	// TODO(cucaroach): can we have kv.Batch NOT reallocate the values?
	b.resetBuffers()

	// Store the index up to the family id so we can reuse the prefixes
	if len(b.rh.TableDesc.GetFamilies()) > 1 {
		b.keyPrefixOffsets = make([]int32, b.rows)
	}
}

func (b *BatchEncoder) resetBuffers() {
	// Keys are taken over by kv.Batch so we need new space.
	keyBuf := make([]byte, b.rows*b.keyBufSize)
	partitionSlices(keyBuf, b.keys)
	// Values are copied so we can re-use.
	partitionSlices(b.valBuf, b.values)
	partitionSlices(b.extraBuf, b.extraKeys)

	for row := 0; row < b.rows; row++ {
		b.lastColIDs[row] = 0
	}
}

func intMax(a, b int) int {
	if a < b {
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
			vec := b.b.ColVec(ord)
			if vec.Nulls().MaybeHasNulls() {
				return sqlerrors.NewNonNullViolationError(k.GetName())
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
			return errors.AssertionFailedf("invalid family sorted column id map")
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
			values := make([]roachpb.Value, b.rows)
			typ := fetchedCols[idx].GetType()
			vec := vecs[idx]
			for row := 0; row < b.rows; row++ {
				// Elided partial index keys will be nil.
				if kys[row] == nil {
					continue
				}
				marshaled, err := MarshalLegacy(typ, vec, row)
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
			b.p.CPutValues(kys, values)
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
			typ := col.GetType()
			vec := vecs[idx]
			lastColIDs := b.lastColIDs
			for row := 0; row < b.rows; row++ {
				if kys[row] == nil {
					continue
				}
				if vec.Nulls().NullAt(row) {
					if !col.IsNullable() {
						return sqlerrors.NewNonNullViolationError(col.GetName())
					}
					continue
				}
				if skip := b.skipColumnNotInPrimaryIndexValue(colID, vec, row); skip {
					continue
				}
				if lastColIDs[row] > colID {
					return errors.AssertionFailedf("cannot write column id %d after %d", colID, lastColIDs[row])
				}
				colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], colID)
				lastColIDs[row] = colID
				var err error
				values[row], err = valuesideEncodeCol(values[row], typ, colIDDelta, vec, row)
				if err != nil {
					return err
				}
				if err := b.rh.CheckRowSize(ctx, &kys[row], values[row], family.ID); err != nil {
					return err
				}
			}
		}

		// Skip empty kvs if we aren't family 0
		if family.ID != 0 {
			for row := 0; row < b.rows; row++ {
				if len(values[row]) == 0 {
					// Tell CPutValues to ignore this KV. We use empty slice
					// instead of nil b/c nil is the partial index skip indicator.
					kys[row] = kys[row][:0]
				}
			}
		}

		//TODO(cucuroach): for updates overwrite makes this a plain put
		b.p.CPutTuples(kys, values)

		if err := b.checkMemory(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BatchEncoder) encodeSecondaryIndex(ctx context.Context, ind catalog.Index) error {
	var err error
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(b.rh.Codec, b.rh.TableDesc.GetID(), ind.GetID())

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
		return b.encodePK(ctx, ind)
	}

	b.setupPrefixes(ind, secondaryIndexKeyPrefix)
	kys := b.keys

	_, err = encodeColumns(ind.IndexDesc().KeySuffixColumnIDs, nil /*directions*/, b.colMap, b.rows, b.b.ColVecs(), b.extraKeys)
	if err != nil {
		return err
	}

	var nulls coldata.Nulls
	if ind.GetType() == descpb.IndexDescriptor_INVERTED {
		// Since the inverted indexes generate multiple keys per row just handle them
		// separately.
		return b.encodeInvertedSecondaryIndex(ind, kys, b.extraKeys)
	} else {
		keyAndSuffixCols := b.rh.TableDesc.IndexFetchSpecKeyAndSuffixColumns(ind)
		keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
		if err := b.encodeIndexKey(keyCols, &nulls); err != nil {
			return err
		}
	}
	noFamilies := b.rh.TableDesc.NumFamilies() == 1 || ind.GetVersion() == descpb.BaseIndexFormatVersion
	for row := 0; row < b.rows; row++ {
		// Elided partial index keys will be empty.
		if len(kys[row]) == 0 {
			continue
		}
		if !ind.IsUnique() || nulls.NullAtChecked(row) {
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
		if err := b.encodeSecondaryIndexNoFamilies(ind, kys, b.extraKeys); err != nil {
			return err
		}
	} else {
		familyToColumns := rowenc.MakeFamilyToColumnMap(ind, b.rh.TableDesc)
		if err := b.encodeSecondaryIndexWithFamilies(familyToColumns, ind, kys, b.extraKeys); err != nil {
			return err
		}
	}
	return b.checkMemory()
}

func (b *BatchEncoder) encodeSecondaryIndexNoFamilies(
	ind catalog.Index, kys []roachpb.Key, extraKeyCols [][]byte,
) error {
	for row := 0; row < b.rows; row++ {
		// Elided partial index keys will be empty.
		if len(kys[row]) == 0 {
			continue
		}
		kys[row] = keys.MakeFamilyKey(kys[row], 0)
	}
	values := b.values
	if ind.IsUnique() {
		for r := 0; r < b.rows; r++ {
			values[r] = extraKeyCols[r]
		}
	}
	cols := rowenc.GetValueColumns(ind)
	if err := b.writeColumnValues(kys, values, ind, cols); err != nil {
		return err
	}
	b.p.InitPutBytes(kys, values)
	return nil
}

func (b *BatchEncoder) encodeSecondaryIndexWithFamilies(
	familyMap map[catid.FamilyID][]rowenc.ValueEncodedColumn,
	ind catalog.Index,
	kys []roachpb.Key,
	extraKeyCols [][]byte,
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
			for r := 0; r < b.rows; r++ {
				values[r] = extraKeyCols[r]
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
		if familyID == 0 {
			b.p.InitPutBytes(kys, values)
		} else {
			b.p.InitPutTuples(kys, values)
		}
		if err := b.checkMemory(); err != nil {
			return err
		}
	}

	return nil
}

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
		for row := 0; row < b.rows; row++ {
			if vec.Nulls().NullAt(row) || len(kys[row]) == 0 {
				continue
			}
			if col.IsComposite && !isComposite(vec, row) {
				continue
			}
			if lastColIDs[row] > col.ColID {
				return errors.AssertionFailedf("cannot write column id %d after %d", col.ColID, lastColIDs[row])
			}
			colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], col.ColID)
			lastColIDs[row] = col.ColID
			values[row], err = valuesideEncodeCol(values[row], vec.Type(), colIDDelta, vec, row)
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
	count int,
	vecs []coldata.Vec,
	keys []T,
) (*coldata.Nulls, error) {
	var nulls coldata.Nulls
	var err error
	for colIdx, id := range columnIDs {
		var vec coldata.Vec
		var typ *types.T
		i, ok := colMap.Get(id)
		if ok {
			vec = vecs[i]
			typ = vec.Type()
			if vec.Nulls().MaybeHasNulls() {
				nulls = nulls.Or(*vec.Nulls())
			}
		}
		dir := encoding.Ascending
		if directions != nil {
			dir, err = directions.Get(colIdx)
			if err != nil {
				return nil, err
			}
		}
		if err := encodeKeys(keys, typ, dir, vec, 0, count); err != nil {
			return nil, err
		}
	}
	return &nulls, nil
}

func (b *BatchEncoder) initFamily(familyIndex, familyID int) {
	kys := b.keys
	if familyIndex == 0 {
		// First family, set up keyPrefixOffsets and append family ID.
		for row := 0; row < b.rows; row++ {
			// Elided partial index keys will be nil.
			if b.keys[row] == nil {
				continue
			}
			// Record where we need to reset the key to for next family.
			if b.keyPrefixOffsets != nil && familyIndex == 0 {
				b.keyPrefixOffsets[row] = int32(len(kys[row]))
			}
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(familyID))
		}
	} else {
		// For the rest of families set keys for new family up by copying the old
		// keys up to end of PK prefix and then appending family id.
		keyBuf := make([]byte, b.rows*b.keyBufSize)
		for row := 0; row < b.rows; row++ {
			// Elided partial index keys will be nil.
			if b.keys[row] == nil {
				continue
			}
			offset := row * b.keyBufSize
			// Save old slice
			prefix := kys[row][:b.keyPrefixOffsets[row]]
			// Set slice to new space
			kys[row] = keyBuf[offset : offset : b.keyBufSize+offset]
			// Append prefix
			kys[row] = append(kys[row], prefix...)
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(familyID))
			// Also reset lastColIDs
			b.lastColIDs[row] = 0
		}
		valBuf := make([]byte, b.rows*b.valBufSize)
		partitionSlices(valBuf, b.values)
	}
}

func (b *BatchEncoder) setupPrefixes(ind catalog.Index, prefix []byte) {
	// Partial index support, we will use this to leave keys empty for rows that
	// don't satisfy predicate.
	var piPreds coldata.Bools
	if b.partialIndexes != nil {
		piPreds = b.partialIndexes[ind.GetID()]
	}
	for row := 0; row < b.rows; row++ {
		if piPreds != nil && !piPreds.Get(row) {
			b.keys[row] = nil
			continue
		}
		b.keys[row] = append(b.keys[row], prefix...)
	}
}

func (b *BatchEncoder) checkMemory() error {
	if b.mutationQuotaCheck() {
		return ErrOverMemLimit
	}
	return nil
}

func (b *BatchEncoder) skipColumnNotInPrimaryIndexValue(
	colID catid.ColumnID, vec coldata.Vec, row int,
) bool {
	// Reuse this function but fake out the value and handle composites here.
	if skip := b.rh.SkipColumnNotInPrimaryIndexValue(colID, tree.DNull); skip {
		if !b.compositeColumnIDs.Contains(int(colID)) {
			return true
		}
		return !isComposite(vec, row)
	}
	return false
}

func isComposite(vec coldata.Vec, row int) bool {
	switch vec.CanonicalTypeFamily() {
	case types.FloatFamily:
		f := tree.DFloat(vec.Float64()[row])
		return f.IsComposite()
	case types.DecimalFamily:
		d := tree.DDecimal{Decimal: vec.Decimal()[row]}
		return d.IsComposite()
	default:
		d := vec.Datum().Get(row)
		if cdatum, ok := d.(tree.CompositeDatum); ok {
			return cdatum.IsComposite()
		}
	}
	return false
}
