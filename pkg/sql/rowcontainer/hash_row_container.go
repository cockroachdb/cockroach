// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowcontainer

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type columns []uint32

// RowMarkerIterator is a RowIterator that can be used to mark rows.
type RowMarkerIterator interface {
	RowIterator
	// Reset resets this iterator to point at a bucket that matches the given
	// row. This will cause RowIterator.Rewind to rewind to the front of the
	// input row's bucket.
	Reset(ctx context.Context, row rowenc.EncDatumRow) error
	Mark(ctx context.Context) error
	IsMarked(ctx context.Context) bool
}

// HashRowContainer is a container used to store rows according to an encoding
// of given equality columns. The stored rows can then be probed to return a
// bucket of matching rows. Additionally, each stored row can be marked and all
// rows that are unmarked can be iterated over. An example of where this is
// useful is in full/outer joins. The caller can mark all matched rows and
// iterate over the unmarked rows to produce a result.
type HashRowContainer interface {
	// Init initializes the HashRowContainer with the given equality columns.
	//	- shouldMark specifies whether the caller cares about marking rows. If
	//	  not, the HashRowContainer will not perform any row marking logic. This
	//	  is meant to optimize space usage and runtime.
	//	- types is the schema of rows that will be added to this container.
	//	- storedEqCols are the equality columns of rows stored in this
	// 	  container.
	// 	  i.e. when adding a row, the columns specified by storedEqCols are used
	// 	  to get the bucket that the row should be added to.
	//	- encodeNull indicates whether rows with NULL equality columns should be
	//	  stored or skipped.
	Init(
		ctx context.Context, shouldMark bool, types []*types.T, storedEqCols columns, encodeNull bool,
	) error
	AddRow(context.Context, rowenc.EncDatumRow) error
	// IsEmpty returns true if no rows have been added to the container so far.
	IsEmpty() bool

	// NewBucketIterator returns a RowMarkerIterator that iterates over a bucket
	// of rows that match the given row on equality columns. This iterator can
	// also be used to mark rows.
	// Rows are marked because of the use of this interface by the hashJoiner.
	// Given a row, the hashJoiner does not necessarily want to emit all rows
	// that match on equality columns. There is an additional `ON` clause that
	// specifies an arbitrary expression that matching rows must pass to be
	// emitted. For full/outer joins, this is tracked through marking rows if
	// they match and then iterating over all unmarked rows to emit those that
	// did not match.
	// 	- probeEqCols are the equality columns of the given row that are used to
	// 	  get the bucket of matching rows.
	NewBucketIterator(
		ctx context.Context, row rowenc.EncDatumRow, probeEqCols columns,
	) (RowMarkerIterator, error)

	// NewUnmarkedIterator returns a RowIterator that iterates over unmarked
	// rows. If shouldMark was false in Init(), this iterator iterates over all
	// rows.
	NewUnmarkedIterator(context.Context) RowIterator

	// Close frees up resources held by the HashRowContainer.
	Close(context.Context)
}

// columnEncoder is a utility struct used by implementations of HashRowContainer
// to encode equality columns, the result of which is used as a key to a bucket.
type columnEncoder struct {
	scratch []byte
	// types for the "key" columns (equality columns)
	keyTypes   []*types.T
	datumAlloc rowenc.DatumAlloc
	encodeNull bool
}

func (e *columnEncoder) init(typs []*types.T, keyCols columns, encodeNull bool) {
	e.keyTypes = make([]*types.T, len(keyCols))
	for i, c := range keyCols {
		e.keyTypes[i] = typs[c]
	}
	e.encodeNull = encodeNull
}

// encodeColumnsOfRow returns the encoding for the grouping columns. This is
// then used as our group key to determine which bucket to add to.
// If the row contains any NULLs and encodeNull is false, hasNull is true and
// no encoding is returned. If encodeNull is true, hasNull is never set.
func encodeColumnsOfRow(
	da *rowenc.DatumAlloc,
	appendTo []byte,
	row rowenc.EncDatumRow,
	cols columns,
	colTypes []*types.T,
	encodeNull bool,
) (encoding []byte, hasNull bool, err error) {
	for i, colIdx := range cols {
		if row[colIdx].IsNull() && !encodeNull {
			return nil, true, nil
		}
		// Note: we cannot compare VALUE encodings because they contain column IDs
		// which can vary.
		// TODO(radu): we should figure out what encoding is readily available and
		// use that (though it needs to be consistent across all rows). We could add
		// functionality to compare VALUE encodings ignoring the column ID.
		appendTo, err = row[colIdx].Encode(colTypes[i], da, descpb.DatumEncoding_ASCENDING_KEY, appendTo)
		if err != nil {
			return appendTo, false, err
		}
	}
	return appendTo, false, nil
}

// encodeEqualityCols returns the encoding of the specified columns of the given
// row. The returned byte slice is only valid until the next call to
// encodeEqualityColumns().
func (e *columnEncoder) encodeEqualityCols(
	ctx context.Context, row rowenc.EncDatumRow, eqCols columns,
) ([]byte, error) {
	encoded, hasNull, err := encodeColumnsOfRow(
		&e.datumAlloc, e.scratch, row, eqCols, e.keyTypes, e.encodeNull,
	)
	if err != nil {
		return nil, err
	}
	e.scratch = encoded[:0]
	if hasNull {
		log.Fatal(ctx, "cannot process rows with NULL in an equality column")
	}
	return encoded, nil
}

// storedEqColsToOrdering returns an ordering based on storedEqCols to be used
// by the row containers (this will result in rows with the same equality
// columns occurring contiguously in the keyspace).
func storedEqColsToOrdering(storedEqCols columns) colinfo.ColumnOrdering {
	ordering := make(colinfo.ColumnOrdering, len(storedEqCols))
	for i := range ordering {
		ordering[i] = colinfo.ColumnOrderInfo{
			ColIdx:    int(storedEqCols[i]),
			Direction: encoding.Ascending,
		}
	}
	return ordering
}

const sizeOfBucket = int64(unsafe.Sizeof([]int{}))
const sizeOfRowIdx = int64(unsafe.Sizeof(int(0)))
const sizeOfBoolSlice = int64(unsafe.Sizeof([]bool{}))
const sizeOfBool = int64(unsafe.Sizeof(false))

// HashMemRowContainer is an in-memory implementation of a HashRowContainer.
// The rows are stored in an underlying MemRowContainer and an accompanying
// map stores the mapping from equality column encodings to indices in the
// MemRowContainer corresponding to matching rows.
// NOTE: Once a row is marked, adding more rows to the HashMemRowContainer
// results in undefined behavior. It is not necessary to do otherwise for the
// current usage of HashMemRowContainer.
type HashMemRowContainer struct {
	*MemRowContainer
	columnEncoder

	// shouldMark specifies whether the caller cares about marking rows. If not,
	// marked is never initialized.
	shouldMark bool

	// marked specifies for each row in MemRowContainer whether that row has
	// been marked. Used for iterating over unmarked rows.
	marked []bool

	// markMemoryReserved specifies whether the HashMemRowContainer's memory
	// account already accounts for the memory needed to mark the rows in the
	// HashMemRowContainer.
	markMemoryReserved bool

	// buckets contains the indices into MemRowContainer for a given group
	// key (which is the encoding of storedEqCols).
	buckets map[string][]int
	// bucketsAcc is the memory account for the buckets. The datums themselves
	// are all in the MemRowContainer.
	bucketsAcc mon.BoundAccount

	// storedEqCols contains the indices of the columns of a row that are
	// encoded and used as a key into buckets when adding a row.
	storedEqCols columns
}

var _ HashRowContainer = &HashMemRowContainer{}

// MakeHashMemRowContainer creates a HashMemRowContainer. This rowContainer
// must still be Close()d by the caller.
func MakeHashMemRowContainer(
	evalCtx *tree.EvalContext, memMonitor *mon.BytesMonitor, typs []*types.T, storedEqCols columns,
) HashMemRowContainer {
	mrc := &MemRowContainer{}
	mrc.InitWithMon(storedEqColsToOrdering(storedEqCols), typs, evalCtx, memMonitor)
	return HashMemRowContainer{
		MemRowContainer: mrc,
		buckets:         make(map[string][]int),
		bucketsAcc:      memMonitor.MakeBoundAccount(),
	}
}

// Init implements the HashRowContainer interface. types is ignored because the
// schema is inferred from the MemRowContainer.
func (h *HashMemRowContainer) Init(
	_ context.Context, shouldMark bool, typs []*types.T, storedEqCols columns, encodeNull bool,
) error {
	if h.storedEqCols != nil {
		return errors.New("HashMemRowContainer has already been initialized")
	}
	h.columnEncoder.init(typs, storedEqCols, encodeNull)
	h.shouldMark = shouldMark
	h.storedEqCols = storedEqCols
	return nil
}

// AddRow adds a row to the HashMemRowContainer. This row is unmarked by default.
func (h *HashMemRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
	rowIdx := h.Len()
	// Note that it is important that we add the row to a bucket first before
	// adding it to the row container because we want to make sure that if an
	// error is encountered in addRowToBucket, the row hasn't been added to the
	// container - this will allow us to fall back to disk if necessary without
	// erroneously adding the same row twice.
	if err := h.addRowToBucket(ctx, row, rowIdx); err != nil {
		return err
	}
	return h.MemRowContainer.AddRow(ctx, row)
}

// IsEmpty implements the HashRowContainer interface.
func (h *HashMemRowContainer) IsEmpty() bool {
	return h.Len() == 0
}

// Close implements the HashRowContainer interface.
func (h *HashMemRowContainer) Close(ctx context.Context) {
	h.MemRowContainer.Close(ctx)
	h.bucketsAcc.Close(ctx)
}

// addRowToBucket is a helper function that encodes the equality columns of the
// given row and appends the rowIdx to the matching bucket.
func (h *HashMemRowContainer) addRowToBucket(
	ctx context.Context, row rowenc.EncDatumRow, rowIdx int,
) error {
	encoded, err := h.encodeEqualityCols(ctx, row, h.storedEqCols)
	if err != nil {
		return err
	}

	bucket, ok := h.buckets[string(encoded)]

	usage := sizeOfRowIdx
	if !ok {
		usage += int64(len(encoded))
		usage += sizeOfBucket
	}

	if err := h.bucketsAcc.Grow(ctx, usage); err != nil {
		return err
	}

	h.buckets[string(encoded)] = append(bucket, rowIdx)
	return nil
}

// ReserveMarkMemoryMaybe is a utility function to grow the
// HashMemRowContainer's memory account by the memory needed to mark all rows.
// It is a noop if h.markMemoryReserved is true.
func (h *HashMemRowContainer) ReserveMarkMemoryMaybe(ctx context.Context) error {
	if h.markMemoryReserved {
		return nil
	}
	if err := h.bucketsAcc.Grow(ctx, sizeOfBoolSlice+(sizeOfBool*int64(h.Len()))); err != nil {
		return err
	}
	h.markMemoryReserved = true
	return nil
}

// hashMemRowBucketIterator iterates over the rows in a bucket.
type hashMemRowBucketIterator struct {
	*HashMemRowContainer
	probeEqCols columns
	// rowIdxs are the indices of rows in the bucket.
	rowIdxs []int
	curIdx  int
}

var _ RowMarkerIterator = &hashMemRowBucketIterator{}

// NewBucketIterator implements the HashRowContainer interface.
func (h *HashMemRowContainer) NewBucketIterator(
	ctx context.Context, row rowenc.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	ret := &hashMemRowBucketIterator{
		HashMemRowContainer: h,
		probeEqCols:         probeEqCols,
	}

	if err := ret.Reset(ctx, row); err != nil {
		return nil, err
	}
	return ret, nil
}

// Rewind implements the RowIterator interface.
func (i *hashMemRowBucketIterator) Rewind() {
	i.curIdx = 0
}

// Valid implements the RowIterator interface.
func (i *hashMemRowBucketIterator) Valid() (bool, error) {
	return i.curIdx < len(i.rowIdxs), nil
}

// Next implements the RowIterator interface.
func (i *hashMemRowBucketIterator) Next() {
	i.curIdx++
}

// Row implements the RowIterator interface.
func (i *hashMemRowBucketIterator) Row() (rowenc.EncDatumRow, error) {
	return i.EncRow(i.rowIdxs[i.curIdx]), nil
}

// IsMarked implements the RowMarkerIterator interface.
func (i *hashMemRowBucketIterator) IsMarked(ctx context.Context) bool {
	if !i.shouldMark {
		log.Fatal(ctx, "hash mem row container not set up for marking")
	}
	if i.marked == nil {
		return false
	}

	return i.marked[i.rowIdxs[i.curIdx]]
}

// Mark implements the RowMarkerIterator interface.
func (i *hashMemRowBucketIterator) Mark(ctx context.Context) error {
	if !i.shouldMark {
		log.Fatal(ctx, "hash mem row container not set up for marking")
	}
	if i.marked == nil {
		if !i.markMemoryReserved {
			panic("mark memory should have been reserved already")
		}
		i.marked = make([]bool, i.Len())
	}

	i.marked[i.rowIdxs[i.curIdx]] = true
	return nil
}

func (i *hashMemRowBucketIterator) Reset(ctx context.Context, row rowenc.EncDatumRow) error {
	encoded, err := i.encodeEqualityCols(ctx, row, i.probeEqCols)
	if err != nil {
		return err
	}
	i.rowIdxs = i.buckets[string(encoded)]
	return nil
}

// Close implements the RowIterator interface.
func (i *hashMemRowBucketIterator) Close() {}

// hashMemRowIterator iterates over all unmarked rows in a HashMemRowContainer.
type hashMemRowIterator struct {
	*HashMemRowContainer
	curIdx int

	// curKey contains the key that would be assigned to the current row if it
	// were to be put on disk. It is needed to optimize the recreation of the
	// iterators when HashDiskBackedRowContainer spills to disk and is computed
	// once, right before the spilling occurs.
	curKey []byte
}

var _ RowIterator = &hashMemRowIterator{}

// NewUnmarkedIterator implements the HashRowContainer interface.
func (h *HashMemRowContainer) NewUnmarkedIterator(ctx context.Context) RowIterator {
	return &hashMemRowIterator{HashMemRowContainer: h}
}

// Rewind implements the RowIterator interface.
func (i *hashMemRowIterator) Rewind() {
	i.curIdx = -1
	// Next will advance curIdx to the first unmarked row.
	i.Next()
}

// Valid implements the RowIterator interface.
func (i *hashMemRowIterator) Valid() (bool, error) {
	return i.curIdx < i.Len(), nil
}

// computeKey calculates the key for the current row as if the row is put on
// disk. This method must be kept in sync with AddRow() of DiskRowContainer.
func (i *hashMemRowIterator) computeKey() error {
	valid, err := i.Valid()
	if err != nil {
		return err
	}

	var row rowenc.EncDatumRow
	if valid {
		row = i.EncRow(i.curIdx)
	} else {
		if i.curIdx == 0 {
			// There are no rows in the container, so the key corresponding to the
			// "current" row is nil.
			i.curKey = nil
			return nil
		}
		// The iterator points at right after all the rows in the container, so we
		// will "simulate" the key corresponding to the non-existent row as the key
		// to the last existing row plus one (plus one part is done below where we
		// append the index of the row to curKey).
		row = i.EncRow(i.curIdx - 1)
	}

	i.curKey = i.curKey[:0]
	for _, col := range i.storedEqCols {
		var err error
		i.curKey, err = row[col].Encode(i.types[col], &i.columnEncoder.datumAlloc, descpb.DatumEncoding_ASCENDING_KEY, i.curKey)
		if err != nil {
			return err
		}
	}
	i.curKey = encoding.EncodeUvarintAscending(i.curKey, uint64(i.curIdx))
	return nil
}

// Next implements the RowIterator interface.
func (i *hashMemRowIterator) Next() {
	// Move the curIdx to the next unmarked row.
	i.curIdx++
	if i.marked != nil {
		for ; i.curIdx < len(i.marked) && i.marked[i.curIdx]; i.curIdx++ {
		}
	}
}

// Row implements the RowIterator interface.
func (i *hashMemRowIterator) Row() (rowenc.EncDatumRow, error) {
	return i.EncRow(i.curIdx), nil
}

// Close implements the RowIterator interface.
func (i *hashMemRowIterator) Close() {}

// HashDiskRowContainer is an on-disk implementation of a HashRowContainer.
// The rows are stored in an underlying DiskRowContainer with an extra boolean
// column to keep track of that row's mark.
type HashDiskRowContainer struct {
	DiskRowContainer
	columnEncoder

	diskMonitor *mon.BytesMonitor
	// shouldMark specifies whether the caller cares about marking rows. If not,
	// rows are stored with one less column (which usually specifies that row's
	// mark).
	shouldMark    bool
	engine        diskmap.Factory
	scratchEncRow rowenc.EncDatumRow
}

var _ HashRowContainer = &HashDiskRowContainer{}

var encodedTrue = encoding.EncodeBoolValue(nil, encoding.NoColumnID, true)

// MakeHashDiskRowContainer creates a HashDiskRowContainer with the given engine
// as the underlying store that rows are stored on. shouldMark specifies whether
// the HashDiskRowContainer should set itself up to mark rows.
func MakeHashDiskRowContainer(
	diskMonitor *mon.BytesMonitor, e diskmap.Factory,
) HashDiskRowContainer {
	return HashDiskRowContainer{
		diskMonitor: diskMonitor,
		engine:      e,
	}
}

// Init implements the HashRowContainer interface.
func (h *HashDiskRowContainer) Init(
	_ context.Context, shouldMark bool, typs []*types.T, storedEqCols columns, encodeNull bool,
) error {
	h.columnEncoder.init(typs, storedEqCols, encodeNull)
	h.shouldMark = shouldMark
	storedTypes := typs
	if h.shouldMark {
		// Add a boolean column to the end of the rows to implement marking rows.
		storedTypes = make([]*types.T, len(typs)+1)
		copy(storedTypes, typs)
		storedTypes[len(storedTypes)-1] = types.Bool

		h.scratchEncRow = make(rowenc.EncDatumRow, len(storedTypes))
		// Initialize the last column of the scratch row we use in AddRow() to
		// be unmarked.
		h.scratchEncRow[len(h.scratchEncRow)-1] = rowenc.DatumToEncDatum(
			types.Bool,
			tree.MakeDBool(false),
		)
	}

	h.DiskRowContainer = MakeDiskRowContainer(h.diskMonitor, storedTypes, storedEqColsToOrdering(storedEqCols), h.engine)
	return nil
}

// AddRow adds a row to the HashDiskRowContainer. This row is unmarked by
// default.
func (h *HashDiskRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
	var err error
	if h.shouldMark {
		// len(h.scratchEncRow) == len(row) + 1 if h.shouldMark == true. The
		// last column has been initialized to a false mark in Init().
		copy(h.scratchEncRow, row)
		err = h.DiskRowContainer.AddRow(ctx, h.scratchEncRow)
	} else {
		err = h.DiskRowContainer.AddRow(ctx, row)
	}
	return err
}

// IsEmpty implements the HashRowContainer interface.
func (h *HashDiskRowContainer) IsEmpty() bool {
	return h.DiskRowContainer.Len() == 0
}

// hashDiskRowBucketIterator iterates over the rows in a bucket.
type hashDiskRowBucketIterator struct {
	*diskRowIterator
	*HashDiskRowContainer
	probeEqCols columns
	// haveMarkedRows returns true if we've marked rows since the last time we
	// recreated our underlying diskRowIterator.
	haveMarkedRows bool
	// encodedEqCols is the encoding of the equality columns of the rows in the
	// bucket that this iterator iterates over.
	encodedEqCols []byte
	// Temporary buffer used for constructed marked values.
	tmpBuf []byte
}

var _ RowMarkerIterator = &hashDiskRowBucketIterator{}

// NewBucketIterator implements the HashRowContainer interface.
func (h *HashDiskRowContainer) NewBucketIterator(
	ctx context.Context, row rowenc.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	ret := &hashDiskRowBucketIterator{
		HashDiskRowContainer: h,
		probeEqCols:          probeEqCols,
		diskRowIterator:      h.NewIterator(ctx).(*diskRowIterator),
	}
	if err := ret.Reset(ctx, row); err != nil {
		ret.Close()
		return nil, err
	}
	return ret, nil
}

// Rewind implements the RowIterator interface.
func (i *hashDiskRowBucketIterator) Rewind() {
	i.SeekGE(i.encodedEqCols)
}

// Valid implements the RowIterator interface.
func (i *hashDiskRowBucketIterator) Valid() (bool, error) {
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return ok, err
	}
	// Since the underlying map is sorted, once the key prefix does not equal
	// the encoded equality columns, we have gone past the end of the bucket.
	return bytes.HasPrefix(i.UnsafeKey(), i.encodedEqCols), nil
}

// Row implements the RowIterator interface.
func (i *hashDiskRowBucketIterator) Row() (rowenc.EncDatumRow, error) {
	row, err := i.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}

	// Remove the mark from the end of the row.
	if i.HashDiskRowContainer.shouldMark {
		row = row[:len(row)-1]
	}
	return row, nil
}

func (i *hashDiskRowBucketIterator) Reset(ctx context.Context, row rowenc.EncDatumRow) error {
	encoded, err := i.HashDiskRowContainer.encodeEqualityCols(ctx, row, i.probeEqCols)
	if err != nil {
		return err
	}
	i.encodedEqCols = append(i.encodedEqCols[:0], encoded...)
	if i.haveMarkedRows {
		// We have to recreate our iterator if we need to flush marks to disk.
		// TODO(jordan): do this less by keeping a cache of written marks.
		i.haveMarkedRows = false
		i.diskRowIterator.Close()
		i.diskRowIterator = i.HashDiskRowContainer.NewIterator(ctx).(*diskRowIterator)
	}
	return nil
}

// IsMarked implements the RowMarkerIterator interface.
func (i *hashDiskRowBucketIterator) IsMarked(ctx context.Context) bool {
	if !i.HashDiskRowContainer.shouldMark {
		log.Fatal(ctx, "hash disk row container not set up for marking")
	}
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return false
	}

	rowVal := i.UnsafeValue()
	return bytes.Equal(rowVal[len(rowVal)-len(encodedTrue):], encodedTrue)
}

// Mark implements the RowMarkerIterator interface.
func (i *hashDiskRowBucketIterator) Mark(ctx context.Context) error {
	if !i.HashDiskRowContainer.shouldMark {
		log.Fatal(ctx, "hash disk row container not set up for marking")
	}
	i.haveMarkedRows = true
	markBytes := encodedTrue
	// rowVal are the non-equality encoded columns, the last of which is the
	// column we use to mark a row.
	rowVal := append(i.tmpBuf[:0], i.UnsafeValue()...)
	originalLen := len(rowVal)
	rowVal = append(rowVal, markBytes...)

	// Write the new encoding of mark over the old encoding of mark and truncate
	// the extra bytes.
	copy(rowVal[originalLen-len(markBytes):], rowVal[originalLen:])
	rowVal = rowVal[:originalLen]
	i.tmpBuf = rowVal

	// These marks only matter when using a hashDiskRowIterator to iterate over
	// unmarked rows. The writes are flushed when creating a NewIterator() in
	// NewUnmarkedIterator().
	return i.HashDiskRowContainer.bufferedRows.Put(i.UnsafeKey(), rowVal)
}

// hashDiskRowIterator iterates over all unmarked rows in a
// HashDiskRowContainer.
type hashDiskRowIterator struct {
	*diskRowIterator
}

var _ RowIterator = &hashDiskRowIterator{}

// NewUnmarkedIterator implements the HashRowContainer interface.
func (h *HashDiskRowContainer) NewUnmarkedIterator(ctx context.Context) RowIterator {
	if h.shouldMark {
		return &hashDiskRowIterator{
			diskRowIterator: h.NewIterator(ctx).(*diskRowIterator),
		}
	}
	return h.NewIterator(ctx)
}

// Rewind implements the RowIterator interface.
func (i *hashDiskRowIterator) Rewind() {
	i.diskRowIterator.Rewind()
	// If the current row is marked, move the iterator to the next unmarked row.
	if i.isRowMarked() {
		i.Next()
	}
}

// Next implements the RowIterator interface.
func (i *hashDiskRowIterator) Next() {
	i.diskRowIterator.Next()
	for i.isRowMarked() {
		i.diskRowIterator.Next()
	}
}

// Row implements the RowIterator interface.
func (i *hashDiskRowIterator) Row() (rowenc.EncDatumRow, error) {
	row, err := i.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}

	// Remove the mark from the end of the row.
	row = row[:len(row)-1]
	return row, nil
}

// isRowMarked returns true if the current row is marked or false if it wasn't
// marked or there was an error establishing the row's validity. Subsequent
// calls to Valid() will uncover this error.
func (i *hashDiskRowIterator) isRowMarked() bool {
	// isRowMarked is not necessarily called after Valid().
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return false
	}

	rowVal := i.UnsafeValue()
	return bytes.Equal(rowVal[len(rowVal)-len(encodedTrue):], encodedTrue)
}

// HashDiskBackedRowContainer is a hashRowContainer that uses a
// HashMemRowContainer to store rows and spills to disk automatically if memory
// usage exceeds a given budget. When spilled to disk, the rows are stored with
// an extra boolean column to keep track of that row's mark.
type HashDiskBackedRowContainer struct {
	// src is the current hashRowContainer that is being used to store rows.
	// All the hashRowContainer methods are redefined rather than delegated
	// to an embedded struct because of how defer works:
	//  rc.init(...)
	//  defer rc.Close(ctx)
	// Close will call HashMemRowContainer.Close(ctx) even after spilling to
	// disk.
	src HashRowContainer

	hmrc *HashMemRowContainer
	hdrc *HashDiskRowContainer

	// shouldMark specifies whether the caller cares about marking rows.
	shouldMark   bool
	types        []*types.T
	storedEqCols columns
	encodeNull   bool

	evalCtx       *tree.EvalContext
	memoryMonitor *mon.BytesMonitor
	diskMonitor   *mon.BytesMonitor
	engine        diskmap.Factory
	scratchEncRow rowenc.EncDatumRow

	// allRowsIterators keeps track of all iterators created via
	// NewAllRowsIterator(). If the container spills to disk, these become
	// invalid, so the container actively recreates the iterators, advances them
	// to appropriate positions, and updates each iterator in-place.
	allRowsIterators []*AllRowsIterator
}

var _ HashRowContainer = &HashDiskBackedRowContainer{}

// NewHashDiskBackedRowContainer makes a HashDiskBackedRowContainer.
func NewHashDiskBackedRowContainer(
	evalCtx *tree.EvalContext,
	memoryMonitor *mon.BytesMonitor,
	diskMonitor *mon.BytesMonitor,
	engine diskmap.Factory,
) *HashDiskBackedRowContainer {
	return &HashDiskBackedRowContainer{
		evalCtx:          evalCtx,
		memoryMonitor:    memoryMonitor,
		diskMonitor:      diskMonitor,
		engine:           engine,
		allRowsIterators: make([]*AllRowsIterator, 0, 1),
	}
}

// Init implements the hashRowContainer interface.
func (h *HashDiskBackedRowContainer) Init(
	ctx context.Context, shouldMark bool, types []*types.T, storedEqCols columns, encodeNull bool,
) error {
	h.shouldMark = shouldMark
	h.types = types
	h.storedEqCols = storedEqCols
	h.encodeNull = encodeNull
	if shouldMark {
		// We might need to preserve the marks when spilling to disk which requires
		// adding an extra boolean column to the row when read from memory.
		h.scratchEncRow = make(rowenc.EncDatumRow, len(types)+1)
	}

	hmrc := MakeHashMemRowContainer(h.evalCtx, h.memoryMonitor, types, storedEqCols)
	h.hmrc = &hmrc
	h.src = h.hmrc
	if err := h.hmrc.Init(ctx, shouldMark, types, storedEqCols, encodeNull); err != nil {
		if spilled, spillErr := h.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
	}

	return nil
}

// AddRow adds a row to the HashDiskBackedRowContainer. This row is unmarked by default.
func (h *HashDiskBackedRowContainer) AddRow(ctx context.Context, row rowenc.EncDatumRow) error {
	if err := h.src.AddRow(ctx, row); err != nil {
		if spilled, spillErr := h.spillIfMemErr(ctx, err); !spilled && spillErr == nil {
			// The error was not an out of memory error.
			return err
		} else if spillErr != nil {
			// A disk spill was attempted but there was an error in doing so.
			return spillErr
		}
		// Add the row that caused the memory error.
		return h.src.AddRow(ctx, row)
	}
	return nil
}

// IsEmpty implements the HashRowContainer interface.
func (h *HashDiskBackedRowContainer) IsEmpty() bool {
	return h.src.IsEmpty()
}

// Close implements the HashRowContainer interface.
func (h *HashDiskBackedRowContainer) Close(ctx context.Context) {
	if h.hdrc != nil {
		h.hdrc.Close(ctx)
	}
	h.hmrc.Close(ctx)
}

// UsingDisk returns whether or not the HashDiskBackedRowContainer is currently
// using disk.
func (h *HashDiskBackedRowContainer) UsingDisk() bool {
	return h.hdrc != nil
}

// ReserveMarkMemoryMaybe attempts to reserve memory for marks if we're using
// an in-memory container at the moment. If there is not enough memory left, it
// spills to disk.
func (h *HashDiskBackedRowContainer) ReserveMarkMemoryMaybe(ctx context.Context) error {
	if !h.UsingDisk() {
		// We're assuming that the disk space is infinite, so we only need to
		// reserve the memory for marks if we're using in-memory container.
		if err := h.hmrc.ReserveMarkMemoryMaybe(ctx); err != nil {
			return h.SpillToDisk(ctx)
		}
	}
	return nil
}

// spillIfMemErr checks err and calls SpillToDisk if the given err is an out of
// memory error. Returns whether the HashDiskBackedRowContainer spilled to disk
// and an error if one occurred while doing so.
func (h *HashDiskBackedRowContainer) spillIfMemErr(ctx context.Context, err error) (bool, error) {
	if !sqlerrors.IsOutOfMemoryError(err) {
		return false, nil
	}
	if spillErr := h.SpillToDisk(ctx); spillErr != nil {
		return false, spillErr
	}
	log.VEventf(ctx, 2, "spilled to disk: %v", err)
	return true, nil
}

// SpillToDisk creates a disk row container, injects all the data from the
// in-memory container into it, and clears the in-memory one afterwards.
func (h *HashDiskBackedRowContainer) SpillToDisk(ctx context.Context) error {
	if h.UsingDisk() {
		return errors.New("already using disk")
	}
	hdrc := MakeHashDiskRowContainer(h.diskMonitor, h.engine)
	if err := hdrc.Init(ctx, h.shouldMark, h.types, h.storedEqCols, h.encodeNull); err != nil {
		return err
	}

	// We compute the "current" keys of the iterators that will need to be
	// recreated.
	if err := h.computeKeysForAllRowsIterators(); err != nil {
		return err
	}

	// rowIdx is used to look up the mark on the row and is only updated and used
	// if marks are present.
	rowIdx := 0
	i := h.hmrc.NewFinalIterator(ctx)
	defer i.Close()
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return err
		}
		if h.shouldMark && h.hmrc.marked != nil {
			// We need to preserve the mark on this row.
			copy(h.scratchEncRow, row)
			h.scratchEncRow[len(h.types)] = rowenc.EncDatum{Datum: tree.MakeDBool(tree.DBool(h.hmrc.marked[rowIdx]))}
			row = h.scratchEncRow
			rowIdx++
		}
		if err := hdrc.AddRow(ctx, row); err != nil {
			return err
		}
	}
	h.hmrc.Clear(ctx)

	h.src = &hdrc
	h.hdrc = &hdrc

	return h.recreateAllRowsIterators(ctx)
}

// NewBucketIterator implements the hashRowContainer interface.
func (h *HashDiskBackedRowContainer) NewBucketIterator(
	ctx context.Context, row rowenc.EncDatumRow, probeEqCols columns,
) (RowMarkerIterator, error) {
	return h.src.NewBucketIterator(ctx, row, probeEqCols)
}

// NewUnmarkedIterator implements the hashRowContainer interface.
func (h *HashDiskBackedRowContainer) NewUnmarkedIterator(ctx context.Context) RowIterator {
	return h.src.NewUnmarkedIterator(ctx)
}

// Sort sorts the underlying row container based on stored equality columns
// which forces all rows from the same hash bucket to be contiguous.
func (h *HashDiskBackedRowContainer) Sort(ctx context.Context) {
	if !h.UsingDisk() && len(h.storedEqCols) > 0 {
		// We need to explicitly sort only if we're using in-memory container since
		// if we're using disk, the underlying sortedDiskMap will be sorted
		// already.
		h.hmrc.Sort(ctx)
	}
}

// AllRowsIterator iterates over all rows in HashDiskBackedRowContainer which
// should be initialized to not do marking. This iterator will be recreated
// in-place if the container spills to disk.
type AllRowsIterator struct {
	RowIterator

	container *HashDiskBackedRowContainer
}

// Close implements RowIterator interface.
func (i *AllRowsIterator) Close() {
	i.RowIterator.Close()
	for j, iterator := range i.container.allRowsIterators {
		if i == iterator {
			i.container.allRowsIterators = append(i.container.allRowsIterators[:j], i.container.allRowsIterators[j+1:]...)
			return
		}
	}
}

// NewAllRowsIterator creates AllRowsIterator that can iterate over all rows
// (equivalent to an unmarked iterator when the container doesn't do marking)
// and will be recreated if the spilling to disk occurs.
func (h *HashDiskBackedRowContainer) NewAllRowsIterator(
	ctx context.Context,
) (*AllRowsIterator, error) {
	if h.shouldMark {
		return nil, errors.Errorf("AllRowsIterator can only be created when the container doesn't do marking")
	}
	i := AllRowsIterator{h.src.NewUnmarkedIterator(ctx), h}
	h.allRowsIterators = append(h.allRowsIterators, &i)
	return &i, nil
}

func (h *HashDiskBackedRowContainer) computeKeysForAllRowsIterators() error {
	var oldIterator *hashMemRowIterator
	var ok bool
	for _, iterator := range h.allRowsIterators {
		if oldIterator, ok = (*iterator).RowIterator.(*hashMemRowIterator); !ok {
			return errors.Errorf("the iterator is unexpectedly not hashMemRowIterator")
		}
		if err := oldIterator.computeKey(); err != nil {
			return err
		}
	}
	return nil
}

func (h *HashDiskBackedRowContainer) recreateAllRowsIterators(ctx context.Context) error {
	var oldIterator *hashMemRowIterator
	var ok bool
	for _, iterator := range h.allRowsIterators {
		if oldIterator, ok = (*iterator).RowIterator.(*hashMemRowIterator); !ok {
			return errors.Errorf("the iterator is unexpectedly not hashMemRowIterator")
		}
		newIterator := h.NewUnmarkedIterator(ctx)
		newIterator.(*diskRowIterator).SeekGE(oldIterator.curKey)
		(*iterator).RowIterator.Close()
		iterator.RowIterator = newIterator
	}
	return nil
}
