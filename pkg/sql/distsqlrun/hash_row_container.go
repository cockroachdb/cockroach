// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/pkg/errors"
)

// rowMarkerIterator is a rowIterator that can be used to mark rows.
type rowMarkerIterator interface {
	rowIterator
	Mark(ctx context.Context, mark bool) error
	IsMarked(ctx context.Context) bool
}

// hashRowContainer is a container used to store rows according to an encoding
// of given equality columns. The stored rows can then be probed to return a
// bucket of matching rows. Additionally, each stored row can be marked and all
// rows that are unmarked can be iterated over. An example of where this is
// useful is in full/outer joins. The caller can mark all matched rows and
// iterate over the unmarked rows to produce a result.
type hashRowContainer interface {
	// Init initializes the hashRowContainer with the given equality columns.
	//	- shouldMark specifies whether the caller cares about marking rows. If
	//	  not, the hashRowContainer will not perform any row marking logic. This
	//	  is meant to optimize space usage and runtime.
	//	- types is the schema of rows that will be added to this container.
	//	- storedEqCols are the equality columns of rows stored in this
	// 	  container.
	// 	  i.e. when adding a row, the columns specified by storedEqCols are used
	// 	  to get the bucket that the row should be added to.
	Init(
		ctx context.Context, shouldMark bool, types []sqlbase.ColumnType, storedEqCols columns,
	) error
	AddRow(context.Context, sqlbase.EncDatumRow) error

	// NewBucketIterator returns a rowMarkerIterator that iterates over a bucket
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
		ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
	) (rowMarkerIterator, error)

	// NewUnmarkedIterator returns a rowIterator that iterates over unmarked
	// rows. If shouldMark was false in Init(), this iterator iterates over all
	// rows.
	NewUnmarkedIterator(context.Context) rowIterator

	// Close frees up resources held by the hashRowContainer.
	Close(context.Context)
}

// columnEncoder is a utility struct used by implementations of hashRowContainer
// to encode equality columns, the result of which is used as a key to a bucket.
type columnEncoder struct {
	scratch []byte
	// types for the "key" columns (equality columns)
	keyTypes   []sqlbase.ColumnType
	datumAlloc sqlbase.DatumAlloc
}

func (e *columnEncoder) init(types []sqlbase.ColumnType, keyCols columns) {
	e.keyTypes = make([]sqlbase.ColumnType, len(keyCols))
	for i, c := range keyCols {
		e.keyTypes[i] = types[c]
	}
}

// encodeEqualityCols returns the encoding of the specified columns of the given
// row. The returned byte slice is only valid until the next call to
// encodeEqualityColumns().
// TODO(asubiotto): This logic could be shared with the diskRowContainer.
func (e *columnEncoder) encodeEqualityCols(
	ctx context.Context, row sqlbase.EncDatumRow, eqCols columns,
) ([]byte, error) {
	encoded, hasNull, err := encodeColumnsOfRow(
		&e.datumAlloc, e.scratch, row, eqCols, e.keyTypes, false, /* encodeNull */
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

const sizeOfBucket = int64(unsafe.Sizeof([]int{}))
const sizeOfRowIdx = int64(unsafe.Sizeof(int(0)))
const sizeOfBoolSlice = int64(unsafe.Sizeof([]bool{}))
const sizeOfBool = int64(unsafe.Sizeof(false))

// hashMemRowContainer is an in-memory implementation of a hashRowContainer.
// The rows are stored in an underlying memRowContainer and an accompanying
// map stores the mapping from equality column encodings to indices in the
// memRowContainer corresponding to matching rows.
// NOTE: Once a row is marked, adding more rows to the hashMemRowContainer
// results in undefined behavior. It is not necessary to do otherwise for the
// current usage of hashMemRowContainer.
type hashMemRowContainer struct {
	*memRowContainer
	columnEncoder

	// shouldMark specifies whether the caller cares about marking rows. If not,
	// marked is never initialized.
	shouldMark bool

	// marked specifies for each row in memRowContainer whether that row has
	// been marked. Used for iterating over unmarked rows.
	marked []bool

	// markMemoryReserved specifies whether the hashMemRowContainer's memory
	// account already accounts for the memory needed to mark the rows in the
	// hashMemRowContainer.
	markMemoryReserved bool

	// buckets contains the indices into memRowContainer for a given group
	// key (which is the encoding of storedEqCols).
	buckets map[string][]int
	// bucketsAcc is the memory account for the buckets. The datums themselves
	// are all in the memRowContainer.
	bucketsAcc mon.BoundAccount

	// storedEqCols contains the indices of the columns of a row that are
	// encoded and used as a key into buckets when adding a row.
	storedEqCols columns
}

var _ hashRowContainer = &hashMemRowContainer{}

// makeHashMemRowContainer creates a hashMemRowContainer from the given
// rowContainer. This rowContainer must still be Close()d by the caller.
func makeHashMemRowContainer(rowContainer *memRowContainer) hashMemRowContainer {
	return hashMemRowContainer{
		memRowContainer: rowContainer,
		buckets:         make(map[string][]int),
		bucketsAcc:      rowContainer.evalCtx.Mon.MakeBoundAccount(),
	}
}

// Init implements the hashRowContainer interface. types is ignored because the
// schema is inferred from the memRowContainer.
func (h *hashMemRowContainer) Init(
	ctx context.Context, shouldMark bool, _ []sqlbase.ColumnType, storedEqCols columns,
) error {
	if h.storedEqCols != nil {
		return errors.New("hashMemRowContainer has already been initialized")
	}
	h.columnEncoder.init(h.memRowContainer.types, storedEqCols)
	h.shouldMark = shouldMark
	h.storedEqCols = storedEqCols

	// Build buckets from the rowContainer.
	for rowIdx := 0; rowIdx < h.Len(); rowIdx++ {
		if err := h.addRowToBucket(ctx, h.EncRow(rowIdx), rowIdx); err != nil {
			return err
		}
	}
	return nil
}

// AddRow adds a row to the hashMemRowContainer. This row is unmarked by default.
func (h *hashMemRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	rowIdx := h.Len()
	if err := h.memRowContainer.AddRow(ctx, row); err != nil {
		return err
	}
	return h.addRowToBucket(ctx, row, rowIdx)
}

// Close implements the hashRowContainer interface.
func (h *hashMemRowContainer) Close(ctx context.Context) {
	h.bucketsAcc.Close(ctx)
}

// addRowToBucket is a helper function that encodes the equality columns of the
// given row and appends the rowIdx to the matching bucket.
func (h *hashMemRowContainer) addRowToBucket(
	ctx context.Context, row sqlbase.EncDatumRow, rowIdx int,
) error {
	encoded, err := h.encodeEqualityCols(ctx, row, h.storedEqCols)
	if err != nil {
		return err
	}

	_, ok := h.buckets[string(encoded)]

	usage := sizeOfRowIdx
	if !ok {
		usage += int64(len(encoded))
		usage += sizeOfBucket
	}

	if err := h.bucketsAcc.Grow(ctx, usage); err != nil {
		return err
	}

	h.buckets[string(encoded)] = append(h.buckets[string(encoded)], rowIdx)
	return nil
}

// reserveMarkMemoryMaybe is a utility function to grow the
// hashMemRowContainer's memory account by the memory needed to mark all rows.
// It is a noop if h.markMemoryReserved is true.
func (h *hashMemRowContainer) reserveMarkMemoryMaybe(ctx context.Context) error {
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
	*hashMemRowContainer
	// rowIdxs are the indices of rows in the bucket.
	rowIdxs []int
	curIdx  int
}

var _ rowMarkerIterator = &hashMemRowBucketIterator{}

// NewBucketIterator implements the hashRowContainer interface.
func (h *hashMemRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (rowMarkerIterator, error) {
	encoded, err := h.encodeEqualityCols(ctx, row, probeEqCols)
	if err != nil {
		return nil, err
	}

	return &hashMemRowBucketIterator{hashMemRowContainer: h, rowIdxs: h.buckets[string(encoded)]}, nil
}

// Rewind implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Rewind() {
	i.curIdx = 0
}

// Valid implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Valid() (bool, error) {
	return i.curIdx < len(i.rowIdxs), nil
}

// Next implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Next() {
	i.curIdx++
}

// Row implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(i.rowIdxs[i.curIdx]), nil
}

// IsMarked implements the rowMarkerIterator interface.
func (i *hashMemRowBucketIterator) IsMarked(ctx context.Context) bool {
	if !i.shouldMark {
		log.Fatal(ctx, "hash mem row container not set up for marking")
	}
	if i.marked == nil {
		return false
	}

	return i.marked[i.rowIdxs[i.curIdx]]
}

// Mark implements the rowMarkerIterator interface.
func (i *hashMemRowBucketIterator) Mark(ctx context.Context, mark bool) error {
	if !i.shouldMark {
		log.Fatal(ctx, "hash mem row container not set up for marking")
	}
	if i.marked == nil {
		if !i.markMemoryReserved {
			panic("mark memory should have been reserved already")
		}
		i.marked = make([]bool, i.Len())
	}

	i.marked[i.rowIdxs[i.curIdx]] = mark
	return nil
}

// Close implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Close() {}

// hashMemRowIterator iterates over all unmarked rows in a hashMemRowContainer.
type hashMemRowIterator struct {
	*hashMemRowContainer
	curIdx int
}

var _ rowIterator = &hashMemRowIterator{}

// NewUnmarkedIterator implements the hashRowContainer interface.
func (h *hashMemRowContainer) NewUnmarkedIterator(ctx context.Context) rowIterator {
	return &hashMemRowIterator{hashMemRowContainer: h}
}

// Rewind implements the rowIterator interface.
func (i *hashMemRowIterator) Rewind() {
	i.curIdx = -1
	// Next will advance curIdx to the first unmarked row.
	i.Next()
}

// Valid implements the rowIterator interface.
func (i *hashMemRowIterator) Valid() (bool, error) {
	return i.curIdx < i.Len(), nil
}

// Next implements the rowIterator interface.
func (i *hashMemRowIterator) Next() {
	// Move the curIdx to the next unmarked row.
	i.curIdx++
	if i.marked != nil {
		for ; i.curIdx < len(i.marked) && i.marked[i.curIdx]; i.curIdx++ {
		}
	}
}

// Row implements the rowIterator interface.
func (i *hashMemRowIterator) Row() (sqlbase.EncDatumRow, error) {
	return i.EncRow(i.curIdx), nil
}

// Close implements the rowIterator interface.
func (i *hashMemRowIterator) Close() {}

// hashDiskRowContainer is an on-disk implementation of a hashRowContainer.
// The rows are stored in an underlying diskRowContainer with an extra boolean
// column to keep track of that row's mark.
type hashDiskRowContainer struct {
	diskRowContainer
	columnEncoder

	diskMonitor *mon.BytesMonitor
	// shouldMark specifies whether the caller cares about marking rows. If not,
	// rows are stored with one less column (which usually specifies that row's
	// mark).
	shouldMark    bool
	engine        engine.Engine
	scratchEncRow sqlbase.EncDatumRow
}

var _ hashRowContainer = &hashDiskRowContainer{}

var (
	encodedTrue  = encoding.EncodeBoolValue(nil, encoding.NoColumnID, true)
	encodedFalse = encoding.EncodeBoolValue(nil, encoding.NoColumnID, false)
)

// makeHashDiskRowContainer creates a hashDiskRowContainer with the given engine
// as the underlying store that rows are stored on. shouldMark specifies whether
// the hashDiskRowContainer should set itself up to mark rows.
func makeHashDiskRowContainer(diskMonitor *mon.BytesMonitor, e engine.Engine) hashDiskRowContainer {
	return hashDiskRowContainer{
		diskMonitor: diskMonitor,
		engine:      e,
	}
}

// Init implements the hashRowContainer interface.
func (h *hashDiskRowContainer) Init(
	ctx context.Context, shouldMark bool, types []sqlbase.ColumnType, storedEqCols columns,
) error {
	h.columnEncoder.init(types, storedEqCols)
	// Provide the diskRowContainer with an ordering on the equality columns of
	// the rows that we will store. This will result in rows with the
	// same equality columns ocurring contiguously in the keyspace.
	ordering := make(sqlbase.ColumnOrdering, len(storedEqCols))
	for i := range ordering {
		ordering[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    int(storedEqCols[i]),
			Direction: encoding.Ascending,
		}
	}

	h.shouldMark = shouldMark

	storedTypes := types
	if h.shouldMark {
		// Add a boolean column to the end of the rows to implement marking rows.
		storedTypes = make([]sqlbase.ColumnType, len(types)+1)
		copy(storedTypes, types)
		storedTypes[len(storedTypes)-1] = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}

		h.scratchEncRow = make(sqlbase.EncDatumRow, len(storedTypes))
		// Initialize the last column of the scratch row we use in AddRow() to
		// be unmarked.
		h.scratchEncRow[len(h.scratchEncRow)-1] = sqlbase.DatumToEncDatum(
			sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL},
			tree.MakeDBool(false),
		)
	}

	h.diskRowContainer = makeDiskRowContainer(ctx, h.diskMonitor, storedTypes, ordering, h.engine)
	return nil
}

// AddRow adds a row to the hashDiskRowContainer. This row is unmarked by
// default.
func (h *hashDiskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	var err error
	if h.shouldMark {
		// len(h.scratchEncRow) == len(row) + 1 if h.shouldMark == true. The
		// last column has been initialized to a false mark in Init().
		copy(h.scratchEncRow, row)
		err = h.diskRowContainer.AddRow(ctx, h.scratchEncRow)
	} else {
		err = h.diskRowContainer.AddRow(ctx, row)
	}
	return err
}

// hashDiskRowBucketIterator iterates over the rows in a bucket.
type hashDiskRowBucketIterator struct {
	diskRowIterator
	hashDiskRowContainer *hashDiskRowContainer
	// encodedEqCols is the encoding of the equality columns of the rows in the
	// bucket that this iterator iterates over.
	encodedEqCols []byte
}

var _ rowMarkerIterator = hashDiskRowBucketIterator{}

// NewBucketIterator implements the hashRowContainer interface.
func (h *hashDiskRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow, probeEqCols columns,
) (rowMarkerIterator, error) {
	encoded, err := h.encodeEqualityCols(ctx, row, probeEqCols)
	if err != nil {
		return nil, err
	}
	encodedEqCols := make([]byte, len(encoded))
	copy(encodedEqCols, encoded)

	return hashDiskRowBucketIterator{
		diskRowIterator:      h.NewIterator(ctx).(diskRowIterator),
		hashDiskRowContainer: h,
		encodedEqCols:        encodedEqCols,
	}, nil
}

// Rewind implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Rewind() {
	i.Seek(i.encodedEqCols)
}

// Valid implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Valid() (bool, error) {
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return ok, err
	}
	// Since the underlying map is sorted, once the key prefix does not equal
	// the encoded equality columns, we have gone past the end of the bucket.
	// TODO(asubiotto): Make UnsafeKey() and UnsafeValue() part of the
	// SortedDiskMapIterator interface to avoid allocation here, in Mark(), and
	// isRowMarked().
	return bytes.HasPrefix(i.Key(), i.encodedEqCols), nil
}

// Row implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Row() (sqlbase.EncDatumRow, error) {
	row, err := i.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}

	// Remove the mark from the end of the row.
	if i.hashDiskRowContainer.shouldMark {
		row = row[:len(row)-1]
	}
	return row, nil
}

// IsMarked implements the rowMarkerIterator interface.
func (i hashDiskRowBucketIterator) IsMarked(ctx context.Context) bool {
	if !i.hashDiskRowContainer.shouldMark {
		log.Fatal(ctx, "hash disk row container not set up for marking")
	}
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return false
	}

	rowVal := i.Value()
	return bytes.Equal(rowVal[len(rowVal)-len(encodedTrue):], encodedTrue)
}

// Mark implements the rowMarkerIterator interface.
func (i hashDiskRowBucketIterator) Mark(ctx context.Context, mark bool) error {
	if !i.hashDiskRowContainer.shouldMark {
		log.Fatal(ctx, "hash disk row container not set up for marking")
	}
	markBytes := encodedFalse
	if mark {
		markBytes = encodedTrue
	}
	// rowVal are the non-equality encoded columns, the last of which is the
	// column we use to mark a row.
	rowVal := i.Value()
	originalLen := len(rowVal)
	rowVal = append(rowVal, markBytes...)

	// Write the new encoding of mark over the old encoding of mark and truncate
	// the extra bytes.
	copy(rowVal[originalLen-len(markBytes):], rowVal[originalLen:])
	rowVal = rowVal[:originalLen]

	// These marks only matter when using a hashDiskRowIterator to iterate over
	// unmarked rows. The writes are flushed when creating a NewIterator() in
	// NewUnmarkedIterator().
	return i.hashDiskRowContainer.bufferedRows.Put(i.Key(), rowVal)
}

// hashDiskRowIterator iterates over all unmarked rows in a
// hashDiskRowContainer.
type hashDiskRowIterator struct {
	diskRowIterator
}

var _ rowIterator = hashDiskRowIterator{}

// NewUnmarkedIterator implements the hashRowContainer interface.
func (h *hashDiskRowContainer) NewUnmarkedIterator(ctx context.Context) rowIterator {
	if h.shouldMark {
		return hashDiskRowIterator{
			diskRowIterator: h.NewIterator(ctx).(diskRowIterator),
		}
	}
	return h.NewIterator(ctx)
}

// Rewind implements the rowIterator interface.
func (i hashDiskRowIterator) Rewind() {
	i.diskRowIterator.Rewind()
	// If the current row is marked, move the iterator to the next unmarked row.
	if i.isRowMarked() {
		i.Next()
	}
}

// Next implements the rowIterator interface.
func (i hashDiskRowIterator) Next() {
	i.diskRowIterator.Next()
	for i.isRowMarked() {
		i.diskRowIterator.Next()
	}
}

// Row implements the rowIterator interface.
func (i hashDiskRowIterator) Row() (sqlbase.EncDatumRow, error) {
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
func (i hashDiskRowIterator) isRowMarked() bool {
	// isRowMarked is not necessarily called after Valid().
	ok, err := i.diskRowIterator.Valid()
	if !ok || err != nil {
		return false
	}

	rowVal := i.Value()
	return bytes.Equal(rowVal[len(rowVal)-len(encodedTrue):], encodedTrue)
}
