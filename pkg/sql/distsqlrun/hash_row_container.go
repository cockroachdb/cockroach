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
//
// Author: Alfonso Subiotto Marqués

package distsqlrun

import (
	"bytes"
	"fmt"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// rowMarkerIterator is a rowIterator that can be used to mark rows.
type rowMarkerIterator interface {
	rowIterator
	Mark(ctx context.Context, mark bool) error
}

// hashRowContainer is a container used to store rows according to an encoding
// of given equality columns. The stored rows can then be probed to return a
// bucket of matching rows. Additionally, each stored row can be marked and all
// rows that are unmarked can be iterated over. An example of where this is
// useful is in full/outer joins. The caller can mark all matched rows and
// iterate over the unmarked rows to produce a result.
type hashRowContainer interface {
	// Init initializes the hashRowContainer with the given equality columns.
	// 	- storedEqCols are the equality columns of rows stored in this
	// 	  container.
	// 	  i.e. when adding a row, the columns specified by storedEqCols are used
	// 	  to get the bucket that the row should be added to.
	// 	- probeEqCols are the equality columns of rows used to probe the rows
	// 	  stored in this container.
	// 	  i.e. when probing for matching rows given a probe row, the columns
	// 	  specified by probeEqCols are used to get the bucket of matching rows.
	Init(ctx context.Context, storedEqCols, probeEqCols columns) error
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
	NewBucketIterator(context.Context, sqlbase.EncDatumRow) (rowMarkerIterator, error)

	// NewUnmarkedIterator returns a rowIterator that iterates over unmarked
	// rows.
	NewUnmarkedIterator(context.Context) rowIterator

	// Close frees up resources held by the hashRowContainer.
	Close(context.Context)
}

// columnEncoder is a utility struct used by implementations of hashRowContainer
// to encode equality columns, the result of which is used as a key to a bucket.
type columnEncoder struct {
	scratch    []byte
	datumAlloc sqlbase.DatumAlloc
}

// TODO(asubiotto): This logic could be shared with the diskRowContainer.
func (e columnEncoder) encodeEqualityCols(
	ctx context.Context, row sqlbase.EncDatumRow, eqCols columns,
) ([]byte, error) {
	encoded, hasNull, err := encodeColumnsOfRow(
		&e.datumAlloc, e.scratch, row, eqCols, false, /* encodeNull */
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
// current usage of hashMemRowContainer and allows us to assume that a memory
// error can only occur at the start of the marking phase, thus not having to
// deal with half-emitted buckets and marks when falling back to disk.
type hashMemRowContainer struct {
	// TODO(asubiotto): This memRowContainer then has an underlying
	// sqlbase.RowContainer. This can be cleaned up.
	*memRowContainer
	columnEncoder

	// marked specifies for each row in memRowContainer whether that row has
	// been marked. Used for iterating over unmarked rows.
	marked []bool

	// buckets contains the indices into memRowContainer for a given group
	// key (which is the encoding of storedEqCols).
	buckets map[string][]int
	// bucketsAcc is the memory account for the buckets. The datums themselves
	// are all in the memRowContainer.
	bucketsAcc mon.BoundAccount

	// {stored, probe}eqCols contain the indices of the columns of a row that
	// are encoded and used as a key into buckets. probeEqCols are used in
	// NewBucketIterator(), and storedEqCols are used in AddRow().
	storedEqCols columns
	probeEqCols  columns
}

var _ hashRowContainer = &hashMemRowContainer{}

// makeHashMemRowContainer creates a hashMemRowContainer from the given
// rowContainer. This rowContainer must still be Close()d by the caller.
func makeHashMemRowContainer(
	ctx context.Context, rowContainer *memRowContainer,
) hashMemRowContainer {
	return hashMemRowContainer{
		memRowContainer: rowContainer,
		buckets:         make(map[string][]int),
		bucketsAcc:      rowContainer.evalCtx.Mon.MakeBoundAccount(),
	}
}

// Init implements the hashRowContainer interface.
func (h *hashMemRowContainer) Init(ctx context.Context, storedEqCols, probeEqCols columns) error {
	if h.storedEqCols != nil || h.probeEqCols != nil {
		return errors.New("hashMemRowContainer has already been initialized")
	}

	h.storedEqCols = storedEqCols
	h.probeEqCols = probeEqCols

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
	ctx context.Context, row sqlbase.EncDatumRow,
) (rowMarkerIterator, error) {
	encoded, err := h.encodeEqualityCols(ctx, row, h.probeEqCols)
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

// Mark implements the rowMarkerIterator interface.
func (i *hashMemRowBucketIterator) Mark(ctx context.Context, mark bool) error {
	if i.marked == nil {
		if err := i.bucketsAcc.Grow(ctx, sizeOfBoolSlice+(sizeOfBool*int64(i.Len()))); err != nil {
			return err
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

// hashDiskRowContainer is an in-memory implementation of a hashRowContainer.
// The rows are stored in an underlying diskRowContainer with an extra boolean
// column to keep track of that row's mark.
type hashDiskRowContainer struct {
	diskRowContainer
	columnEncoder

	rowContainer *memRowContainer
	engine       engine.Engine
	probeEqCols  columns
}

var _ hashRowContainer = &hashDiskRowContainer{}

var (
	encodedTrue  = encoding.EncodeBoolValue(nil, encoding.NoColumnID, true)
	encodedFalse = encoding.EncodeBoolValue(nil, encoding.NoColumnID, false)
)

// makeHashDiskRowContainer creates a hashDiskRowContainer with the rows from
// the given rowContainer. The rowContainer is not consumed until after a call
// to Init().
// The caller must still Close() the rowContainer.
// Arguments:
// 	- rowContainer contains the initial set of rows that this
// 	  hashDiskRowContainer will be initialized with.
// 	- e is the underlying store that rows are stored on.
func makeHashDiskRowContainer(rowContainer *memRowContainer, e engine.Engine) hashDiskRowContainer {
	return hashDiskRowContainer{rowContainer: rowContainer, engine: e}
}

// Init implements the hashRowContainer interface. Note that Init() consumes
// rows from the rowContainer and deletes them.
func (h *hashDiskRowContainer) Init(ctx context.Context, storedEqCols, probeEqCols columns) error {
	if h.probeEqCols != nil {
		return errors.New("hashDiskRowContainer has already been initialized")
	}
	// Provide the diskRowContainer with an ordering of the equality columns of
	// the rows that we will store. This will result in rows with the
	// same equality columns ocurring contiguously in the keyspace.
	ordering := make(sqlbase.ColumnOrdering, len(storedEqCols))
	for i := range ordering {
		ordering[i] = sqlbase.ColumnOrderInfo{
			ColIdx:    int(storedEqCols[i]),
			Direction: encoding.Ascending,
		}
	}

	// Add a boolean column to the end of the rows to implement marking rows.
	types := make([]sqlbase.ColumnType, len(h.rowContainer.types)+1)
	copy(types, h.rowContainer.types)
	types[len(types)-1] = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}

	// Make a diskRowContainer but manually transfer the rows because of the
	// new schema.
	diskRows, err := makeDiskRowContainer(ctx, types, ordering, memRowContainer{}, h.engine)
	if err != nil {
		return err
	}

	i := h.rowContainer.NewIterator(ctx)
	defer i.Close()

	h.diskRowContainer = diskRows
	h.probeEqCols = probeEqCols
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
		if err := h.AddRow(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

// AddRow adds a row to the hashDiskRowContainer. This row is unmarked by
// default.
func (h *hashDiskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	return h.diskRowContainer.AddRow(
		ctx,
		append(
			row,
			sqlbase.DatumToEncDatum(
				sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL},
				parser.MakeDBool(false),
			),
		),
	)
}

// hashDiskRowBucketIterator iterates over the rows in a bucket.
type hashDiskRowBucketIterator struct {
	diskRowIterator
	// put writes the value for the given key to the underlying
	// diskRowContainer.
	put func(key []byte, val []byte) error
	// encodedEqCols is the encoding of the equality columns of the rows in the
	// bucket that this iterator iterates over.
	encodedEqCols []byte
}

var _ rowMarkerIterator = hashDiskRowBucketIterator{}

// NewBucketIterator implements the hashRowContainer interface.
func (h *hashDiskRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow,
) (rowMarkerIterator, error) {
	encoded, err := h.encodeEqualityCols(ctx, row, h.probeEqCols)
	if err != nil {
		return nil, err
	}
	encodedEqCols := make([]byte, len(encoded))
	copy(encodedEqCols, encoded)

	return hashDiskRowBucketIterator{
		diskRowIterator: h.NewIterator(ctx).(diskRowIterator),
		put:             h.bufferedRows.Put,
		encodedEqCols:   encodedEqCols,
	}, nil
}

// Rewind implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Rewind() {
	i.Seek(i.encodedEqCols)
}

// Valid implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Valid() (bool, error) {
	ok, err := i.diskRowIterator.Valid()
	if err != nil {
		return false, err
	}
	// TODO(asubiotto): Make UnsafeKey() and UnsafeValue() part of the
	// SortedDiskMapIterator interface to avoid allocation here, in SetMarks(),
	// and isRowMarked().
	if ok && !bytes.HasPrefix(i.Key(), i.encodedEqCols) {
		return false, nil
	}
	return ok, nil
}

// Row implements the rowIterator interface.
func (i hashDiskRowBucketIterator) Row() (sqlbase.EncDatumRow, error) {
	row, err := i.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}

	// Remove the mark from the end of the row.
	row = row[:len(row)-1]
	return row, nil
}

func (i hashDiskRowBucketIterator) Mark(ctx context.Context, mark bool) error {
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

	return i.put(i.Key(), rowVal)
}

// hashDiskRowIterator iterates over all unmarked rows in a
// hashDiskRowContainer.
type hashDiskRowIterator struct {
	diskRowIterator
}

var _ rowIterator = hashDiskRowIterator{}

// NewUnmarkedIterator implements the hashRowContainer interface.
func (h hashDiskRowContainer) NewUnmarkedIterator(ctx context.Context) rowIterator {
	return hashDiskRowIterator{
		diskRowIterator: h.NewIterator(ctx).(diskRowIterator),
	}
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

	fmt.Println("returning row", row)

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
