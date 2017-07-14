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
	"unsafe"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// hashRowContainer is a container used to store rows according to an encoding
// of given equality columns. The stored rows can then be probed to return a
// bucket of matching rows. Additionally, each stored row can be marked and all
// rows that are unmarked can be iterated over. An example of where this is
// useful is in full/outer joins. The caller can mark all matched rows and
// iterate over the unmarked rows to produce a result.
type hashRowContainer interface {
	AddRow(context.Context, sqlbase.EncDatumRow) error
	// SetMarks marks all rows that match the given row on equality columns.
	// The marks must be provided in the same order that rows are iterated over
	// by NewBucketIterator().
	// TODO(asubiotto): Switch this to mark through the iterator. Will avoid the
	// caller having to allocate marks.
	SetMarks(context.Context, sqlbase.EncDatumRow, []bool) error

	// NewBucketIterator returns a rowIterator that iterates over a bucket of
	// rows that match the given row on equality columns.
	NewBucketIterator(context.Context, sqlbase.EncDatumRow) (rowIterator, error)

	// NewIterator returns a rowIterator that iterates over unmarked rows.
	NewIterator(context.Context) rowIterator

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
// NOTE: Once SetMarks() is called, adding more rows to the hashMemRowContainer
// results in undefined behavior. This behavior might be surprising but it is
// not necessary to do otherwise for the current usage of hashMemRowContainer.
// Changing hashMemRowContainer.marked to be a sparse array of row indices will
// avoid this problem.
type hashMemRowContainer struct {
	// TODO(asubiotto): This memRowContainer then has an underlying
	// sqlbase.RowContainer. This can be cleaned up.
	*memRowContainer
	columnEncoder

	// marked specifies for each row in memRowContainer whether that row has
	// been marked. Used for iterating over unmarked rows.
	// TODO(asubiotto): Make this a sparse array of row indices.
	marked []bool

	// buckets contains the indices into memRowContainer for a given group
	// key (which is the encoding of storedEqCols).
	buckets map[string][]int
	// bucketsAcc is the memory account for the buckets. The datums themselves
	// are all in the memRowContainer.
	bucketsAcc mon.BoundAccount

	// {stored, probe}eqCols contain the indices of the columns of a row that
	// are encoded and used as a key into buckets. probeEqCols are used in
	// NewBucketIterator() and SetMarks(), and storedEqCols are used in
	// AddRow().
	storedEqCols columns
	probeEqCols  columns
}

var _ hashRowContainer = &hashMemRowContainer{}

// makeHashRowContainer creates a hashMemRowContainer from the given
// rowContainer. This rowContainer must still be Close()d by the caller.
func makeHashRowContainer(
	ctx context.Context, storedEqCols, probeEqCols columns, rowContainer *memRowContainer,
) (hashMemRowContainer, error) {
	h := hashMemRowContainer{
		memRowContainer: rowContainer,
		buckets:         make(map[string][]int),
		bucketsAcc:      rowContainer.evalCtx.Mon.MakeBoundAccount(),
		storedEqCols:    storedEqCols,
		probeEqCols:     probeEqCols,
	}

	// Build buckets from the rowContainer.
	for rowIdx := 0; rowIdx < h.Len(); rowIdx++ {
		if err := h.addRowToBuckets(ctx, h.EncRow(rowIdx), rowIdx); err != nil {
			return hashMemRowContainer{}, err
		}
	}

	return h, nil
}

// AddRow adds a row to the hashMemRowContainer. This row is unmarked by
// default.
func (h *hashMemRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	rowIdx := h.Len()
	if err := h.memRowContainer.AddRow(ctx, row); err != nil {
		return err
	}
	return h.addRowToBuckets(ctx, row, rowIdx)
}

// SetMarks sets the marks for all the stored rows that match the given row on
// equality columns. The marks must be provided in the same order that the rows
// are stored in the matching bucket.
func (h *hashMemRowContainer) SetMarks(
	ctx context.Context, row sqlbase.EncDatumRow, marks []bool,
) error {
	encoded, err := h.encodeEqualityCols(ctx, row, h.probeEqCols)
	if err != nil {
		return err
	}

	rowIdxs, ok := h.buckets[string(encoded)]
	if !ok {
		return errors.New("no rows matching equality columns found")
	}
	if len(marks) != len(rowIdxs) {
		return errors.New("marks aren't the same length as rows")
	}
	if h.marked == nil {
		if err := h.bucketsAcc.Grow(ctx, sizeOfBoolSlice+(sizeOfBool*int64(h.Len()))); err != nil {
			return err
		}

		h.marked = make([]bool, h.Len())
	}

	for i, rowIdx := range rowIdxs {
		h.marked[rowIdx] = marks[i]
	}
	return nil
}

// Close implements the hashRowContainer interface.
func (h *hashMemRowContainer) Close(ctx context.Context) {
	h.bucketsAcc.Close(ctx)
}

// addRowToBuckets is a helper function that encodes the equality columns of the
// given row and appends the rowIdx to the matching bucket.
func (h *hashMemRowContainer) addRowToBuckets(
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

var _ rowIterator = &hashMemRowBucketIterator{}

// NewBucketIterator implements the hashRowContainer interface.
func (h *hashMemRowContainer) NewBucketIterator(
	ctx context.Context, row sqlbase.EncDatumRow,
) (rowIterator, error) {
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

// Close implements the rowIterator interface.
func (i *hashMemRowBucketIterator) Close() {}

// hashMemRowIterator iterates over all unmarked rows in a hashMemRowContainer.
type hashMemRowIterator struct {
	*hashMemRowContainer
	curIdx int
}

var _ rowIterator = &hashMemRowIterator{}

func (h *hashMemRowContainer) NewIterator(ctx context.Context) rowIterator {
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
