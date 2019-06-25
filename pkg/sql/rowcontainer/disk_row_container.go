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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// DiskRowContainer is a SortableRowContainer that stores rows on disk according
// to the ordering specified in DiskRowContainer.ordering. The underlying store
// is a SortedDiskMap so the sorting itself is delegated. Use an iterator
// created through NewIterator() to read the rows in sorted order.
type DiskRowContainer struct {
	diskMap diskmap.SortedDiskMap
	// diskAcc keeps track of disk usage.
	diskAcc mon.BoundAccount
	// bufferedRows buffers writes to the diskMap.
	bufferedRows  diskmap.SortedDiskMapBatchWriter
	scratchKey    []byte
	scratchVal    []byte
	scratchEncRow sqlbase.EncDatumRow

	// lastReadKey is used to implement NewFinalIterator. Refer to the method's
	// comment for more information.
	lastReadKey []byte

	// topK is set by callers through InitTopK. Since rows are kept in sorted
	// order, topK will simply limit iterators to read the first k rows.
	topK int

	// rowID is used as a key suffix to prevent duplicate rows from overwriting
	// each other.
	rowID uint64

	// types is the schema of rows in the container.
	types []types.T
	// ordering is the order in which rows should be sorted.
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding
	// valueIdxs holds the indexes of the columns that we encode as values. The
	// columns described by ordering will be encoded as keys. See
	// MakeDiskRowContainer() for more encoding specifics.
	valueIdxs []int

	diskMonitor *mon.BytesMonitor
	engine      diskmap.Factory

	datumAlloc sqlbase.DatumAlloc
}

var _ SortableRowContainer = &DiskRowContainer{}

// MakeDiskRowContainer creates a DiskRowContainer with the given engine as the
// underlying store that rows are stored on.
// Arguments:
// 	- diskMonitor is used to monitor this DiskRowContainer's disk usage.
// 	- types is the schema of rows that will be added to this container.
// 	- ordering is the output ordering; the order in which rows should be sorted.
// 	- e is the underlying store that rows are stored on.
func MakeDiskRowContainer(
	diskMonitor *mon.BytesMonitor,
	types []types.T,
	ordering sqlbase.ColumnOrdering,
	e diskmap.Factory,
) DiskRowContainer {
	diskMap := e.NewSortedDiskMap()
	d := DiskRowContainer{
		diskMap:       diskMap,
		diskAcc:       diskMonitor.MakeBoundAccount(),
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
		diskMonitor:   diskMonitor,
		engine:        e,
	}
	d.bufferedRows = d.diskMap.NewBatchWriter()

	// The ordering is specified for a subset of the columns. These will be
	// encoded as a key in the given order according to the given direction so
	// that the sorting can be delegated to the underlying SortedDiskMap. To
	// avoid converting encoding.Direction to sqlbase.DatumEncoding we do this
	// once at initialization and store the conversions in d.encodings.
	// We encode the other columns as values. The indexes of these columns are
	// kept around in d.valueIdxs to have them ready in hot paths.
	// For composite columns that are specified in d.ordering, the Datum is
	// encoded both in the key for comparison and in the value for decoding.
	orderingIdxs := make(map[int]struct{})
	for _, orderInfo := range d.ordering {
		orderingIdxs[orderInfo.ColIdx] = struct{}{}
	}
	d.valueIdxs = make([]int, 0, len(d.types))
	for i := range d.types {
		// TODO(asubiotto): A datum of a type for which HasCompositeKeyEncoding
		// returns true may not necessarily need to be encoded in the value, so
		// make this more fine-grained. See IsComposite() methods in
		// pkg/sql/parser/datum.go.
		if _, ok := orderingIdxs[i]; !ok || sqlbase.HasCompositeKeyEncoding(d.types[i].Family()) {
			d.valueIdxs = append(d.valueIdxs, i)
		}
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		d.encodings[i] = sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
	}

	return d
}

// Len is part of the SortableRowContainer interface.
func (d *DiskRowContainer) Len() int {
	return int(d.rowID)
}

// AddRow is part of the SortableRowContainer interface.
//
// Note: if key calculation changes, computeKey() of hashMemRowIterator should
// be changed accordingly.
func (d *DiskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(d.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(d.types))
	}

	for i, orderInfo := range d.ordering {
		col := orderInfo.ColIdx
		var err error
		d.scratchKey, err = row[col].Encode(&d.types[col], &d.datumAlloc, d.encodings[i], d.scratchKey)
		if err != nil {
			return err
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchVal, err = row[i].Encode(&d.types[i], &d.datumAlloc, sqlbase.DatumEncoding_VALUE, d.scratchVal)
		if err != nil {
			return err
		}
	}

	// Put a unique row to keep track of duplicates. Note that this will not
	// mess with key decoding.
	d.scratchKey = encoding.EncodeUvarintAscending(d.scratchKey, d.rowID)
	if err := d.diskAcc.Grow(ctx, int64(len(d.scratchKey)+len(d.scratchVal))); err != nil {
		return pgerror.Wrapf(err, pgcode.OutOfMemory,
			"this query requires additional disk space")
	}
	if err := d.bufferedRows.Put(d.scratchKey, d.scratchVal); err != nil {
		return err
	}
	d.scratchKey = d.scratchKey[:0]
	d.scratchVal = d.scratchVal[:0]
	d.rowID++
	return nil
}

// Sort is a noop because the use of a SortedDiskMap as the underlying store
// keeps the rows in sorted order.
func (d *DiskRowContainer) Sort(context.Context) {}

// Reorder implements ReorderableRowContainer. It creates a new
// DiskRowContainer with the requested ordering and adds a row one by one from
// the current DiskRowContainer, the latter is closed at the end.
func (d *DiskRowContainer) Reorder(ctx context.Context, ordering sqlbase.ColumnOrdering) error {
	// We need to create a new DiskRowContainer since its ordering can only be
	// changed at initialization.
	newContainer := MakeDiskRowContainer(d.diskMonitor, d.types, ordering, d.engine)
	i := d.NewFinalIterator(ctx)
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
		if err := newContainer.AddRow(ctx, row); err != nil {
			return err
		}
	}
	d.Close(ctx)
	*d = newContainer
	return nil
}

// InitTopK limits iterators to read the first k rows.
func (d *DiskRowContainer) InitTopK() {
	d.topK = d.Len()
}

// MaybeReplaceMax adds row to the DiskRowContainer. The SortedDiskMap will
// sort this row into the top k if applicable.
func (d *DiskRowContainer) MaybeReplaceMax(ctx context.Context, row sqlbase.EncDatumRow) error {
	return d.AddRow(ctx, row)
}

// UnsafeReset is part of the SortableRowContainer interface.
func (d *DiskRowContainer) UnsafeReset(ctx context.Context) error {
	_ = d.bufferedRows.Close(ctx)
	if err := d.diskMap.Clear(); err != nil {
		return err
	}
	d.diskAcc.Clear(ctx)
	d.bufferedRows = d.diskMap.NewBatchWriter()
	d.lastReadKey = nil
	d.rowID = 0
	return nil
}

// Close is part of the SortableRowContainer interface.
func (d *DiskRowContainer) Close(ctx context.Context) {
	// We can ignore the error here because the flushed data is immediately cleared
	// in the following Close.
	_ = d.bufferedRows.Close(ctx)
	d.diskMap.Close(ctx)
	d.diskAcc.Close(ctx)
}

// keyValToRow decodes a key and a value byte slice stored with AddRow() into
// a sqlbase.EncDatumRow. The returned EncDatumRow is only valid until the next
// call to keyValToRow().
func (d *DiskRowContainer) keyValToRow(k []byte, v []byte) (sqlbase.EncDatumRow, error) {
	for i, orderInfo := range d.ordering {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(d.types[orderInfo.ColIdx].Family()) {
			// Skip over the encoded key.
			encLen, err := encoding.PeekLength(k)
			if err != nil {
				return nil, err
			}
			k = k[encLen:]
			continue
		}
		var err error
		col := orderInfo.ColIdx
		d.scratchEncRow[col], k, err = sqlbase.EncDatumFromBuffer(&d.types[col], d.encodings[i], k)
		if err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"unable to decode row, column idx %d", errors.Safe(col))
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(&d.types[i], sqlbase.DatumEncoding_VALUE, v)
		if err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"unable to decode row, value idx %d", errors.Safe(i))
		}
	}
	return d.scratchEncRow, nil
}

// diskRowIterator iterates over the rows in a DiskRowContainer.
type diskRowIterator struct {
	rowContainer *DiskRowContainer
	diskmap.SortedDiskMapIterator
}

var _ RowIterator = &diskRowIterator{}

func (d *DiskRowContainer) newIterator(ctx context.Context) diskRowIterator {
	if err := d.bufferedRows.Flush(); err != nil {
		log.Fatal(ctx, err)
	}
	return diskRowIterator{rowContainer: d, SortedDiskMapIterator: d.diskMap.NewIterator()}
}

//NewIterator is part of the SortableRowContainer interface.
func (d *DiskRowContainer) NewIterator(ctx context.Context) RowIterator {
	i := d.newIterator(ctx)
	if d.topK > 0 {
		return &diskRowTopKIterator{RowIterator: &i, k: d.topK}
	}
	return &i
}

// Row returns the current row. The returned sqlbase.EncDatumRow is only valid
// until the next call to Row().
func (r *diskRowIterator) Row() (sqlbase.EncDatumRow, error) {
	if ok, err := r.Valid(); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "unable to check row validity")
	} else if !ok {
		return nil, errors.AssertionFailedf("invalid row")
	}

	return r.rowContainer.keyValToRow(r.Key(), r.Value())
}

func (r *diskRowIterator) Close() {
	if r.SortedDiskMapIterator != nil {
		r.SortedDiskMapIterator.Close()
	}
}

type diskRowFinalIterator struct {
	diskRowIterator
}

var _ RowIterator = &diskRowFinalIterator{}

// NewFinalIterator returns an iterator that reads rows exactly once throughout
// the lifetime of a DiskRowContainer. Rows are not actually discarded from the
// DiskRowContainer, but the lastReadKey is kept track of in order to serve as
// the start key for future diskRowFinalIterators.
// NOTE: Don't use NewFinalIterator if you passed in an ordering for the rows
// and will be adding rows between iterations. New rows could sort before the
// current row.
func (d *DiskRowContainer) NewFinalIterator(ctx context.Context) RowIterator {
	i := diskRowFinalIterator{diskRowIterator: d.newIterator(ctx)}
	if d.topK > 0 {
		return &diskRowTopKIterator{RowIterator: &i, k: d.topK}
	}
	return &i
}

func (r *diskRowFinalIterator) Rewind() {
	r.Seek(r.diskRowIterator.rowContainer.lastReadKey)
	if r.diskRowIterator.rowContainer.lastReadKey != nil {
		r.Next()
	}
}

func (r *diskRowFinalIterator) Row() (sqlbase.EncDatumRow, error) {
	row, err := r.diskRowIterator.Row()
	if err != nil {
		return nil, err
	}
	r.diskRowIterator.rowContainer.lastReadKey = r.Key()
	return row, nil
}

type diskRowTopKIterator struct {
	RowIterator
	position int
	// k is the limit of rows to read.
	k int
}

var _ RowIterator = &diskRowTopKIterator{}

func (d *diskRowTopKIterator) Rewind() {
	d.RowIterator.Rewind()
	d.position = 0
}

func (d *diskRowTopKIterator) Valid() (bool, error) {
	if d.position >= d.k {
		return false, nil
	}
	return d.RowIterator.Valid()
}

func (d *diskRowTopKIterator) Next() {
	d.position++
	d.RowIterator.Next()
}
