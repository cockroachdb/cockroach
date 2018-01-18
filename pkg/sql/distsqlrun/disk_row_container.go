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
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// diskRowContainer is a sortableRowContainer that stores rows on disk according
// to the ordering specified in diskRowContainer.ordering. The underlying store
// is a SortedDiskMap so the sorting itself is delegated. Use an iterator
// created through NewIterator() to read the rows in sorted order.
type diskRowContainer struct {
	diskMap engine.SortedDiskMap
	// diskAcc keeps track of disk usage.
	diskAcc mon.BoundAccount
	// bufferedRows buffers writes to the diskMap.
	bufferedRows  engine.SortedDiskMapBatchWriter
	scratchKey    []byte
	scratchVal    []byte
	scratchEncRow sqlbase.EncDatumRow

	// rowID is used as a key suffix to prevent duplicate rows from overwriting
	// each other.
	rowID uint64

	// types is the schema of rows in the container.
	types []sqlbase.ColumnType
	// ordering is the order in which rows should be sorted.
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding
	// valueIdxs holds the indexes of the columns that we encode as values. The
	// columns described by ordering will be encoded as keys. See
	// makeDiskRowContainer() for more encoding specifics.
	valueIdxs []int

	datumAlloc sqlbase.DatumAlloc
}

var _ sortableRowContainer = &diskRowContainer{}

// makeDiskRowContainer creates a diskRowContainer with the given engine as the
// underlying store that rows are stored on.
// Arguments:
// 	- diskMonitor is used to monitor this diskRowContainer's disk usage.
// 	- types is the schema of rows that will be added to this container.
// 	- ordering is the output ordering; the order in which rows should be sorted.
// 	- e is the underlying store that rows are stored on.
func makeDiskRowContainer(
	ctx context.Context,
	diskMonitor *mon.BytesMonitor,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	e engine.Engine,
) diskRowContainer {
	diskMap := engine.NewRocksDBMap(e)
	d := diskRowContainer{
		diskMap:       diskMap,
		diskAcc:       diskMonitor.MakeBoundAccount(),
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
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
		// TODO(asubiotto): A datum of a type for with HasCompositeKeyEncoding
		// returns true may not necessarily need to be encoded in the value, so
		// make this more fine-grained. See IsComposite() methods in
		// pkg/sql/parser/datum.go.
		if _, ok := orderingIdxs[i]; !ok || sqlbase.HasCompositeKeyEncoding(d.types[i].SemanticType) {
			d.valueIdxs = append(d.valueIdxs, i)
		}
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		d.encodings[i] = sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
	}

	return d
}

func (d *diskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
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
		return errors.Wrapf(err, "this query requires additional disk space")
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
func (d *diskRowContainer) Sort(context.Context) {}

func (d *diskRowContainer) Close(ctx context.Context) {
	// We can ignore the error here because the flushed data is immediately cleared
	// in the following Close.
	_ = d.bufferedRows.Close(ctx)
	d.diskMap.Close(ctx)
	d.diskAcc.Close(ctx)
}

// keyValToRow decodes a key and a value byte slice stored with AddRow() into
// a sqlbase.EncDatumRow. The returned EncDatumRow is only valid until the next
// call to keyValToRow().
func (d *diskRowContainer) keyValToRow(k []byte, v []byte) (sqlbase.EncDatumRow, error) {
	for i, orderInfo := range d.ordering {
		// Types with composite key encodings are decoded from the value.
		if sqlbase.HasCompositeKeyEncoding(d.types[orderInfo.ColIdx].SemanticType) {
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
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(&d.types[i], sqlbase.DatumEncoding_VALUE, v)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	return d.scratchEncRow, nil
}

// diskRowIterator iterates over the rows in a diskRowContainer.
type diskRowIterator struct {
	rowContainer *diskRowContainer
	engine.SortedDiskMapIterator
}

var _ rowIterator = diskRowIterator{}

func (d *diskRowContainer) NewIterator(ctx context.Context) rowIterator {
	if err := d.bufferedRows.Flush(); err != nil {
		log.Fatal(ctx, err)
	}
	return diskRowIterator{rowContainer: d, SortedDiskMapIterator: d.diskMap.NewIterator()}
}

// Row returns the current row. The returned sqlbase.EncDatumRow is only valid
// until the next call to Row().
func (r diskRowIterator) Row() (sqlbase.EncDatumRow, error) {
	if ok, err := r.Valid(); err != nil {
		return nil, errors.Wrap(err, "unable to check row validity")
	} else if !ok {
		return nil, errors.New("invalid row")
	}

	return r.rowContainer.keyValToRow(r.Key(), r.Value())
}
