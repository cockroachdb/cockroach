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
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// diskRowContainer is a sortableRowContainer that stores rows on disk according
// to the ordering specified in diskRowContainer.ordering. The underlying store
// is a SortedDiskMap so the sorting itself is delegated. Use an iterator
// created through NewIterator() to read the rows in sorted order.
type diskRowContainer struct {
	diskMap SortedDiskMap
	// bufferedRows buffers writes to the diskMap.
	bufferedRows  SortedDiskMapBatchWriter
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

// makeDiskRowContainer creates a diskRowContainer with the rows from the passed
// in rowContainer. Note that makeDiskRowContainer consumes the rows from
// rowContainer and deletes them so rowContainer cannot be used after creating
// a diskRowContainer. The caller must still Close() the rowContainer.
// Arguments:
// 	- tempStorageID is the ID of the processor that is calling
// 	  makeDiskRowContainer. It is used as a prefix in the underlying
// 	  SortedDiskMap to have a private keyspace.
// 	- types is the schema of rows that will be added to this container.
// 	- ordering is the output ordering; the order in which rows should be sorted.
// 	- rowContainer contains the initial set of rows that this diskRowContainer
// 	  is created with.
// 	- e is the underlying store that rows are stored on.
func makeDiskRowContainer(
	ctx context.Context,
	tempStorageID uint64,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	rowContainer memRowContainer,
	e engine.Engine,
) (diskRowContainer, error) {
	// Use the tempStorageID as the prefix.
	diskMap, err := NewRocksDBMap(tempStorageID, e)
	if err != nil {
		return diskRowContainer{}, err
	}
	d := diskRowContainer{
		diskMap:       diskMap,
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

	i := rowContainer.NewIterator(ctx)
	defer i.Close()

	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return diskRowContainer{}, err
		} else if !ok {
			break
		}
		row, err := i.Row()
		if err != nil {
			return diskRowContainer{}, err
		}
		if err := d.AddRow(ctx, row); err != nil {
			return diskRowContainer{}, errors.Wrap(err, "could not add row")
		}
	}
	return d, nil
}

func (d *diskRowContainer) AddRow(ctx context.Context, row sqlbase.EncDatumRow) error {
	if len(row) != len(d.types) {
		log.Fatalf(ctx, "invalid row length %d, expected %d", len(row), len(d.types))
	}

	for i, orderInfo := range d.ordering {
		var err error
		d.scratchKey, err = row[orderInfo.ColIdx].Encode(&d.datumAlloc, d.encodings[i], d.scratchKey)
		if err != nil {
			return err
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchVal, err = row[i].Encode(&d.datumAlloc, sqlbase.DatumEncoding_VALUE, d.scratchVal)
		if err != nil {
			return err
		}
	}

	// Put a unique row to keep track of duplicates. Note that this will not
	// mess with key decoding.
	if err := d.bufferedRows.Put(
		encoding.EncodeUvarintAscending(d.scratchKey, d.rowID),
		d.scratchVal,
	); err != nil {
		return err
	}
	d.scratchKey = d.scratchKey[:0]
	d.scratchVal = d.scratchVal[:0]
	d.rowID++
	return nil
}

// Sort is a noop because the use of a SortedDiskMap as the underlying store
// keeps the rows in sorted order.
func (d *diskRowContainer) Sort() {}

func (d *diskRowContainer) Close(ctx context.Context) {
	d.bufferedRows.Close(ctx)
	d.diskMap.Close(ctx)
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
		d.scratchEncRow[orderInfo.ColIdx], k, err = sqlbase.EncDatumFromBuffer(d.types[orderInfo.ColIdx], d.encodings[i], k)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(d.types[i], sqlbase.DatumEncoding_VALUE, v)
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode row")
		}
	}
	return d.scratchEncRow, nil
}

// diskRowIterator iterates over the rows in a diskRowContainer.
type diskRowIterator struct {
	rowContainer *diskRowContainer
	SortedDiskMapIterator
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
