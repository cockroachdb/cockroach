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
	"encoding/binary"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type diskRowContainer struct {
	diskMap       SortedDiskMap
	bufferedRows  SortedDiskMapWriteBatch
	scratchKey    []byte
	scratchVal    []byte
	scratchEncRow sqlbase.EncDatumRow

	// scratchRowId and rowId are used to prevent duplicate rows from
	// overwriting each other.
	scratchRowId []byte
	rowId        uint64

	types    []sqlbase.ColumnType
	ordering sqlbase.ColumnOrdering
	// encodings keeps around the DatumEncoding equivalents of the encoding
	// directions in ordering to avoid conversions in hot paths.
	encodings []sqlbase.DatumEncoding
	// valueIdxs holds the indexes of the columns that we encode as values.
	valueIdxs []int

	datumAlloc sqlbase.DatumAlloc
}

var DistSQLUseLocalStorage = settings.RegisterBoolSetting(
	"sql.defaults.distsql.localstorage",
	"Distributed SQL query execution uses disk for large queries",
	true,
)

var _ sortableRowContainer = &diskRowContainer{}

// makeDiskRowContainer creates a diskRowContainer with the rows from the passed
// in rowContainer. Note that makeDiskRowContainer consumes the rows from
// rowContainer and deletes them so rowContainer cannot be used after creating
// a diskRowContainer. The caller must still Close() the rowContainer.
func makeDiskRowContainer(
	ctx context.Context,
	processorId uint64,
	types []sqlbase.ColumnType,
	ordering sqlbase.ColumnOrdering,
	rowContainer memRowContainer,
	e engine.Engine,
) (diskRowContainer, error) {
	// Use the processorId as the prefix.
	diskMap, err := NewRocksDBMap(processorId, e)
	if err != nil {
		return diskRowContainer{}, err
	}
	d := diskRowContainer{
		diskMap:       diskMap,
		types:         types,
		ordering:      ordering,
		scratchEncRow: make(sqlbase.EncDatumRow, len(types)),
	}
	d.bufferedRows = d.diskMap.NewWriteBatch()

	// The ordering is specified for a subset of the columns. For the other
	// columns and for types that cannot be decoded from a key encoding, we get
	// the indexes to have them ready for hot paths.
	// A row is added to this container by storing the columns according to
	// d.ordering as an encoded key into a SortedDiskMap. The remaining columns
	// are stored in the value associated with the key. For the case in which
	// types whose key encodings cannot be decoded (at the time of writing only
	// CollatedString) are specified in d.ordering, the Datum is encoded both in
	// the key for comparison and in the value for decoding.
	orderingIndexes := make(map[int]struct{})
	for _, orderInfo := range d.ordering {
		orderingIndexes[orderInfo.ColIdx] = struct{}{}
	}
	d.valueIdxs = make([]int, 0, len(d.types))
	for i := range d.types {
		if _, ok := orderingIndexes[i]; ok && d.types[i].SemanticType != sqlbase.ColumnType_COLLATEDSTRING {
			continue
		}
		d.valueIdxs = append(d.valueIdxs, i)
	}

	d.encodings = make([]sqlbase.DatumEncoding, len(d.ordering))
	for i, orderInfo := range ordering {
		datumEncoding, err := sqlbase.EncodingDirToDatumEncoding(orderInfo.Direction)
		if err != nil {
			return diskRowContainer{}, errors.Wrap(err, "unable to make diskRowContainer")
		}
		d.encodings[i] = datumEncoding
	}

	i := rowContainer.NewIterator(ctx)
	defer i.Close()

	for i.Rewind(); i.Valid(); i.Next() {
		if err := d.AddRow(ctx, i.Row(ctx)); err != nil {
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
	// d.scratchRowId is of length 8 to store d.rowId, which is a uint64.
	d.scratchRowId = append(d.scratchRowId[:0], 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(d.scratchRowId, d.rowId)
	d.bufferedRows.Put(append(d.scratchKey, d.scratchRowId...), d.scratchVal)
	d.scratchKey = d.scratchKey[:0]
	d.scratchVal = d.scratchVal[:0]
	d.rowId++
	return nil
}

func (d *diskRowContainer) Sort() {}

// TODO(asubiotto): Plumb context down.
func (d *diskRowContainer) Close(_ context.Context) {
	d.bufferedRows.Close()
	d.diskMap.Close()
}

// keyValToRow decodes a key and a value byte slice stored with AddRow() into
// a sqlbase.EncDatumRow. The returned EncDatumRow is only valid until the next
// call to keyValToRow().
func (d *diskRowContainer) keyValToRow(ctx context.Context, k []byte, v []byte) sqlbase.EncDatumRow {
	for i, orderInfo := range d.ordering {
		// CollatedStrings are decoded from the value.
		if d.types[orderInfo.ColIdx].SemanticType == sqlbase.ColumnType_COLLATEDSTRING {
			continue
		}
		var err error
		d.scratchEncRow[orderInfo.ColIdx], k, err = sqlbase.EncDatumFromBuffer(d.types[orderInfo.ColIdx], d.encodings[i], k)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to decode row"))
		}
	}
	for _, i := range d.valueIdxs {
		var err error
		d.scratchEncRow[i], v, err = sqlbase.EncDatumFromBuffer(d.types[i], sqlbase.DatumEncoding_VALUE, v)
		if err != nil {
			log.Fatal(ctx, errors.Wrap(err, "unable to decode row"))
		}
	}
	return d.scratchEncRow
}

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
func (r diskRowIterator) Row(ctx context.Context) sqlbase.EncDatumRow {
	if !r.Valid() {
		log.Fatal(ctx, "invalid row")
	}

	return r.rowContainer.keyValToRow(ctx, r.Key(), r.Value())
}
