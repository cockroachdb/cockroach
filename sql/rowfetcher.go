// Copyright 2016 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// rowFetcher handles fetching kvs and forming table rows.
// Usage:
//   var rf rowFetcher
//   err := rf.init(..)
//   // Handle err
//   pErr := rf.startScan(..)
//   // Handle pErr
//   for {
//      row, pErr := rf.nextRow()
//      // Handle pErr
//      if row == nil {
//         // Done
//         break
//      }
//      // Process row
//   }
type rowFetcher struct {
	// -- Fields initialized once --

	desc             *TableDescriptor
	index            *IndexDescriptor
	reverse          bool
	isSecondaryIndex bool
	indexColumnDirs  []encoding.Direction

	// For each column in desc.Columns, indicates if the value is needed (used
	// as an optimization when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in desc.Columns.
	colIdxMap map[ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into desc.Columns).
	indexColIdx []int

	// -- Fields updated during a scan --

	kvFetcher        kvFetcher
	keyValTypes      []parser.Datum // the index key value types for the current row
	keyVals          []parser.Datum // the index key values for the current row
	implicitValTypes []parser.Datum // the implicit value types for unique indexes
	implicitVals     []parser.Datum // the implicit values for unique indexes
	indexKey         []byte         // the index key of the current row
	row              parser.DTuple

	// The current key/value, unless kvEnd is true.
	kv    client.KeyValue
	kvEnd bool

	// Buffered allocation of decoded datums.
	alloc datumAlloc
}

// init sets up a rowFetcher for a given table and index. If we are using a
// non-primary index, valNeededForCol can only be true for the columns in the
// index.
func (rf *rowFetcher) init(
	desc *TableDescriptor,
	colIdxMap map[ColumnID]int,
	index *IndexDescriptor,
	reverse, isSecondaryIndex bool,
	valNeededForCol []bool,
) error {
	rf.desc = desc
	rf.colIdxMap = colIdxMap
	rf.index = index
	rf.reverse = reverse
	rf.isSecondaryIndex = isSecondaryIndex
	rf.valNeededForCol = valNeededForCol
	rf.row = make([]parser.Datum, len(rf.desc.Columns))

	var indexColumnIDs []ColumnID
	indexColumnIDs, rf.indexColumnDirs = index.fullColumnIDs()

	rf.indexColIdx = make([]int, len(indexColumnIDs))
	for i, id := range indexColumnIDs {
		rf.indexColIdx[i] = rf.colIdxMap[id]
	}

	if isSecondaryIndex {
		for i, needed := range valNeededForCol {
			if needed && !index.containsColumnID(desc.Columns[i].ID) {
				return util.Errorf("requested column %s not in index", desc.Columns[i].Name)
			}
		}
	}

	var err error
	// Prepare our index key vals slice.
	rf.keyValTypes, err = makeKeyVals(rf.desc, indexColumnIDs)
	if err != nil {
		return err
	}
	rf.keyVals = make([]parser.Datum, len(rf.keyValTypes))

	if isSecondaryIndex && index.Unique {
		// Unique secondary indexes have a value that is the primary index
		// key. Prepare implicitVals for use in decoding this value.
		// Primary indexes only contain ascendingly-encoded values. If this
		// ever changes, we'll probably have to figure out the directions here too.
		rf.implicitValTypes, err = makeKeyVals(desc, index.ImplicitColumnIDs)
		if err != nil {
			return err
		}
		rf.implicitVals = make([]parser.Datum, len(rf.implicitValTypes))
	}
	return nil
}

// startScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *rowFetcher) startScan(txn *client.Txn, spans spans, limitHint int64) *roachpb.Error {
	if len(spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix.
		start := roachpb.Key(MakeIndexKeyPrefix(rf.desc.ID, rf.index.ID))
		spans = []span{{start: start, end: start.PrefixEnd()}}
	}

	rf.indexKey = nil

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// For a secondary index, we have one key per row.
		if !rf.isSecondaryIndex {
			// We have a sentinel key per row plus at most one key per non-PK column. Of course, we
			// may have other keys due to a schema change, but this is only a hint.
			firstBatchLimit *= int64(1 + len(rf.desc.Columns) - len(rf.index.ColumnIDs))
		}
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	rf.kvFetcher = makeKVFetcher(txn, spans, rf.reverse, firstBatchLimit)

	// Retrieve the first key.
	_, pErr := rf.nextKey()
	return pErr
}

// nextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (rf *rowFetcher) nextKey() (rowDone bool, pErr *roachpb.Error) {
	var ok bool
	ok, rf.kv, pErr = rf.kvFetcher.nextKV()
	if pErr != nil {
		return false, pErr
	}
	rf.kvEnd = !ok

	// For unique secondary indexes, the index-key does not distinguish one row
	// from the next if both rows contain identical values along with a
	// NULL. Consider the keys:
	//
	//   /test/unique_idx/NULL/0
	//   /test/unique_idx/NULL/1
	//
	// The index-key extracted from the above keys is /test/unique_idx/NULL. The
	// trailing /0 and /1 are the primary key used to unique-ify the keys when a
	// NULL is present. Currently we don't detect NULLs on decoding. If we did we
	// could detect this case and enlarge the index-key. A simpler fix for this
	// problem is to simply always output a row for each key scanned from a
	// secondary index as secondary indexes have only one key per row.

	if rf.indexKey != nil &&
		(rf.isSecondaryIndex || rf.kvEnd ||
			!bytes.HasPrefix(rf.kv.Key, rf.indexKey)) {
		// The current key belongs to a new row. Output the current row.
		rf.indexKey = nil

		// Fill in any missing values with NULLs
		for i, col := range rf.desc.Columns {
			if rf.valNeededForCol[i] && rf.row[i] == nil {
				if !col.Nullable {
					panic("Non-nullable column with no value!")
				}
				rf.row[i] = parser.DNull
			}
		}
		return true, nil
	}
	return false, nil
}

func prettyDatums(vals []parser.Datum) string {
	var buf bytes.Buffer
	for _, v := range vals {
		fmt.Fprintf(&buf, "/%v", v)
	}
	return buf.String()
}

func (rf *rowFetcher) readIndexKey(k roachpb.Key) (remaining []byte, err error) {
	return decodeIndexKey(&rf.alloc, rf.desc, rf.index.ID, rf.keyValTypes, rf.keyVals,
		rf.indexColumnDirs, k)
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *rowFetcher) processKV(kv client.KeyValue, debugStrings bool) (
	prettyKey string, prettyValue string, err error,
) {
	remaining, err := rf.readIndexKey(kv.Key)
	if err != nil {
		return "", "", err
	}

	if debugStrings {
		prettyKey = fmt.Sprintf("/%s/%s%s", rf.desc.Name, rf.index.Name, prettyDatums(rf.keyVals))
	}

	if rf.indexKey == nil {
		// This is the first key for the row.
		rf.indexKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		// Reset the row to nil; it will get filled in with the column
		// values as we decode the key-value pairs for the row.
		for i := range rf.row {
			rf.row[i] = nil
		}

		// Fill in the column values that are part of the index key.
		for i, v := range rf.keyVals {
			rf.row[rf.indexColIdx[i]] = v
		}
	}

	if !rf.isSecondaryIndex && len(remaining) > 0 {
		_, colID, err := encoding.DecodeUvarintAscending(remaining)
		if err != nil {
			return "", "", err
		}

		idx, ok := rf.colIdxMap[ColumnID(colID)]
		if ok && (debugStrings || rf.valNeededForCol[idx]) {
			if debugStrings {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, rf.desc.Columns[idx].Name)
			}
			kind := rf.desc.Columns[idx].Type.Kind
			value, err := unmarshalColumnValue(&rf.alloc, kind, kv.Value)
			if err != nil {
				return "", "", err
			}
			prettyValue = value.String()
			if rf.row[idx] != nil {
				panic(fmt.Sprintf("duplicate value for column %d", idx))
			}
			rf.row[idx] = value
			if log.V(3) {
				log.Infof("Scan %s -> %v", kv.Key, value)
			}
		} else {
			// No need to unmarshal the column value. Either the column was part of
			// the index key or it isn't needed.
			if log.V(3) {
				log.Infof("Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
		}
	} else {
		if rf.implicitVals != nil {
			// This is a unique index; decode the implicit column values from
			// the value.
			_, err := decodeKeyVals(&rf.alloc, rf.implicitValTypes, rf.implicitVals, nil,
				kv.ValueBytes())
			if err != nil {
				return "", "", err
			}
			for i, id := range rf.index.ImplicitColumnIDs {
				if idx, ok := rf.colIdxMap[id]; ok && rf.valNeededForCol[idx] {
					rf.row[idx] = rf.implicitVals[i]
				}
			}
			if debugStrings {
				prettyValue = prettyDatums(rf.implicitVals)
			}
		}

		if log.V(2) {
			if rf.implicitVals != nil {
				log.Infof("Scan %s -> %s", kv.Key, prettyDatums(rf.implicitVals))
			} else {
				log.Infof("Scan %s", kv.Key)
			}
		}
	}

	if debugStrings && prettyValue == "" {
		prettyValue = parser.DNull.String()
	}

	return prettyKey, prettyValue, nil
}

// nextRow processes keys until we complete one row, which is returned as a
// DTuple. The row contains one value per table column, regardless of the index
// used; values that are not needed (as per valNeededForCol) are nil.
//
// The DTuple should not be modified and is only valid until the next call. When
// there are no more rows, the DTuple is nil.
func (rf *rowFetcher) nextRow() (parser.DTuple, *roachpb.Error) {
	if rf.kvEnd {
		return nil, nil
	}

	// All of the columns for a particular row will be grouped together. We loop
	// over the key/value pairs and decode the key to extract the columns encoded
	// within the key and the column ID. We use the column ID to lookup the
	// column and decode the value. All of these values go into a map keyed by
	// column name. When the index key changes we output a row containing the
	// current values.
	for {
		_, _, err := rf.processKV(rf.kv, false)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rowDone, pErr := rf.nextKey()
		if pErr != nil {
			return nil, pErr
		}
		if rowDone {
			return rf.row, nil
		}
	}
}

// nextKeyDebug processes one key at a time and returns a pretty printed key and
// value. If we completed a row, the row is returned as well (see nextRow). If
// there are no more keys, prettyKey is "".
func (rf *rowFetcher) nextKeyDebug() (
	prettyKey string, prettyValue string, row parser.DTuple, pErr *roachpb.Error,
) {
	if rf.kvEnd {
		return "", "", nil, nil
	}
	prettyKey, prettyValue, err := rf.processKV(rf.kv, true)
	if err != nil {
		return "", "", nil, roachpb.NewError(err)
	}
	rowDone, pErr := rf.nextKey()
	if pErr != nil {
		return "", "", nil, pErr
	}
	if rowDone {
		row = rf.row
	}
	return prettyKey, prettyValue, row, nil
}
