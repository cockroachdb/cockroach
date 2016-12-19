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

package sqlbase

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// RowFetcher handles fetching kvs and forming table rows.
// Usage:
//   var rf RowFetcher
//   err := rf.Init(..)
//   // Handle err
//   err := rf.StartScan(..)
//   // Handle err
//   for {
//      row, err := rf.NextRow()
//      // Handle err
//      if row == nil {
//         // Done
//         break
//      }
//      // Process row
//   }
type RowFetcher struct {
	// -- Fields initialized once --

	desc             *TableDescriptor
	index            *IndexDescriptor
	reverse          bool
	isSecondaryIndex bool
	indexColumnDirs  []encoding.Direction

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []ColumnDescriptor

	// For each column in cols, indicates if the value is needed (used as an
	// optimization when the upper layer doesn't need all values).
	valNeededForCol []bool

	// Map used to get the index for columns in cols.
	colIdxMap map[ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into cols).
	indexColIdx []int

	// -- Fields updated during a scan --

	kvFetcher      kvFetcher
	keyVals        []EncDatum  // the index key values for the current row
	implicitVals   EncDatumRow // the implicit values for unique indexes
	indexKey       []byte      // the index key of the current row
	row            EncDatumRow
	decodedRow     parser.DTuple
	prettyValueBuf bytes.Buffer

	// The current key/value, unless kvEnd is true.
	kv                client.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	// Buffered allocation of decoded datums.
	alloc DatumAlloc
}

// Init sets up a RowFetcher for a given table and index. If we are using a
// non-primary index, valNeededForCol can only be true for the columns in the
// index.
func (rf *RowFetcher) Init(
	desc *TableDescriptor,
	colIdxMap map[ColumnID]int,
	index *IndexDescriptor,
	reverse, isSecondaryIndex bool,
	cols []ColumnDescriptor,
	valNeededForCol []bool,
) error {
	rf.desc = desc
	rf.colIdxMap = colIdxMap
	rf.index = index
	rf.reverse = reverse
	rf.isSecondaryIndex = isSecondaryIndex
	rf.cols = cols
	rf.valNeededForCol = valNeededForCol
	rf.row = make([]EncDatum, len(rf.cols))
	rf.decodedRow = make([]parser.Datum, len(rf.cols))

	var indexColumnIDs []ColumnID
	indexColumnIDs, rf.indexColumnDirs = index.FullColumnIDs()

	rf.indexColIdx = make([]int, len(indexColumnIDs))
	for i, id := range indexColumnIDs {
		rf.indexColIdx[i] = rf.colIdxMap[id]
	}

	if isSecondaryIndex {
		for i, needed := range valNeededForCol {
			if needed && !index.ContainsColumnID(rf.cols[i].ID) {
				return errors.Errorf("requested column %s not in index", rf.cols[i].Name)
			}
		}
	}

	var err error
	// Prepare our index key vals slice.
	rf.keyVals, err = MakeEncodedKeyVals(rf.desc, indexColumnIDs)
	if err != nil {
		return err
	}

	if isSecondaryIndex && index.Unique {
		// Unique secondary indexes have a value that is the primary index
		// key. Prepare implicitVals for use in decoding this value.
		// Primary indexes only contain ascendingly-encoded values. If this
		// ever changes, we'll probably have to figure out the directions here too.
		rf.implicitVals, err = MakeEncodedKeyVals(desc, index.ImplicitColumnIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *RowFetcher) StartScan(
	txn *client.Txn, spans roachpb.Spans, limitBatches bool, limitHint int64,
) error {
	if len(spans) == 0 {
		// If no spans were specified retrieve all of the keys that start with our
		// index key prefix.
		start := roachpb.Key(MakeIndexKeyPrefix(rf.desc, rf.index.ID))
		spans = []roachpb.Span{{Key: start, EndKey: start.PrefixEnd()}}
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
			firstBatchLimit *= int64(1 + len(rf.cols) - len(rf.index.ColumnIDs))
		}
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	var err error
	rf.kvFetcher, err = makeKVFetcher(txn, spans, rf.reverse, limitBatches, firstBatchLimit)
	if err != nil {
		return err
	}

	// Retrieve the first key.
	_, err = rf.NextKey()
	return err
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
// TODO(andrei): change to return error
func (rf *RowFetcher) NextKey() (rowDone bool, err error) {
	var ok bool

	for {
		ok, rf.kv, err = rf.kvFetcher.nextKV()
		if err != nil {
			return false, err
		}
		rf.kvEnd = !ok
		if rf.kvEnd {
			return true, nil
		}

		rf.keyRemainingBytes, ok, err = rf.ReadIndexKey(rf.kv.Key)
		if err != nil {
			return false, err
		}
		if !ok {
			// The key did not match the descriptor, which means it's
			// interleaved data from some other table or index.
			continue
		}

		// For unique secondary indexes, the index-key does not distinguish one row
		// from the next if both rows contain identical values along with a NULL.
		// Consider the keys:
		//
		//   /test/unique_idx/NULL/0
		//   /test/unique_idx/NULL/1
		//
		// The index-key extracted from the above keys is /test/unique_idx/NULL. The
		// trailing /0 and /1 are the primary key used to unique-ify the keys when a
		// NULL is present. Currently we don't detect NULLs on decoding. If we did
		// we could detect this case and enlarge the index-key. A simpler fix for
		// this problem is to simply always output a row for each key scanned from a
		// secondary index as secondary indexes have only one key per row.
		if rf.indexKey != nil && (rf.isSecondaryIndex || !bytes.HasPrefix(rf.kv.Key, rf.indexKey)) {
			// The current key belongs to a new row. Output the current row.
			rf.indexKey = nil
			return true, nil
		}
		return false, nil
	}
}

func prettyEncDatums(vals []EncDatum) string {
	var buf bytes.Buffer
	for _, v := range vals {
		if err := v.EnsureDecoded(&DatumAlloc{}); err != nil {
			fmt.Fprintf(&buf, "error decoding: %v", err)
		}
		fmt.Fprintf(&buf, "/%v", v.Datum)
	}
	return buf.String()
}

// ReadIndexKey decodes an index key for the fetcher's table.
func (rf *RowFetcher) ReadIndexKey(k roachpb.Key) (remaining []byte, ok bool, err error) {
	return DecodeIndexKey(&rf.alloc, rf.desc, rf.index.ID, rf.keyVals,
		rf.indexColumnDirs, k)
}

// ProcessKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *RowFetcher) ProcessKV(
	kv client.KeyValue, debugStrings bool,
) (prettyKey string, prettyValue string, err error) {
	if debugStrings {
		prettyKey = fmt.Sprintf("/%s/%s%s", rf.desc.Name, rf.index.Name, prettyEncDatums(rf.keyVals))
	}

	if rf.indexKey == nil {
		// This is the first key for the row.
		rf.indexKey = []byte(kv.Key[:len(kv.Key)-len(rf.keyRemainingBytes)])

		// Reset the row to nil; it will get filled in with the column
		// values as we decode the key-value pairs for the row.
		for i := range rf.row {
			rf.row[i].UnsetDatum()
		}

		// Fill in the column values that are part of the index key.
		for i, v := range rf.keyVals {
			rf.row[rf.indexColIdx[i]] = v
		}
	}

	if !rf.isSecondaryIndex && len(rf.keyRemainingBytes) > 0 {
		_, familyID, err := encoding.DecodeUvarintAscending(rf.keyRemainingBytes)
		if err != nil {
			return "", "", err
		}

		family, err := rf.desc.FindFamilyByID(FamilyID(familyID))
		if err != nil {
			return "", "", err
		}

		switch kv.Value.GetTag() {
		case roachpb.ValueType_TUPLE:
			prettyKey, prettyValue, err = rf.processValueTuple(family, kv, debugStrings, prettyKey)
		default:
			prettyKey, prettyValue, err = rf.processValueSingle(family, kv, debugStrings, prettyKey)
		}
		if err != nil {
			return "", "", err
		}
	} else {
		if rf.implicitVals != nil {
			// This is a unique index; decode the implicit column values from
			// the value.
			_, err := DecodeKeyVals(&rf.alloc, rf.implicitVals, nil, kv.ValueBytes())
			if err != nil {
				return "", "", err
			}
			for i, id := range rf.index.ImplicitColumnIDs {
				if idx, ok := rf.colIdxMap[id]; ok && rf.valNeededForCol[idx] {
					rf.row[idx] = rf.implicitVals[i]
				}
			}
			if debugStrings {
				prettyValue = prettyEncDatums(rf.implicitVals)
			}
		}

		if log.V(2) {
			if rf.implicitVals != nil {
				log.Infof(context.TODO(), "Scan %s -> %s", kv.Key, prettyEncDatums(rf.implicitVals))
			} else {
				log.Infof(context.TODO(), "Scan %s", kv.Key)
			}
		}
	}

	if debugStrings && prettyValue == "" {
		prettyValue = parser.DNull.String()
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column
// family.DefaultColumnID), setting values in the rf.row accordingly. The key is
// only used for logging.
func (rf *RowFetcher) processValueSingle(
	family *ColumnFamilyDescriptor, kv client.KeyValue, debugStrings bool, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix

	// If this is the sentinel family, a value is not expected, so we're done.
	if family.ID == keys.SentinelFamilyID {
		return "", "", nil
	}

	colID := family.DefaultColumnID
	if colID == 0 {
		return "", "", errors.Errorf("single entry value with no default column id")
	}

	idx, ok := rf.colIdxMap[colID]
	if ok && (debugStrings || rf.valNeededForCol[idx]) {
		if debugStrings {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, rf.desc.Columns[idx].Name)
		}
		typ := rf.cols[idx].Type
		// TODO(arjun): The value is a directly marshaled single value, so we
		// unmarshal it eagerly here. This can potentially be optimized out,
		// although that would require changing UnmarshalColumnValue to operate
		// on bytes, and for Encode/DecodeTableValue to operate on marshaled
		// single values.
		value, err := UnmarshalColumnValue(&rf.alloc, typ, kv.Value)
		if err != nil {
			return "", "", err
		}
		if debugStrings {
			prettyValue = value.String()
		}
		if !rf.row[idx].IsUnset() {
			panic(fmt.Sprintf("duplicate value for column %d", idx))
		}
		rf.row[idx] = DatumToEncDatum(typ, value)
		if log.V(3) {
			log.Infof(context.TODO(), "Scan %s -> %v", kv.Key, value)
		}
	} else {
		// No need to unmarshal the column value. Either the column was part of
		// the index key or it isn't needed.
		if log.V(3) {
			log.Infof(context.TODO(), "Scan %s -> [%d] (skipped)", kv.Key, colID)
		}
	}

	return prettyKey, prettyValue, nil
}

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *RowFetcher) processValueTuple(
	family *ColumnFamilyDescriptor, kv client.KeyValue, debugStrings bool, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if debugStrings {
		rf.prettyValueBuf.Reset()
	}

	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}

	var colIDDiff uint32
	var lastColID ColumnID
	for len(tupleBytes) > 0 {
		_, _, colIDDiff, _, err = encoding.DecodeValueTag(tupleBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + ColumnID(colIDDiff)
		lastColID = colID
		idx, ok := rf.colIdxMap[colID]
		if !ok || !rf.valNeededForCol[idx] {
			// This column wasn't requested, so read its length and skip it.
			_, len, err := encoding.PeekValueLength(tupleBytes)
			if err != nil {
				return "", "", err
			}
			tupleBytes = tupleBytes[len:]
			if log.V(3) {
				log.Infof(context.TODO(), "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}

		if debugStrings {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, rf.desc.Columns[idx].Name)
		}

		var encValue EncDatum
		encValue, tupleBytes, err =
			EncDatumFromBuffer(rf.cols[idx].Type, DatumEncoding_VALUE, tupleBytes)
		if err != nil {
			return "", "", err
		}
		if debugStrings {
			err := encValue.EnsureDecoded(&rf.alloc)
			if err != nil {
				return "", "", err
			}
			fmt.Fprintf(&rf.prettyValueBuf, "/%v", encValue.Datum)
		}
		if !rf.row[idx].IsUnset() {
			panic(fmt.Sprintf("duplicate value for column %d", idx))
		}
		rf.row[idx] = encValue
		if log.V(3) {
			log.Infof(context.TODO(), "Scan %d -> %v", idx, encValue)
		}
	}

	if debugStrings {
		prettyValue = rf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per valNeededForCol) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil.
func (rf *RowFetcher) NextRow() (EncDatumRow, error) {
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
		_, _, err := rf.ProcessKV(rf.kv, false)
		if err != nil {
			return nil, err
		}
		rowDone, err := rf.NextKey()
		if err != nil {
			return nil, err
		}
		if rowDone {
			rf.finalizeRow()
			return rf.row, nil
		}
	}
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a DTuple.
// The DTuple should not be modified and is only valid until the next call.
// When there are no more rows, the DTuple is nil.
func (rf *RowFetcher) NextRowDecoded() (parser.DTuple, error) {
	encRow, err := rf.NextRow()
	if err != nil {
		return nil, err
	}
	if encRow == nil {
		return nil, nil
	}
	err = EncDatumRowToDTuple(rf.decodedRow, encRow, &rf.alloc)
	if err != nil {
		return nil, err
	}
	return rf.decodedRow, nil
}

// NextKeyDebug processes one key at a time and returns a pretty printed key and
// value. If we completed a row, the row is returned as well (see nextRow). If
// there are no more keys, prettyKey is "".
func (rf *RowFetcher) NextKeyDebug() (prettyKey string, prettyValue string, row EncDatumRow, err error) {
	if rf.kvEnd {
		return "", "", nil, nil
	}
	prettyKey, prettyValue, err = rf.ProcessKV(rf.kv, true)
	if err != nil {
		return "", "", nil, err
	}
	rowDone, err := rf.NextKey()
	if err != nil {
		return "", "", nil, err
	}
	if rowDone {
		rf.finalizeRow()
		row = rf.row
	}
	return prettyKey, prettyValue, row, nil
}

func (rf *RowFetcher) finalizeRow() {
	// Fill in any missing values with NULLs
	for i, col := range rf.cols {
		if rf.valNeededForCol[i] && rf.row[i].IsUnset() {
			if !col.Nullable {
				panic("Non-nullable column with no value!")
			}
			rf.row[i] = EncDatum{
				Type:  col.Type,
				Datum: parser.DNull,
			}
		}
	}
}

// Key returns the next key (the key that follows the last returned row).
func (rf *RowFetcher) Key() roachpb.Key {
	return rf.kv.Key
}
