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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// returnRangeInfo, if set, causes the underlying kvFetcher to return
	// information about the ranges descriptors/leases uses in servicing the
	// requests. This has some cost, so it's only enabled by DistSQL when this
	// info is actually useful for correcting the plan (e.g. not for the PK-side
	// of an index-join).
	// If set, GetRangeInfo() can be used to retrieve the accumulated info.
	returnRangeInfo bool

	// -- Fields updated during a scan --

	kvFetcher      kvFetcher
	keyVals        []EncDatum  // the index key values for the current row
	extraVals      EncDatumRow // the extra column values for unique indexes
	indexKey       []byte      // the index key of the current row
	row            EncDatumRow
	decodedRow     parser.Datums
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
	returnRangeInfo bool,
) error {
	rf.desc = desc
	rf.colIdxMap = colIdxMap
	rf.index = index
	rf.reverse = reverse
	rf.isSecondaryIndex = isSecondaryIndex
	rf.cols = cols
	// rf.valNeededForCol is modified below.
	rf.valNeededForCol = append([]bool(nil), valNeededForCol...)
	rf.returnRangeInfo = returnRangeInfo
	rf.row = make([]EncDatum, len(rf.cols))
	rf.decodedRow = make([]parser.Datum, len(rf.cols))

	var indexColumnIDs []ColumnID
	indexColumnIDs, rf.indexColumnDirs = index.FullColumnIDs()

	rf.indexColIdx = make([]int, len(indexColumnIDs))
	for i, id := range indexColumnIDs {
		rf.indexColIdx[i] = rf.colIdxMap[id]
	}
	// Add composite key columns to valNeededForCol so that, like other key
	// columns, their values are decoded.
	for _, id := range index.CompositeColumnIDs {
		if idx, ok := colIdxMap[id]; ok {
			rf.valNeededForCol[idx] = true
		}
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
		// key. Prepare extraVals for use in decoding this value.
		// Primary indexes only contain ascendingly-encoded values. If this
		// ever changes, we'll probably have to figure out the directions here too.
		rf.extraVals, err = MakeEncodedKeyVals(desc, index.ExtraColumnIDs)
		if err != nil {
			return err
		}
	}

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *RowFetcher) StartScan(
	ctx context.Context, txn *client.Txn, spans roachpb.Spans, limitBatches bool, limitHint int64,
) error {
	if len(spans) == 0 {
		panic("no spans")
	}

	rf.indexKey = nil

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up of more
		// than one key.
		firstBatchLimit = limitHint * int64(rf.desc.KeysPerRow(rf.index.ID))
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	var err error
	rf.kvFetcher, err = makeKVFetcher(txn, spans, rf.reverse, limitBatches, firstBatchLimit, rf.returnRangeInfo)
	if err != nil {
		return err
	}

	// Retrieve the first key.
	_, err = rf.NextKey(ctx)
	return err
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
// TODO(andrei): change to return error
func (rf *RowFetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
	var ok bool

	for {
		ok, rf.kv, err = rf.kvFetcher.nextKV(ctx)
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

	// Composite columns that are not present use the key in the index. Record
	// those values here for use later on if the datums are not present in
	// the value.
	unsetCols := map[int]EncDatum{}

	if !rf.isSecondaryIndex && len(rf.keyRemainingBytes) > 0 {
		_, familyID, err := encoding.DecodeUvarintAscending(rf.keyRemainingBytes)
		if err != nil {
			return "", "", err
		}

		family, err := rf.desc.FindFamilyByID(FamilyID(familyID))
		if err != nil {
			return "", "", err
		}

		if familyID == 0 {
			// This value contains values for the composite columns. Clear the
			// undecodable bytes that came from the key so that there is no panic for
			// a duplicate value.
			for _, colID := range rf.index.CompositeColumnIDs {
				if idx, ok := rf.colIdxMap[colID]; ok {
					unsetCols[idx] = rf.row[idx]
					rf.row[idx].UnsetDatum()
				}
			}
		}

		switch kv.Value.GetTag() {
		case roachpb.ValueType_TUPLE:
			prettyKey, prettyValue, err = rf.processValueTuple(kv, debugStrings, prettyKey)
		default:
			prettyKey, prettyValue, err = rf.processValueSingle(family, kv, debugStrings, prettyKey)
		}
		if err != nil {
			return "", "", err
		}
	} else {
		for _, colID := range rf.index.CompositeColumnIDs {
			if idx, ok := rf.colIdxMap[colID]; ok {
				unsetCols[idx] = rf.row[idx]
				rf.row[idx].UnsetDatum()
			}
		}

		valueBytes := kv.ValueBytes()
		if rf.extraVals != nil {
			// This is a unique index; decode the extra column values from
			// the value.
			var err error
			valueBytes, err = DecodeKeyVals(&rf.alloc, rf.extraVals, nil, valueBytes)
			if err != nil {
				return "", "", err
			}
			for i, id := range rf.index.ExtraColumnIDs {
				if idx, ok := rf.colIdxMap[id]; ok && rf.valNeededForCol[idx] {
					rf.row[idx] = rf.extraVals[i]
				}
			}
			if debugStrings {
				prettyValue = prettyEncDatums(rf.extraVals)
			}
		}

		if log.V(2) {
			if rf.extraVals != nil {
				log.Infof(context.TODO(), "Scan %s -> %s", kv.Key, prettyEncDatums(rf.extraVals))
			} else {
				log.Infof(context.TODO(), "Scan %s", kv.Key)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = rf.processValueBytes(kv, valueBytes, debugStrings, prettyKey)
			if err != nil {
				return "", "", err
			}
		}
	}

	for idx, ed := range unsetCols {
		if rf.row[idx].IsUnset() {
			rf.row[idx] = ed
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

func (rf *RowFetcher) processValueBytes(
	kv client.KeyValue, bytes []byte, debugStrings bool, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if debugStrings {
		rf.prettyValueBuf.Reset()
	}

	var colIDDiff uint32
	var lastColID ColumnID
	for len(bytes) > 0 {
		_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + ColumnID(colIDDiff)
		lastColID = colID
		idx, ok := rf.colIdxMap[colID]
		if !ok || !rf.valNeededForCol[idx] {
			// This column wasn't requested, so read its length and skip it.
			_, len, err := encoding.PeekValueLength(bytes)
			if err != nil {
				return "", "", err
			}
			bytes = bytes[len:]
			if log.V(3) {
				log.Infof(context.TODO(), "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}

		if debugStrings {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, rf.desc.Columns[idx].Name)
		}

		var encValue EncDatum
		encValue, bytes, err =
			EncDatumFromBuffer(rf.cols[idx].Type, DatumEncoding_VALUE, bytes)
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

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *RowFetcher) processValueTuple(
	kv client.KeyValue, debugStrings bool, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}
	return rf.processValueBytes(kv, tupleBytes, debugStrings, prettyKeyPrefix)
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per valNeededForCol) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil.
func (rf *RowFetcher) NextRow(ctx context.Context) (EncDatumRow, error) {
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
		rowDone, err := rf.NextKey(ctx)
		if err != nil {
			return nil, err
		}
		if rowDone {
			rf.finalizeRow()
			return rf.row, nil
		}
	}
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a Datums.
// The Datums should not be modified and is only valid until the next call.
// When there are no more rows, the Datums is nil.
func (rf *RowFetcher) NextRowDecoded(ctx context.Context) (parser.Datums, error) {
	encRow, err := rf.NextRow(ctx)
	if err != nil {
		return nil, err
	}
	if encRow == nil {
		return nil, nil
	}
	err = EncDatumRowToDatums(rf.decodedRow, encRow, &rf.alloc)
	if err != nil {
		return nil, err
	}
	return rf.decodedRow, nil
}

// NextKeyDebug processes one key at a time and returns a pretty printed key and
// value. If we completed a row, the row is returned as well (see nextRow). If
// there are no more keys, prettyKey is "".
func (rf *RowFetcher) NextKeyDebug(
	ctx context.Context,
) (prettyKey string, prettyValue string, row EncDatumRow, err error) {
	if rf.kvEnd {
		return "", "", nil, nil
	}
	prettyKey, prettyValue, err = rf.ProcessKV(rf.kv, true)
	if err != nil {
		return "", "", nil, err
	}
	rowDone, err := rf.NextKey(ctx)
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
// Key returns nil when there are no more rows.
func (rf *RowFetcher) Key() roachpb.Key {
	return rf.kv.Key
}

// GetRangeInfo returns information about the ranges where the rows came from.
// The RangeInfo's are deduped and not ordered.
func (rf *RowFetcher) GetRangeInfo() []roachpb.RangeInfo {
	return rf.kvFetcher.getRangesInfo()
}
