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

package sqlbase

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// debugRowFetch can be used to turn on some low-level debugging logs. We use
// this to avoid using log.V in the hot path.
const debugRowFetch = false

type kvFetcher interface {
	nextBatch(ctx context.Context) (bool, []roachpb.KeyValue, error)
	getRangesInfo() []roachpb.RangeInfo
}

type tableInfo struct {
	// -- Fields initialized once --

	// Used to determine whether a key retrieved belongs to the span we
	// want to scan.
	spans            roachpb.Spans
	desc             *TableDescriptor
	index            *IndexDescriptor
	isSecondaryIndex bool
	indexColumnDirs  []encoding.Direction
	// equivSignature is an equivalence class for each unique table-index
	// pair. It allows us to check if an index key belongs to a given
	// table-index.
	equivSignature []byte

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []ColumnDescriptor

	// The set of ColumnIDs that are required.
	neededCols util.FastIntSet

	// The set of indexes into the cols array that are required for columns
	// in the value part.
	neededValueColsByIdx util.FastIntSet

	// The number of needed columns from the value part of the row. Once we've
	// seen this number of value columns for a particular row, we can stop
	// decoding values in that row.
	neededValueCols int

	// Map used to get the index for columns in cols.
	colIdxMap map[ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColIdx []int

	// -- Fields updated during a scan --

	keyValTypes []ColumnType
	extraTypes  []ColumnType
	keyVals     []EncDatum
	extraVals   []EncDatum
	row         EncDatumRow
	decodedRow  tree.Datums

	// hasLast indicates whether there was a previously scanned k/v.
	hasLast bool
	// lastDatums is a buffer for the current key. It is only present when
	// doing a physical check in order to verify round-trip encoding.
	// It is required because RowFetcher.kv is overwritten before NextRow
	// returns.
	lastKV roachpb.KeyValue
	// lastDatums is a buffer for the previously scanned k/v datums. It is
	// only present when doing a physical check in order to verify
	// ordering.
	lastDatums tree.Datums
}

// RowFetcherTableArgs are the arguments passed to RowFetcher.Init
// for a given table that includes descriptors and row information.
type RowFetcherTableArgs struct {
	// The spans of keys to return for the given table. RowFetcher
	// ignores keys outside these spans.
	// This is irrelevant if RowFetcher is initialize with only one
	// table.
	Spans            roachpb.Spans
	Desc             *TableDescriptor
	Index            *IndexDescriptor
	ColIdxMap        map[ColumnID]int
	IsSecondaryIndex bool
	Cols             []ColumnDescriptor
	// The indexes (0 to # of columns - 1) of the columns to return.
	ValNeededForCol util.FastIntSet
}

// RowFetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var rf RowFetcher
//   err := rf.Init(..)
//   // Handle err
//   err := rf.StartScan(..)
//   // Handle err
//   for {
//      res, err := rf.NextRow()
//      // Handle err
//      if res.row == nil {
//         // Done
//         break
//      }
//      // Process res.row
//   }
type RowFetcher struct {
	// tables is a slice of all the tables and their descriptors for which
	// rows are returned.
	tables []tableInfo

	// allEquivSignatures is a map used for checking if an equivalence
	// signature belongs to any table or table's ancestor. It also maps the
	// string representation of every table's and every table's ancestors'
	// signature to the table's index in 'tables' for lookup during decoding.
	// If 2+ tables share the same ancestor signature, allEquivSignatures
	// will map the signature to the largest 'tables' index.
	// The full signature for a given table in 'tables' will always map to
	// its own index in 'tables'.
	allEquivSignatures map[string]int

	// reverse denotes whether or not the spans should be read in reverse
	// or not when StartScan is invoked.
	reverse bool

	// maxKeysPerRow memoizes the maximum number of keys per row
	// out of all the tables. This is used to calculate the kvFetcher's
	// firstBatchLimit.
	maxKeysPerRow int

	// True if the index key must be decoded.
	// If there is more than one table, the index key must always be decoded.
	// This is only false if there are no needed columns and the (single)
	// table has no interleave children.
	mustDecodeIndexKey bool

	knownPrefixLength int

	// returnRangeInfo, if set, causes the underlying kvFetcher to return
	// information about the ranges descriptors/leases uses in servicing the
	// requests. This has some cost, so it's only enabled by DistSQL when this
	// info is actually useful for correcting the plan (e.g. not for the PK-side
	// of an index-join).
	// If set, GetRangeInfo() can be used to retrieve the accumulated info.
	returnRangeInfo bool

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when beginning a new scan.
	traceKV bool

	// -- Fields updated during a scan --

	kvFetcher      kvFetcher
	indexKey       []byte // the index key of the current row
	prettyValueBuf *bytes.Buffer

	valueColsFound int // how many needed cols we've found so far in the value

	rowReadyTable *tableInfo // the table for which a row was fully decoded and ready for output
	currentTable  *tableInfo // the most recent table for which a key was decoded
	keySigBuf     []byte     // buffer for the index key's signature
	keyRestBuf    []byte     // buffer for the rest of the index key that is not part of the signature

	// The current key/value, unless kvEnd is true.
	kv                roachpb.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	kvs []roachpb.KeyValue

	// isCheck indicates whether or not we are running checks for k/v
	// correctness. It is set only during SCRUB commands.
	isCheck bool

	// Buffered allocation of decoded datums.
	alloc *DatumAlloc
}

// Init sets up a RowFetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *RowFetcher) Init(
	reverse,
	returnRangeInfo bool, isCheck bool, alloc *DatumAlloc, tables ...RowFetcherTableArgs,
) error {
	if len(tables) == 0 {
		panic("no tables to fetch from")
	}

	rf.reverse = reverse
	rf.returnRangeInfo = returnRangeInfo
	rf.alloc = alloc
	rf.allEquivSignatures = make(map[string]int, len(tables))
	rf.isCheck = isCheck

	// We must always decode the index key if we need to distinguish between
	// rows from more than one table.
	rf.mustDecodeIndexKey = len(tables) >= 2

	rf.tables = make([]tableInfo, 0, len(tables))
	for tableIdx, tableArgs := range tables {
		table := tableInfo{
			spans:            tableArgs.Spans,
			desc:             tableArgs.Desc,
			colIdxMap:        tableArgs.ColIdxMap,
			index:            tableArgs.Index,
			isSecondaryIndex: tableArgs.IsSecondaryIndex,
			cols:             tableArgs.Cols,
			row:              make([]EncDatum, len(tableArgs.Cols)),
			decodedRow:       make([]tree.Datum, len(tableArgs.Cols)),
		}

		var err error
		if len(tables) > 1 {
			// We produce references to every signature's reference.
			equivSignatures, err := TableEquivSignatures(table.desc, table.index)
			if err != nil {
				return err
			}
			for i, sig := range equivSignatures {
				// We always map the table's equivalence signature (last
				// 'sig' in 'equivSignatures') to its tableIdx.
				// This allows us to overwrite previous "ancestor
				// signatures" (see below).
				if i == len(equivSignatures)-1 {
					rf.allEquivSignatures[string(sig)] = tableIdx
					break
				}
				// Map each table's ancestors' signatures to -1 so
				// we know during ReadIndexKey if the parsed index
				// key belongs to ancestor or one of our tables.
				// We must check if the signature has already been set
				// since it's possible for a later 'table' to have an
				// ancestor that is a previous 'table', and we do not
				// want to overwrite the previous table's tableIdx.
				if _, exists := rf.allEquivSignatures[string(sig)]; !exists {
					rf.allEquivSignatures[string(sig)] = -1
				}
			}
			// The last signature is the given table's equivalence signature.
			table.equivSignature = equivSignatures[len(equivSignatures)-1]
		}

		// Scan through the entire columns map to see which columns are
		// required.
		for col, idx := range table.colIdxMap {
			if tableArgs.ValNeededForCol.Contains(idx) {
				// The idx-th column is required.
				table.neededCols.Add(int(col))
			}
		}

		rf.knownPrefixLength = len(MakeIndexKeyPrefix(table.desc, table.index.ID))

		var indexColumnIDs []ColumnID
		indexColumnIDs, table.indexColumnDirs = table.index.FullColumnIDs()

		table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()
		neededIndexCols := 0
		table.indexColIdx = make([]int, len(indexColumnIDs))
		for i, id := range indexColumnIDs {
			colIdx, ok := table.colIdxMap[id]
			if ok {
				table.indexColIdx[i] = colIdx
				if table.neededCols.Contains(int(id)) {
					neededIndexCols++
					table.neededValueColsByIdx.Remove(colIdx)
				}
			} else {
				table.indexColIdx[i] = -1
				if table.neededCols.Contains(int(id)) {
					panic(fmt.Sprintf("needed column %d not in colIdxMap", id))
				}
			}
		}

		// - If there is more than one table, we have to decode the index key to
		//   figure out which table the row belongs to.
		// - If there are interleaves, we need to read the index key in order to
		//   determine whether this row is actually part of the index we're scanning.
		// - If there are needed columns from the index key, we need to read it.
		//
		// Otherwise, we can completely avoid decoding the index key.
		if !rf.mustDecodeIndexKey && (neededIndexCols > 0 || len(table.index.InterleavedBy) > 0 || len(table.index.Interleave.Ancestors) > 0) {
			rf.mustDecodeIndexKey = true
		}

		// The number of columns we need to read from the value part of the key.
		// It's the total number of needed columns minus the ones we read from the
		// index key, except for composite columns.
		table.neededValueCols = table.neededCols.Len() - neededIndexCols + len(table.index.CompositeColumnIDs)

		if table.isSecondaryIndex {
			for i := range table.cols {
				if table.neededCols.Contains(int(table.cols[i].ID)) && !table.index.ContainsColumnID(table.cols[i].ID) {
					return fmt.Errorf("requested column %s not in index", table.cols[i].Name)
				}
			}
		}

		// Prepare our index key vals slice.
		table.keyValTypes, err = GetColumnTypes(table.desc, indexColumnIDs)
		if err != nil {
			return err
		}
		table.keyVals = make([]EncDatum, len(indexColumnIDs))

		if hasExtraCols(&table) {
			// Unique secondary indexes have a value that is the
			// primary index key.
			// Primary indexes only contain ascendingly-encoded
			// values. If this ever changes, we'll probably have to
			// figure out the directions here too.
			table.extraTypes, err = GetColumnTypes(table.desc, table.index.ExtraColumnIDs)
			table.extraVals = make([]EncDatum, len(table.index.ExtraColumnIDs))
			if err != nil {
				return err
			}
		}

		// Keep track of the maximum keys per row to accommodate a
		// limitHint when StartScan is invoked.
		if keysPerRow := table.desc.KeysPerRow(table.index.ID); keysPerRow > rf.maxKeysPerRow {
			rf.maxKeysPerRow = keysPerRow
		}

		rf.tables = append(rf.tables, table)
	}

	if len(tables) == 1 {
		// If there is more than one table, currentTable will be
		// updated every time NextKey is invoked and rowReadyTable
		// will be updated when a row is fully decoded.
		rf.currentTable = &(rf.tables[0])
		rf.rowReadyTable = &(rf.tables[0])
	}

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *RowFetcher) StartScan(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	if len(spans) == 0 {
		panic("no spans")
	}

	rf.traceKV = traceKV

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = limitHint * int64(rf.maxKeysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	f, err := makeKVFetcher(txn, spans, rf.reverse, limitBatches, firstBatchLimit, rf.returnRangeInfo)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f)
}

// StartScanFrom initializes and starts a scan from the given kvFetcher. Can be
// used multiple times.
func (rf *RowFetcher) StartScanFrom(ctx context.Context, f kvFetcher) error {
	rf.indexKey = nil
	rf.kvFetcher = f
	rf.kvs = nil
	// Retrieve the first key.
	_, err := rf.NextKey(ctx)
	return err
}

func (rf *RowFetcher) nextKV(ctx context.Context) (ok bool, kv roachpb.KeyValue, err error) {
	if len(rf.kvs) != 0 {
		kv = rf.kvs[0]
		rf.kvs = rf.kvs[1:]
		return true, kv, nil
	}
	ok, rf.kvs, err = rf.kvFetcher.nextBatch(ctx)
	if err != nil {
		return ok, kv, err
	}
	if !ok {
		return false, kv, nil
	}
	return rf.nextKV(ctx)
}

// IndexKeyString returns the currently searching index key up to `numCols`.
// For example, if there is an index on 2 columns and the current index key
// is '/Table/81/1/12/13/1', IndexKeyString(1) will return 'Table/81/1/12'.
func (rf *RowFetcher) IndexKeyString(numCols int) string {
	if rf.kv.Key == nil {
		return ""
	}
	splitKey := strings.Split(rf.kv.Key.String(), "/")
	// Take example index key from above, will be split into:
	// ["", "Table", "81", "1", "12", "13", "1"], first 4 are always returned
	// plus any additional columns requested.
	targetSlashes := 4 + numCols
	if targetSlashes > len(splitKey) {
		return strings.Join(splitKey, "/")
	}
	return strings.Join(splitKey[:targetSlashes], "/")
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (rf *RowFetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
	var ok bool

	for {
		ok, rf.kv, err = rf.nextKV(ctx)
		if err != nil {
			return false, err
		}
		rf.kvEnd = !ok
		if rf.kvEnd {
			// No more keys in the scan. We need to transition
			// rf.rowReadyTable to rf.currentTable for the last
			// row.
			rf.rowReadyTable = rf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, ok, err = rf.ReadIndexKey(rf.kv.Key)
			if err != nil {
				return false, err
			}
			if !ok {
				// The key did not match any of the table
				// descriptors, which means it's interleaved
				// data from some other table or index.
				continue
			}
		} else {
			// We still need to consume the key until the family
			// id, so processKV can know whether we've finished a
			// row or not.
			prefixLen, err := keys.GetRowPrefixLength(rf.kv.Key)
			if err != nil {
				return false, err
			}
			rf.keyRemainingBytes = rf.kv.Key[prefixLen:]
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
		// If rf.rowReadyTable differs from rf.currentTable, this denotes
		// a row is ready for output.
		if rf.indexKey != nil && (rf.currentTable.isSecondaryIndex || !bytes.HasPrefix(rf.kv.Key, rf.indexKey) || rf.rowReadyTable != rf.currentTable) {
			// The current key belongs to a new row. Output the
			// current row.
			rf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

func (rf *RowFetcher) prettyEncDatums(types []ColumnType, vals []EncDatum) string {
	var buf bytes.Buffer
	for i, v := range vals {
		if err := v.EnsureDecoded(&types[i], rf.alloc); err != nil {
			fmt.Fprintf(&buf, "error decoding: %v", err)
		}
		fmt.Fprintf(&buf, "/%v", v.Datum)
	}
	return buf.String()
}

// ReadIndexKey decodes an index key for a given table.
// It returns whether or not the key is for any of the tables initialized
// in RowFetcher, and the remaining part of the key if it is.
func (rf *RowFetcher) ReadIndexKey(key roachpb.Key) (remaining []byte, ok bool, err error) {
	// If there is only one table to check keys for, there is no need
	// to go through the equivalence signature checks.
	if len(rf.tables) == 1 {
		return DecodeIndexKeyWithoutTableIDIndexIDPrefix(
			rf.currentTable.desc,
			rf.currentTable.index,
			rf.currentTable.keyValTypes,
			rf.currentTable.keyVals,
			rf.currentTable.indexColumnDirs,
			key[rf.knownPrefixLength:],
		)
	}

	// Make a copy of the initial key for validating whether it's within
	// the table's specified spans.
	initialKey := key

	// key now contains the bytes in the key (if match) that are not part
	// of the signature in order.
	tableIdx, key, match, err := IndexKeyEquivSignature(key, rf.allEquivSignatures, rf.keySigBuf, rf.keyRestBuf)
	if err != nil {
		return nil, false, err
	}
	// The index key does not belong to our table because either:
	// !match:	    part of the index key's signature did not match any of
	//		    rf.allEquivSignatures.
	// tableIdx == -1:  index key belongs to an ancestor.
	if !match || tableIdx == -1 {
		return nil, false, nil
	}

	// The index key is not within our specified span of keys for the
	// particular table.
	// TODO(richardwu): ContainsKey checks every span within spans. We
	// can check that spans is ordered (or sort it) and memoize
	// the last span we've checked for each table. We can pass in this
	// information to ContainsKey as a hint for which span to start
	// checking first.
	if !rf.tables[tableIdx].spans.ContainsKey(initialKey) {
		return nil, false, nil
	}

	// Either a new table is encountered or the rowReadyTable differs from
	// the currentTable (the rowReadyTable was outputted in the previous
	// read). We transition the references.
	if &rf.tables[tableIdx] != rf.currentTable || rf.rowReadyTable != rf.currentTable {
		rf.rowReadyTable = rf.currentTable
		rf.currentTable = &rf.tables[tableIdx]

		// rf.rowReadyTable is nil if this is the very first key.
		// We want to ensure this does not differ from rf.currentTable
		// to prevent another transition.
		if rf.rowReadyTable == nil {
			rf.rowReadyTable = rf.currentTable
		}
	}

	// We can simply decode all the column values we retrieved
	// when processing the index key. The column values are at the
	// front of the key.
	if key, err = DecodeKeyVals(
		rf.currentTable.keyValTypes,
		rf.currentTable.keyVals,
		rf.currentTable.indexColumnDirs,
		key,
	); err != nil {
		return nil, false, err
	}

	return key, true, nil
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *RowFetcher) processKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := rf.currentTable

	if rf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s%s",
			table.desc.Name,
			table.index.Name,
			rf.prettyEncDatums(table.keyValTypes, table.keyVals),
		)
	}

	// Either this is the first key of the fetch or the first key of a new
	// row.
	if rf.indexKey == nil {
		// This is the first key for the row.
		rf.indexKey = []byte(kv.Key[:len(kv.Key)-len(rf.keyRemainingBytes)])

		// Reset the row to nil; it will get filled in with the column
		// values as we decode the key-value pairs for the row.
		// We only need to reset the needed columns in the value component, because
		// non-needed columns are never set and key columns are unconditionally set
		// below.
		for idx, ok := table.neededValueColsByIdx.Next(0); ok; idx, ok = table.neededValueColsByIdx.Next(idx + 1) {
			table.row[idx].UnsetDatum()
		}

		// Fill in the column values that are part of the index key.
		for i := range table.keyVals {
			if idx := table.indexColIdx[i]; idx != -1 {
				table.row[idx] = table.keyVals[i]
			}
		}

		rf.valueColsFound = 0
	}

	if table.neededCols.Empty() {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = tree.DNull.String()
		}
		return prettyKey, prettyValue, nil
	}

	if !table.isSecondaryIndex && len(rf.keyRemainingBytes) > 0 {
		// If familyID is 0, kv.Value contains values for composite key columns.
		// These columns already have a table.row value assigned above, but that value
		// (obtained from the key encoding) might not be correct (e.g. for decimals,
		// it might not contain the right number of trailing 0s; for collated
		// strings, it is one of potentially many strings with the same collation
		// key).
		//
		// In these cases, the correct value will be present in family 0 and the
		// table.row value gets overwritten.

		switch kv.Value.GetTag() {
		case roachpb.ValueType_TUPLE:
			// In this case, we don't need to decode the column family ID, because
			// the ValueType_TUPLE encoding includes the column id with every encoded
			// column value.
			prettyKey, prettyValue, err = rf.processValueTuple(ctx, table, kv, prettyKey)
		default:
			var familyID uint64
			_, familyID, err = encoding.DecodeUvarintAscending(rf.keyRemainingBytes)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			var family *ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(FamilyID(familyID))
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, family, kv, prettyKey)
		}
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
		}
	} else {
		valueBytes, err := kv.Value.GetBytes()
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
		}

		if hasExtraCols(table) {
			// This is a unique secondary index; decode the extra
			// column values from the value.
			var err error
			valueBytes, err = DecodeKeyVals(
				table.extraTypes,
				table.extraVals,
				nil,
				valueBytes,
			)
			if err != nil {
				return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
			}
			for i, id := range table.index.ExtraColumnIDs {
				if table.neededCols.Contains(int(id)) {
					table.row[table.colIdxMap[id]] = table.extraVals[i]
				}
			}
			if rf.traceKV {
				prettyValue = rf.prettyEncDatums(table.extraTypes, table.extraVals)
			}
		}

		if debugRowFetch {
			if hasExtraCols(table) {
				log.Infof(ctx, "Scan %s -> %s", kv.Key, rf.prettyEncDatums(table.extraTypes, table.extraVals))
			} else {
				log.Infof(ctx, "Scan %s", kv.Key)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = rf.processValueBytes(
				ctx, table, kv, valueBytes, prettyKey,
			)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if rf.traceKV && prettyValue == "" {
		prettyValue = tree.DNull.String()
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column
// family.DefaultColumnID), setting values in table.row accordingly. The key is
// only used for logging.
func (rf *RowFetcher) processValueSingle(
	ctx context.Context,
	table *tableInfo,
	family *ColumnFamilyDescriptor,
	kv roachpb.KeyValue,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix

	// If this is the row sentinel (in the legacy pre-family format),
	// a value is not expected, so we're done.
	if family.ID == 0 {
		return "", "", nil
	}

	colID := family.DefaultColumnID
	if colID == 0 {
		return "", "", errors.Errorf("single entry value with no default column id")
	}

	if rf.traceKV || table.neededCols.Contains(int(colID)) {
		if idx, ok := table.colIdxMap[colID]; ok {
			if rf.traceKV {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
			}
			typ := table.cols[idx].Type
			// TODO(arjun): The value is a directly marshaled single value, so we
			// unmarshal it eagerly here. This can potentially be optimized out,
			// although that would require changing UnmarshalColumnValue to operate
			// on bytes, and for Encode/DecodeTableValue to operate on marshaled
			// single values.
			value, err := UnmarshalColumnValue(rf.alloc, typ, kv.Value)
			if err != nil {
				return "", "", err
			}
			if rf.traceKV {
				prettyValue = value.String()
			}
			table.row[idx] = DatumToEncDatum(typ, value)
			if debugRowFetch {
				log.Infof(ctx, "Scan %s -> %v", kv.Key, value)
			}
			return prettyKey, prettyValue, nil
		}
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if debugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
	}
	return prettyKey, prettyValue, nil
}

func (rf *RowFetcher) processValueBytes(
	ctx context.Context,
	table *tableInfo,
	kv roachpb.KeyValue,
	valueBytes []byte,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if rf.traceKV {
		if rf.prettyValueBuf == nil {
			rf.prettyValueBuf = &bytes.Buffer{}
		}
		rf.prettyValueBuf.Reset()
	}

	var colIDDiff uint32
	var lastColID ColumnID
	var typeOffset, dataOffset int
	var typ encoding.Type
	for len(valueBytes) > 0 && rf.valueColsFound < table.neededValueCols {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + ColumnID(colIDDiff)
		lastColID = colID
		if !table.neededCols.Contains(int(colID)) {
			// This column wasn't requested, so read its length and skip it.
			len, err := encoding.PeekValueLengthWithOffsetsAndType(valueBytes, dataOffset, typ)
			if err != nil {
				return "", "", err
			}
			valueBytes = valueBytes[len:]
			if debugRowFetch {
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}
		idx := table.colIdxMap[colID]

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
		}

		var encValue EncDatum
		encValue, valueBytes, err = EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
			dataOffset, typ)
		if err != nil {
			return "", "", err
		}
		if rf.traceKV {
			err := encValue.EnsureDecoded(&table.cols[idx].Type, rf.alloc)
			if err != nil {
				return "", "", err
			}
			fmt.Fprintf(rf.prettyValueBuf, "/%v", encValue.Datum)
		}
		table.row[idx] = encValue
		rf.valueColsFound++
		if debugRowFetch {
			log.Infof(ctx, "Scan %d -> %v", idx, encValue)
		}
	}
	if rf.traceKV {
		prettyValue = rf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *RowFetcher) processValueTuple(
	ctx context.Context, table *tableInfo, kv roachpb.KeyValue, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}
	return rf.processValueBytes(ctx, table, kv, tupleBytes, prettyKeyPrefix)
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per neededCols) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil. The error returned may
// be a scrub.ScrubError, which the caller is responsible for unwrapping.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *RowFetcher) NextRow(
	ctx context.Context,
) (row EncDatumRow, table *TableDescriptor, index *IndexDescriptor, err error) {
	if rf.kvEnd {
		return nil, nil, nil, nil
	}

	// All of the columns for a particular row will be grouped together. We
	// loop over the key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column
	// ID to lookup the column and decode the value. All of these values go
	// into a map keyed by column name. When the index key changes we
	// output a row containing the current values.
	for {
		prettyKey, prettyVal, err := rf.processKV(ctx, rf.kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		if rf.isCheck {
			rf.rowReadyTable.lastKV = rf.kv
		}
		rowDone, err := rf.NextKey(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if rowDone {
			err := rf.finalizeRow()
			return rf.rowReadyTable.row, rf.rowReadyTable.desc, rf.rowReadyTable.index, err
		}
	}
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a Datums.
// The Datums should not be modified and is only valid until the next call.
// When there are no more rows, the Datums is nil.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *RowFetcher) NextRowDecoded(
	ctx context.Context,
) (datums tree.Datums, table *TableDescriptor, index *IndexDescriptor, err error) {
	row, table, index, err := rf.NextRow(ctx)
	if err != nil {
		err = scrub.UnwrapScrubError(err)
		return nil, nil, nil, err
	}
	if row == nil {
		return nil, nil, nil, nil
	}

	for i, encDatum := range row {
		if encDatum.IsUnset() {
			rf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := encDatum.EnsureDecoded(&rf.rowReadyTable.cols[i].Type, rf.alloc); err != nil {
			return nil, nil, nil, err
		}
		rf.rowReadyTable.decodedRow[i] = encDatum.Datum
	}

	return rf.rowReadyTable.decodedRow, table, index, nil
}

// NextRowWithErrors calls NextRow to fetch the next row and also run
// additional additional logic for physical checks. The Datums should
// not be modified and are only valid until the next call. When there
// are no more rows, the Datums is nil. The checks executed include:
//  - k/v data round-trips, i.e. it decodes and re-encodes to the same
//    value.
//  - There is no extra unexpected or incorrect data encoded in the k/v
//    pair.
//  - Decoded keys follow the same ordering as their encoding.
func (rf *RowFetcher) NextRowWithErrors(ctx context.Context) (EncDatumRow, error) {
	row, table, index, err := rf.NextRow(ctx)
	if row == nil {
		return nil, nil
	} else if err != nil {
		// If this is not already a wrapped error, we will consider it to be
		// a generic physical error.
		// FIXME(joey): This may not be needed if we capture all the errors
		// encountered. This is a TBD when this change is polished.
		if !scrub.IsScrubError(err) {
			err = scrub.WrapError(scrub.PhysicalError, err)
		}
		return row, err
	}

	// Decode the row in-place. The following check datum encoding
	// functions require that the table.row datums are decoded.
	for i := range row {
		if row[i].IsUnset() {
			rf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := row[i].EnsureDecoded(&rf.rowReadyTable.cols[i].Type, rf.alloc); err != nil {
			return nil, err
		}
		rf.rowReadyTable.decodedRow[i] = row[i].Datum
	}

	if index.ID == table.PrimaryIndex.ID {
		err = rf.checkPrimaryIndexDatumEncodings(ctx)
	} else {
		err = rf.checkSecondaryIndexDatumEncodings(ctx)
	}
	if err != nil {
		return row, err
	}

	err = rf.checkKeyOrdering(ctx)

	return row, err
}

// checkPrimaryIndexDatumEncodings will run a round-trip encoding check
// on all values in the buffered row. This check is specific to primary
// index datums.
func (rf *RowFetcher) checkPrimaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	scratch := make([]byte, 1024)
	colIDToColumn := make(map[ColumnID]ColumnDescriptor)
	for _, col := range table.desc.Columns {
		colIDToColumn[col.ID] = col
	}

	rh := rowHelper{TableDesc: table.desc, Indexes: table.desc.Indexes}

	for _, family := range table.desc.Families {
		var lastColID ColumnID
		familySortedColumnIDs, ok := rh.sortedColumnFamily(family.ID)
		if !ok {
			panic("invalid family sorted column id map")
		}

		for _, colID := range familySortedColumnIDs {
			rowVal := table.row[table.colIdxMap[colID]]
			if rowVal.IsNull() {
				// Column is not present.
				continue
			}

			if skip, err := rh.skipColumnInPK(colID, family.ID, rowVal.Datum); err != nil {
				log.Errorf(ctx, "unexpected error: %s", err)
				continue
			} else if skip {
				continue
			}

			col := colIDToColumn[colID]

			if lastColID > col.ID {
				panic(fmt.Errorf("cannot write column id %d after %d", col.ID, lastColID))
			}
			colIDDiff := col.ID - lastColID
			lastColID = col.ID

			if result, err := EncodeTableValue([]byte(nil), colIDDiff, rowVal.Datum,
				scratch); err != nil {
				log.Errorf(ctx, "Could not re-encode column %s, value was %#v. Got error %s",
					col.Name, rowVal.Datum, err)
			} else if !bytes.Equal(result, rowVal.encoded) {
				return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
					"value failed to round-trip encode. Column=%s colIDDiff=%d Key=%s expected %#v, got: %#v",
					col.Name, colIDDiff, rf.kv.Key, rowVal.encoded, result))
			}
		}
	}
	return nil
}

// checkSecondaryIndexDatumEncodings will run a round-trip encoding
// check on all values in the buffered row. This check is specific to
// secondary index datums.
func (rf *RowFetcher) checkSecondaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	colToEncDatum := make(map[ColumnID]EncDatum, len(table.row))
	values := make(tree.Datums, len(table.row))
	for i, col := range table.cols {
		colToEncDatum[col.ID] = table.row[i]
		values[i] = table.row[i].Datum
	}

	indexEntries, err := EncodeSecondaryIndex(table.desc, table.index, table.colIdxMap, values)
	if err != nil {
		return err
	}

	for _, indexEntry := range indexEntries {
		// We ignore the first 4 bytes of the values. These bytes are a
		// checksum which are not set by EncodeSecondaryIndex.
		if !indexEntry.Key.Equal(rf.rowReadyTable.lastKV.Key) {
			return scrub.WrapError(scrub.IndexKeyDecodingError, errors.Errorf(
				"secondary index key failed to round-trip encode. expected %#v, got: %#v",
				rf.rowReadyTable.lastKV.Key, indexEntry.Key))
		} else if !indexEntry.Value.EqualData(table.lastKV.Value) {
			return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
				"secondary index value failed to round-trip encode. expected %#v, got: %#v",
				rf.rowReadyTable.lastKV.Value, indexEntry.Value))
		}
	}
	return nil
}

// checkKeyOrdering verifies that the datums decoded for the current key
// have the same ordering as the encoded key.
func (rf *RowFetcher) checkKeyOrdering(ctx context.Context) error {
	defer func() {
		rf.rowReadyTable.lastDatums = append(tree.Datums(nil), rf.rowReadyTable.decodedRow...)
	}()

	if !rf.rowReadyTable.hasLast {
		rf.rowReadyTable.hasLast = true
		return nil
	}

	evalCtx := tree.EvalContext{}
	for i, id := range rf.rowReadyTable.index.ColumnIDs {
		idx := rf.rowReadyTable.colIdxMap[id]
		result := rf.rowReadyTable.decodedRow[idx].Compare(&evalCtx, rf.rowReadyTable.lastDatums[idx])
		expectedDirection := rf.rowReadyTable.index.ColumnDirections[i]
		if rf.reverse && expectedDirection == IndexDescriptor_ASC {
			expectedDirection = IndexDescriptor_DESC
		} else if rf.reverse && expectedDirection == IndexDescriptor_DESC {
			expectedDirection = IndexDescriptor_ASC
		}

		if expectedDirection == IndexDescriptor_ASC && result < 0 ||
			expectedDirection == IndexDescriptor_DESC && result > 0 {
			return scrub.WrapError(scrub.IndexKeyDecodingError,
				errors.Errorf("key ordering did not match datum ordering. IndexDescriptor=%s",
					expectedDirection))
		}
	}
	return nil
}

func (rf *RowFetcher) finalizeRow() error {
	table := rf.rowReadyTable
	// Fill in any missing values with NULLs
	for i := range table.cols {
		if rf.valueColsFound == table.neededValueCols {
			// Found all cols - done!
			return nil
		}
		if table.neededCols.Contains(int(table.cols[i].ID)) && table.row[i].IsUnset() {
			if !table.cols[i].Nullable {
				var indexColValues []string
				for _, idx := range table.indexColIdx {
					if idx != -1 {
						indexColValues = append(indexColValues, table.row[idx].String(&table.cols[idx].Type))
					} else {
						indexColValues = append(indexColValues, "?")
					}
				}
				if rf.isCheck {
					return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
						"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
						table.desc.Name, table.cols[i].Name, table.index.Name,
						strings.Join(table.index.ColumnNames, ","), strings.Join(indexColValues, ",")))
				}
				panic(fmt.Sprintf(
					"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.desc.Name, table.cols[i].Name, table.index.Name,
					strings.Join(table.index.ColumnNames, ","), strings.Join(indexColValues, ",")))
			}
			table.row[i] = EncDatum{
				Datum: tree.DNull,
			}
			// We've set valueColsFound to the number of present columns in the row
			// already, in processValueBytes. Now, we're filling in columns that have
			// no encoded values with NULL - so we increment valueColsFound to permit
			// early exit from this loop once all needed columns are filled in.
			rf.valueColsFound++
		}
	}
	return nil
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

// Only unique secondary indexes have extra columns to decode (namely the
// primary index columns).
func hasExtraCols(table *tableInfo) bool {
	return table.isSecondaryIndex && table.index.Unique
}
