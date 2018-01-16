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
	nextKV(ctx context.Context) (bool, roachpb.KeyValue, error)
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

	// Map used to get the index for columns in cols.
	colIdxMap map[ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into cols).
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
	// It is required because MultiRowFetcher.kv is overwritten before NextRow
	// returns.
	lastKV roachpb.KeyValue
	// lastDatums is a buffer for the previously scanned k/v datums. It is
	// only present when doing a physical check in order to verify
	// ordering.
	lastDatums tree.Datums
}

// MultiRowFetcherTableArgs are the arguments passed to MultiRowFetcher.Init
// for a given table that includes descriptors and row information.
type MultiRowFetcherTableArgs struct {
	// The spans of keys to return for the given table. MultiRowFetcher
	// ignores keys outside these spans.
	// This is irrelevant if MultiRowFetcher is initialize with only one
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

// MultiRowFetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var mrf MultiRowFetcher
//   err := mrf.Init(..)
//   // Handle err
//   err := mrf.StartScan(..)
//   // Handle err
//   for {
//      res, err := mrf.NextRow()
//      // Handle err
//      if res.row == nil {
//         // Done
//         break
//      }
//      // Process res.row
//   }
type MultiRowFetcher struct {
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

	rowReadyTable *tableInfo // the table for which a row was fully decoded and ready for output
	currentTable  *tableInfo // the most recent table for which a key was decoded
	keySigBuf     []byte     // buffer for the index key's signature
	keyRestBuf    []byte     // buffer for the rest of the index key that is not part of the signature

	// The current key/value, unless kvEnd is true.
	kv                roachpb.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	// isCheck indicates whether or not we are running checks for k/v
	// correctness. It is set only during SCRUB commands.
	isCheck bool

	// Buffered allocation of decoded datums.
	alloc *DatumAlloc
}

// Init sets up a MultiRowFetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (mrf *MultiRowFetcher) Init(
	reverse,
	returnRangeInfo bool,
	isCheck bool,
	alloc *DatumAlloc,
	tables ...MultiRowFetcherTableArgs,
) error {
	if len(tables) == 0 {
		panic("no tables to fetch from")
	}

	mrf.reverse = reverse
	mrf.returnRangeInfo = returnRangeInfo
	mrf.alloc = alloc
	mrf.allEquivSignatures = make(map[string]int, len(tables))
	mrf.isCheck = isCheck

	// We must always decode the index key if we need to distinguish between
	// rows from more than one table.
	mrf.mustDecodeIndexKey = len(tables) >= 2

	mrf.tables = make([]tableInfo, 0, len(tables))
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
					mrf.allEquivSignatures[string(sig)] = tableIdx
					break
				}
				// Map each table's ancestors' signatures to -1 so
				// we know during ReadIndexKey if the parsed index
				// key belongs to ancestor or one of our tables.
				// We must check if the signature has already been set
				// since it's possible for a later 'table' to have an
				// ancestor that is a previous 'table', and we do not
				// want to overwrite the previous table's tableIdx.
				if _, exists := mrf.allEquivSignatures[string(sig)]; !exists {
					mrf.allEquivSignatures[string(sig)] = -1
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

		// If there is more than one table, we have to decode the index key to
		// figure out which table the row belongs to.
		// If there are interleaves, we need to read the index key in order to
		// determine whether this row is actually part of the index we're scanning.
		// If we need to return any values from the row, we also have to read the
		// index key to either get those values directly or determine the row's
		// column family id to map the row values to their columns.
		// Otherwise, we can completely avoid decoding the index key.
		// TODO(jordan): Relax this restriction. Ideally we could skip doing key
		// reading work if we need values from outside of the key, but not from
		// inside of the key.
		if !mrf.mustDecodeIndexKey && (!table.neededCols.Empty() || len(table.index.InterleavedBy) > 0 || len(table.index.Interleave.Ancestors) > 0) {
			mrf.mustDecodeIndexKey = true
		}

		var indexColumnIDs []ColumnID
		indexColumnIDs, table.indexColumnDirs = table.index.FullColumnIDs()

		table.indexColIdx = make([]int, len(indexColumnIDs))
		for i, id := range indexColumnIDs {
			table.indexColIdx[i] = table.colIdxMap[id]
		}

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
		if keysPerRow := table.desc.KeysPerRow(table.index.ID); keysPerRow > mrf.maxKeysPerRow {
			mrf.maxKeysPerRow = keysPerRow
		}

		mrf.tables = append(mrf.tables, table)
	}

	if len(tables) == 1 {
		// If there is more than one table, currentTable will be
		// updated every time NextKey is invoked and rowReadyTable
		// will be updated when a row is fully decoded.
		mrf.currentTable = &(mrf.tables[0])
		mrf.rowReadyTable = &(mrf.tables[0])
	}

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (mrf *MultiRowFetcher) StartScan(
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

	mrf.traceKV = traceKV

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := limitHint
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = limitHint * int64(mrf.maxKeysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	f, err := makeKVFetcher(txn, spans, mrf.reverse, limitBatches, firstBatchLimit, mrf.returnRangeInfo)
	if err != nil {
		return err
	}
	return mrf.StartScanFrom(ctx, &f)
}

// StartScanFrom initializes and starts a scan from the given kvFetcher. Can be
// used multiple times.
func (mrf *MultiRowFetcher) StartScanFrom(ctx context.Context, f kvFetcher) error {
	mrf.indexKey = nil
	mrf.kvFetcher = f
	// Retrieve the first key.
	_, err := mrf.NextKey(ctx)
	return err
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (mrf *MultiRowFetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
	var ok bool

	for {
		ok, mrf.kv, err = mrf.kvFetcher.nextKV(ctx)
		if err != nil {
			return false, err
		}
		mrf.kvEnd = !ok
		if mrf.kvEnd {
			// No more keys in the scan. We need to transition
			// mrf.rowReadyTable to mrf.currentTable for the last
			// row.
			mrf.rowReadyTable = mrf.currentTable
			return true, nil
		}

		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if mrf.mustDecodeIndexKey || mrf.traceKV {
			mrf.keyRemainingBytes, ok, err = mrf.ReadIndexKey(mrf.kv.Key)
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
			prefixLen, err := keys.GetRowPrefixLength(mrf.kv.Key)
			if err != nil {
				return false, err
			}
			mrf.keyRemainingBytes = mrf.kv.Key[prefixLen:]
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
		// If mrf.rowReadyTable differs from mrf.currentTable, this denotes
		// a row is ready for output.
		if mrf.indexKey != nil && (mrf.currentTable.isSecondaryIndex || !bytes.HasPrefix(mrf.kv.Key, mrf.indexKey) || mrf.rowReadyTable != mrf.currentTable) {
			// The current key belongs to a new row. Output the
			// current row.
			mrf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

func (mrf *MultiRowFetcher) prettyEncDatums(types []ColumnType, vals []EncDatum) string {
	var buf bytes.Buffer
	for i, v := range vals {
		if err := v.EnsureDecoded(&types[i], mrf.alloc); err != nil {
			fmt.Fprintf(&buf, "error decoding: %v", err)
		}
		fmt.Fprintf(&buf, "/%v", v.Datum)
	}
	return buf.String()
}

// ReadIndexKey decodes an index key for a given table.
// It returns whether or not the key is for any of the tables initialized
// in MultiRowFetcher, and the remaining part of the key if it is.
func (mrf *MultiRowFetcher) ReadIndexKey(key roachpb.Key) (remaining []byte, ok bool, err error) {
	// If there is only one table to check keys for, there is no need
	// to go through the equivalence signature checks.
	if len(mrf.tables) == 1 {
		return DecodeIndexKey(
			mrf.currentTable.desc,
			mrf.currentTable.index,
			mrf.currentTable.keyValTypes,
			mrf.currentTable.keyVals,
			mrf.currentTable.indexColumnDirs,
			key,
		)
	}

	// Make a copy of the initial key for validating whether it's within
	// the table's specified spans.
	initialKey := key

	// key now contains the bytes in the key (if match) that are not part
	// of the signature in order.
	tableIdx, key, match, err := IndexKeyEquivSignature(key, mrf.allEquivSignatures, mrf.keySigBuf, mrf.keyRestBuf)
	if err != nil {
		return nil, false, err
	}
	// The index key does not belong to our table because either:
	// !match:	    part of the index key's signature did not match any of
	//		    mrf.allEquivSignatures.
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
	if !mrf.tables[tableIdx].spans.ContainsKey(initialKey) {
		return nil, false, nil
	}

	// Either a new table is encountered or the rowReadyTable differs from
	// the currentTable (the rowReadyTable was outputted in the previous
	// read). We transition the references.
	if &mrf.tables[tableIdx] != mrf.currentTable || mrf.rowReadyTable != mrf.currentTable {
		mrf.rowReadyTable = mrf.currentTable
		mrf.currentTable = &mrf.tables[tableIdx]

		// mrf.rowReadyTable is nil if this is the very first key.
		// We want to ensure this does not differ from mrf.currentTable
		// to prevent another transition.
		if mrf.rowReadyTable == nil {
			mrf.rowReadyTable = mrf.currentTable
		}
	}

	// We can simply decode all the column values we retrieved
	// when processing the index key. The column values are at the
	// front of the key.
	if key, err = DecodeKeyVals(
		mrf.currentTable.keyVals,
		mrf.currentTable.indexColumnDirs,
		key,
	); err != nil {
		return nil, false, err
	}

	return key, true, nil
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (mrf *MultiRowFetcher) processKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := mrf.currentTable

	if mrf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s%s",
			table.desc.Name,
			table.index.Name,
			mrf.prettyEncDatums(table.keyValTypes, table.keyVals),
		)
	}

	// Either this is the first key of the fetch or the first key of a new
	// row.
	if mrf.indexKey == nil {
		// This is the first key for the row.
		mrf.indexKey = []byte(kv.Key[:len(kv.Key)-len(mrf.keyRemainingBytes)])

		// Reset the row to nil; it will get filled in with the column
		// values as we decode the key-value pairs for the row.
		for i := range table.row {
			table.row[i].UnsetDatum()
		}

		// Fill in the column values that are part of the index key.
		for i := range table.keyVals {
			table.row[table.indexColIdx[i]] = table.keyVals[i]
		}
	}

	if table.neededCols.Empty() {
		// We don't need to decode any values.
		if mrf.traceKV {
			prettyValue = tree.DNull.String()
		}
		return prettyKey, prettyValue, nil
	}

	if !table.isSecondaryIndex && len(mrf.keyRemainingBytes) > 0 {
		_, familyID, err := encoding.DecodeUvarintAscending(mrf.keyRemainingBytes)
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
		}

		family, err := table.desc.FindFamilyByID(FamilyID(familyID))
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
		}

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
			prettyKey, prettyValue, err = mrf.processValueTuple(ctx, table, kv, prettyKey)
		default:
			prettyKey, prettyValue, err = mrf.processValueSingle(ctx, table, family, kv, prettyKey)
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
			if mrf.traceKV {
				prettyValue = mrf.prettyEncDatums(table.extraTypes, table.extraVals)
			}
		}

		if debugRowFetch {
			if hasExtraCols(table) {
				log.Infof(ctx, "Scan %s -> %s", kv.Key, mrf.prettyEncDatums(table.extraTypes, table.extraVals))
			} else {
				log.Infof(ctx, "Scan %s", kv.Key)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = mrf.processValueBytes(
				ctx, table, kv, valueBytes, prettyKey,
			)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if mrf.traceKV && prettyValue == "" {
		prettyValue = tree.DNull.String()
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column
// family.DefaultColumnID), setting values in table.row accordingly. The key is
// only used for logging.
func (mrf *MultiRowFetcher) processValueSingle(
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

	if mrf.traceKV || table.neededCols.Contains(int(colID)) {
		if idx, ok := table.colIdxMap[colID]; ok {
			if mrf.traceKV {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
			}
			typ := table.cols[idx].Type
			// TODO(arjun): The value is a directly marshaled single value, so we
			// unmarshal it eagerly here. This can potentially be optimized out,
			// although that would require changing UnmarshalColumnValue to operate
			// on bytes, and for Encode/DecodeTableValue to operate on marshaled
			// single values.
			value, err := UnmarshalColumnValue(mrf.alloc, typ, kv.Value)
			if err != nil {
				return "", "", err
			}
			if mrf.traceKV {
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

func (mrf *MultiRowFetcher) processValueBytes(
	ctx context.Context,
	table *tableInfo,
	kv roachpb.KeyValue,
	valueBytes []byte,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if mrf.traceKV {
		if mrf.prettyValueBuf == nil {
			mrf.prettyValueBuf = &bytes.Buffer{}
		}
		mrf.prettyValueBuf.Reset()
	}

	var colIDDiff uint32
	var lastColID ColumnID
	var typeOffset, dataOffset int
	var typ encoding.Type
	for len(valueBytes) > 0 {
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

		if mrf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
		}

		var encValue EncDatum
		encValue, valueBytes, err = EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset, dataOffset, typ)
		if err != nil {
			return "", "", err
		}
		if mrf.traceKV {
			err := encValue.EnsureDecoded(&table.cols[idx].Type, mrf.alloc)
			if err != nil {
				return "", "", err
			}
			fmt.Fprintf(mrf.prettyValueBuf, "/%v", encValue.Datum)
		}
		table.row[idx] = encValue
		if debugRowFetch {
			log.Infof(ctx, "Scan %d -> %v", idx, encValue)
		}
	}
	if mrf.traceKV {
		prettyValue = mrf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the mrf.row accordingly. The key is only used for logging.
func (mrf *MultiRowFetcher) processValueTuple(
	ctx context.Context, table *tableInfo, kv roachpb.KeyValue, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}
	return mrf.processValueBytes(ctx, table, kv, tupleBytes, prettyKeyPrefix)
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per table column, regardless of the
// index used; values that are not needed (as per neededCols) are nil. The
// EncDatumRow should not be modified and is only valid until the next call.
// When there are no more rows, the EncDatumRow is nil. The error returned may
// be a scrub.ScrubError, which the caller is responsible for unwrapping.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (mrf *MultiRowFetcher) NextRow(
	ctx context.Context,
) (row EncDatumRow, table *TableDescriptor, index *IndexDescriptor, err error) {
	if mrf.kvEnd {
		return nil, nil, nil, nil
	}

	// All of the columns for a particular row will be grouped together. We
	// loop over the key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column
	// ID to lookup the column and decode the value. All of these values go
	// into a map keyed by column name. When the index key changes we
	// output a row containing the current values.
	for {
		prettyKey, prettyVal, err := mrf.processKV(ctx, mrf.kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if mrf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		if mrf.isCheck {
			mrf.rowReadyTable.lastKV = mrf.kv
		}
		rowDone, err := mrf.NextKey(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if rowDone {
			err := mrf.finalizeRow()
			return mrf.rowReadyTable.row, mrf.rowReadyTable.desc, mrf.rowReadyTable.index, err
		}
	}
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a Datums.
// The Datums should not be modified and is only valid until the next call.
// When there are no more rows, the Datums is nil.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (mrf *MultiRowFetcher) NextRowDecoded(
	ctx context.Context,
) (datums tree.Datums, table *TableDescriptor, index *IndexDescriptor, err error) {
	row, table, index, err := mrf.NextRow(ctx)
	if err != nil {
		err = scrub.UnwrapScrubError(err)
		return nil, nil, nil, err
	}
	if row == nil {
		return nil, nil, nil, nil
	}

	for i, encDatum := range row {
		if encDatum.IsUnset() {
			mrf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := encDatum.EnsureDecoded(&mrf.rowReadyTable.cols[i].Type, mrf.alloc); err != nil {
			return nil, nil, nil, err
		}
		mrf.rowReadyTable.decodedRow[i] = encDatum.Datum
	}

	return mrf.rowReadyTable.decodedRow, table, index, nil
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
func (mrf *MultiRowFetcher) NextRowWithErrors(ctx context.Context) (EncDatumRow, error) {
	row, table, index, err := mrf.NextRow(ctx)
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
			mrf.rowReadyTable.decodedRow[i] = tree.DNull
			continue
		}
		if err := row[i].EnsureDecoded(&mrf.rowReadyTable.cols[i].Type, mrf.alloc); err != nil {
			return nil, err
		}
		mrf.rowReadyTable.decodedRow[i] = row[i].Datum
	}

	if index.ID == table.PrimaryIndex.ID {
		err = mrf.checkPrimaryIndexDatumEncodings(ctx)
	} else {
		err = mrf.checkSecondaryIndexDatumEncodings(ctx)
	}
	if err != nil {
		return row, err
	}

	err = mrf.checkKeyOrdering(ctx)

	return row, err
}

// checkPrimaryIndexDatumEncodings will run a round-trip encoding check
// on all values in the buffered row. This check is specific to primary
// index datums.
func (mrf *MultiRowFetcher) checkPrimaryIndexDatumEncodings(ctx context.Context) error {
	table := mrf.rowReadyTable
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
					col.Name, colIDDiff, mrf.kv.Key, rowVal.encoded, result))
			}
		}
	}
	return nil
}

// checkSecondaryIndexDatumEncodings will run a round-trip encoding
// check on all values in the buffered row. This check is specific to
// secondary index datums.
func (mrf *MultiRowFetcher) checkSecondaryIndexDatumEncodings(ctx context.Context) error {
	table := mrf.rowReadyTable
	colToEncDatum := make(map[ColumnID]EncDatum, len(table.row))
	values := make(tree.Datums, len(table.row))
	for i, col := range table.cols {
		colToEncDatum[col.ID] = table.row[i]
		values[i] = table.row[i].Datum
	}

	indexEntry, err := EncodeSecondaryIndex(table.desc, table.index, table.colIdxMap, values)
	if err != nil {
		return err
	}

	// We ignore the first 4 bytes of the values. These bytes are a
	// checksum which are not set by EncodeSecondaryIndex.
	if !indexEntry.Key.Equal(mrf.rowReadyTable.lastKV.Key) {
		return scrub.WrapError(scrub.IndexKeyDecodingError, errors.Errorf(
			"secondary index key failed to round-trip encode. expected %#v, got: %#v",
			mrf.rowReadyTable.lastKV.Key, indexEntry.Key))
	} else if !bytes.Equal(indexEntry.Value.RawBytes[4:], table.lastKV.Value.RawBytes[4:]) {
		return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
			"secondary index value failed to round-trip encode. expected %#v, got: %#v",
			mrf.rowReadyTable.lastKV.Value.RawBytes[4:], indexEntry.Value.RawBytes[4:]))
	}
	return nil
}

// checkKeyOrdering verifies that the datums decoded for the current key
// have the same ordering as the encoded key.
func (mrf *MultiRowFetcher) checkKeyOrdering(ctx context.Context) error {
	defer func() {
		mrf.rowReadyTable.lastDatums = append(tree.Datums(nil), mrf.rowReadyTable.decodedRow...)
	}()

	if !mrf.rowReadyTable.hasLast {
		mrf.rowReadyTable.hasLast = true
		return nil
	}

	evalCtx := tree.EvalContext{}
	for i, id := range mrf.rowReadyTable.index.ColumnIDs {
		idx := mrf.rowReadyTable.colIdxMap[id]
		result := mrf.rowReadyTable.decodedRow[idx].Compare(&evalCtx, mrf.rowReadyTable.lastDatums[idx])
		expectedDirection := mrf.rowReadyTable.index.ColumnDirections[i]
		if mrf.reverse && expectedDirection == IndexDescriptor_ASC {
			expectedDirection = IndexDescriptor_DESC
		} else if mrf.reverse && expectedDirection == IndexDescriptor_DESC {
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

func (mrf *MultiRowFetcher) finalizeRow() error {
	table := mrf.rowReadyTable
	// Fill in any missing values with NULLs
	for i := range table.cols {
		if table.neededCols.Contains(int(table.cols[i].ID)) && table.row[i].IsUnset() {
			if !table.cols[i].Nullable {
				var indexColValues []string
				for i := range table.indexColIdx {
					indexColValues = append(indexColValues, table.row[i].String(&table.cols[i].Type))
				}
				if mrf.isCheck {
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
		}
	}
	return nil
}

// Key returns the next key (the key that follows the last returned row).
// Key returns nil when there are no more rows.
func (mrf *MultiRowFetcher) Key() roachpb.Key {
	return mrf.kv.Key
}

// GetRangeInfo returns information about the ranges where the rows came from.
// The RangeInfo's are deduped and not ordered.
func (mrf *MultiRowFetcher) GetRangeInfo() []roachpb.RangeInfo {
	return mrf.kvFetcher.getRangesInfo()
}

// Only unique secondary indexes have extra columns to decode (namely the
// primary index columns).
func hasExtraCols(table *tableInfo) bool {
	return table.isSecondaryIndex && table.index.Unique
}
