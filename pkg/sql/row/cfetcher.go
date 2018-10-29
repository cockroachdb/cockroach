// Copyright 2018 The Cockroach Authors.
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

package row

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Only unique secondary indexes have extra columns to decode (namely the
// primary index columns).
func cHasExtraCols(table *cTableInfo) bool {
	return table.isSecondaryIndex && table.index.Unique
}

type cTableInfo struct {
	// -- Fields initialized once --

	// Used to determine whether a key retrieved belongs to the span we
	// want to scan.
	spans            roachpb.Spans
	desc             *sqlbase.TableDescriptor
	index            *sqlbase.IndexDescriptor
	isSecondaryIndex bool
	indexColumnDirs  []sqlbase.IndexDescriptor_Direction

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []sqlbase.ColumnDescriptor

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
	colIdxMap map[sqlbase.ColumnID]int

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColOrdinals []int

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	extraValColOrdinals []int

	// -- Fields updated during a scan --

	rowIdx uint16
	batch  exec.ColBatch

	colvecs []exec.ColVec

	keyValTypes []sqlbase.ColumnType
	extraTypes  []sqlbase.ColumnType
}

// CFetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var rf CFetcher
//   err := rf.Init(..)
//   // Handle err
//   err := rf.StartScan(..)
//   // Handle err
//   for {
//      res, err := rf.NextBatch()
//      // Handle err
//      if res.colBatch.Length() == 0 {
//         // Done
//         break
//      }
//      // Process res.colBatch
//   }
type CFetcher struct {
	// tables is a slice of all the tables and their descriptors for which
	// rows are returned.
	tables []cTableInfo

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

	// knownPrefixLength is the number of bytes in the index key prefix this
	// Fetcher is configured for. The index key prefix is the table id, index
	// id pair at the start of the key.
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

	rowReadyTable *cTableInfo // the table for which a row was fully decoded and ready for output
	currentTable  *cTableInfo // the most recent table for which a key was decoded

	// The current key/value, unless kvEnd is true.
	kv                roachpb.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	kvs []roachpb.KeyValue

	batchResponse []byte
	batchNumKvs   int64

	// isCheck indicates whether or not we are running checks for k/v
	// correctness. It is set only during SCRUB commands.
	isCheck bool
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *CFetcher) Init(
	reverse,
	returnRangeInfo bool, isCheck bool, tables ...FetcherTableArgs,
) error {
	if len(tables) == 0 {
		panic("no tables to fetch from")
	}

	rf.reverse = reverse
	rf.returnRangeInfo = returnRangeInfo
	rf.isCheck = isCheck

	// We must always decode the index key if we need to distinguish between
	// rows from more than one table.
	nTables := len(tables)
	multipleTables := nTables >= 2
	rf.mustDecodeIndexKey = multipleTables
	if multipleTables {
		rf.allEquivSignatures = make(map[string]int, len(tables))
	}

	if cap(rf.tables) >= nTables {
		rf.tables = rf.tables[:nTables]
	} else {
		rf.tables = make([]cTableInfo, nTables)
	}
	for tableIdx, tableArgs := range tables {
		oldTable := rf.tables[tableIdx]

		table := cTableInfo{
			spans:            tableArgs.Spans,
			desc:             tableArgs.Desc,
			colIdxMap:        tableArgs.ColIdxMap,
			index:            tableArgs.Index,
			isSecondaryIndex: tableArgs.IsSecondaryIndex,
			cols:             tableArgs.Cols,

			// These slice fields might get re-allocated below, so reslice them from
			// the old table here in case they've got enough capacity already.
			indexColOrdinals:    oldTable.indexColOrdinals[:0],
			extraValColOrdinals: oldTable.extraValColOrdinals[:0],
		}
		typs := make([]types.T, len(table.cols))
		for i := range typs {
			typs[i] = types.FromColumnType(table.cols[i].Type)
			if typs[i] == types.Unhandled {
				return errors.Errorf("unhandled type %+v", table.cols[i].Type)
			}
		}
		table.batch = exec.NewMemBatch(typs)
		table.colvecs = table.batch.ColVecs()

		var err error
		if multipleTables {
			panic("CFetcher doesn't support multi-table reads yet.")
		}

		// Scan through the entire columns map to see which columns are
		// required.
		for col, idx := range table.colIdxMap {
			if tableArgs.ValNeededForCol.Contains(idx) {
				// The idx-th column is required.
				table.neededCols.Add(int(col))
			}
		}

		rf.knownPrefixLength = len(sqlbase.MakeIndexKeyPrefix(table.desc, table.index.ID))

		var indexColumnIDs []sqlbase.ColumnID
		indexColumnIDs, table.indexColumnDirs = table.index.FullColumnIDs()

		table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()
		neededIndexCols := 0
		nIndexCols := len(indexColumnIDs)
		if cap(table.indexColOrdinals) >= nIndexCols {
			table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
		} else {
			table.indexColOrdinals = make([]int, nIndexCols)
		}
		for i, id := range indexColumnIDs {
			colIdx, ok := table.colIdxMap[id]
			if ok {
				table.indexColOrdinals[i] = colIdx
				if table.neededCols.Contains(int(id)) {
					neededIndexCols++
					table.neededValueColsByIdx.Remove(colIdx)
				}
			} else {
				table.indexColOrdinals[i] = -1
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
		table.keyValTypes, err = sqlbase.GetColumnTypes(table.desc, indexColumnIDs)
		if err != nil {
			return err
		}
		if cHasExtraCols(&table) {
			// Unique secondary indexes have a value that is the
			// primary index key.
			// Primary indexes only contain ascendingly-encoded
			// values. If this ever changes, we'll probably have to
			// figure out the directions here too.
			table.extraTypes, err = sqlbase.GetColumnTypes(table.desc, table.index.ExtraColumnIDs)
			nExtraColumns := len(table.index.ExtraColumnIDs)
			if cap(table.extraValColOrdinals) >= nExtraColumns {
				table.extraValColOrdinals = table.extraValColOrdinals[:nExtraColumns]
			} else {
				table.extraValColOrdinals = make([]int, nExtraColumns)
			}
			for i, id := range table.index.ExtraColumnIDs {
				if table.neededCols.Contains(int(id)) {
					table.extraValColOrdinals[i] = table.colIdxMap[id]
				} else {
					table.extraValColOrdinals[i] = -1
				}
			}
			if err != nil {
				return err
			}
		}

		// Keep track of the maximum keys per row to accommodate a
		// limitHint when StartScan is invoked.
		if keysPerRow := table.desc.KeysPerRow(table.index.ID); keysPerRow > rf.maxKeysPerRow {
			rf.maxKeysPerRow = keysPerRow
		}

		rf.tables[tableIdx] = table
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
func (rf *CFetcher) StartScan(
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
func (rf *CFetcher) StartScanFrom(ctx context.Context, f kvFetcher) error {
	rf.indexKey = nil
	rf.kvFetcher = f
	rf.kvs = nil
	rf.batchNumKvs = 0
	rf.batchResponse = nil
	// Retrieve the first key.
	_, err := rf.NextKey(ctx)
	return err
}

// Pops off the first kv stored in rf.kvs. If none are found attempts to fetch
// the next batch until there are no more kvs to fetch.
// Returns whether or not there are more kvs to fetch, the kv that was fetched,
// and any errors that may have occurred.
func (rf *CFetcher) nextKV(ctx context.Context) (ok bool, kv roachpb.KeyValue, err error) {
	if len(rf.kvs) != 0 {
		kv = rf.kvs[0]
		rf.kvs = rf.kvs[1:]
		return true, kv, nil
	}
	if rf.batchNumKvs > 0 {
		rf.batchNumKvs--
		var key []byte
		var rawBytes []byte
		var err error
		key, _, rawBytes, rf.batchResponse, err = enginepb.ScanDecodeKeyValue(rf.batchResponse)
		if err != nil {
			return false, kv, err
		}
		return true, roachpb.KeyValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes: rawBytes,
			},
		}, nil
	}

	var numKeys int64
	ok, rf.kvs, rf.batchResponse, numKeys, err = rf.kvFetcher.nextBatch(ctx)
	if rf.batchResponse != nil {
		rf.batchNumKvs = numKeys
	}
	if err != nil {
		return ok, kv, err
	}
	if !ok {
		return false, kv, nil
	}
	return rf.nextKV(ctx)
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (rf *CFetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
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

		// unchangedPrefix will be set to true if we can skip decoding the index key
		// completely, because the last key we saw has identical prefix to the
		// current key.
		unchangedPrefix := rf.indexKey != nil && bytes.HasPrefix(rf.kv.Key, rf.indexKey)
		if unchangedPrefix {
			keySuffix := rf.kv.Key[len(rf.indexKey):]
			if _, foundSentinel := encoding.DecodeIfInterleavedSentinel(keySuffix); foundSentinel {
				// We found an interleaved sentinel, which means that the key we just
				// found belongs to a different interleave. That means we have to go
				// through with index key decoding.
				unchangedPrefix = false
			} else {
				rf.keyRemainingBytes = keySuffix
			}
		}
		// See Init() for a detailed description of when we can get away with not
		// reading the index key.
		if unchangedPrefix {
			// Skip decoding!
			// We must set the rowReadyTable to the currentTable like ReadIndexKey
			// would do. This will happen when we see 2 rows in a row with the same
			// prefix. If the previous prefix was from a different table, then we must
			// update the ready table to the current table, updating the fetcher state
			// machine to recognize that the next row that it outputs will be from
			// rf.currentTable, which will be set to the table of the key that was
			// last sent to ReadIndexKey.
			//
			// TODO(jordan): this is a major (but correct) mess. The fetcher is past
			// due for a refactor, now that it's (more) clear what the state machine
			// it's trying to model is.
		} else if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, ok, err = colencoding.DecodeIndexKeyToCols(
				rf.currentTable.colvecs,
				rf.currentTable.rowIdx,
				rf.currentTable.desc,
				rf.currentTable.index,
				rf.currentTable.indexColOrdinals,
				rf.currentTable.keyValTypes,
				rf.currentTable.indexColumnDirs,
				rf.kv.Key[rf.knownPrefixLength:],
			)
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
		switch {
		case rf.currentTable.isSecondaryIndex:
			// Secondary indexes have only one key per row.
			rowDone = true
		case !bytes.HasPrefix(rf.kv.Key, rf.indexKey):
			// If the prefix of the key has changed, current key is from a different
			// row than the previous one.
			rowDone = true
		case rf.rowReadyTable != rf.currentTable:
			// For rowFetchers with more than one table, if the table changes the row
			// is done.
			rowDone = true
		default:
			rowDone = false
		}

		if rf.indexKey != nil && rowDone {
			// The current key belongs to a new row. Output the
			// current row.
			rf.indexKey = nil
			return true, nil
		}

		return false, nil
	}
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *CFetcher) processKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := rf.currentTable

	if rf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s?",
			table.desc.Name,
			table.index.Name,
			// TODO(jordan): handle this case. Can pull out values from the column
			// slices.
			//rf.prettyEncDatums(table.keyValTypes, table.keyVals),
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
			// TODO(jordan): null out batch.
			//table.row[idx].UnsetDatum()
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

			var family *sqlbase.ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(sqlbase.FamilyID(familyID))
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

		if cHasExtraCols(table) {
			// This is a unique secondary index; decode the extra
			// column values from the value.
			var err error
			valueBytes, err = colencoding.DecodeKeyValsToCols(
				table.colvecs,
				table.rowIdx,
				table.extraValColOrdinals,
				table.extraTypes,
				nil,
				valueBytes,
			)
			if err != nil {
				return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
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
func (rf *CFetcher) processValueSingle(
	ctx context.Context,
	table *cTableInfo,
	family *sqlbase.ColumnFamilyDescriptor,
	kv roachpb.KeyValue,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	return "", "", errors.New("CFetcher doesn't support directly marshaled single values yet")
}

func (rf *CFetcher) processValueBytes(
	ctx context.Context,
	table *cTableInfo,
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
	var lastColID sqlbase.ColumnID
	var typeOffset, dataOffset int
	var typ encoding.Type
	for len(valueBytes) > 0 && rf.valueColsFound < table.neededValueCols {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + sqlbase.ColumnID(colIDDiff)
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

		vec := table.batch.ColVec(idx)

		valTyp := &table.cols[idx].Type
		valueBytes, err = colencoding.DecodeTableValueToCol(vec, table.rowIdx, typ, dataOffset, valTyp, valueBytes[typeOffset:])
		if err != nil {
			return "", "", err
		}
		if rf.traceKV {
			fmt.Fprintf(rf.prettyValueBuf, "/?")
		}
		rf.valueColsFound++
	}
	if rf.traceKV {
		prettyValue = rf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *CFetcher) processValueTuple(
	ctx context.Context, table *cTableInfo, kv roachpb.KeyValue, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	tupleBytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", "", err
	}
	return rf.processValueBytes(ctx, table, kv, tupleBytes, prettyKeyPrefix)
}

// NextBatch processes keys until we complete one batch of rows, ColBatchSize
// in length, which are returned in columnar format as an exec.ColBatch. The
// batch contains one ColVec per table column, regardless of the index used;
// columns that are not needed (as per neededCols) are empty. The
// ColBatch should not be modified and is only valid until the next call.
// When there are no more rows, the ColBatch.Length is 0.
// It also returns the table and index descriptor associated with the row
// (relevant when more than one table is specified during initialization).
func (rf *CFetcher) NextBatch(
	ctx context.Context,
) (
	batch exec.ColBatch,
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	err error,
) {
	rf.rowReadyTable.rowIdx = 0
	if rf.kvEnd {
		rf.rowReadyTable.batch.SetLength(0)
		return rf.rowReadyTable.batch, nil, nil, nil
	}

	// All of the columns for a particular row will be grouped together. We
	// loop over the key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column
	// ID to lookup the column and decode the value. All of these values go
	// into a map keyed by column name. When the index key changes we
	// output a row containing the current values.
	for rf.rowReadyTable.rowIdx < exec.ColBatchSize {
		prettyKey, prettyVal, err := rf.processKV(ctx, rf.kv)
		if err != nil {
			return nil, nil, nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		rowDone, err := rf.NextKey(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if rowDone {
			err := rf.finalizeRow()
			if err != nil {
				return nil, nil, nil, err
			}
			rf.rowReadyTable.rowIdx++
		}
		if rf.kvEnd {
			break
		}
	}
	rf.rowReadyTable.batch.SetLength(rf.rowReadyTable.rowIdx)
	return rf.rowReadyTable.batch, rf.rowReadyTable.desc, rf.rowReadyTable.index, err
}

func (rf *CFetcher) finalizeRow() error {
	table := rf.rowReadyTable
	// Fill in any missing values with NULLs
	for i := range table.cols {
		if rf.valueColsFound == table.neededValueCols {
			// Found all cols - done!
			return nil
		}
		if table.neededCols.Contains(int(table.cols[i].ID)) && table.batch.ColVec(i).NullAt(table.rowIdx) {
			// If the row was deleted, we'll be missing any non-primary key
			// columns, including nullable ones, but this is expected.
			if !table.cols[i].Nullable {
				var indexColValues []string
				for i := range table.indexColOrdinals {
					indexColValues = append(indexColValues, "?"+string(i))
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
			table.colvecs[i].SetNull(table.rowIdx)
			// We've set valueColsFound to the number of present columns in the row
			// already, in processValueBytes. Now, we're filling in columns that have
			// no encoded values with NULL - so we increment valueColsFound to permit
			// early exit from this loop once all needed columns are filled in.
			rf.valueColsFound++
		}
	}
	return nil
}
