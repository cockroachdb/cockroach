// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DebugRowFetch can be used to turn on some low-level debugging logs. We use
// this to avoid using log.V in the hot path.
const DebugRowFetch = false

// noOutputColumn is a sentinel value to denote that a system column is not
// part of the output.
const noOutputColumn = -1

type kvBatchFetcher interface {
	// nextBatch returns the next batch of rows. Returns false in the first
	// parameter if there are no more keys in the scan. May return either a slice
	// of KeyValues or a batchResponse, numKvs pair, depending on the server
	// version - both must be handled by calling code.
	nextBatch(ctx context.Context) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, err error)

	close(ctx context.Context)
}

type tableInfo struct {
	// -- Fields initialized once --

	// Used to determine whether a key retrieved belongs to the span we
	// want to scan.
	spans            roachpb.Spans
	desc             catalog.TableDescriptor
	index            catalog.Index
	isSecondaryIndex bool
	indexColumnDirs  []descpb.IndexDescriptor_Direction
	// equivSignature is an equivalence class for each unique table-index
	// pair. It allows us to check if an index key belongs to a given
	// table-index.
	equivSignature []byte

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []catalog.Column

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
	colIdxMap catalog.TableColMap

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColIdx []int

	// knownPrefixLength is the number of bytes in the index key prefix this
	// Fetcher is configured for. The index key prefix is the table id, index
	// id pair at the start of the key.
	knownPrefixLength int

	// -- Fields updated during a scan --

	keyValTypes []*types.T
	extraTypes  []*types.T
	keyVals     []rowenc.EncDatum
	extraVals   []rowenc.EncDatum
	row         rowenc.EncDatumRow
	decodedRow  tree.Datums

	// The following fields contain MVCC metadata for each row and may be
	// returned to users of Fetcher immediately after NextRow returns.
	//
	// rowLastModified is the timestamp of the last time any family in the row
	// was modified in any way.
	rowLastModified hlc.Timestamp
	// timestampOutputIdx controls at what row ordinal to write the timestamp.
	timestampOutputIdx int

	// Fields for outputting the tableoid system column.
	tableOid     tree.Datum
	oidOutputIdx int

	// rowIsDeleted is true when the row has been deleted. This is only
	// meaningful when kv deletion tombstones are returned by the kvBatchFetcher,
	// which the one used by `StartScan` (the common case) doesnt. Notably,
	// changefeeds use this by providing raw kvs with tombstones unfiltered via
	// `StartScanFrom`.
	rowIsDeleted bool

	// hasLast indicates whether there was a previously scanned k/v.
	hasLast bool
	// lastDatums is a buffer for the current key. It is only present when
	// doing a physical check in order to verify round-trip encoding.
	// It is required because Fetcher.kv is overwritten before NextRow
	// returns.
	lastKV roachpb.KeyValue
	// lastDatums is a buffer for the previously scanned k/v datums. It is
	// only present when doing a physical check in order to verify
	// ordering.
	lastDatums tree.Datums
}

// FetcherTableArgs are the arguments passed to Fetcher.Init
// for a given table that includes descriptors and row information.
type FetcherTableArgs struct {
	// The spans of keys to return for the given table. Fetcher
	// ignores keys outside these spans.
	// This is irrelevant if Fetcher is initialize with only one
	// table.
	Spans            roachpb.Spans
	Desc             catalog.TableDescriptor
	Index            catalog.Index
	ColIdxMap        catalog.TableColMap
	IsSecondaryIndex bool
	Cols             []catalog.Column
	// The indexes (0 to # of columns - 1) of the columns to return.
	ValNeededForCol util.FastIntSet
}

// InitCols initializes the columns in FetcherTableArgs.
func (fta *FetcherTableArgs) InitCols(
	desc catalog.TableDescriptor,
	scanVisibility execinfrapb.ScanVisibility,
	withSystemColumns bool,
	virtualColumn catalog.Column,
) {
	cols := make([]catalog.Column, 0, len(desc.AllColumns()))
	if scanVisibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = append(cols, desc.ReadableColumns()...)
	} else {
		cols = append(cols, desc.PublicColumns()...)
	}
	if virtualColumn != nil {
		for i, col := range cols {
			if col.GetID() == virtualColumn.GetID() {
				cols[i] = virtualColumn
				break
			}
		}
	}
	if withSystemColumns {
		cols = append(cols, desc.SystemColumns()...)
	}
	fta.Cols = make([]catalog.Column, len(cols))
	for i, col := range cols {
		fta.Cols[i] = col
	}
}

// Fetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var rf Fetcher
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
type Fetcher struct {
	// codec is used to encode and decode sql keys.
	codec keys.SQLCodec

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
	// out of all the tables. This is used to calculate the kvBatchFetcher's
	// firstBatchLimit.
	maxKeysPerRow int

	// True if the index key must be decoded.
	// If there is more than one table, the index key must always be decoded.
	// This is only false if there are no needed columns and the (single)
	// table has no interleave children.
	mustDecodeIndexKey bool

	// lockStrength represents the row-level locking mode to use when fetching
	// rows.
	lockStrength descpb.ScanLockingStrength

	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy descpb.ScanLockingWaitPolicy

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when beginning a new scan.
	traceKV bool

	// mvccDecodeStrategy controls whether or not MVCC timestamps should
	// be decoded from KV's fetched. It is set if any of the requested tables
	// are required to produce an MVCC timestamp system column.
	mvccDecodeStrategy MVCCDecodingStrategy

	// -- Fields updated during a scan --

	kvFetcher      *KVFetcher
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

	// isCheck indicates whether or not we are running checks for k/v
	// correctness. It is set only during SCRUB commands.
	isCheck bool

	// IgnoreUnexpectedNulls allows Fetcher to return null values for non-nullable
	// columns and is only used for decoding for error messages or debugging.
	IgnoreUnexpectedNulls bool

	// Buffered allocation of decoded datums.
	alloc *rowenc.DatumAlloc

	// Memory monitor for the bytes fetched by this fetcher.
	mon *mon.BytesMonitor
}

// Reset resets this Fetcher, preserving the memory capacity that was used
// for the tables slice, and the slices within each of the tableInfo objects
// within tables. This permits reuse of this objects without forcing total
// reallocation of all of those slice fields.
func (rf *Fetcher) Reset() {
	*rf = Fetcher{
		tables: rf.tables[:0],
	}
}

// Close releases resources held by this fetcher.
func (rf *Fetcher) Close(ctx context.Context) {
	if rf.kvFetcher != nil {
		rf.kvFetcher.Close(ctx)
	}
	if rf.mon != nil {
		rf.mon.Stop(ctx)
	}
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *Fetcher) Init(
	ctx context.Context,
	codec keys.SQLCodec,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	isCheck bool,
	alloc *rowenc.DatumAlloc,
	memMonitor *mon.BytesMonitor,
	tables ...FetcherTableArgs,
) error {
	if len(tables) == 0 {
		return errors.AssertionFailedf("no tables to fetch from")
	}

	rf.codec = codec
	rf.reverse = reverse
	rf.lockStrength = lockStrength
	rf.lockWaitPolicy = lockWaitPolicy
	rf.alloc = alloc
	rf.isCheck = isCheck

	if memMonitor != nil {
		rf.mon = execinfra.NewMonitor(ctx, memMonitor, "fetcher-mem")
	}

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
		rf.tables = make([]tableInfo, nTables)
	}
	for tableIdx, tableArgs := range tables {
		oldTable := rf.tables[tableIdx]

		table := tableInfo{
			spans:            tableArgs.Spans,
			desc:             tableArgs.Desc,
			colIdxMap:        tableArgs.ColIdxMap,
			index:            tableArgs.Index,
			isSecondaryIndex: tableArgs.IsSecondaryIndex,
			cols:             tableArgs.Cols,
			row:              make(rowenc.EncDatumRow, len(tableArgs.Cols)),
			decodedRow:       make(tree.Datums, len(tableArgs.Cols)),

			// These slice fields might get re-allocated below, so reslice them from
			// the old table here in case they've got enough capacity already.
			indexColIdx:        oldTable.indexColIdx[:0],
			keyVals:            oldTable.keyVals[:0],
			extraVals:          oldTable.extraVals[:0],
			timestampOutputIdx: noOutputColumn,
			oidOutputIdx:       noOutputColumn,
		}

		var err error
		if multipleTables {
			// We produce references to every signature's reference.
			equivSignatures, err := rowenc.TableEquivSignatures(table.desc, table.index)
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
		for _, col := range table.cols {
			idx := table.colIdxMap.GetDefault(col.GetID())
			if tableArgs.ValNeededForCol.Contains(idx) {
				// The idx-th column is required.
				table.neededCols.Add(int(col.GetID()))

				// Set up any system column metadata, if this column is a system column.
				switch colinfo.GetSystemColumnKindFromColumnID(col.GetID()) {
				case descpb.SystemColumnKind_MVCCTIMESTAMP:
					table.timestampOutputIdx = idx
					rf.mvccDecodeStrategy = MVCCDecodingRequired
				case descpb.SystemColumnKind_TABLEOID:
					table.oidOutputIdx = idx
					table.tableOid = tree.NewDOid(tree.DInt(tableArgs.Desc.GetID()))
				}
			}
		}

		table.knownPrefixLength = len(
			rowenc.MakeIndexKeyPrefix(codec, table.desc, table.index.GetID()),
		)

		var indexColumnIDs []descpb.ColumnID
		indexColumnIDs, table.indexColumnDirs = catalog.FullIndexColumnIDs(table.index)

		table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()
		neededIndexCols := 0
		nIndexCols := len(indexColumnIDs)
		if cap(table.indexColIdx) >= nIndexCols {
			table.indexColIdx = table.indexColIdx[:nIndexCols]
		} else {
			table.indexColIdx = make([]int, nIndexCols)
		}
		for i, id := range indexColumnIDs {
			colIdx, ok := table.colIdxMap.Get(id)
			if ok {
				table.indexColIdx[i] = colIdx
				if table.neededCols.Contains(int(id)) {
					neededIndexCols++
					table.neededValueColsByIdx.Remove(colIdx)
				}
			} else {
				table.indexColIdx[i] = -1
				if table.neededCols.Contains(int(id)) {
					return errors.AssertionFailedf("needed column %d not in colIdxMap", id)
				}
			}
		}

		// In order to track #40410 more effectively, check that the contents of
		// table.neededValueColsByIdx are valid.
		for idx, ok := table.neededValueColsByIdx.Next(0); ok; idx, ok = table.neededValueColsByIdx.Next(idx + 1) {
			if idx >= len(table.row) || idx < 0 {
				return errors.AssertionFailedf(
					"neededValueColsByIdx contains an invalid index. column %d requested, but table has %d columns",
					idx,
					len(table.row),
				)
			}
		}

		// - If there is more than one table, we have to decode the index key to
		//   figure out which table the row belongs to.
		// - If there are interleaves, we need to read the index key in order to
		//   determine whether this row is actually part of the index we're scanning.
		// - If there are needed columns from the index key, we need to read it.
		//
		// Otherwise, we can completely avoid decoding the index key.
		if !rf.mustDecodeIndexKey && (neededIndexCols > 0 || table.index.NumInterleavedBy() > 0 || table.index.NumInterleaveAncestors() > 0) {
			rf.mustDecodeIndexKey = true
		}

		// The number of columns we need to read from the value part of the key.
		// It's the total number of needed columns minus the ones we read from the
		// index key, except for composite columns.
		table.neededValueCols = table.neededCols.Len() - neededIndexCols + table.index.NumCompositeColumns()

		if table.isSecondaryIndex {
			colIDs := table.index.CollectKeyColumnIDs()
			colIDs.UnionWith(table.index.CollectSecondaryStoredColumnIDs())
			colIDs.UnionWith(table.index.CollectKeySuffixColumnIDs())
			for i := range table.cols {
				if table.neededCols.Contains(int(table.cols[i].GetID())) && !colIDs.Contains(table.cols[i].GetID()) {
					return errors.Errorf("requested column %s not in index", table.cols[i].GetName())
				}
			}
		}

		// Prepare our index key vals slice.
		table.keyValTypes, err = colinfo.GetColumnTypes(table.desc, indexColumnIDs, table.keyValTypes)
		if err != nil {
			return err
		}
		if cap(table.keyVals) >= nIndexCols {
			table.keyVals = table.keyVals[:nIndexCols]
		} else {
			table.keyVals = make([]rowenc.EncDatum, nIndexCols)
		}

		if hasExtraCols(&table) {
			// Unique secondary indexes have a value that is the
			// primary index key.
			// Primary indexes only contain ascendingly-encoded
			// values. If this ever changes, we'll probably have to
			// figure out the directions here too.
			table.extraTypes, err = colinfo.GetColumnTypes(table.desc, table.index.IndexDesc().KeySuffixColumnIDs, table.extraTypes)
			nExtraColumns := table.index.NumKeySuffixColumns()
			if cap(table.extraVals) >= nExtraColumns {
				table.extraVals = table.extraVals[:nExtraColumns]
			} else {
				table.extraVals = make([]rowenc.EncDatum, nExtraColumns)
			}
			if err != nil {
				return err
			}
		}

		// Keep track of the maximum keys per row to accommodate a
		// limitHint when StartScan is invoked.
		keysPerRow, err := table.desc.KeysPerRow(table.index.GetID())
		if err != nil {
			return err
		}
		if keysPerRow > rf.maxKeysPerRow {
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

// GetTables returns all tables that this Fetcher was initialized with.
func (rf *Fetcher) GetTables() []catalog.Descriptor {
	ret := make([]catalog.Descriptor, len(rf.tables))
	for i := range rf.tables {
		ret[i] = rf.tables[i].desc
	}
	return ret
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *Fetcher) StartScan(
	ctx context.Context,
	txn *kv.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
	forceProductionKVBatchSize bool,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("no spans")
	}

	rf.traceKV = traceKV
	f, err := makeKVBatchFetcher(
		txn,
		spans,
		rf.reverse,
		limitBatches,
		rf.firstBatchLimit(limitHint),
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.mon,
		forceProductionKVBatchSize,
	)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f)
}

// TestingInconsistentScanSleep introduces a sleep inside the fetcher after
// every KV batch (for inconsistent scans, currently used only for table
// statistics collection).
// TODO(radu): consolidate with forceProductionKVBatchSize into a
// FetcherTestingKnobs struct.
var TestingInconsistentScanSleep time.Duration

// StartInconsistentScan initializes and starts an inconsistent scan, where each
// KV batch can be read at a different historical timestamp.
//
// The scan uses the initial timestamp, until it becomes older than
// maxTimestampAge; at this time the timestamp is bumped by the amount of time
// that has passed. See the documentation for TableReaderSpec for more
// details.
//
// Can be used multiple times.
func (rf *Fetcher) StartInconsistentScan(
	ctx context.Context,
	db *kv.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
	forceProductionKVBatchSize bool,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("no spans")
	}

	txnTimestamp := initialTimestamp
	txnStartTime := timeutil.Now()
	if txnStartTime.Sub(txnTimestamp.GoTime()) >= maxTimestampAge {
		return errors.Errorf(
			"AS OF SYSTEM TIME: cannot specify timestamp older than %s for this operation",
			maxTimestampAge,
		)
	}
	txn := kv.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */)
	txn.SetFixedTimestamp(ctx, txnTimestamp)
	if log.V(1) {
		log.Infof(ctx, "starting inconsistent scan at timestamp %v", txnTimestamp)
	}

	sendFn := func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
		if now := timeutil.Now(); now.Sub(txnTimestamp.GoTime()) >= maxTimestampAge {
			// Time to bump the transaction. First commit the old one (should be a no-op).
			if err := txn.Commit(ctx); err != nil {
				return nil, err
			}
			// Advance the timestamp by the time that passed.
			txnTimestamp = txnTimestamp.Add(now.Sub(txnStartTime).Nanoseconds(), 0 /* logical */)
			txnStartTime = now
			txn = kv.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */)
			txn.SetFixedTimestamp(ctx, txnTimestamp)

			if log.V(1) {
				log.Infof(ctx, "bumped inconsistent scan timestamp to %v", txnTimestamp)
			}
		}

		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		if TestingInconsistentScanSleep != 0 {
			time.Sleep(TestingInconsistentScanSleep)
		}
		return res, nil
	}

	// TODO(radu): we should commit the last txn. Right now the commit is a no-op
	// on read transactions, but perhaps one day it will release some resources.

	rf.traceKV = traceKV
	f, err := makeKVBatchFetcherWithSendFunc(
		sendFunc(sendFn),
		spans,
		rf.reverse,
		limitBatches,
		rf.firstBatchLimit(limitHint),
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.mon,
		forceProductionKVBatchSize,
		txn.AdmissionHeader(),
		txn.DB().SQLKVResponseAdmissionQ,
	)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f)
}

func (rf *Fetcher) firstBatchLimit(limitHint int64) int64 {
	if limitHint == 0 {
		return 0
	}
	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	// The limitHint is a row limit, but each row could be made up of more than
	// one key. We take the maximum possible keys per row out of all the table
	// rows we could potentially scan over.
	//
	// We add an extra key to make sure we form the last row.
	return limitHint*int64(rf.maxKeysPerRow) + 1
}

// StartScanFrom initializes and starts a scan from the given kvBatchFetcher. Can be
// used multiple times.
func (rf *Fetcher) StartScanFrom(ctx context.Context, f kvBatchFetcher) error {
	rf.indexKey = nil
	if rf.kvFetcher != nil {
		rf.kvFetcher.Close(ctx)
	}
	rf.kvFetcher = newKVFetcher(f)
	// Retrieve the first key.
	_, err := rf.NextKey(ctx)
	return err
}

// setNextKV sets the next KV to process to the input KV. needsCopy, if true,
// causes the input kv to be deep copied. needsCopy should be set to true if
// the input KV is pointing to the last KV of a batch, so that the batch can
// be garbage collected before fetching the next one.
// gcassert:inline
func (rf *Fetcher) setNextKV(kv roachpb.KeyValue, needsCopy bool) {
	if !needsCopy {
		rf.kv = kv
		return
	}

	// If we've made it to the very last key in the batch, copy out the key
	// so that the GC can reclaim the large backing slice before we call
	// NextKV() again.
	kvCopy := roachpb.KeyValue{}
	kvCopy.Key = make(roachpb.Key, len(kv.Key))
	copy(kvCopy.Key, kv.Key)
	kvCopy.Value.RawBytes = make([]byte, len(kv.Value.RawBytes))
	copy(kvCopy.Value.RawBytes, kv.Value.RawBytes)
	kvCopy.Value.Timestamp = kv.Value.Timestamp
	rf.kv = kvCopy
}

// NextKey retrieves the next key/value and sets kv/kvEnd. Returns whether a row
// has been completed.
func (rf *Fetcher) NextKey(ctx context.Context) (rowDone bool, err error) {
	var moreKVs bool

	for {
		var finalReferenceToBatch bool
		var kv roachpb.KeyValue
		moreKVs, kv, finalReferenceToBatch, err = rf.kvFetcher.NextKV(ctx, rf.mvccDecodeStrategy)
		if err != nil {
			return false, ConvertFetchError(ctx, rf, err)
		}
		rf.setNextKV(kv, finalReferenceToBatch)

		rf.kvEnd = !moreKVs
		if rf.kvEnd {
			// No more keys in the scan. We need to transition
			// rf.rowReadyTable to rf.currentTable for the last
			// row.
			//
			// NB: this assumes that the KV layer will never split a range
			// between column families, which is a brittle assumption.
			// See:
			// https://github.com/cockroachdb/cockroach/pull/42056
			rf.rowReadyTable = rf.currentTable
			return true, nil
		}

		// foundNull is set when decoding a new index key for a row finds a NULL value
		// in the index key. This is used when decoding unique secondary indexes in order
		// to tell whether they have extra columns appended to the key.
		var foundNull bool

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
			rf.rowReadyTable = rf.currentTable
		} else if rf.mustDecodeIndexKey || rf.traceKV {
			rf.keyRemainingBytes, moreKVs, foundNull, err = rf.ReadIndexKey(rf.kv.Key)
			if err != nil {
				return false, err
			}
			if !moreKVs {
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
		// NULL is present. When a null is present in the index key, we cut off more
		// of the index key so that the prefix includes the primary key columns.
		//
		// Note that we do not need to do this for non-unique secondary indexes because
		// the extra columns in the primary key will _always_ be there, so we can decode
		// them when processing the index. The difference with unique secondary indexes
		// is that the extra columns are not always there, and are used to unique-ify
		// the index key, rather than provide the primary key column values.
		if foundNull && rf.currentTable.isSecondaryIndex && rf.currentTable.index.IsUnique() && len(rf.currentTable.desc.GetFamilies()) != 1 {
			for i := 0; i < rf.currentTable.index.NumKeySuffixColumns(); i++ {
				var err error
				// Slice off an extra encoded column from rf.keyRemainingBytes.
				rf.keyRemainingBytes, err = rowenc.SkipTableKey(rf.keyRemainingBytes)
				if err != nil {
					return false, err
				}
			}
		}

		switch {
		case len(rf.currentTable.desc.GetFamilies()) == 1:
			// If we only have one family, we know that there is only 1 k/v pair per row.
			rowDone = true
		case !unchangedPrefix:
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

func (rf *Fetcher) prettyEncDatums(types []*types.T, vals []rowenc.EncDatum) string {
	var buf strings.Builder
	for i, v := range vals {
		if err := v.EnsureDecoded(types[i], rf.alloc); err != nil {
			buf.WriteString("/{error decoding: ")
			buf.WriteString(err.Error())
			buf.WriteByte('}')
			continue
		}
		buf.WriteByte('/')
		buf.WriteString(v.Datum.String())
	}
	return buf.String()
}

// ReadIndexKey decodes an index key for a given table.
// It returns whether or not the key is for any of the tables initialized
// in Fetcher, and the remaining part of the key if it is.
// ReadIndexKey additionally returns whether or not it encountered a null while decoding.
func (rf *Fetcher) ReadIndexKey(
	key roachpb.Key,
) (remaining []byte, ok bool, foundNull bool, err error) {
	// If there is only one table to check keys for, there is no need
	// to go through the equivalence signature checks.
	if len(rf.tables) == 1 {
		return rowenc.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
			rf.currentTable.desc,
			rf.currentTable.index,
			rf.currentTable.keyValTypes,
			rf.currentTable.keyVals,
			rf.currentTable.indexColumnDirs,
			key[rf.currentTable.knownPrefixLength:],
		)
	}

	// Make a copy of the initial key for validating whether it's within
	// the table's specified spans.
	initialKey := key

	key, err = rf.codec.StripTenantPrefix(key)
	if err != nil {
		return nil, false, false, err
	}

	// key now contains the bytes in the key (if match) that are not part
	// of the signature in order.
	tableIdx, key, match, err := rowenc.IndexKeyEquivSignature(key, rf.allEquivSignatures, rf.keySigBuf, rf.keyRestBuf)
	if err != nil {
		return nil, false, false, err
	}
	// The index key does not belong to our table because either:
	// !match:	    part of the index key's signature did not match any of
	//		    rf.allEquivSignatures.
	// tableIdx == -1:  index key belongs to an ancestor.
	if !match || tableIdx == -1 {
		return nil, false, false, nil
	}

	// The index key is not within our specified span of keys for the
	// particular table.
	// TODO(richardwu): ContainsKey checks every span within spans. We
	// can check that spans is ordered (or sort it) and memoize
	// the last span we've checked for each table. We can pass in this
	// information to ContainsKey as a hint for which span to start
	// checking first.
	if !rf.tables[tableIdx].spans.ContainsKey(initialKey) {
		return nil, false, false, nil
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
	// when processing the ind
	// ex key. The column values are at the
	// front of the key.
	if key, foundNull, err = rowenc.DecodeKeyVals(
		rf.currentTable.keyValTypes,
		rf.currentTable.keyVals,
		rf.currentTable.indexColumnDirs,
		key,
	); err != nil {
		return nil, false, false, err
	}

	return key, true, foundNull, nil
}

// KeyToDesc implements the KeyToDescTranslator interface. The implementation is
// used by ConvertFetchError.
func (rf *Fetcher) KeyToDesc(key roachpb.Key) (catalog.TableDescriptor, bool) {
	if rf.currentTable != nil && len(key) < rf.currentTable.knownPrefixLength {
		return nil, false
	}
	if _, ok, _, err := rf.ReadIndexKey(key); !ok || err != nil {
		return nil, false
	}
	return rf.currentTable.desc, true
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *Fetcher) processKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := rf.currentTable

	if rf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s%s",
			table.desc.GetName(),
			table.index.GetName(),
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

		// Reset the MVCC metadata for the next row.

		// set rowLastModified to a sentinel that's before any real timestamp.
		// As kvs are iterated for this row, it keeps track of the greatest
		// timestamp seen.
		table.rowLastModified = hlc.Timestamp{}
		// All row encodings (both before and after column families) have a
		// sentinel kv (column family 0) that is always present when a row is
		// present, even if that row is all NULLs. Thus, a row is deleted if and
		// only if the first kv in it a tombstone (RawBytes is empty).
		table.rowIsDeleted = len(kv.Value.RawBytes) == 0
	}

	if table.rowLastModified.Less(kv.Value.Timestamp) {
		table.rowLastModified = kv.Value.Timestamp
	}

	if table.neededCols.Empty() {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = tree.DNull.String()
		}
		return prettyKey, prettyValue, nil
	}

	// For covering secondary indexes, allow for decoding as a primary key.
	if table.index.GetEncodingType() == descpb.PrimaryIndexEncoding &&
		len(rf.keyRemainingBytes) > 0 {
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

			var family *descpb.ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(descpb.FamilyID(familyID))
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, family, kv, prettyKey)
		}
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
		}
	} else {
		tag := kv.Value.GetTag()
		var valueBytes []byte
		switch tag {
		case roachpb.ValueType_BYTES:
			// If we have the ValueType_BYTES on a secondary index, then we know we
			// are looking at column family 0. Column family 0 stores the extra primary
			// key columns if they are present, so we decode them here.
			valueBytes, err = kv.Value.GetBytes()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
			if hasExtraCols(table) {
				// This is a unique secondary index; decode the extra
				// column values from the value.
				var err error
				valueBytes, _, err = rowenc.DecodeKeyVals(
					table.extraTypes,
					table.extraVals,
					nil,
					valueBytes,
				)
				if err != nil {
					return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				for i := 0; i < table.index.NumKeySuffixColumns(); i++ {
					id := table.index.GetKeySuffixColumnID(i)
					if table.neededCols.Contains(int(id)) {
						table.row[table.colIdxMap.GetDefault(id)] = table.extraVals[i]
					}
				}
				if rf.traceKV {
					prettyValue = rf.prettyEncDatums(table.extraTypes, table.extraVals)
				}
			}
		case roachpb.ValueType_TUPLE:
			valueBytes, err = kv.Value.GetTuple()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}

		if DebugRowFetch {
			if hasExtraCols(table) && tag == roachpb.ValueType_BYTES {
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
func (rf *Fetcher) processValueSingle(
	ctx context.Context,
	table *tableInfo,
	family *descpb.ColumnFamilyDescriptor,
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
		if idx, ok := table.colIdxMap.Get(colID); ok {
			if rf.traceKV {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.DeletableColumns()[idx].GetName())
			}
			if len(kv.Value.RawBytes) == 0 {
				return prettyKey, "", nil
			}
			typ := table.cols[idx].GetType()
			// TODO(arjun): The value is a directly marshaled single value, so we
			// unmarshal it eagerly here. This can potentially be optimized out,
			// although that would require changing UnmarshalColumnValue to operate
			// on bytes, and for Encode/DecodeTableValue to operate on marshaled
			// single values.
			value, err := rowenc.UnmarshalColumnValue(rf.alloc, typ, kv.Value)
			if err != nil {
				return "", "", err
			}
			if rf.traceKV {
				prettyValue = value.String()
			}
			table.row[idx] = rowenc.DatumToEncDatum(typ, value)
			if DebugRowFetch {
				log.Infof(ctx, "Scan %s -> %v", kv.Key, value)
			}
			return prettyKey, prettyValue, nil
		}
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if DebugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
	}
	return prettyKey, prettyValue, nil
}

func (rf *Fetcher) processValueBytes(
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
	var lastColID descpb.ColumnID
	var typeOffset, dataOffset int
	var typ encoding.Type
	for len(valueBytes) > 0 && rf.valueColsFound < table.neededValueCols {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + descpb.ColumnID(colIDDiff)
		lastColID = colID
		if !table.neededCols.Contains(int(colID)) {
			// This column wasn't requested, so read its length and skip it.
			len, err := encoding.PeekValueLengthWithOffsetsAndType(valueBytes, dataOffset, typ)
			if err != nil {
				return "", "", err
			}
			valueBytes = valueBytes[len:]
			if DebugRowFetch {
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}
		idx := table.colIdxMap.GetDefault(colID)

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.DeletableColumns()[idx].GetName())
		}

		var encValue rowenc.EncDatum
		encValue, valueBytes, err = rowenc.EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
			dataOffset, typ)
		if err != nil {
			return "", "", err
		}
		if rf.traceKV {
			err := encValue.EnsureDecoded(table.cols[idx].GetType(), rf.alloc)
			if err != nil {
				return "", "", err
			}
			fmt.Fprintf(rf.prettyValueBuf, "/%v", encValue.Datum)
		}
		table.row[idx] = encValue
		rf.valueColsFound++
		if DebugRowFetch {
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
func (rf *Fetcher) processValueTuple(
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
func (rf *Fetcher) NextRow(
	ctx context.Context,
) (row rowenc.EncDatumRow, table catalog.TableDescriptor, index catalog.Index, err error) {
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
func (rf *Fetcher) NextRowDecoded(
	ctx context.Context,
) (datums tree.Datums, table catalog.TableDescriptor, index catalog.Index, err error) {
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
		if err := encDatum.EnsureDecoded(rf.rowReadyTable.cols[i].GetType(), rf.alloc); err != nil {
			return nil, nil, nil, err
		}
		rf.rowReadyTable.decodedRow[i] = encDatum.Datum
	}

	return rf.rowReadyTable.decodedRow, table, index, nil
}

// RowLastModified may only be called after NextRow has returned a non-nil row
// and returns the timestamp of the last modification to that row.
func (rf *Fetcher) RowLastModified() hlc.Timestamp {
	return rf.rowReadyTable.rowLastModified
}

// RowIsDeleted may only be called after NextRow has returned a non-nil row and
// returns true if that row was most recently deleted. This method is only
// meaningful when the configured kvBatchFetcher returns deletion tombstones, which
// the normal one (via `StartScan`) does not.
func (rf *Fetcher) RowIsDeleted() bool {
	return rf.rowReadyTable.rowIsDeleted
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
func (rf *Fetcher) NextRowWithErrors(ctx context.Context) (rowenc.EncDatumRow, error) {
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
		if err := row[i].EnsureDecoded(rf.rowReadyTable.cols[i].GetType(), rf.alloc); err != nil {
			return nil, err
		}
		rf.rowReadyTable.decodedRow[i] = row[i].Datum
	}

	if index.GetID() == table.GetPrimaryIndexID() {
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
func (rf *Fetcher) checkPrimaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	scratch := make([]byte, 1024)
	colIDToColumn := make(map[descpb.ColumnID]catalog.Column)
	for _, col := range table.desc.PublicColumns() {
		colIDToColumn[col.GetID()] = col
	}

	rh := rowHelper{TableDesc: table.desc, Indexes: table.desc.PublicNonPrimaryIndexes()}

	return table.desc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		var lastColID descpb.ColumnID
		familyID := family.ID
		familySortedColumnIDs, ok := rh.sortedColumnFamily(familyID)
		if !ok {
			return errors.AssertionFailedf("invalid family sorted column id map for family %d", familyID)
		}

		for _, colID := range familySortedColumnIDs {
			rowVal := table.row[table.colIdxMap.GetDefault(colID)]
			if rowVal.IsNull() {
				// Column is not present.
				continue
			}

			if skip, err := rh.skipColumnNotInPrimaryIndexValue(colID, rowVal.Datum); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "unable to determine skip")
			} else if skip {
				continue
			}

			col := colIDToColumn[colID]
			if col == nil {
				return errors.AssertionFailedf("column mapping not found for column %d", colID)
			}

			if lastColID > col.GetID() {
				return errors.AssertionFailedf("cannot write column id %d after %d", col.GetID(), lastColID)
			}
			colIDDiff := col.GetID() - lastColID
			lastColID = col.GetID()

			if result, err := rowenc.EncodeTableValue([]byte(nil), colIDDiff, rowVal.Datum,
				scratch); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err, "could not re-encode column %s, value was %#v",
					col.GetName(), rowVal.Datum)
			} else if !rowVal.BytesEqual(result) {
				return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
					"value failed to round-trip encode. Column=%s colIDDiff=%d Key=%s expected %#v, got: %#v",
					col.GetName(), colIDDiff, rf.kv.Key, rowVal.EncodedString(), result))
			}
		}
		return nil
	})
}

// checkSecondaryIndexDatumEncodings will run a round-trip encoding
// check on all values in the buffered row. This check is specific to
// secondary index datums.
func (rf *Fetcher) checkSecondaryIndexDatumEncodings(ctx context.Context) error {
	table := rf.rowReadyTable
	colToEncDatum := make(map[descpb.ColumnID]rowenc.EncDatum, len(table.row))
	values := make(tree.Datums, len(table.row))
	for i, col := range table.cols {
		colToEncDatum[col.GetID()] = table.row[i]
		values[i] = table.row[i].Datum
	}

	// The below code makes incorrect checks (#45256).
	indexEntries, err := rowenc.EncodeSecondaryIndex(
		rf.codec, table.desc, table.index, table.colIdxMap, values, false /* includeEmpty */)
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
		} else if !indexEntry.Value.EqualTagAndData(table.lastKV.Value) {
			return scrub.WrapError(scrub.IndexValueDecodingError, errors.Errorf(
				"secondary index value failed to round-trip encode. expected %#v, got: %#v",
				rf.rowReadyTable.lastKV.Value, indexEntry.Value))
		}
	}
	return nil
}

// checkKeyOrdering verifies that the datums decoded for the current key
// have the same ordering as the encoded key.
func (rf *Fetcher) checkKeyOrdering(ctx context.Context) error {
	defer func() {
		rf.rowReadyTable.lastDatums = append(tree.Datums(nil), rf.rowReadyTable.decodedRow...)
	}()

	if !rf.rowReadyTable.hasLast {
		rf.rowReadyTable.hasLast = true
		return nil
	}

	evalCtx := tree.EvalContext{}
	// Iterate through columns in order, comparing each value to the value in the
	// previous row in that column. When the first column with a differing value
	// is found, compare the values to ensure the ordering matches the column
	// ordering.
	for i := 0; i < rf.rowReadyTable.index.NumKeyColumns(); i++ {
		id := rf.rowReadyTable.index.GetKeyColumnID(i)
		idx := rf.rowReadyTable.colIdxMap.GetDefault(id)
		result := rf.rowReadyTable.decodedRow[idx].Compare(&evalCtx, rf.rowReadyTable.lastDatums[idx])
		expectedDirection := rf.rowReadyTable.index.GetKeyColumnDirection(i)
		if rf.reverse && expectedDirection == descpb.IndexDescriptor_ASC {
			expectedDirection = descpb.IndexDescriptor_DESC
		} else if rf.reverse && expectedDirection == descpb.IndexDescriptor_DESC {
			expectedDirection = descpb.IndexDescriptor_ASC
		}

		if result != 0 {
			if expectedDirection == descpb.IndexDescriptor_ASC && result < 0 ||
				expectedDirection == descpb.IndexDescriptor_DESC && result > 0 {
				return scrub.WrapError(scrub.IndexKeyDecodingError,
					errors.Errorf("key ordering did not match datum ordering. IndexDescriptor=%s",
						expectedDirection))
			}
			// After the first column with a differing value is found, the remaining
			// columns are skipped (see #32874).
			break
		}
	}
	return nil
}

func (rf *Fetcher) finalizeRow() error {
	table := rf.rowReadyTable

	// Fill in any system columns if requested.
	if table.timestampOutputIdx != noOutputColumn {
		// TODO (rohany): Datums are immutable, so we can't store a DDecimal on the
		//  fetcher and change its contents with each row. If that assumption gets
		//  lifted, then we can avoid an allocation of a new decimal datum here.
		dec := rf.alloc.NewDDecimal(tree.DDecimal{Decimal: tree.TimestampToDecimal(rf.RowLastModified())})
		table.row[table.timestampOutputIdx] = rowenc.EncDatum{Datum: dec}
	}
	if table.oidOutputIdx != noOutputColumn {
		table.row[table.oidOutputIdx] = rowenc.EncDatum{Datum: table.tableOid}
	}

	// Fill in any missing values with NULLs
	for i := range table.cols {
		if rf.valueColsFound == table.neededValueCols {
			// Found all cols - done!
			return nil
		}
		if table.neededCols.Contains(int(table.cols[i].GetID())) && table.row[i].IsUnset() {
			// If the row was deleted, we'll be missing any non-primary key
			// columns, including nullable ones, but this is expected.
			if !table.cols[i].IsNullable() && !table.rowIsDeleted && !rf.IgnoreUnexpectedNulls {
				var indexColValues []string
				for _, idx := range table.indexColIdx {
					if idx != -1 {
						indexColValues = append(indexColValues, table.row[idx].String(table.cols[idx].GetType()))
					} else {
						indexColValues = append(indexColValues, "?")
					}
				}
				err := errors.AssertionFailedf(
					"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.desc.GetName(), table.cols[i].GetName(), table.index.GetName(),
					strings.Join(table.index.IndexDesc().KeyColumnNames, ","), strings.Join(indexColValues, ","))

				if rf.isCheck {
					return scrub.WrapError(scrub.UnexpectedNullValueError, err)
				}
				return err
			}
			table.row[i] = rowenc.EncDatum{
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
func (rf *Fetcher) Key() roachpb.Key {
	return rf.kv.Key
}

// PartialKey returns a partial slice of the next key (the key that follows the
// last returned row) containing nCols columns, without the ending column
// family. Returns nil when there are no more rows.
func (rf *Fetcher) PartialKey(nCols int) (roachpb.Key, error) {
	if rf.kv.Key == nil {
		return nil, nil
	}
	n, err := consumeIndexKeyWithoutTableIDIndexIDPrefix(
		rf.currentTable.index, nCols, rf.kv.Key[rf.currentTable.knownPrefixLength:])
	if err != nil {
		return nil, err
	}
	return rf.kv.Key[:n+rf.currentTable.knownPrefixLength], nil
}

// GetBytesRead returns total number of bytes read by the underlying KVFetcher.
func (rf *Fetcher) GetBytesRead() int64 {
	return rf.kvFetcher.GetBytesRead()
}

// Only unique secondary indexes have extra columns to decode (namely the
// primary index columns).
func hasExtraCols(table *tableInfo) bool {
	return table.isSecondaryIndex && table.index.IsUnique()
}

// consumeIndexKeyWithoutTableIDIndexIDPrefix consumes an index key that's
// already pre-stripped of its table ID index ID prefix, up to nCols columns,
// returning the number of bytes consumed. For example, given an input key
// with values (6,7,8,9) such as /Table/60/1/6/7/#/61/1/8/9, stripping 3 columns
// from this key would eat all but the final, 4th column 9 in this example,
// producing /Table/60/1/6/7/#/61/1/8. If nCols was 2, instead, the result
// would include the trailing table ID index ID pair, since that's a more
// precise key: /Table/60/1/6/7/#/61/1.
func consumeIndexKeyWithoutTableIDIndexIDPrefix(
	index catalog.Index, nCols int, key []byte,
) (int, error) {
	origKeyLen := len(key)
	consumedCols := 0
	for i := 0; i < index.NumInterleaveAncestors(); i++ {
		ancestor := index.GetInterleaveAncestor(i)
		length := int(ancestor.SharedPrefixLen)
		// Skip up to length values.
		for j := 0; j < length; j++ {
			if consumedCols == nCols {
				// We're done early, in the middle of an interleave.
				return origKeyLen - len(key), nil
			}
			l, err := encoding.PeekLength(key)
			if err != nil {
				return 0, err
			}
			key = key[l:]
			consumedCols++
		}
		var ok bool
		key, ok = encoding.DecodeIfInterleavedSentinel(key)
		if !ok {
			return 0, errors.New("unexpected lack of sentinel key")
		}

		// Skip the TableID/IndexID pair for each ancestor except for the
		// first, which has already been skipped in our input.
		for j := 0; j < 2; j++ {
			idLen, err := encoding.PeekLength(key)
			if err != nil {
				return 0, err
			}
			key = key[idLen:]
		}
	}

	// Decode the remaining values in the key, in the final interleave.
	for ; consumedCols < nCols; consumedCols++ {
		l, err := encoding.PeekLength(key)
		if err != nil {
			return 0, err
		}
		key = key[l:]
	}

	return origKeyLen - len(key), nil
}
