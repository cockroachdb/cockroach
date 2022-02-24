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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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

// KVBatchFetcher abstracts the logic of fetching KVs in batches.
type KVBatchFetcher interface {
	// nextBatch returns the next batch of rows. Returns false in the first
	// parameter if there are no more keys in the scan. May return either a slice
	// of KeyValues or a batchResponse, numKvs pair, depending on the server
	// version - both must be handled by calling code.
	nextBatch(ctx context.Context) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, err error)

	close(ctx context.Context)
}

type tableInfo struct {
	// -- Fields initialized once --
	spec descpb.IndexFetchSpec

	// The set of indexes into spec.FetchedColumns that are required for columns
	// in the value part.
	neededValueColsByIdx util.FastIntSet

	// The number of needed columns from the value part of the row. Once we've
	// seen this number of value columns for a particular row, we can stop
	// decoding values in that row.
	neededValueCols int

	// Map used to get the index for columns in spec.FetchedColumns.
	colIdxMap catalog.TableColMap

	// One value per column that is part of the key; each value is a column index
	// (into spec.FetchedColumns); -1 if we don't need the value for that column.
	indexColIdx []int

	// -- Fields updated during a scan --

	keyVals    []rowenc.EncDatum
	extraVals  []rowenc.EncDatum
	row        rowenc.EncDatumRow
	decodedRow tree.Datums

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
	// meaningful when kv deletion tombstones are returned by the KVBatchFetcher,
	// which the one used by `StartScan` (the common case) doesnt. Notably,
	// changefeeds use this by providing raw kvs with tombstones unfiltered via
	// `StartScanFrom`.
	rowIsDeleted bool
}

// Fetcher handles fetching kvs and forming table rows for a single table.
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
	table tableInfo

	// reverse denotes whether or not the spans should be read in reverse
	// or not when StartScan is invoked.
	reverse bool

	// True if the index key must be decoded. This is only false if there are no
	// needed columns.
	mustDecodeIndexKey bool

	// lockStrength represents the row-level locking mode to use when fetching
	// rows.
	lockStrength descpb.ScanLockingStrength

	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy descpb.ScanLockingWaitPolicy

	// lockTimeout specifies the maximum amount of time that the fetcher will
	// wait while attempting to acquire a lock on a key or while blocking on an
	// existing lock in order to perform a non-locking read on a key.
	lockTimeout time.Duration

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when beginning a new scan.
	traceKV bool

	// mvccDecodeStrategy controls whether or not MVCC timestamps should
	// be decoded from KV's fetched.
	mvccDecodeStrategy MVCCDecodingStrategy

	// -- Fields updated during a scan --

	kvFetcher *KVFetcher
	// indexKey stores the index key of the current row, up to (and not including)
	// any family ID.
	indexKey       []byte
	prettyValueBuf *bytes.Buffer

	valueColsFound int // how many needed cols we've found so far in the value

	// The current key/value, unless kvEnd is true.
	kv                roachpb.KeyValue
	keyRemainingBytes []byte
	kvEnd             bool

	// IgnoreUnexpectedNulls allows Fetcher to return null values for non-nullable
	// columns and is only used for decoding for error messages or debugging.
	IgnoreUnexpectedNulls bool

	// Buffered allocation of decoded datums.
	alloc *tree.DatumAlloc

	// Memory monitor and memory account for the bytes fetched by this fetcher.
	mon             *mon.BytesMonitor
	kvFetcherMemAcc *mon.BoundAccount
}

// Reset resets this Fetcher, preserving the memory capacity that was used
// for the tables slice, and the slices within each of the tableInfo objects
// within tables. This permits reuse of this objects without forcing total
// reallocation of all of those slice fields.
func (rf *Fetcher) Reset() {
	*rf = Fetcher{
		table: rf.table,
	}
}

// Close releases resources held by this fetcher.
func (rf *Fetcher) Close(ctx context.Context) {
	if rf.kvFetcher != nil {
		rf.kvFetcher.Close(ctx)
	}
	if rf.mon != nil {
		rf.kvFetcherMemAcc.Close(ctx)
		rf.mon.Stop(ctx)
	}
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *Fetcher) Init(
	ctx context.Context,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	lockTimeout time.Duration,
	alloc *tree.DatumAlloc,
	memMonitor *mon.BytesMonitor,
	spec *descpb.IndexFetchSpec,
) error {
	if spec.Version != descpb.IndexFetchSpecVersionInitial {
		return errors.Newf("unsupported IndexFetchSpec version %d", spec.Version)
	}
	rf.reverse = reverse
	rf.lockStrength = lockStrength
	rf.lockWaitPolicy = lockWaitPolicy
	rf.lockTimeout = lockTimeout
	rf.alloc = alloc

	if memMonitor != nil {
		rf.mon = mon.NewMonitorInheritWithLimit("fetcher-mem", 0 /* limit */, memMonitor)
		rf.mon.Start(ctx, memMonitor, mon.BoundAccount{})
		memAcc := rf.mon.MakeBoundAccount()
		rf.kvFetcherMemAcc = &memAcc
	}

	table := &rf.table
	*table = tableInfo{
		spec:       *spec,
		row:        make(rowenc.EncDatumRow, len(spec.FetchedColumns)),
		decodedRow: make(tree.Datums, len(spec.FetchedColumns)),

		// These slice fields might get re-allocated below, so reslice them from
		// the old table here in case they've got enough capacity already.
		indexColIdx:        rf.table.indexColIdx[:0],
		keyVals:            rf.table.keyVals[:0],
		extraVals:          rf.table.extraVals[:0],
		timestampOutputIdx: noOutputColumn,
		oidOutputIdx:       noOutputColumn,
	}

	for idx := range spec.FetchedColumns {
		colID := spec.FetchedColumns[idx].ColumnID
		table.colIdxMap.Set(colID, idx)
		if colinfo.IsColIDSystemColumn(colID) {
			switch colinfo.GetSystemColumnKindFromColumnID(colID) {
			case catpb.SystemColumnKind_MVCCTIMESTAMP:
				table.timestampOutputIdx = idx
				rf.mvccDecodeStrategy = MVCCDecodingRequired

			case catpb.SystemColumnKind_TABLEOID:
				table.oidOutputIdx = idx
				table.tableOid = tree.NewDOid(tree.DInt(spec.TableID))
			}
		}
	}

	if len(spec.FetchedColumns) > 0 {
		table.neededValueColsByIdx.AddRange(0, len(spec.FetchedColumns)-1)
	}

	nExtraCols := 0
	// Unique secondary indexes have extra columns to decode from the value (namely
	// the primary index columns).
	if table.spec.IsSecondaryIndex && table.spec.IsUniqueIndex {
		nExtraCols = int(table.spec.NumKeySuffixColumns)
	}
	nIndexCols := len(spec.KeyAndSuffixColumns) - nExtraCols

	neededIndexCols := 0
	compositeIndexCols := 0
	if cap(table.indexColIdx) >= nIndexCols {
		table.indexColIdx = table.indexColIdx[:nIndexCols]
	} else {
		table.indexColIdx = make([]int, nIndexCols)
	}
	for i := 0; i < nIndexCols; i++ {
		id := spec.KeyAndSuffixColumns[i].ColumnID
		colIdx, ok := table.colIdxMap.Get(id)
		if ok {
			table.indexColIdx[i] = colIdx
			neededIndexCols++
			table.neededValueColsByIdx.Remove(colIdx)
		} else {
			table.indexColIdx[i] = -1
		}
		if spec.KeyAndSuffixColumns[i].IsComposite {
			compositeIndexCols++
		}
	}

	// If there are needed columns from the index key, we need to read it;
	// otherwise, we can completely avoid decoding the index key.
	rf.mustDecodeIndexKey = neededIndexCols > 0

	// The number of columns we need to read from the value part of the key.
	// It's the total number of needed columns minus the ones we read from the
	// index key, except for composite columns.
	table.neededValueCols = len(spec.FetchedColumns) - neededIndexCols + compositeIndexCols

	if cap(table.keyVals) >= nIndexCols {
		table.keyVals = table.keyVals[:nIndexCols]
	} else {
		table.keyVals = make([]rowenc.EncDatum, nIndexCols)
	}

	if nExtraCols > 0 {
		// Unique secondary indexes have a value that is the
		// primary index key.
		// Primary indexes only contain ascendingly-encoded
		// values. If this ever changes, we'll probably have to
		// figure out the directions here too.
		if cap(table.extraVals) >= nExtraCols {
			table.extraVals = table.extraVals[:nExtraCols]
		} else {
			table.extraVals = make([]rowenc.EncDatum, nExtraCols)
		}
	}

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
//
// batchBytesLimit controls whether bytes limits are placed on the batches. If
// set, bytes limits will be used to protect against running out of memory (on
// both this client node, and on the server).
//
// If batchBytesLimit is set, rowLimitHint can also be set to control the number of
// rows that will be scanned by the first batch. If set, subsequent batches (if
// any) will have progressively higher limits (up to a fixed max). The idea with
// row limits is to make the execution of LIMIT queries efficient: if the caller
// has some idea about how many rows need to be read to ultimately satisfy the
// query, the Fetcher uses it. Even if this hint proves insufficient, the
// Fetcher continues to set row limits (in addition to bytes limits) on the
// argument that some number of rows will eventually satisfy the query and we
// likely don't need to scan `spans` fully. The bytes limit, on the other hand,
// is simply intended to protect against OOMs.
func (rf *Fetcher) StartScan(
	ctx context.Context,
	txn *kv.Txn,
	spans roachpb.Spans,
	batchBytesLimit rowinfra.BytesLimit,
	rowLimitHint rowinfra.RowLimit,
	traceKV bool,
	forceProductionKVBatchSize bool,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("no spans")
	}

	f, err := makeKVBatchFetcher(
		ctx,
		makeKVBatchFetcherDefaultSendFunc(txn),
		spans,
		rf.reverse,
		batchBytesLimit,
		rf.rowLimitToKeyLimit(rowLimitHint),
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.lockTimeout,
		rf.kvFetcherMemAcc,
		forceProductionKVBatchSize,
		txn.AdmissionHeader(),
		txn.DB().SQLKVResponseAdmissionQ,
	)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f, traceKV)
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
	batchBytesLimit rowinfra.BytesLimit,
	rowLimitHint rowinfra.RowLimit,
	traceKV bool,
	forceProductionKVBatchSize bool,
	qualityOfService sessiondatapb.QoSLevel,
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
	txn := kv.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */, qualityOfService)
	if err := txn.SetFixedTimestamp(ctx, txnTimestamp); err != nil {
		return err
	}
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
			txn = kv.NewTxnWithSteppingEnabled(ctx, db, 0 /* gatewayNodeID */, qualityOfService)
			if err := txn.SetFixedTimestamp(ctx, txnTimestamp); err != nil {
				return nil, err
			}

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

	f, err := makeKVBatchFetcher(
		ctx,
		sendFunc(sendFn),
		spans,
		rf.reverse,
		batchBytesLimit,
		rf.rowLimitToKeyLimit(rowLimitHint),
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.lockTimeout,
		rf.kvFetcherMemAcc,
		forceProductionKVBatchSize,
		txn.AdmissionHeader(),
		txn.DB().SQLKVResponseAdmissionQ,
	)
	if err != nil {
		return err
	}
	return rf.StartScanFrom(ctx, &f, traceKV)
}

func (rf *Fetcher) rowLimitToKeyLimit(rowLimitHint rowinfra.RowLimit) rowinfra.KeyLimit {
	if rowLimitHint == 0 {
		return 0
	}
	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	// The rowLimitHint is a row limit, but each row could be made up of more than
	// one key. We take the maximum possible keys per row out of all the table
	// rows we could potentially scan over.
	//
	// We add an extra key to make sure we form the last row.
	return rowinfra.KeyLimit(int64(rowLimitHint)*int64(rf.table.spec.MaxKeysPerRow) + 1)
}

// StartScanFrom initializes and starts a scan from the given KVBatchFetcher. Can be
// used multiple times.
func (rf *Fetcher) StartScanFrom(ctx context.Context, f KVBatchFetcher, traceKV bool) error {
	rf.traceKV = traceKV
	rf.indexKey = nil
	if rf.kvFetcher != nil {
		rf.kvFetcher.Close(ctx)
	}
	rf.kvFetcher = newKVFetcher(f)
	rf.kvEnd = false
	// Retrieve the first key.
	_, err := rf.nextKey(ctx)
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

// nextKey retrieves the next key/value and sets kv/kvEnd. Returns whether the
// key indicates a new row (as opposed to another family for the current row).
func (rf *Fetcher) nextKey(ctx context.Context) (newRow bool, _ error) {
	ok, kv, finalReferenceToBatch, err := rf.kvFetcher.NextKV(ctx, rf.mvccDecodeStrategy)
	if err != nil {
		return false, ConvertFetchError(&rf.table.spec, err)
	}
	rf.setNextKV(kv, finalReferenceToBatch)

	if !ok {
		// No more keys in the scan.
		rf.kvEnd = true
		return true, nil
	}

	// unchangedPrefix will be set to true if the current KV belongs to the same
	// row as the previous KV (i.e. the last and current keys have identical
	// prefix). In this case, we can skip decoding the index key completely.
	unchangedPrefix := rf.table.spec.MaxKeysPerRow > 1 && rf.indexKey != nil && bytes.HasPrefix(rf.kv.Key, rf.indexKey)
	if unchangedPrefix {
		// Skip decoding!
		rf.keyRemainingBytes = rf.kv.Key[len(rf.indexKey):]
		return false, nil
	}

	// The current key belongs to a new row.
	if rf.mustDecodeIndexKey {
		var foundNull bool
		rf.keyRemainingBytes, foundNull, err = rf.DecodeIndexKey(rf.kv.Key)
		if err != nil {
			return false, err
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
		if foundNull && rf.table.spec.IsSecondaryIndex && rf.table.spec.IsUniqueIndex && rf.table.spec.MaxKeysPerRow > 1 {
			for i := 0; i < int(rf.table.spec.NumKeySuffixColumns); i++ {
				var err error
				// Slice off an extra encoded column from rf.keyRemainingBytes.
				rf.keyRemainingBytes, err = keyside.Skip(rf.keyRemainingBytes)
				if err != nil {
					return false, err
				}
			}
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

	rf.indexKey = nil
	return true, nil
}

func (rf *Fetcher) prettyKeyDatums(
	cols []descpb.IndexFetchSpec_KeyColumn, vals []rowenc.EncDatum,
) string {
	var buf strings.Builder
	for i, v := range vals {
		buf.WriteByte('/')
		if err := v.EnsureDecoded(cols[i].Type, rf.alloc); err != nil {
			buf.WriteByte('?')
		} else {
			buf.WriteString(v.Datum.String())
		}
	}
	return buf.String()
}

// DecodeIndexKey decodes an index key and returns the remaining key and whether
// it encountered a null while decoding.
func (rf *Fetcher) DecodeIndexKey(key roachpb.Key) (remaining []byte, foundNull bool, err error) {
	key = key[rf.table.spec.KeyPrefixLength:]
	return rowenc.DecodeKeyValsUsingSpec(rf.table.spec.KeyAndSuffixColumns, key, rf.table.keyVals)
}

// processKV processes the given key/value, setting values in the row
// accordingly. If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *Fetcher) processKV(
	ctx context.Context, kv roachpb.KeyValue,
) (prettyKey string, prettyValue string, err error) {
	table := &rf.table

	if rf.traceKV {
		prettyKey = fmt.Sprintf(
			"/%s/%s%s",
			table.spec.TableName,
			table.spec.IndexName,
			rf.prettyKeyDatums(table.spec.KeyAndSuffixColumns, table.keyVals),
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

	if len(table.spec.FetchedColumns) == 0 {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = "<undecoded>"
		}
		return prettyKey, prettyValue, nil
	}

	// For covering secondary indexes, allow for decoding as a primary key.
	if table.spec.EncodingType == descpb.PrimaryIndexEncoding &&
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
			var tupleBytes []byte
			tupleBytes, err = kv.Value.GetTuple()
			if err != nil {
				break
			}
			prettyKey, prettyValue, err = rf.processValueBytes(ctx, table, kv, tupleBytes, prettyKey)
		default:
			var familyID uint64
			_, familyID, err = encoding.DecodeUvarintAscending(rf.keyRemainingBytes)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			// If familyID is 0, this is the row sentinel (in the legacy pre-family format),
			// and a value is not expected, so we're done.
			if familyID != 0 {
				// Find the default column ID for the family.
				var defaultColumnID descpb.ColumnID
				for _, f := range table.spec.FamilyDefaultColumns {
					if f.FamilyID == descpb.FamilyID(familyID) {
						defaultColumnID = f.DefaultColumnID
						break
					}
				}
				if defaultColumnID == 0 {
					return "", "", errors.Errorf("single entry value with no default column id")
				}

				prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, defaultColumnID, kv, prettyKey)
			}
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
			if len(table.extraVals) > 0 {
				extraCols := table.spec.KeySuffixColumns()
				// This is a unique secondary index; decode the extra
				// column values from the value.
				var err error
				valueBytes, _, err = rowenc.DecodeKeyValsUsingSpec(
					extraCols,
					valueBytes,
					table.extraVals,
				)
				if err != nil {
					return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				for i := range extraCols {
					if idx, ok := table.colIdxMap.Get(extraCols[i].ColumnID); ok {
						table.row[idx] = table.extraVals[i]
					}
				}
				if rf.traceKV {
					prettyValue = rf.prettyKeyDatums(extraCols, table.extraVals)
				}
			}
		case roachpb.ValueType_TUPLE:
			valueBytes, err = kv.Value.GetTuple()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
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
		prettyValue = "<undecoded>"
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column colID), setting
// values in table.row accordingly. The key is only used for logging.
func (rf *Fetcher) processValueSingle(
	ctx context.Context,
	table *tableInfo,
	colID descpb.ColumnID,
	kv roachpb.KeyValue,
	prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	idx, ok := table.colIdxMap.Get(colID)
	if !ok {
		// No need to unmarshal the column value. Either the column was part of
		// the index key or it isn't needed.
		if DebugRowFetch {
			log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
		}
		return prettyKey, "", nil
	}

	if rf.traceKV {
		prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.spec.FetchedColumns[idx].Name)
	}
	if len(kv.Value.RawBytes) == 0 {
		return prettyKey, "", nil
	}
	typ := table.spec.FetchedColumns[idx].Type
	// TODO(arjun): The value is a directly marshaled single value, so we
	// unmarshal it eagerly here. This can potentially be optimized out,
	// although that would require changing UnmarshalColumnValue to operate
	// on bytes, and for Encode/DecodeTableValue to operate on marshaled
	// single values.
	value, err := valueside.UnmarshalLegacy(rf.alloc, typ, kv.Value)
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
		idx, ok := table.colIdxMap.Get(colID)
		if !ok {
			// This column wasn't requested, so read its length and skip it.
			numBytes, err := encoding.PeekValueLengthWithOffsetsAndType(valueBytes, dataOffset, typ)
			if err != nil {
				return "", "", err
			}
			valueBytes = valueBytes[numBytes:]
			if DebugRowFetch {
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", kv.Key, colID)
			}
			continue
		}

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.spec.FetchedColumns[idx].Name)
		}

		var encValue rowenc.EncDatum
		encValue, valueBytes, err = rowenc.EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
			dataOffset, typ)
		if err != nil {
			return "", "", err
		}
		if rf.traceKV {
			err := encValue.EnsureDecoded(table.spec.FetchedColumns[idx].Type, rf.alloc)
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

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per IndexFetchSpec.FetchedColumns.
//
// The EncDatumRow should not be modified and is only valid until the next call.
//
// When there are no more rows, the EncDatumRow is nil.
func (rf *Fetcher) NextRow(ctx context.Context) (row rowenc.EncDatumRow, err error) {
	if rf.kvEnd {
		return nil, nil
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
			return nil, err
		}
		if rf.traceKV {
			log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		rowDone, err := rf.nextKey(ctx)
		if err != nil {
			return nil, err
		}
		if rowDone {
			err := rf.finalizeRow()
			return rf.table.row, err
		}
	}
}

// NextRowInto calls NextRow and copies the results into the given EncDatumRow
// slice according to the given column map.
//
// Values for columns that are not in the map are ignored. EncDatums in
// destination that don't correspond to a fetcher column are not modified.
//
// If there are no more rows, returns ok=false.
func (rf *Fetcher) NextRowInto(
	ctx context.Context, destination rowenc.EncDatumRow, colIdxMap catalog.TableColMap,
) (ok bool, err error) {
	row, err := rf.NextRow(ctx)
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}

	for i := range rf.table.spec.FetchedColumns {
		if ord, ok := colIdxMap.Get(rf.table.spec.FetchedColumns[i].ColumnID); ok {
			destination[ord] = row[i]
		}
	}
	return true, nil
}

// NextRowDecoded calls NextRow and decodes the EncDatumRow into a Datums.
// The Datums should not be modified and is only valid until the next call.
// When there are no more rows, the Datums is nil.
func (rf *Fetcher) NextRowDecoded(ctx context.Context) (datums tree.Datums, err error) {
	row, err := rf.NextRow(ctx)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	for i, encDatum := range row {
		if encDatum.IsUnset() {
			rf.table.decodedRow[i] = tree.DNull
			continue
		}
		if err := encDatum.EnsureDecoded(rf.table.spec.FetchedColumns[i].Type, rf.alloc); err != nil {
			return nil, err
		}
		rf.table.decodedRow[i] = encDatum.Datum
	}

	return rf.table.decodedRow, nil
}

// NextRowDecodedInto calls NextRow and decodes the EncDatumRow into Datums,
// storing the results in the given destination slice according to the column
// mapping.
//
// Values for columns that are not in the map are ignored. Datums in
// destination that don't correspond to a fetcher column are not modified.
//
// If there are no more rows, returns ok=false.
func (rf *Fetcher) NextRowDecodedInto(
	ctx context.Context, destination tree.Datums, colIdxMap catalog.TableColMap,
) (ok bool, err error) {
	row, err := rf.NextRow(ctx)
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}

	for i := range rf.table.spec.FetchedColumns {
		col := &rf.table.spec.FetchedColumns[i]
		ord, ok := colIdxMap.Get(col.ColumnID)
		if !ok {
			// Column not in map, ignore.
			continue
		}
		encDatum := row[i]
		if encDatum.IsUnset() {
			destination[ord] = tree.DNull
			continue
		}
		if err := encDatum.EnsureDecoded(col.Type, rf.alloc); err != nil {
			return false, err
		}
		destination[ord] = encDatum.Datum
	}

	return true, nil
}

// RowLastModified may only be called after NextRow has returned a non-nil row
// and returns the timestamp of the last modification to that row.
func (rf *Fetcher) RowLastModified() hlc.Timestamp {
	return rf.table.rowLastModified
}

// RowIsDeleted may only be called after NextRow has returned a non-nil row and
// returns true if that row was most recently deleted. This method is only
// meaningful when the configured KVBatchFetcher returns deletion tombstones, which
// the normal one (via `StartScan`) does not.
func (rf *Fetcher) RowIsDeleted() bool {
	return rf.table.rowIsDeleted
}

func (rf *Fetcher) finalizeRow() error {
	table := &rf.table

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
	for i := range table.spec.FetchedColumns {
		col := &table.spec.FetchedColumns[i]
		if rf.valueColsFound == table.neededValueCols {
			// Found all cols - done!
			return nil
		}
		if table.row[i].IsUnset() {
			// If the row was deleted, we'll be missing any non-primary key
			// columns, including nullable ones, but this is expected. If the column
			// is not yet active, we can also expect NULLs.
			if col.IsNonNullable && !table.rowIsDeleted && !rf.IgnoreUnexpectedNulls {
				var indexColValues []string
				for _, idx := range table.indexColIdx {
					if idx != -1 {
						indexColValues = append(indexColValues, table.row[idx].String(table.spec.FetchedColumns[idx].Type))
					} else {
						indexColValues = append(indexColValues, "?")
					}
				}
				var indexColNames []string
				for i := range table.spec.KeyFullColumns() {
					indexColNames = append(indexColNames, table.spec.KeyAndSuffixColumns[i].Name)
				}
				return errors.AssertionFailedf(
					"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.spec.TableName, col.Name, table.spec.IndexName,
					strings.Join(indexColNames, ","), strings.Join(indexColValues, ","))
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
	partialKeyLength := int(rf.table.spec.KeyPrefixLength)
	for consumedCols := 0; consumedCols < nCols; consumedCols++ {
		l, err := encoding.PeekLength(rf.kv.Key[partialKeyLength:])
		if err != nil {
			return nil, err
		}
		partialKeyLength += l
	}
	return rf.kv.Key[:partialKeyLength], nil
}

// GetBytesRead returns total number of bytes read by the underlying KVFetcher.
func (rf *Fetcher) GetBytesRead() int64 {
	return rf.kvFetcher.GetBytesRead()
}
