// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// noOutputColumn is a sentinel value to denote that a system column is not
// part of the output.
const noOutputColumn = -1

// KVBatchFetcherResponse contains a response from the KVBatchFetcher.
type KVBatchFetcherResponse struct {
	// MoreKVs indicates whether this response contains some keys from the
	// fetch.
	//
	// If true, then the fetch might (or might not) be complete at this point -
	// another call to NextBatch() is needed to find out. If false, then the
	// fetch has already been completed and this response doesn't have any
	// fetched data.
	//
	// Note that it is possible that MoreKVs is true when neither KVs,
	// BatchResponse, nor ColBatch is set. This can occur when there was nothing
	// to fetch for a Scan or a ReverseScan request, so the caller should just
	// skip over such a response.
	MoreKVs bool
	// Only one of KVs, BatchResponse, and ColBatch will be set. Which one is
	// set depends on the request type (KVs is used for Gets and BatchResponse /
	// ColBatch for Scans and ReverseScans). All must be handled by calling
	// code.
	//
	// KVs, if set, is a slice of roachpb.KeyValue, the deserialized kv pairs
	// that were fetched.
	KVs []roachpb.KeyValue
	// BatchResponse, if set, is either a packed byte slice containing the keys
	// and values (for BATCH_RESPONSE scan format) or serialized representation
	// of a coldata.Batch (for COL_BATCH_RESPONSE scan format). An empty
	// BatchResponse and nil ColBatch indicate that nothing was fetched for the
	// corresponding ScanRequest, and the caller is expected to skip over the
	// response.
	BatchResponse []byte
	// ColBatch is used for COL_BATCH_RESPONSE scan format when the request was
	// evaluated locally. An empty BatchResponse and nil ColBatch indicate that
	// nothing was fetched for the corresponding ScanRequest, and the caller is
	// expected to skip over the response.
	//
	// Note that this batch will be accounted for by the txnKVFetcher, and the
	// memory reservation is only released when the fetcher performs another
	// BatchRequest.
	//
	// Note that the datum-backed vectors in this batch are "incomplete" in a
	// sense they are missing the eval.Context. It is the caller's
	// responsibility to update all datum-backed vectors with the eval context.
	ColBatch coldata.Batch
	// spanID is the ID associated with the span that generated this response.
	spanID int
	// kvPairsRead tracks the number of key-values pairs that were just fetched
	// (meaning that we needed to issue a BatchRequest to produce this
	// response). Notably, if we already had some buffered responses from the
	// previous BatchResponse, this number will remain zero.
	kvPairsRead int64
}

// KVBatchFetcher abstracts the logic of fetching KVs in batches.
type KVBatchFetcher interface {
	// SetupNextFetch prepares the fetch of the next set of spans. Can be called
	// multiple times.
	//
	// The fetcher takes ownership of the spans slice, will perform the memory
	// accounting for it, and might modify it. The caller is only allowed to
	// reuse the spans slice after all rows have been fetched (i.e. NextBatch()
	// returned KVBatchFetcherResponse.MoreKVs=false) or the fetcher has been
	// closed.
	//
	// The fetcher can also modify the spanIDs slice but will **not** perform
	// memory accounting for it. If spanIDs is non-nil, then it must be of the
	// same length as spans.
	//
	// spansCanOverlap indicates whether spans might be unordered and
	// overlapping. If true, then spanIDs must be non-nil.
	//
	// NOTE: if spansCanOverlap is true and a single span can touch multiple
	// ranges, then fetched rows from different spans can be interspersed with
	// one another. See the comment on txnKVFetcher.SetupNextFetch for more
	// details.
	SetupNextFetch(
		ctx context.Context,
		spans roachpb.Spans,
		spanIDs []int,
		batchBytesLimit rowinfra.BytesLimit,
		firstBatchKeyLimit rowinfra.KeyLimit,
		spansCanOverlap bool,
	) error

	// NextBatch returns the next batch of rows. See KVBatchFetcherResponse for
	// details on what is returned.
	NextBatch(ctx context.Context) (KVBatchFetcherResponse, error)

	// GetBytesRead returns the number of bytes read by this fetcher. It is safe
	// for concurrent use and is able to handle a case of uninitialized fetcher.
	GetBytesRead() int64

	// GetKVPairsRead returns the number of key-value pairs read by this
	// fetcher throughout its lifetime. It is safe for concurrent use and is
	// able to handle a case of uninitialized fetcher.
	GetKVPairsRead() int64

	// GetBatchRequestsIssued returns the number of BatchRequests issued by this
	// fetcher throughout its lifetime. It is safe for concurrent use and is
	// able to handle a case of uninitialized fetcher.
	GetBatchRequestsIssued() int64

	// Close releases the resources of this KVBatchFetcher. Must be called once
	// the fetcher is no longer in use. Note that observability-related methods
	// can still be safely called after Close.
	Close(ctx context.Context)
}

type tableInfo struct {
	// -- Fields initialized once --
	spec fetchpb.IndexFetchSpec

	// The set of indexes into spec.FetchedColumns that are required for columns
	// in the value part.
	neededValueColsByIdx intsets.Fast

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

	// Fields for outputting the OriginID system column.
	rowLastOriginID   int
	originIDOutputIdx int

	// Fields for outputting the OriginTimestamp system column.
	rowLastOriginTimestamp hlc.Timestamp
	// rowLastModifiedWithoutOriginTimestamp is the largest MVCC
	// timestamp seen for any column family that _doesn't_ have an
	// OriginTimestamp set. If this is greater than the
	// rowLastOriginTimestamp field, we output NULL for origin
	// timestamp.
	rowLastModifiedWithoutOriginTimestamp hlc.Timestamp
	originTimestampOutputIdx              int

	// rowIsDeleted is true when the row has been deleted. This is only
	// meaningful when kv deletion tombstones are returned by the KVBatchFetcher,
	// which the one used by `StartScan` (the common case) doesnt. Notably,
	// changefeeds use this by providing raw kvs with tombstones unfiltered via
	// `ConsumeKVProvider`.
	rowIsDeleted bool
}

// Fetcher handles fetching kvs and forming table rows for a single table.
// Usage:
//
//	var rf Fetcher
//	err := rf.Init(..)
//	// Handle err
//	err := rf.StartScan(..)
//	// Handle err
//	for {
//	   res, err := rf.NextRow()
//	   // Handle err
//	   if res.row == nil {
//	      // Done
//	      break
//	   }
//	   // Process res.row
//	}
type Fetcher struct {
	args  FetcherInitArgs
	table tableInfo

	// True if the index key must be decoded. This is only false if there are no
	// needed columns.
	mustDecodeIndexKey bool

	// mvccDecodeStrategy controls whether or not MVCC timestamps should
	// be decoded from KV's fetched.
	mvccDecodeStrategy storage.MVCCDecodingStrategy

	shouldRequestRawMVCCKeys bool

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

	// spanID is associated with the input span that produced the data in kv.
	spanID int

	// IgnoreUnexpectedNulls allows Fetcher to return null values for non-nullable
	// columns and is only used for decoding for error messages or debugging.
	IgnoreUnexpectedNulls bool

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

// TraceKVVerbosity is the verbosity level at which pretty printed KVs
// are logged.
const TraceKVVerbosity = 2

// FetcherInitArgs contains arguments for Fetcher.Init.
type FetcherInitArgs struct {
	// StreamingKVFetcher, if non-nil, contains the KVFetcher that uses the
	// kvstreamer.Streamer API under the hood. The caller is then expected to
	// use only StartScan() method.
	StreamingKVFetcher *KVFetcher
	// WillUseKVProvider, if true, indicates that the caller will only use
	// ConsumeKVProvider() method and will use its own KVBatchFetcher
	// implementation.
	WillUseKVProvider bool
	// Txn is the txn for the fetch. It might be nil, and the caller is expected
	// to either provide the txn later via SetTxn() or to only use
	// ConsumeKVProvider method.
	Txn *kv.Txn
	// Reverse denotes whether or not the spans should be read in reverse or not
	// when StartScan* methods are invoked.
	Reverse bool
	// LockStrength represents the row-level locking mode to use when fetching
	// rows.
	LockStrength descpb.ScanLockingStrength
	// LockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	LockWaitPolicy descpb.ScanLockingWaitPolicy
	// LockDurability represents the row-level locking durability to use.
	LockDurability descpb.ScanLockingDurability
	// LockTimeout specifies the maximum amount of time that the fetcher will
	// wait while attempting to acquire a lock on a key or while blocking on an
	// existing lock in order to perform a non-locking read on a key.
	LockTimeout time.Duration
	// DeadlockTimeout specifies the amount of time before pushing the lock holder
	// for deadlock detection.
	DeadlockTimeout time.Duration
	// Alloc is used for buffered allocation of decoded datums.
	Alloc      *tree.DatumAlloc
	MemMonitor *mon.BytesMonitor
	Spec       *fetchpb.IndexFetchSpec
	// TraceKV indicates whether or not session tracing is enabled.
	TraceKV bool
	// TraceKVEvery controls how often KVs are sampled for logging with traceKV
	// enabled.
	TraceKVEvery               *util.EveryN
	ForceProductionKVBatchSize bool
	// SpansCanOverlap indicates whether the spans in a given batch can overlap
	// with one another. If it is true, spans that correspond to the same row must
	// share the same span ID, since the span IDs are used to determine when a new
	// row is being processed. In practice, this means that span IDs must be
	// passed in when SpansCanOverlap is true.
	SpansCanOverlap bool
}

// Init sets up a Fetcher for a given table and index.
func (rf *Fetcher) Init(ctx context.Context, args FetcherInitArgs) error {
	if args.Spec.Version != fetchpb.IndexFetchSpecVersionInitial {
		return errors.Newf("unsupported IndexFetchSpec version %d", args.Spec.Version)
	}

	rf.args = args

	if args.MemMonitor != nil {
		rf.mon = mon.NewMonitorInheritWithLimit(
			"fetcher-mem", 0 /* limit */, args.MemMonitor, false, /* longLiving */
		)
		rf.mon.StartNoReserved(ctx, args.MemMonitor)
		memAcc := rf.mon.MakeBoundAccount()
		rf.kvFetcherMemAcc = &memAcc
	}

	table := &rf.table
	*table = tableInfo{
		spec:       *args.Spec,
		row:        make(rowenc.EncDatumRow, len(args.Spec.FetchedColumns)),
		decodedRow: make(tree.Datums, len(args.Spec.FetchedColumns)),

		// These slice fields might get re-allocated below, so reslice them from
		// the old table here in case they've got enough capacity already.
		indexColIdx:              rf.table.indexColIdx[:0],
		keyVals:                  rf.table.keyVals[:0],
		extraVals:                rf.table.extraVals[:0],
		timestampOutputIdx:       noOutputColumn,
		oidOutputIdx:             noOutputColumn,
		originIDOutputIdx:        noOutputColumn,
		originTimestampOutputIdx: noOutputColumn,
	}

	for idx := range args.Spec.FetchedColumns {
		colID := args.Spec.FetchedColumns[idx].ColumnID
		table.colIdxMap.Set(colID, idx)
		if colinfo.IsColIDSystemColumn(colID) {
			switch colinfo.GetSystemColumnKindFromColumnID(colID) {
			case catpb.SystemColumnKind_MVCCTIMESTAMP:
				table.timestampOutputIdx = idx
				rf.mvccDecodeStrategy = storage.MVCCDecodingRequired

			case catpb.SystemColumnKind_TABLEOID:
				table.oidOutputIdx = idx
				table.tableOid = tree.NewDOid(oid.Oid(args.Spec.TableID))

			case catpb.SystemColumnKind_ORIGINID:
				table.originIDOutputIdx = idx
				rf.mvccDecodeStrategy = storage.MVCCDecodingRequired
				rf.shouldRequestRawMVCCKeys = true

			case catpb.SystemColumnKind_ORIGINTIMESTAMP:
				table.originTimestampOutputIdx = idx
				rf.mvccDecodeStrategy = storage.MVCCDecodingRequired
				rf.shouldRequestRawMVCCKeys = true
			}
		}
	}

	if len(args.Spec.FetchedColumns) > 0 {
		table.neededValueColsByIdx.AddRange(0, len(args.Spec.FetchedColumns)-1)
	}

	nExtraCols := 0
	// Unique secondary indexes have extra columns to decode from the value (namely
	// the primary index columns).
	if table.spec.IsSecondaryIndex && table.spec.IsUniqueIndex {
		nExtraCols = int(table.spec.NumKeySuffixColumns)
	}
	nIndexCols := len(args.Spec.KeyAndSuffixColumns) - nExtraCols

	neededIndexCols := 0
	compositeIndexCols := 0
	if cap(table.indexColIdx) >= nIndexCols {
		table.indexColIdx = table.indexColIdx[:nIndexCols]
	} else {
		table.indexColIdx = make([]int, nIndexCols)
	}
	for i := 0; i < nIndexCols; i++ {
		id := args.Spec.KeyAndSuffixColumns[i].ColumnID
		colIdx, ok := table.colIdxMap.Get(id)
		if ok {
			table.indexColIdx[i] = colIdx
			neededIndexCols++
			table.neededValueColsByIdx.Remove(colIdx)
		} else {
			table.indexColIdx[i] = -1
		}
		if args.Spec.KeyAndSuffixColumns[i].IsComposite {
			compositeIndexCols++
		}
	}

	// If there are needed columns from the index key, we need to read it;
	// otherwise, we can completely avoid decoding the index key.
	rf.mustDecodeIndexKey = neededIndexCols > 0

	// The number of columns we need to read from the value part of the key.
	// It's the total number of needed columns minus the ones we read from the
	// index key, except for composite columns.
	table.neededValueCols = len(args.Spec.FetchedColumns) - neededIndexCols + compositeIndexCols

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

	if args.StreamingKVFetcher != nil {
		if args.WillUseKVProvider {
			return errors.AssertionFailedf(
				"StreamingKVFetcher is non-nil when WillUseKVProvider is true",
			)
		}
		rf.kvFetcher = args.StreamingKVFetcher
	} else if !args.WillUseKVProvider {
		var kvPairsRead int64
		var batchRequestsIssued int64
		fetcherArgs := newTxnKVFetcherArgs{
			reverse:                    args.Reverse,
			lockStrength:               args.LockStrength,
			lockWaitPolicy:             args.LockWaitPolicy,
			lockDurability:             args.LockDurability,
			lockTimeout:                args.LockTimeout,
			deadlockTimeout:            args.DeadlockTimeout,
			acc:                        rf.kvFetcherMemAcc,
			rawMVCCValues:              rf.shouldRequestRawMVCCKeys,
			forceProductionKVBatchSize: args.ForceProductionKVBatchSize,
			kvPairsRead:                &kvPairsRead,
			batchRequestsIssued:        &batchRequestsIssued,
		}
		if args.Txn != nil {
			fetcherArgs.sendFn = makeSendFunc(args.Txn, args.Spec.External, &batchRequestsIssued)
			fetcherArgs.admission.requestHeader = args.Txn.AdmissionHeader()
			fetcherArgs.admission.responseQ = args.Txn.DB().SQLKVResponseAdmissionQ
			fetcherArgs.admission.pacerFactory = args.Txn.DB().AdmissionPacerFactory
			fetcherArgs.admission.settingsValues = args.Txn.DB().SettingsValues()
		}
		rf.kvFetcher = newKVFetcher(newTxnKVFetcherInternal(fetcherArgs))
	}

	return nil
}

// SetTxn updates the Fetcher to use the provided txn. An error is returned if
// the underlying KVBatchFetcher is not a txnKVFetcher.
//
// This method is designed to support two use cases:
//   - providing the Fetcher with the txn when the txn was not available during
//     Fetcher.Init;
//   - allowing the caller to update the Fetcher to use the new txn. In this case,
//     the caller should be careful since reads performed under different txns
//     do not provide consistent view of the data.
//
// Note that this resets the number of batch requests issued by the Fetcher.
// Consider using GetBatchRequestsIssued if that information is needed.
func (rf *Fetcher) SetTxn(txn *kv.Txn) error {
	var batchRequestsIssued int64
	sendFn := makeSendFunc(txn, rf.args.Spec.External, &batchRequestsIssued)
	return rf.setTxnAndSendFn(txn, sendFn)
}

// setTxnAndSendFn peeks inside of the KVFetcher to update the underlying
// txnKVFetcher with the new txn and sendFn.
func (rf *Fetcher) setTxnAndSendFn(txn *kv.Txn, sendFn sendFunc) error {
	f, ok := rf.kvFetcher.KVBatchFetcher.(*txnKVFetcher)
	if !ok {
		return errors.AssertionFailedf(
			"unexpectedly the KVBatchFetcher is %T and not *txnKVFetcher", rf.kvFetcher.KVBatchFetcher,
		)
	}
	f.setTxnAndSendFn(txn, sendFn)
	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times. Cannot be used if WillUseKVProvider was set to true in Init().
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly (if Init() was called with
// non-nil memory monitor). The caller can only reuse the spans slice after the
// fetcher has been closed, and if the caller does, it becomes responsible for
// the memory accounting.
//
// The fetcher also takes ownership of the spanIDs slice - it can modify the
// slice, but it will **not** perform the memory accounting. It is the caller's
// responsibility to track the memory under the spanIDs slice, and the slice
// can only be reused once the fetcher has been closed. Notably, the capacity of
// the slice will not be increased by the fetcher.
//
// spanIDs is a slice of user-defined identifiers of spans. If spanIDs is
// non-nil, then it must be of the same length as spans, and some methods of the
// Fetcher will return the fetched row with the span ID that corresponds to the
// original span that the row belongs to. Nil spanIDs can be used to indicate
// that the caller is not interested in that information.
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
//
// Batch limits can only be used if the spans are ordered.
func (rf *Fetcher) StartScan(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	batchBytesLimit rowinfra.BytesLimit,
	rowLimitHint rowinfra.RowLimit,
) error {
	if rf.args.WillUseKVProvider {
		return errors.AssertionFailedf("StartScan is called instead of ConsumeKVProvider")
	}
	if len(spans) == 0 {
		return errors.AssertionFailedf("no spans")
	}

	if err := rf.kvFetcher.SetupNextFetch(
		ctx, spans, spanIDs, batchBytesLimit, rf.rowLimitToKeyLimit(rowLimitHint), rf.args.SpansCanOverlap,
	); err != nil {
		return err
	}

	return rf.startScan(ctx)
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
// Can be used multiple times. Cannot be used if WillUseKVProvider was set to
// true in Init().
//
// Batch limits can only be used if the spans are ordered.
func (rf *Fetcher) StartInconsistentScan(
	ctx context.Context,
	db *kv.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	batchBytesLimit rowinfra.BytesLimit,
	rowLimitHint rowinfra.RowLimit,
	qualityOfService sessiondatapb.QoSLevel,
) error {
	if rf.args.StreamingKVFetcher != nil {
		return errors.AssertionFailedf("StartInconsistentScan is called instead of StartScan")
	}
	if rf.args.WillUseKVProvider {
		return errors.AssertionFailedf("StartInconsistentScan is called instead of ConsumeKVProvider")
	}
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

	sendFn := func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
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

		log.VEventf(ctx, 2, "inconsistent scan: sending a batch with %d requests", len(ba.Requests))
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

	if err := rf.setTxnAndSendFn(txn, sendFn); err != nil {
		return err
	}

	if err := rf.kvFetcher.SetupNextFetch(
		ctx, spans, nil, batchBytesLimit,
		rf.rowLimitToKeyLimit(rowLimitHint), false, /* spansCanOverlap */
	); err != nil {
		return err
	}

	return rf.startScan(ctx)
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

// ConsumeKVProvider initializes and starts a "scan" of the given KVProvider.
// Can be used multiple times. Cannot be used if WillUseKVProvider was set to
// false in Init().
func (rf *Fetcher) ConsumeKVProvider(ctx context.Context, f *KVProvider) error {
	if !rf.args.WillUseKVProvider {
		return errors.AssertionFailedf("ConsumeKVProvider is called instead of StartScan")
	}
	if rf.kvFetcher == nil {
		rf.kvFetcher = newKVFetcher(f)
	} else {
		rf.kvFetcher.Close(ctx)
		rf.kvFetcher.reset(f)
	}

	return rf.startScan(ctx)
}

func (rf *Fetcher) startScan(ctx context.Context) error {
	rf.indexKey = nil
	rf.kvEnd = false
	// Retrieve the first key.
	var err error
	_, rf.spanID, err = rf.nextKey(ctx)
	return err
}

// nextKey retrieves the next key/value and sets kv/kvEnd. Returns whether the
// key indicates a new row (as opposed to another family for the current row).
func (rf *Fetcher) nextKey(ctx context.Context) (newRow bool, spanID int, _ error) {
	ok, kv, spanID, err := rf.kvFetcher.nextKV(ctx, rf.mvccDecodeStrategy)
	if err != nil {
		return false, 0, ConvertFetchError(&rf.table.spec, err)
	}
	rf.kv = kv

	if !ok {
		// No more keys in the scan.
		rf.kvEnd = true
		return true, 0, nil
	}

	// unchangedPrefix will be set to true if the current KV belongs to the same
	// row as the previous KV (i.e. the last and current keys have identical
	// prefix). In this case, we can skip decoding the index key completely.
	// If the spans can overlap, it is not sufficient to check the prefix of the
	// key in order to determine whether a new row is being processed. Instead, we
	// have to rely on checking if the associated span ID has changed. Note that
	// we cannot use the span ID check unconditionally. This is because it is
	// possible for multiple span IDs to be associated with a given row when the
	// spans cannot overlap.
	unchangedPrefix := (!rf.args.SpansCanOverlap || rf.spanID == spanID) &&
		rf.table.spec.MaxKeysPerRow > 1 && rf.indexKey != nil && bytes.HasPrefix(rf.kv.Key, rf.indexKey)
	if unchangedPrefix {
		// Skip decoding!
		rf.keyRemainingBytes = rf.kv.Key[len(rf.indexKey):]
		return false, spanID, nil
	}

	// The current key belongs to a new row.
	if rf.mustDecodeIndexKey {
		var foundNull bool
		rf.keyRemainingBytes, foundNull, err = rf.DecodeIndexKey(rf.kv.Key)
		if err != nil {
			return false, 0, err
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
					return false, 0, err
				}
			}
		}
	} else {
		// We still need to consume the key until the family
		// id, so processKV can know whether we've finished a
		// row or not.
		prefixLen, err := keys.GetRowPrefixLength(rf.kv.Key)
		if err != nil {
			return false, 0, err
		}

		rf.keyRemainingBytes = rf.kv.Key[prefixLen:]
	}

	rf.indexKey = nil
	return true, spanID, nil
}

func (rf *Fetcher) prettyKeyDatums(
	cols []fetchpb.IndexFetchSpec_KeyColumn, vals []rowenc.EncDatum,
) string {
	var buf strings.Builder
	for i, v := range vals {
		buf.WriteByte('/')
		if err := v.EnsureDecoded(cols[i].Type, rf.args.Alloc); err != nil {
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

	mkPrettyKey := func() string {
		return fmt.Sprintf(
			"/%s/%s%s",
			table.spec.TableName,
			table.spec.IndexName,
			rf.prettyKeyDatums(table.spec.KeyAndSuffixColumns, table.keyVals),
		)
	}

	if rf.args.TraceKV {
		prettyKey = mkPrettyKey()
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
		table.rowLastOriginID = 0
		table.rowLastOriginTimestamp = hlc.Timestamp{}
		table.rowLastModifiedWithoutOriginTimestamp = hlc.Timestamp{}
		// All row encodings (both before and after column families) have a
		// sentinel kv (column family 0) that is always present when a row is
		// present, even if that row is all NULLs. Thus, a row is deleted if and
		// only if the first kv in it a tombstone (RawBytes is empty).
		table.rowIsDeleted = !kv.Value.IsPresent()
	}

	if table.rowLastModified.Less(kv.Value.Timestamp) {
		table.rowLastModified = kv.Value.Timestamp
	}
	if vh, err := kv.Value.GetMVCCValueHeader(); err == nil {
		if table.rowLastOriginTimestamp.LessEq(vh.OriginTimestamp) {
			table.rowLastOriginID = int(vh.OriginID)
			table.rowLastOriginTimestamp = vh.OriginTimestamp
		}
		if vh.OriginTimestamp.IsEmpty() && table.rowLastModifiedWithoutOriginTimestamp.Less(kv.Value.Timestamp) {
			table.rowLastModifiedWithoutOriginTimestamp = kv.Value.Timestamp
		}
	}

	if len(table.spec.FetchedColumns) == 0 {
		// We don't need to decode any values.
		if rf.args.TraceKV {
			prettyValue = "<undecoded>"
		}
		return prettyKey, prettyValue, nil
	}

	// For covering secondary indexes, allow for decoding as a primary key.
	if table.spec.EncodingType == catenumpb.PrimaryIndexEncoding &&
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
			prettyKey, prettyValue, err = rf.processValueBytes(table, tupleBytes, prettyKey)
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
					if kv.Value.GetTag() == roachpb.ValueType_UNKNOWN {
						// Tombstone for a secondary column family, nothing needs to be done.
					} else {
						if prettyKey == "" {
							prettyKey = mkPrettyKey()
						}
						return "", "",
							errors.Errorf("single entry value with no default column id for key %s", prettyKey)
					}
				} else {
					prettyKey, prettyValue, err = rf.processValueSingle(table, defaultColumnID, kv, prettyKey)
				}
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
				if rf.args.TraceKV {
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
			prettyKey, prettyValue, err = rf.processValueBytes(table, valueBytes, prettyKey)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if rf.args.TraceKV && prettyValue == "" {
		prettyValue = "<undecoded>"
	}

	return prettyKey, prettyValue, nil
}

// processValueSingle processes the given value (of column colID), setting
// values in table.row accordingly. The key is only used for logging.
func (rf *Fetcher) processValueSingle(
	table *tableInfo, colID descpb.ColumnID, kv roachpb.KeyValue, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	idx, ok := table.colIdxMap.Get(colID)
	if !ok {
		// No need to unmarshal the column value. Either the column was part of
		// the index key or it isn't needed.
		return prettyKey, "", nil
	}

	if rf.args.TraceKV {
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
	value, err := valueside.UnmarshalLegacy(rf.args.Alloc, typ, kv.Value)
	if err != nil {
		return "", "", err
	}
	if rf.args.TraceKV {
		prettyValue = value.String()
	}
	table.row[idx] = rowenc.DatumToEncDatum(typ, value)
	return prettyKey, prettyValue, nil
}

func (rf *Fetcher) processValueBytes(
	table *tableInfo, valueBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if rf.args.TraceKV {
		if rf.prettyValueBuf == nil {
			rf.prettyValueBuf = &bytes.Buffer{}
		}
		rf.prettyValueBuf.Reset()
	}
	neededCols := rf.table.neededValueCols - rf.valueColsFound
	colOrds, err := rowenc.DecodeValueBytes(table.colIdxMap, valueBytes, neededCols, table.row)
	if err != nil {
		return "", "", err
	}
	rf.valueColsFound += colOrds.Len()
	if rf.args.TraceKV {
		for colOrd, ok := colOrds.Next(0); ok; colOrd, ok = colOrds.Next(colOrd + 1) {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.spec.FetchedColumns[colOrd].Name)
			err = table.row[colOrd].EnsureDecoded(table.spec.FetchedColumns[colOrd].Type, rf.args.Alloc)
			if err != nil {
				return "", "", err
			}
			fmt.Fprintf(rf.prettyValueBuf, "/%v", table.row[colOrd].Datum)
		}
		prettyValue = rf.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

// NextRow processes keys until we complete one row, which is returned as an
// EncDatumRow. The row contains one value per IndexFetchSpec.FetchedColumns.
// The ID associated with the span that produced this row is returned (0 if nil
// spanIDs were provided to StartScan).
//
// The EncDatumRow should not be modified and is only valid until the next call.
//
// When there are no more rows, the EncDatumRow is nil.
func (rf *Fetcher) NextRow(ctx context.Context) (row rowenc.EncDatumRow, spanID int, err error) {
	if rf.kvEnd {
		return nil, 0, nil
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
			return nil, 0, err
		}
		// TraceKVEvery is a util.EveryN and not a log.EveryN because
		// log.EveryN will always print under verbosity level 2.
		// The caller may choose to set it to avoid logging
		// too many rows. If unset, we log every KV.
		if rf.args.TraceKV && (rf.args.TraceKVEvery == nil || rf.args.TraceKVEvery.ShouldProcess(timeutil.Now())) {
			log.VEventf(ctx, TraceKVVerbosity, "fetched: %s -> %s", prettyKey, prettyVal)
		}

		rowDone, spanID, err := rf.nextKey(ctx)
		if err != nil {
			return nil, 0, err
		}
		if rowDone {
			err := rf.finalizeRow()
			rowSpanID := rf.spanID
			rf.spanID = spanID
			return rf.table.row, rowSpanID, err
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
	row, _, err := rf.NextRow(ctx)
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
	row, _, err := rf.NextRow(ctx)
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
		if err := encDatum.EnsureDecoded(rf.table.spec.FetchedColumns[i].Type, rf.args.Alloc); err != nil {
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
) (ok bool, spanID int, err error) {
	row, spanID, err := rf.NextRow(ctx)
	if err != nil {
		return false, spanID, err
	}
	if row == nil {
		return false, spanID, nil
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
		if err := encDatum.EnsureDecoded(col.Type, rf.args.Alloc); err != nil {
			return false, spanID, err
		}
		destination[ord] = encDatum.Datum
	}

	return true, spanID, nil
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
		dec := rf.args.Alloc.NewDDecimal(tree.DDecimal{Decimal: eval.TimestampToDecimal(rf.RowLastModified())})
		table.row[table.timestampOutputIdx] = rowenc.EncDatum{Datum: dec}
	}
	if table.oidOutputIdx != noOutputColumn {
		table.row[table.oidOutputIdx] = rowenc.EncDatum{Datum: table.tableOid}
	}

	if table.originIDOutputIdx != noOutputColumn {
		table.row[table.originIDOutputIdx] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(rf.table.rowLastOriginID))}
	}

	if table.originTimestampOutputIdx != noOutputColumn {
		if rf.table.rowLastOriginTimestamp.IsSet() && rf.table.rowLastModifiedWithoutOriginTimestamp.Less(rf.table.rowLastOriginTimestamp) {
			dec := rf.args.Alloc.NewDDecimal(tree.DDecimal{Decimal: eval.TimestampToDecimal(rf.table.rowLastOriginTimestamp)})
			table.row[table.originTimestampOutputIdx] = rowenc.EncDatum{Datum: dec}
		} else {
			table.row[table.originTimestampOutputIdx] = rowenc.NullEncDatum()
		}
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

// GetKVPairsRead returns total number of key-value pairs read by the underlying
// KVFetcher.
func (rf *Fetcher) GetKVPairsRead() int64 {
	if rf == nil || rf.kvFetcher == nil || rf.args.WillUseKVProvider {
		return 0
	}
	return rf.kvFetcher.GetKVPairsRead()
}

// GetBytesRead returns total number of bytes read by the underlying KVFetcher.
func (rf *Fetcher) GetBytesRead() int64 {
	if rf == nil || rf.kvFetcher == nil {
		return 0
	}
	return rf.kvFetcher.GetBytesRead()
}

// GetBatchRequestsIssued returns total number of BatchRequests issued by the
// underlying KVFetcher.
func (rf *Fetcher) GetBatchRequestsIssued() int64 {
	if rf == nil || rf.kvFetcher == nil || rf.args.WillUseKVProvider {
		return 0
	}
	return rf.kvFetcher.GetBatchRequestsIssued()
}
