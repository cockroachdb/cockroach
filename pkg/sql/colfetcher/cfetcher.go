// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

type cTableInfo struct {
	// -- Fields initialized once --

	*cFetcherTableArgs

	// The set of required value-component column ordinals among only needed
	// columns.
	neededValueColsByIdx util.FastIntSet

	// Map used to get the column index based on the descpb.ColumnID.
	// It's kept as a pointer so we don't have to re-allocate to sort it each
	// time.
	orderedColIdxMap *colIdxMap

	// One value per column that is part of the key; each value is a column
	// ordinal among only needed columns; -1 if we don't need the value for
	// that column.
	indexColOrdinals []int

	// The set of column ordinals which are both composite and part of the index
	// key.
	compositeIndexColOrdinals util.FastIntSet

	// One number per column coming from the "key suffix" that is part of the
	// value; each number is a column ordinal among only needed columns; -1 if
	// we don't need the value for that column.
	//
	// The "key suffix" columns are only used for secondary indexes:
	// - for non-unique indexes, these columns are appended to the key (and will
	// be included in indexColOrdinals instead);
	// - for unique indexes, these columns are stored in the value (unless the
	// key contains a NULL value: then the extra columns are appended to the key
	// to unique-ify it).
	extraValColOrdinals []int

	// The following fields contain MVCC metadata for each row and may be
	// returned to users of cFetcher immediately after NextBatch returns.
	//
	// rowLastModified is the timestamp of the last time any family in the row
	// was modified in any way.
	rowLastModified hlc.Timestamp
	// timestampOutputIdx controls at what column ordinal in the output batch to
	// write the timestamp for the MVCC timestamp system column.
	timestampOutputIdx int
	// oidOutputIdx controls at what column ordinal in the output batch to write
	// the value for the tableoid system column.
	oidOutputIdx int

	da tree.DatumAlloc
}

var _ execinfra.Releasable = &cTableInfo{}

var cTableInfoPool = sync.Pool{
	New: func() interface{} {
		return &cTableInfo{
			orderedColIdxMap: &colIdxMap{},
		}
	},
}

func newCTableInfo() *cTableInfo {
	return cTableInfoPool.Get().(*cTableInfo)
}

// Release implements the execinfra.Releasable interface.
func (c *cTableInfo) Release() {
	c.cFetcherTableArgs.Release()
	// Note that all slices are being reused, but there is no need to deeply
	// reset them since all of the slices are of Go native types.
	c.orderedColIdxMap.ords = c.orderedColIdxMap.ords[:0]
	c.orderedColIdxMap.vals = c.orderedColIdxMap.vals[:0]
	*c = cTableInfo{
		orderedColIdxMap:    c.orderedColIdxMap,
		indexColOrdinals:    c.indexColOrdinals[:0],
		extraValColOrdinals: c.extraValColOrdinals[:0],
	}
	cTableInfoPool.Put(c)
}

// colIdxMap is a "map" that contains the ordinals for each ColumnID among the
// columns that need to be fetched. This map is used to figure out what index
// within a row a particular value-component column goes into. Value-component
// columns are encoded with a column id prefix, with the guarantee that within
// any given row, the column ids are always increasing. Because of this
// guarantee, we can store this map as two sorted lists that the fetcher keeps
// an index into, giving fast access during decoding.
//
// It implements sort.Interface to be sortable on vals, while keeping ords
// matched up to the order of vals.
type colIdxMap struct {
	// vals is the sorted list of descpb.ColumnIDs in the table to fetch.
	vals descpb.ColumnIDs
	// ords is the list of ordinals into all columns of the table for each
	// column in vals. The ith entry in ords is the ordinal among all columns of
	// the table for the ith column in vals.
	ords []int
}

// Len implements sort.Interface.
func (m colIdxMap) Len() int {
	return len(m.vals)
}

// Less implements sort.Interface.
func (m colIdxMap) Less(i, j int) bool {
	return m.vals[i] < m.vals[j]
}

// Swap implements sort.Interface.
func (m colIdxMap) Swap(i, j int) {
	m.vals[i], m.vals[j] = m.vals[j], m.vals[i]
	m.ords[i], m.ords[j] = m.ords[j], m.ords[i]
}

type cFetcherArgs struct {
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
	// memoryLimit determines the maximum memory footprint of the output batch.
	memoryLimit int64
	// estimatedRowCount is the optimizer-derived number of expected rows that
	// this fetch will produce, if non-zero.
	estimatedRowCount uint64
	// reverse denotes whether or not the spans should be read in reverse or not
	// when StartScan is invoked.
	reverse bool
	// traceKV indicates whether or not session tracing is enabled. It is set
	// when initializing the fetcher.
	traceKV bool
}

// noOutputColumn is a sentinel value to denote that a system column is not
// part of the output.
const noOutputColumn = -1

// cFetcher handles fetching kvs and forming table rows for an
// arbitrary number of tables.
// Usage:
//   var cf cFetcher
//   err := cf.Init(..)
//   // Handle err
//   err := cf.StartScan(..)
//   // Handle err
//   for {
//      res, err := cf.NextBatch()
//      // Handle err
//      if res.colBatch.Length() == 0 {
//         // Done
//         break
//      }
//      // Process res.colBatch
//   }
//   cf.Close(ctx)
type cFetcher struct {
	cFetcherArgs

	// table is the table that's configured for fetching.
	table *cTableInfo

	// True if the index key must be decoded. This is only false if there are no
	// needed columns.
	mustDecodeIndexKey bool

	// mvccDecodeStrategy controls whether or not MVCC timestamps should
	// be decoded from KV's fetched. It is set if any of the requested tables
	// are required to produce an MVCC timestamp system column.
	mvccDecodeStrategy row.MVCCDecodingStrategy

	// fetcher is the underlying fetcher that provides KVs.
	fetcher *row.KVFetcher
	// bytesRead stores the cumulative number of bytes read by this cFetcher
	// throughout its whole existence (i.e. between its construction and
	// Release()). It accumulates the bytes read statistic across StartScan* and
	// Close methods.
	//
	// The field should not be accessed directly by the users of the cFetcher -
	// getBytesRead() should be used instead.
	bytesRead int64

	// machine contains fields that get updated during the run of the fetcher.
	machine struct {
		// state is the queue of next states of the state machine. The 0th entry
		// is the next state.
		state [3]fetcherState
		// rowIdx is always set to the ordinal of the row we're currently writing to
		// within the current batch. It's incremented as soon as we detect that a row
		// is finished.
		rowIdx int
		// nextKV is the kv to process next.
		nextKV roachpb.KeyValue

		// limitHint is a hint as to the number of rows that the caller expects
		// to be returned from this fetch. It will be decremented whenever a
		// batch is returned by the length of the batch so that it tracks the
		// hint for the rows remaining to be returned. It might become negative
		// indicating that the hint is no longer applicable.
		limitHint int

		// remainingValueColsByIdx is the set of value columns that are yet to be
		// seen during the decoding of the current row.
		remainingValueColsByIdx util.FastIntSet
		// lastRowPrefix is the row prefix for the last row we saw a key for. New
		// keys are compared against this prefix to determine whether they're part
		// of a new row or not.
		lastRowPrefix roachpb.Key
		// prettyValueBuf is a temp buffer used to create strings for tracing.
		prettyValueBuf *bytes.Buffer

		// batch is the output batch the fetcher writes to.
		batch coldata.Batch

		// colvecs are the vectors of batch that have been converted to the well
		// typed columns to avoid expensive type casts on each row.
		colvecs coldata.TypedVecs

		// timestampCol is the underlying ColVec for the timestamp output column,
		// or nil if the timestamp column was not requested. It is pulled out from
		// colvecs to avoid having to cast the vec to decimal on every write.
		timestampCol []apd.Decimal
		// tableoidCol is the same as timestampCol but for the tableoid system column.
		tableoidCol coldata.DatumVec
	}

	// scratch is a scratch space used when decoding bytes-like and decimal
	// keys.
	scratch []byte

	accountingHelper colmem.SetAccountingHelper

	// kvFetcherMemAcc is a memory account that will be used by the underlying
	// KV fetcher.
	kvFetcherMemAcc *mon.BoundAccount

	// maxCapacity if non-zero indicates the target capacity of the output
	// batch. It is set when at the row finalization we realize that the output
	// batch has exceeded the memory limit.
	maxCapacity int
}

func (cf *cFetcher) resetBatch() {
	var reallocated bool
	var minDesiredCapacity int
	if cf.maxCapacity > 0 {
		// If we have already exceeded the memory limit for the output batch, we
		// will only be using the same batch from now on.
		minDesiredCapacity = cf.maxCapacity
	} else if cf.machine.limitHint > 0 && (cf.estimatedRowCount == 0 || uint64(cf.machine.limitHint) < cf.estimatedRowCount) {
		// If we have a limit hint, and either
		//   1) we don't have an estimate, or
		//   2) we have a soft limit,
		// use the hint to size the batch. Note that if it exceeds
		// coldata.BatchSize, ResetMaybeReallocate will chop it down.
		minDesiredCapacity = cf.machine.limitHint
	} else {
		// Otherwise, use the estimate. Note that if the estimate is not
		// present, it'll be 0 and ResetMaybeReallocate will allocate the
		// initial batch of capacity 1 which is the desired behavior.
		//
		// We need to transform our cf.estimatedRowCount, which is a uint64,
		// into an int. We have to be careful: if we just cast it directly, a
		// giant estimate will wrap around and become negative.
		if cf.estimatedRowCount > uint64(coldata.BatchSize()) {
			minDesiredCapacity = coldata.BatchSize()
		} else {
			minDesiredCapacity = int(cf.estimatedRowCount)
		}
	}
	cf.machine.batch, reallocated = cf.accountingHelper.ResetMaybeReallocate(
		cf.table.typs, cf.machine.batch, minDesiredCapacity, cf.memoryLimit,
	)
	if reallocated {
		cf.machine.colvecs.SetBatch(cf.machine.batch)
		// Pull out any requested system column output vecs.
		if cf.table.timestampOutputIdx != noOutputColumn {
			cf.machine.timestampCol = cf.machine.colvecs.DecimalCols[cf.machine.colvecs.ColsMap[cf.table.timestampOutputIdx]]
		}
		if cf.table.oidOutputIdx != noOutputColumn {
			cf.machine.tableoidCol = cf.machine.colvecs.DatumCols[cf.machine.colvecs.ColsMap[cf.table.oidOutputIdx]]
		}
		// Change the allocation size to be the same as the capacity of the
		// batch we allocated above.
		cf.table.da.AllocSize = cf.machine.batch.Capacity()
	}
}

// Init sets up a Fetcher based on the table args. Only columns present in
// tableArgs.cols will be fetched.
func (cf *cFetcher) Init(
	allocator *colmem.Allocator, kvFetcherMemAcc *mon.BoundAccount, tableArgs *cFetcherTableArgs,
) error {
	if tableArgs.spec.Version != descpb.IndexFetchSpecVersionInitial {
		return errors.Newf("unsupported IndexFetchSpec version %d", tableArgs.spec.Version)
	}
	cf.kvFetcherMemAcc = kvFetcherMemAcc
	table := newCTableInfo()
	nCols := tableArgs.ColIdxMap.Len()
	if cap(table.orderedColIdxMap.vals) < nCols {
		table.orderedColIdxMap.vals = make(descpb.ColumnIDs, 0, nCols)
		table.orderedColIdxMap.ords = make([]int, 0, nCols)
	}
	for i := range tableArgs.spec.FetchedColumns {
		id := tableArgs.spec.FetchedColumns[i].ColumnID
		table.orderedColIdxMap.vals = append(table.orderedColIdxMap.vals, id)
		table.orderedColIdxMap.ords = append(table.orderedColIdxMap.ords, tableArgs.ColIdxMap.GetDefault(id))
	}
	sort.Sort(table.orderedColIdxMap)
	*table = cTableInfo{
		cFetcherTableArgs:   tableArgs,
		orderedColIdxMap:    table.orderedColIdxMap,
		indexColOrdinals:    table.indexColOrdinals[:0],
		extraValColOrdinals: table.extraValColOrdinals[:0],
		timestampOutputIdx:  noOutputColumn,
		oidOutputIdx:        noOutputColumn,
	}

	if nCols > 0 {
		table.neededValueColsByIdx.AddRange(0 /* start */, nCols-1)
	}

	// Check for system columns.
	for idx := range tableArgs.spec.FetchedColumns {
		colID := tableArgs.spec.FetchedColumns[idx].ColumnID
		if colinfo.IsColIDSystemColumn(colID) {
			// Set up extra metadata for system columns.
			//
			// Currently the system columns are present in neededValueColsByIdx,
			// but we don't want to include them in that set because the
			// handling of system columns is separate from the standard value
			// decoding process.
			switch colinfo.GetSystemColumnKindFromColumnID(colID) {
			case catpb.SystemColumnKind_MVCCTIMESTAMP:
				table.timestampOutputIdx = idx
				cf.mvccDecodeStrategy = row.MVCCDecodingRequired
				table.neededValueColsByIdx.Remove(idx)
			case catpb.SystemColumnKind_TABLEOID:
				table.oidOutputIdx = idx
				table.neededValueColsByIdx.Remove(idx)
			}
		}
	}

	fullColumns := table.spec.KeyFullColumns()

	nIndexCols := len(fullColumns)
	if cap(table.indexColOrdinals) >= nIndexCols {
		table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
	} else {
		table.indexColOrdinals = make([]int, nIndexCols)
	}
	indexColOrdinals := table.indexColOrdinals
	_ = indexColOrdinals[len(fullColumns)-1]
	needToDecodeDecimalKey := false
	for i := range fullColumns {
		col := &fullColumns[i]
		colIdx, ok := tableArgs.ColIdxMap.Get(col.ColumnID)
		if ok {
			//gcassert:bce
			indexColOrdinals[i] = colIdx
			cf.mustDecodeIndexKey = true
			needToDecodeDecimalKey = needToDecodeDecimalKey || tableArgs.spec.FetchedColumns[colIdx].Type.Family() == types.DecimalFamily
			// A composite column might also have a value encoding which must be
			// decoded. Others can be removed from neededValueColsByIdx.
			if col.IsComposite {
				table.compositeIndexColOrdinals.Add(colIdx)
			} else {
				table.neededValueColsByIdx.Remove(colIdx)
			}
		} else {
			//gcassert:bce
			indexColOrdinals[i] = -1
		}
	}
	if needToDecodeDecimalKey && cap(cf.scratch) < 64 {
		// If we need to decode the decimal key encoding, it might use a scratch
		// byte slice internally, so we'll allocate such a space to be reused
		// for every decimal.
		// TODO(yuzefovich): 64 was chosen arbitrarily, tune it.
		cf.scratch = make([]byte, 64)
	}
	// Unique secondary indexes contain the extra column IDs as part of
	// the value component. We process these separately, so we need to know
	// what extra columns are composite or not.
	if table.spec.NumKeySuffixColumns > 0 && table.spec.IsSecondaryIndex && table.spec.IsUniqueIndex {
		suffixCols := table.spec.KeySuffixColumns()
		for i := range suffixCols {
			id := suffixCols[i].ColumnID
			colIdx, ok := tableArgs.ColIdxMap.Get(id)
			if ok {
				if suffixCols[i].IsComposite {
					table.compositeIndexColOrdinals.Add(colIdx)
					// Note: we account for these composite columns separately: we add
					// them back into the remaining values set in processValueBytes.
					table.neededValueColsByIdx.Remove(colIdx)
				}
			}
		}

		// Unique secondary indexes have a value that is the
		// primary index key.
		// Primary indexes only contain ascendingly-encoded
		// values. If this ever changes, we'll probably have to
		// figure out the directions here too.
		if cap(table.extraValColOrdinals) >= len(suffixCols) {
			table.extraValColOrdinals = table.extraValColOrdinals[:len(suffixCols)]
		} else {
			table.extraValColOrdinals = make([]int, len(suffixCols))
		}

		extraValColOrdinals := table.extraValColOrdinals
		_ = extraValColOrdinals[len(suffixCols)-1]
		for i := range suffixCols {
			idx, ok := tableArgs.ColIdxMap.Get(suffixCols[i].ColumnID)
			if ok {
				//gcassert:bce
				extraValColOrdinals[i] = idx
			} else {
				//gcassert:bce
				extraValColOrdinals[i] = -1
			}
		}
	}

	cf.table = table
	cf.accountingHelper.Init(allocator, cf.table.typs)

	return nil
}

//gcassert:inline
func (cf *cFetcher) setFetcher(f *row.KVFetcher, limitHint rowinfra.RowLimit) {
	cf.fetcher = f
	cf.machine.lastRowPrefix = nil
	cf.machine.limitHint = int(limitHint)
	cf.machine.state[0] = stateResetBatch
	cf.machine.state[1] = stateInitFetch
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly. The caller can only reuse the
// spans slice after the fetcher has been closed (which happens when the fetcher
// emits the first zero batch), and if the caller does, it becomes responsible
// for the memory accounting.
func (cf *cFetcher) StartScan(
	ctx context.Context,
	txn *kv.Txn,
	spans roachpb.Spans,
	bsHeader *roachpb.BoundedStalenessHeader,
	limitBatches bool,
	batchBytesLimit rowinfra.BytesLimit,
	limitHint rowinfra.RowLimit,
	forceProductionKVBatchSize bool,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("no spans")
	}
	if !limitBatches && batchBytesLimit != rowinfra.NoBytesLimit {
		return errors.AssertionFailedf("batchBytesLimit set without limitBatches")
	}

	// If we have a limit hint, we limit the first batch size. Subsequent
	// batches get larger to avoid making things too slow (e.g. in case we have
	// a very restrictive filter and actually have to retrieve a lot of rows).
	firstBatchLimit := rowinfra.KeyLimit(limitHint)
	if firstBatchLimit != 0 {
		// The limitHint is a row limit, but each row could be made up of more
		// than one key. We take the maximum possible keys per row out of all
		// the table rows we could potentially scan over.
		//
		// Note that unlike for the row.Fetcher, we don't need an extra key to
		// form the last row in the cFetcher because we are eagerly finalizing
		// each row once we know that all KVs comprising that row have been
		// fetched. Consider several cases:
		// - the table has only one column family - then we can finalize each
		//   row right after the first KV is decoded;
		// - the table has multiple column families:
		//   - KVs for all column families are present for all rows - then for
		//     each row, when its last KV is fetched, the row can be finalized
		//     (and firstBatchLimit asks exactly for the correct number of KVs);
		//   - KVs for some column families are omitted for some rows - then we
		//     will actually fetch more KVs than necessary, but we'll decode
		//     limitHint number of rows.
		firstBatchLimit = rowinfra.KeyLimit(int(limitHint) * int(cf.table.spec.MaxKeysPerRow))
	}

	f, err := row.NewKVFetcher(
		ctx,
		txn,
		spans,
		bsHeader,
		cf.reverse,
		batchBytesLimit,
		firstBatchLimit,
		cf.lockStrength,
		cf.lockWaitPolicy,
		cf.lockTimeout,
		cf.kvFetcherMemAcc,
		forceProductionKVBatchSize,
	)
	if err != nil {
		return err
	}
	cf.setFetcher(f, limitHint)
	return nil
}

// StartScanStreaming initializes and starts the key-value scan using the
// Streamer API. Can be used multiple times.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly. The caller can only reuse the
// spans slice after the fetcher has been closed (which happens when the fetcher
// emits the first zero batch), and if the caller does, it becomes responsible
// for the memory accounting.
func (cf *cFetcher) StartScanStreaming(
	ctx context.Context,
	streamer *kvstreamer.Streamer,
	spans roachpb.Spans,
	limitHint rowinfra.RowLimit,
) error {
	kvBatchFetcher, err := row.NewTxnKVStreamer(ctx, streamer, spans, cf.lockStrength)
	if err != nil {
		return err
	}
	f := row.NewKVStreamingFetcher(kvBatchFetcher)
	cf.setFetcher(f, limitHint)
	return nil
}

// fetcherState is the state enum for NextBatch.
type fetcherState int

//go:generate stringer -type=fetcherState

const (
	stateInvalid fetcherState = iota

	// stateInitFetch is the empty state of a fetcher: there is no current KV to
	// look at, and there's no current row, either because the fetcher has just
	// started, or because the last row was already finalized.
	//
	//   1. fetch next kv into nextKV buffer
	//     -> decodeFirstKVOfRow
	stateInitFetch

	// stateResetBatch resets the batch of a fetcher, removing nulls and the
	// selection vector.
	stateResetBatch

	// stateDecodeFirstKVOfRow is the state of looking at a key that is part of
	// a row that the fetcher hasn't processed before. s.machine.nextKV must be
	// set.
	//   1. skip common prefix
	//   2. parse key (past common prefix) into row buffer, setting last row prefix buffer
	//   3. parse value into row buffer.
	//   4. 1-cf or secondary index?
	//     -> doneRow(initFetch)
	//   else:
	//     -> fetchNextKVWithUnfinishedRow
	stateDecodeFirstKVOfRow

	// stateFetchNextKVWithUnfinishedRow is the state of getting a new key for
	// the current row. The machine will read a new key from the underlying
	// fetcher, process it, and either add the results to the current row, or
	// shift to a new row.
	//   1. fetch next kv into nextKV buffer
	//   2. skip common prefix
	//   3. check equality to last row prefix buffer
	//   4. no?
	//     -> finalizeRow(decodeFirstKVOfRow)
	//   5. skip to end of last row prefix buffer
	//   6. parse value into row buffer
	//   7. -> fetchNextKVWithUnfinishedRow
	stateFetchNextKVWithUnfinishedRow

	// stateFinalizeRow is the state of finalizing a row. It assumes that no more
	// keys for the current row are present.
	// state[1] must be set, and stateFinalizeRow will transition to that state
	// once it finishes finalizing the row.
	//   1. fill missing nulls
	//   2. bump rowIdx
	//   -> nextState and optionally return if row-by-row or batch full
	stateFinalizeRow

	// stateEmitLastBatch emits the current batch and then transitions to
	// stateFinished.
	stateEmitLastBatch

	// stateFinished is the end state of the state machine - it causes NextBatch
	// to return empty batches forever.
	stateFinished
)

// Turn this on to enable super verbose logging of the fetcher state machine.
const debugState = false

func (cf *cFetcher) setEstimatedRowCount(estimatedRowCount uint64) {
	cf.estimatedRowCount = estimatedRowCount
}

// setNextKV sets the next KV to process to the input KV. needsCopy, if true,
// causes the input kv to be deep copied. needsCopy should be set to true if
// the input KV is pointing to the last KV of a batch, so that the batch can
// be garbage collected before fetching the next one.
// gcassert:inline
func (cf *cFetcher) setNextKV(kv roachpb.KeyValue, needsCopy bool) {
	if !needsCopy {
		cf.machine.nextKV = kv
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
	cf.machine.nextKV = kvCopy
}

// NextBatch processes keys until we complete one batch of rows (subject to the
// limit hint and the memory limit while being max coldata.BatchSize() in
// length), which are returned in columnar format as a coldata.Batch. The batch
// contains one Vec per table column, regardless of the index used; columns that
// are not needed (as per neededCols) are filled with nulls. The Batch should
// not be modified and is only valid until the next call. When there are no more
// rows, the Batch.Length is 0.
func (cf *cFetcher) NextBatch(ctx context.Context) (coldata.Batch, error) {
	for {
		if debugState {
			log.Infof(ctx, "State %s", cf.machine.state[0])
		}
		switch cf.machine.state[0] {
		case stateInvalid:
			return nil, errors.New("invalid fetcher state")
		case stateInitFetch:
			moreKVs, kv, finalReferenceToBatch, err := cf.fetcher.NextKV(ctx, cf.mvccDecodeStrategy)
			if err != nil {
				return nil, cf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				cf.machine.state[0] = stateEmitLastBatch
				continue
			}
			// TODO(jordan): parse the logical longest common prefix of the span
			// into a buffer. The logical longest common prefix is the longest
			// common prefix that contains only full key components. For example,
			// the keys /Table/53/1/foo/bar/10 and /Table/53/1/foo/bop/10 would
			// have LLCS of /Table/53/1/foo, even though they share a b prefix of
			// the next key, since that prefix isn't a complete key component.
			/*
				if newSpan {
				lcs := cf.fetcher.span.LongestCommonPrefix()
				// parse lcs into stuff
				key, matches, err := rowenc.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
					cf.table.desc, cf.table.info.index, cf.table.info.keyValTypes,
					cf.table.keyVals, cf.table.info.indexColumnDirs, kv.Key[cf.table.info.knownPrefixLength:],
				)
				if err != nil {
					// This is expected - the longest common prefix of the keyspan might
					// end half way through a key. Suppress the error and set the actual
					// LCS we'll use later to the decodable components of the key.
				}
				}
			*/

			cf.setNextKV(kv, finalReferenceToBatch)
			cf.machine.state[0] = stateDecodeFirstKVOfRow

		case stateResetBatch:
			cf.resetBatch()
			cf.shiftState()
		case stateDecodeFirstKVOfRow:
			// Reset MVCC metadata for the table, since this is the first KV of a row.
			cf.table.rowLastModified = hlc.Timestamp{}

			// foundNull is set when decoding a new index key for a row finds a NULL value
			// in the index key. This is used when decoding unique secondary indexes in order
			// to tell whether they have extra columns appended to the key.
			var foundNull bool
			if cf.mustDecodeIndexKey {
				if debugState {
					log.Infof(ctx, "decoding first key %s", cf.machine.nextKV.Key)
				}
				var (
					key []byte
					err error
				)
				// For unique secondary indexes on tables with multiple column
				// families, we must check all columns for NULL values in order
				// to determine whether a KV belongs to the same row as the
				// previous KV or a different row.
				checkAllColsForNull := cf.table.spec.IsSecondaryIndex && cf.table.spec.IsUniqueIndex && cf.table.spec.MaxKeysPerRow != 1
				key, foundNull, cf.scratch, err = colencoding.DecodeKeyValsToCols(
					&cf.table.da,
					&cf.machine.colvecs,
					cf.machine.rowIdx,
					cf.table.indexColOrdinals,
					checkAllColsForNull,
					cf.table.spec.KeyFullColumns(),
					nil, /* unseen */
					cf.machine.nextKV.Key[cf.table.spec.KeyPrefixLength:],
					cf.scratch,
				)
				if err != nil {
					return nil, err
				}
				prefix := cf.machine.nextKV.Key[:len(cf.machine.nextKV.Key)-len(key)]
				cf.machine.lastRowPrefix = prefix
			} else {
				prefixLen, err := keys.GetRowPrefixLength(cf.machine.nextKV.Key)
				if err != nil {
					return nil, err
				}
				cf.machine.lastRowPrefix = cf.machine.nextKV.Key[:prefixLen]
			}

			// For unique secondary indexes on tables with multiple column
			// families, the index-key does not distinguish one row from the
			// next if both rows contain identical values along with a NULL.
			// Consider the keys:
			//
			//   /test/unique_idx/NULL/0
			//   /test/unique_idx/NULL/1
			//
			// The index-key extracted from the above keys is
			// /test/unique_idx/NULL. The trailing /0 and /1 are the primary key
			// used to unique-ify the keys when a NULL is present. When a null
			// is present in the index key, we include the primary key columns
			// in lastRowPrefix.
			//
			// Note that we do not need to do this for non-unique secondary
			// indexes because the extra columns in the primary key will
			// _always_ be there, so we can decode them when processing the
			// index. The difference with unique secondary indexes is that the
			// extra columns are not always there, and are used to unique-ify
			// the index key, rather than provide the primary key column values.
			//
			// We also do not need to do this when a table has only one column
			// family because it is guaranteed that there is only one KV per
			// row. We entirely skip the check that determines if the row is
			// unfinished.
			if foundNull && cf.table.spec.IsSecondaryIndex && cf.table.spec.IsUniqueIndex && cf.table.spec.MaxKeysPerRow != 1 {
				// We get the remaining bytes after the computed prefix, and then
				// slice off the extra encoded columns from those bytes. We calculate
				// how many bytes were sliced away, and then extend lastRowPrefix
				// by that amount.
				prefixLen := len(cf.machine.lastRowPrefix)
				remainingBytes := cf.machine.nextKV.Key[prefixLen:]
				origRemainingBytesLen := len(remainingBytes)
				for i := 0; i < int(cf.table.spec.NumKeySuffixColumns); i++ {
					var err error
					// Slice off an extra encoded column from remainingBytes.
					remainingBytes, err = keyside.Skip(remainingBytes)
					if err != nil {
						return nil, err
					}
				}
				cf.machine.lastRowPrefix = cf.machine.nextKV.Key[:prefixLen+(origRemainingBytesLen-len(remainingBytes))]
			}

			familyID, err := cf.getCurrentColumnFamilyID()
			if err != nil {
				return nil, err
			}
			cf.machine.remainingValueColsByIdx.CopyFrom(cf.table.neededValueColsByIdx)
			// Process the current KV's value component.
			if err := cf.processValue(ctx, familyID); err != nil {
				return nil, err
			}
			// Update the MVCC values for this row.
			if cf.table.rowLastModified.Less(cf.machine.nextKV.Value.Timestamp) {
				cf.table.rowLastModified = cf.machine.nextKV.Value.Timestamp
			}
			// If the index has only one column family, then the next KV will
			// always belong to a different row than the current KV.
			if cf.table.spec.MaxKeysPerRow == 1 {
				cf.machine.state[0] = stateFinalizeRow
				cf.machine.state[1] = stateInitFetch
				continue
			}
			// If the table has more than one column family, then the next KV
			// may belong to the same row as the current KV.
			cf.machine.state[0] = stateFetchNextKVWithUnfinishedRow

		case stateFetchNextKVWithUnfinishedRow:
			moreKVs, kv, finalReferenceToBatch, err := cf.fetcher.NextKV(ctx, cf.mvccDecodeStrategy)
			if err != nil {
				return nil, cf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				// No more data. Finalize the row and exit.
				cf.machine.state[0] = stateFinalizeRow
				cf.machine.state[1] = stateEmitLastBatch
				continue
			}
			// TODO(jordan): if nextKV returns newSpan = true, set the new span
			// prefix and indicate that it needs decoding.
			cf.setNextKV(kv, finalReferenceToBatch)
			if debugState {
				log.Infof(ctx, "decoding next key %s", cf.machine.nextKV.Key)
			}

			// TODO(yuzefovich): optimize this prefix check by skipping logical
			// longest common span prefix.
			if !bytes.HasPrefix(kv.Key[cf.table.spec.KeyPrefixLength:], cf.machine.lastRowPrefix[cf.table.spec.KeyPrefixLength:]) {
				// The kv we just found is from a different row.
				cf.machine.state[0] = stateFinalizeRow
				cf.machine.state[1] = stateDecodeFirstKVOfRow
				continue
			}

			familyID, err := cf.getCurrentColumnFamilyID()
			if err != nil {
				return nil, err
			}

			// Process the current KV's value component.
			if err := cf.processValue(ctx, familyID); err != nil {
				return nil, err
			}

			// Update the MVCC values for this row.
			if cf.table.rowLastModified.Less(cf.machine.nextKV.Value.Timestamp) {
				cf.table.rowLastModified = cf.machine.nextKV.Value.Timestamp
			}

			if familyID == cf.table.spec.MaxFamilyID {
				// We know the row can't have any more keys, so finalize the row.
				cf.machine.state[0] = stateFinalizeRow
				cf.machine.state[1] = stateInitFetch
			} else {
				// Continue with current state.
				cf.machine.state[0] = stateFetchNextKVWithUnfinishedRow
			}

		case stateFinalizeRow:
			// Populate the timestamp system column if needed. We have to do it
			// on a per row basis since each row can be modified at a different
			// time.
			if cf.table.timestampOutputIdx != noOutputColumn {
				cf.machine.timestampCol[cf.machine.rowIdx] = tree.TimestampToDecimal(cf.table.rowLastModified)
			}

			// We're finished with a row. Fill the row in with nulls if
			// necessary, perform the memory accounting for the row, bump the
			// row index, emit the batch if necessary, and move to the next
			// state.
			if err := cf.fillNulls(); err != nil {
				return nil, err
			}
			// Note that we haven't set the tableoid value (if that system
			// column is requested) yet, but it is ok for the purposes of the
			// memory accounting - oids are fixed length values and, thus, have
			// already been accounted for when the batch was allocated.
			cf.accountingHelper.AccountForSet(cf.machine.rowIdx)
			cf.machine.rowIdx++
			cf.shiftState()

			var emitBatch bool
			if cf.maxCapacity == 0 && cf.accountingHelper.Allocator.Used() >= cf.memoryLimit {
				cf.maxCapacity = cf.machine.rowIdx
			}
			if cf.machine.rowIdx >= cf.machine.batch.Capacity() ||
				(cf.maxCapacity > 0 && cf.machine.rowIdx >= cf.maxCapacity) ||
				(cf.machine.limitHint > 0 && cf.machine.rowIdx >= cf.machine.limitHint) {
				// We either
				//   1. have no more room in our batch, so output it immediately
				// or
				//   2. we made it to our limit hint, so output our batch early
				//      to make sure that we don't bother filling in extra data
				//      if we don't need to.
				emitBatch = true
				// Update the limit hint to track the expected remaining rows to
				// be fetched.
				//
				// Note that limitHint might become negative at which point we
				// will start ignoring it.
				cf.machine.limitHint -= cf.machine.rowIdx
			}

			if emitBatch {
				cf.pushState(stateResetBatch)
				cf.finalizeBatch()
				return cf.machine.batch, nil
			}

		case stateEmitLastBatch:
			cf.machine.state[0] = stateFinished
			cf.finalizeBatch()
			// Close the fetcher eagerly so that its memory could be GCed.
			cf.Close(ctx)
			return cf.machine.batch, nil

		case stateFinished:
			// Close the fetcher eagerly so that its memory could be GCed.
			cf.Close(ctx)
			return coldata.ZeroBatch, nil
		}
	}
}

// shiftState shifts the state queue to the left, removing the first element and
// clearing the last element.
func (cf *cFetcher) shiftState() {
	copy(cf.machine.state[:2], cf.machine.state[1:])
	cf.machine.state[2] = stateInvalid
}

func (cf *cFetcher) pushState(state fetcherState) {
	copy(cf.machine.state[1:], cf.machine.state[:2])
	cf.machine.state[0] = state
}

// getDatumAt returns the converted datum object at the given (colIdx, rowIdx).
// This function is meant for tracing and should not be used in hot paths.
func (cf *cFetcher) getDatumAt(colIdx int, rowIdx int) tree.Datum {
	res := []tree.Datum{nil}
	colconv.ColVecToDatumAndDeselect(res, cf.machine.colvecs.Vecs[colIdx], 1 /* length */, []int{rowIdx}, &cf.table.da)
	return res[0]
}

// writeDecodedCols writes the stringified representation of the decoded columns
// specified by colOrdinals. -1 in colOrdinals indicates that a column wasn't
// actually decoded (this is represented as "?" in the result). separator is
// inserted between each two subsequent decoded column values (but not before
// the first one).
func (cf *cFetcher) writeDecodedCols(buf *strings.Builder, colOrdinals []int, separator byte) {
	for i, idx := range colOrdinals {
		if i > 0 {
			buf.WriteByte(separator)
		}
		if idx != -1 {
			buf.WriteString(cf.getDatumAt(idx, cf.machine.rowIdx).String())
		} else {
			buf.WriteByte('?')
		}
	}
}

// processValue processes the state machine's current value component, setting
// columns in the rowIdx'th tuple in the current batch depending on what data
// is found in the current value component.
func (cf *cFetcher) processValue(ctx context.Context, familyID descpb.FamilyID) (err error) {
	table := cf.table

	var prettyKey, prettyValue string
	if cf.traceKV {
		defer func() {
			if err == nil {
				log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyValue)
			}
		}()

		var buf strings.Builder
		buf.WriteByte('/')
		buf.WriteString(cf.table.spec.TableName)
		buf.WriteByte('/')
		buf.WriteString(cf.table.spec.IndexName)
		buf.WriteByte('/')
		cf.writeDecodedCols(&buf, cf.table.indexColOrdinals, '/')
		prettyKey = buf.String()
	}

	if len(table.spec.FetchedColumns) == 0 {
		// We don't need to decode any values.
		return nil
	}

	val := cf.machine.nextKV.Value
	if !table.spec.IsSecondaryIndex || table.spec.EncodingType == descpb.PrimaryIndexEncoding {
		// If familyID is 0, kv.Value contains values for composite key columns.
		// These columns already have a table.row value assigned above, but that value
		// (obtained from the key encoding) might not be correct (e.g. for decimals,
		// it might not contain the right number of trailing 0s; for collated
		// strings, it is one of potentially many strings with the same collation
		// key).
		//
		// In these cases, the correct value will be present in family 0 and the
		// table.row value gets overwritten.

		switch val.GetTag() {
		case roachpb.ValueType_TUPLE:
			// In this case, we don't need to decode the column family ID, because
			// the ValueType_TUPLE encoding includes the column id with every encoded
			// column value.
			var tupleBytes []byte
			tupleBytes, err = val.GetTuple()
			if err != nil {
				break
			}
			prettyKey, prettyValue, err = cf.processValueBytes(ctx, table, tupleBytes, prettyKey)

		default:
			// If familyID is 0, this is the row sentinel (in the legacy pre-family format),
			// and a value is not expected, so we're done.
			if familyID == 0 {
				break
			}
			// Find the default column ID for the family.
			var defaultColumnID descpb.ColumnID
			for _, f := range table.spec.FamilyDefaultColumns {
				if f.FamilyID == familyID {
					defaultColumnID = f.DefaultColumnID
					break
				}
			}
			if defaultColumnID == 0 {
				return scrub.WrapError(
					scrub.IndexKeyDecodingError,
					errors.Errorf("single entry value with no default column id"),
				)
			}
			prettyKey, prettyValue, err = cf.processValueSingle(ctx, table, defaultColumnID, prettyKey)
		}
		if err != nil {
			return scrub.WrapError(scrub.IndexValueDecodingError, err)
		}
	} else {
		tag := val.GetTag()
		var valueBytes []byte
		switch tag {
		case roachpb.ValueType_BYTES:
			// If we have the ValueType_BYTES on a secondary index, then we know we
			// are looking at column family 0. Column family 0 stores the extra primary
			// key columns if they are present, so we decode them here.
			valueBytes, err = val.GetBytes()
			if err != nil {
				return scrub.WrapError(scrub.IndexValueDecodingError, err)
			}

			if table.spec.IsSecondaryIndex && table.spec.IsUniqueIndex {
				// This is a unique secondary index; decode the extra
				// column values from the value.
				valueBytes, _, cf.scratch, err = colencoding.DecodeKeyValsToCols(
					&table.da,
					&cf.machine.colvecs,
					cf.machine.rowIdx,
					table.extraValColOrdinals,
					false, /* checkAllColsForNull */
					table.spec.KeySuffixColumns(),
					&cf.machine.remainingValueColsByIdx,
					valueBytes,
					cf.scratch,
				)
				if err != nil {
					return scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				if cf.traceKV && len(table.extraValColOrdinals) > 0 {
					var buf strings.Builder
					buf.WriteByte('/')
					cf.writeDecodedCols(&buf, table.extraValColOrdinals, '/')
					prettyValue = buf.String()
				}
			}
		case roachpb.ValueType_TUPLE:
			valueBytes, err = val.GetTuple()
			if err != nil {
				return scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = cf.processValueBytes(
				ctx, table, valueBytes, prettyKey,
			)
			if err != nil {
				return scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if cf.traceKV && prettyValue == "" {
		prettyValue = "<undecoded>"
	}

	return nil
}

// processValueSingle processes the given value for a single column, setting the
// value in cf.machine.colvecs accordingly.
// The key is only used for logging.
func (cf *cFetcher) processValueSingle(
	ctx context.Context, table *cTableInfo, colID descpb.ColumnID, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix

	if idx, ok := table.ColIdxMap.Get(colID); ok {
		if cf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.spec.FetchedColumns[idx].Name)
		}
		val := cf.machine.nextKV.Value
		if len(val.RawBytes) == 0 {
			return prettyKey, "", nil
		}
		typ := cf.table.spec.FetchedColumns[idx].Type
		err := colencoding.UnmarshalColumnValueToCol(
			&table.da, &cf.machine.colvecs, idx, cf.machine.rowIdx, typ, val,
		)
		if err != nil {
			return "", "", err
		}
		cf.machine.remainingValueColsByIdx.Remove(idx)

		if cf.traceKV {
			prettyValue = cf.getDatumAt(idx, cf.machine.rowIdx).String()
		}
		if row.DebugRowFetch {
			log.Infof(ctx, "Scan %s -> %v", cf.machine.nextKV.Key, "?")
		}
		return prettyKey, prettyValue, nil
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if row.DebugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", cf.machine.nextKV.Key, colID)
	}
	return prettyKey, prettyValue, nil
}

func (cf *cFetcher) processValueBytes(
	ctx context.Context, table *cTableInfo, valueBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if cf.traceKV {
		if cf.machine.prettyValueBuf == nil {
			cf.machine.prettyValueBuf = &bytes.Buffer{}
		}
		cf.machine.prettyValueBuf.Reset()
	}

	// Composite columns that are key encoded in the value (like the pk columns
	// in a unique secondary index) have gotten removed from the set of
	// remaining value columns. So, we need to add them back in here in case
	// they have full value encoded composite values.
	cf.machine.remainingValueColsByIdx.UnionWith(cf.table.compositeIndexColOrdinals)

	var (
		colIDDiff      uint32
		lastColID      descpb.ColumnID
		dataOffset     int
		typ            encoding.Type
		lastColIDIndex int
	)
	// Continue reading data until there's none left or we've finished
	// populating the data for all of the requested columns.
	for len(valueBytes) > 0 && cf.machine.remainingValueColsByIdx.Len() > 0 {
		_, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + descpb.ColumnID(colIDDiff)
		lastColID = colID
		vecIdx := -1
		// Find the ordinal into table.cols for the column ID we just decoded,
		// by advancing through the sorted list of needed value columns until
		// there's a match, or we passed the column ID we're looking for.
		for ; lastColIDIndex < len(table.orderedColIdxMap.vals); lastColIDIndex++ {
			nextID := table.orderedColIdxMap.vals[lastColIDIndex]
			if nextID == colID {
				vecIdx = table.orderedColIdxMap.ords[lastColIDIndex]
				// Since the next value part (if it exists) will belong to the
				// column after the current one, we can advance the index.
				lastColIDIndex++
				break
			} else if nextID > colID {
				break
			}
		}
		if vecIdx == -1 {
			// This column wasn't requested, so read its length and skip it.
			len, err := encoding.PeekValueLengthWithOffsetsAndType(valueBytes, dataOffset, typ)
			if err != nil {
				return "", "", err
			}
			valueBytes = valueBytes[len:]
			if row.DebugRowFetch {
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", cf.machine.nextKV.Key, colID)
			}
			continue
		}

		if cf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.spec.FetchedColumns[vecIdx].Name)
		}

		valueBytes, err = colencoding.DecodeTableValueToCol(
			&table.da, &cf.machine.colvecs, vecIdx, cf.machine.rowIdx, typ,
			dataOffset, cf.table.typs[vecIdx], valueBytes,
		)
		if err != nil {
			return "", "", err
		}
		cf.machine.remainingValueColsByIdx.Remove(vecIdx)
		if cf.traceKV {
			dVal := cf.getDatumAt(vecIdx, cf.machine.rowIdx)
			if _, err := fmt.Fprintf(cf.machine.prettyValueBuf, "/%v", dVal.String()); err != nil {
				return "", "", err
			}
		}
	}
	if cf.traceKV {
		prettyValue = cf.machine.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

func (cf *cFetcher) fillNulls() error {
	table := cf.table
	if cf.machine.remainingValueColsByIdx.Empty() {
		return nil
	}
	for i, ok := cf.machine.remainingValueColsByIdx.Next(0); ok; i, ok = cf.machine.remainingValueColsByIdx.Next(i + 1) {
		// Composite index columns may have a key but no value. Ignore them so we
		// don't incorrectly mark them as null.
		if table.compositeIndexColOrdinals.Contains(i) {
			continue
		}
		if table.spec.FetchedColumns[i].IsNonNullable {
			var indexColValues strings.Builder
			cf.writeDecodedCols(&indexColValues, table.indexColOrdinals, ',')
			var indexColNames []string
			for i := range table.spec.KeyFullColumns() {
				indexColNames = append(indexColNames, table.spec.KeyAndSuffixColumns[i].Name)
			}
			return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
				"non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
				table.spec.TableName, table.spec.FetchedColumns[i].Name, table.spec.IndexName,
				strings.Join(indexColNames, ","), indexColValues.String()))
		}
		cf.machine.colvecs.Nulls[i].SetNull(cf.machine.rowIdx)
	}
	return nil
}

func (cf *cFetcher) finalizeBatch() {
	// Populate the tableoid system column for the whole batch if necessary.
	if cf.table.oidOutputIdx != noOutputColumn {
		id := cf.table.spec.TableID
		for i := 0; i < cf.machine.rowIdx; i++ {
			// Note that we don't need to update the memory accounting because
			// oids are fixed length values and have already been accounted for
			// when finalizing each row.
			cf.machine.tableoidCol.Set(i, cf.table.da.NewDOid(tree.MakeDOid(tree.DInt(id))))
		}
	}
	cf.machine.batch.SetLength(cf.machine.rowIdx)
	cf.machine.rowIdx = 0
}

// getCurrentColumnFamilyID returns the column family id of the key in
// cf.machine.nextKV.Key.
func (cf *cFetcher) getCurrentColumnFamilyID() (descpb.FamilyID, error) {
	// If the table only has 1 column family, and its ID is 0, we know that the
	// key has to be the 0th column family.
	if cf.table.spec.MaxFamilyID == 0 {
		return 0, nil
	}
	// The column family is encoded in the final bytes of the key. The last
	// byte of the key is the length of the column family id encoding
	// itself. See encoding.md for more details, and see MakeFamilyKey for
	// the routine that performs this encoding.
	var id uint64
	_, id, err := encoding.DecodeUvarintAscending(cf.machine.nextKV.Key[len(cf.machine.lastRowPrefix):])
	if err != nil {
		return 0, scrub.WrapError(scrub.IndexKeyDecodingError, err)
	}
	return descpb.FamilyID(id), nil
}

// convertFetchError converts an error generated during a key-value fetch to a
// storage error that will propagate through the exec subsystem unchanged. The
// error may also undergo a mapping to make it more user friendly for SQL
// consumers.
func (cf *cFetcher) convertFetchError(ctx context.Context, err error) error {
	err = row.ConvertFetchError(&cf.table.spec, err)
	err = colexecerror.NewStorageError(err)
	return err
}

// getBytesRead returns the number of bytes read by the cFetcher throughout its
// existence so far. This number accumulates the bytes read statistic across
// StartScan* and Close methods.
func (cf *cFetcher) getBytesRead() int64 {
	if cf.fetcher != nil {
		cf.bytesRead += cf.fetcher.ResetBytesRead()
	}
	return cf.bytesRead
}

var cFetcherPool = sync.Pool{
	New: func() interface{} {
		return &cFetcher{}
	},
}

func (cf *cFetcher) Release() {
	cf.accountingHelper.Release()
	if cf.table != nil {
		cf.table.Release()
	}
	colvecs := cf.machine.colvecs
	colvecs.Reset()
	*cf = cFetcher{
		scratch: cf.scratch[:0],
	}
	cf.machine.colvecs = colvecs
	cFetcherPool.Put(cf)
}

func (cf *cFetcher) Close(ctx context.Context) {
	if cf != nil && cf.fetcher != nil {
		cf.bytesRead += cf.fetcher.GetBytesRead()
		cf.fetcher.Close(ctx)
		cf.fetcher = nil
	}
}
