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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
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
	indexColumnDirs []descpb.IndexDescriptor_Direction

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

	// invertedColOrdinal is a column ordinal among only needed columns,
	// indicating the inverted column; -1 if there is no inverted column or we
	// don't need the value for that column.
	invertedColOrdinal int

	// maxColumnFamilyID is the maximum possible family id for the configured
	// table.
	maxColumnFamilyID descpb.FamilyID

	// knownPrefixLength is the number of bytes in the index key prefix this
	// Fetcher is configured for. The index key prefix is the table id, index
	// id pair at the start of the key.
	knownPrefixLength int

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

	keyValTypes []*types.T
	extraTypes  []*types.T
	// extraValDirections contains len(extraTypes) ASC directions. This will
	// only be used for unique secondary indexes.
	extraValDirections []descpb.IndexDescriptor_Direction

	da rowenc.DatumAlloc
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
		keyValTypes:         c.keyValTypes[:0],
		extraTypes:          c.extraTypes[:0],
		extraValDirections:  c.extraValDirections[:0],
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
//   var rf cFetcher
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
//   rf.Close(ctx)
type cFetcher struct {
	cFetcherArgs

	// table is the table that's configured for fetching.
	table *cTableInfo

	// maxKeysPerRow memoizes the maximum number of keys per row in the index
	// we're fetching from. This is used to calculate the kvBatchFetcher's
	// firstBatchLimit.
	maxKeysPerRow int

	// True if the index key must be decoded. This is only false if there are no
	// needed columns.
	mustDecodeIndexKey bool

	// mvccDecodeStrategy controls whether or not MVCC timestamps should
	// be decoded from KV's fetched. It is set if any of the requested tables
	// are required to produce an MVCC timestamp system column.
	mvccDecodeStrategy row.MVCCDecodingStrategy

	// fetcher is the underlying fetcher that provides KVs.
	fetcher *row.KVFetcher

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

func (rf *cFetcher) resetBatch() {
	var reallocated bool
	var minDesiredCapacity int
	if rf.maxCapacity > 0 {
		// If we have already exceeded the memory limit for the output batch, we
		// will only be using the same batch from now on.
		minDesiredCapacity = rf.maxCapacity
	} else if rf.machine.limitHint > 0 && (rf.estimatedRowCount == 0 || uint64(rf.machine.limitHint) < rf.estimatedRowCount) {
		// If we have a limit hint, and either
		//   1) we don't have an estimate, or
		//   2) we have a soft limit,
		// use the hint to size the batch. Note that if it exceeds
		// coldata.BatchSize, ResetMaybeReallocate will chop it down.
		minDesiredCapacity = rf.machine.limitHint
	} else {
		// Otherwise, use the estimate. Note that if the estimate is not
		// present, it'll be 0 and ResetMaybeReallocate will allocate the
		// initial batch of capacity 1 which is the desired behavior.
		//
		// We need to transform our rf.estimatedRowCount, which is a uint64,
		// into an int. We have to be careful: if we just cast it directly, a
		// giant estimate will wrap around and become negative.
		if rf.estimatedRowCount > uint64(coldata.BatchSize()) {
			minDesiredCapacity = coldata.BatchSize()
		} else {
			minDesiredCapacity = int(rf.estimatedRowCount)
		}
	}
	rf.machine.batch, reallocated = rf.accountingHelper.ResetMaybeReallocate(
		rf.table.typs, rf.machine.batch, minDesiredCapacity, rf.memoryLimit,
	)
	if reallocated {
		rf.machine.colvecs.SetBatch(rf.machine.batch)
		// Pull out any requested system column output vecs.
		if rf.table.timestampOutputIdx != noOutputColumn {
			rf.machine.timestampCol = rf.machine.colvecs.DecimalCols[rf.machine.colvecs.ColsMap[rf.table.timestampOutputIdx]]
		}
		if rf.table.oidOutputIdx != noOutputColumn {
			rf.machine.tableoidCol = rf.machine.colvecs.DatumCols[rf.machine.colvecs.ColsMap[rf.table.oidOutputIdx]]
		}
		// Change the allocation size to be the same as the capacity of the
		// batch we allocated above.
		rf.table.da.AllocSize = rf.machine.batch.Capacity()
	}
}

// Init sets up a Fetcher based on the table args. Only columns present in
// tableArgs.cols will be fetched.
func (rf *cFetcher) Init(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	kvFetcherMemAcc *mon.BoundAccount,
	tableArgs *cFetcherTableArgs,
	hasSystemColumns bool,
) error {
	rf.kvFetcherMemAcc = kvFetcherMemAcc
	table := newCTableInfo()
	nCols := tableArgs.ColIdxMap.Len()
	if cap(table.orderedColIdxMap.vals) < nCols {
		table.orderedColIdxMap.vals = make(descpb.ColumnIDs, 0, nCols)
		table.orderedColIdxMap.ords = make([]int, 0, nCols)
	}
	colDescriptors := tableArgs.cols
	for i := range colDescriptors {
		//gcassert:bce
		id := colDescriptors[i].GetID()
		table.orderedColIdxMap.vals = append(table.orderedColIdxMap.vals, id)
		table.orderedColIdxMap.ords = append(table.orderedColIdxMap.ords, tableArgs.ColIdxMap.GetDefault(id))
	}
	sort.Sort(table.orderedColIdxMap)
	*table = cTableInfo{
		cFetcherTableArgs:   tableArgs,
		orderedColIdxMap:    table.orderedColIdxMap,
		indexColOrdinals:    table.indexColOrdinals[:0],
		extraValColOrdinals: table.extraValColOrdinals[:0],
		keyValTypes:         table.keyValTypes[:0],
		extraTypes:          table.extraTypes[:0],
		extraValDirections:  table.extraValDirections[:0],
		timestampOutputIdx:  noOutputColumn,
		oidOutputIdx:        noOutputColumn,
	}

	if nCols > 0 {
		table.neededValueColsByIdx.AddRange(0 /* start */, nCols-1)
	}

	if hasSystemColumns {
		// System columns, if present, are at the end of colDescriptors.
		nonSystemColOffset := nCols - len(colinfo.AllSystemColumnDescs)
		if nonSystemColOffset < 0 {
			nonSystemColOffset = 0
		}
		for idx := nonSystemColOffset; idx < nCols; idx++ {
			col := colDescriptors[idx].GetID()
			// Set up extra metadata for system columns, if this is a system
			// column.
			//
			// Currently the system columns are present in neededValueColsByIdx,
			// but we don't want to include them in that set because the
			// handling of system columns is separate from the standard value
			// decoding process.
			switch colinfo.GetSystemColumnKindFromColumnID(col) {
			case descpb.SystemColumnKind_MVCCTIMESTAMP:
				table.timestampOutputIdx = idx
				rf.mvccDecodeStrategy = row.MVCCDecodingRequired
				table.neededValueColsByIdx.Remove(idx)
			case descpb.SystemColumnKind_TABLEOID:
				table.oidOutputIdx = idx
				table.neededValueColsByIdx.Remove(idx)
			}
		}
	}

	table.knownPrefixLength = len(rowenc.MakeIndexKeyPrefix(codec, table.desc, table.index.GetID()))

	var indexColumnIDs []descpb.ColumnID
	indexColumnIDs, table.indexColumnDirs = catalog.FullIndexColumnIDs(table.index)

	compositeColumnIDs := util.MakeFastIntSet()
	for i := 0; i < table.index.NumCompositeColumns(); i++ {
		id := table.index.GetCompositeColumnID(i)
		compositeColumnIDs.Add(int(id))
	}

	nIndexCols := len(indexColumnIDs)
	if cap(table.indexColOrdinals) >= nIndexCols {
		table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
	} else {
		table.indexColOrdinals = make([]int, nIndexCols)
	}
	indexColOrdinals := table.indexColOrdinals
	_ = indexColOrdinals[len(indexColumnIDs)-1]
	needToDecodeDecimalKey := false
	for i, id := range indexColumnIDs {
		colIdx, ok := tableArgs.ColIdxMap.Get(id)
		if ok {
			//gcassert:bce
			indexColOrdinals[i] = colIdx
			rf.mustDecodeIndexKey = true
			needToDecodeDecimalKey = needToDecodeDecimalKey || tableArgs.typs[colIdx].Family() == types.DecimalFamily
			// A composite column might also have a value encoding which must be
			// decoded. Others can be removed from neededValueColsByIdx.
			if compositeColumnIDs.Contains(int(id)) {
				table.compositeIndexColOrdinals.Add(colIdx)
			} else {
				table.neededValueColsByIdx.Remove(colIdx)
			}
		} else {
			//gcassert:bce
			indexColOrdinals[i] = -1
		}
	}
	if needToDecodeDecimalKey && cap(rf.scratch) < 64 {
		// If we need to decode the decimal key encoding, it might use a scratch
		// byte slice internally, so we'll allocate such a space to be reused
		// for every decimal.
		// TODO(yuzefovich): 64 was chosen arbitrarily, tune it.
		rf.scratch = make([]byte, 64)
	}
	table.invertedColOrdinal = -1
	if table.index.GetType() == descpb.IndexDescriptor_INVERTED {
		id := table.index.InvertedColumnID()
		colIdx, ok := tableArgs.ColIdxMap.Get(id)
		if ok {
			table.invertedColOrdinal = colIdx
			// TODO(yuzefovich): for some reason the setup of ColBatchScan
			// sometimes doesn't find the inverted column, so we have to be a
			// bit tricky here and overwrite the type to what we need for the
			// inverted column. Figure it out.
			table.typs[colIdx] = types.Bytes
		}
	}
	// Unique secondary indexes contain the extra column IDs as part of
	// the value component. We process these separately, so we need to know
	// what extra columns are composite or not.
	if table.isSecondaryIndex && table.index.IsUnique() {
		for i := 0; i < table.index.NumKeySuffixColumns(); i++ {
			id := table.index.GetKeySuffixColumnID(i)
			colIdx, ok := tableArgs.ColIdxMap.Get(id)
			if ok {
				if compositeColumnIDs.Contains(int(id)) {
					table.compositeIndexColOrdinals.Add(colIdx)
					table.neededValueColsByIdx.Remove(colIdx)
				}
			}
		}
	}

	// Prepare our index key vals slice.
	table.keyValTypes = colinfo.GetColumnTypesFromColDescs(
		colDescriptors, indexColumnIDs, table.keyValTypes,
	)
	if table.index.NumKeySuffixColumns() > 0 {
		// Unique secondary indexes have a value that is the
		// primary index key.
		// Primary indexes only contain ascendingly-encoded
		// values. If this ever changes, we'll probably have to
		// figure out the directions here too.
		table.extraTypes = colinfo.GetColumnTypesFromColDescs(
			colDescriptors, table.index.IndexDesc().KeySuffixColumnIDs, table.extraTypes,
		)
		nExtraColumns := table.index.NumKeySuffixColumns()
		if cap(table.extraValColOrdinals) >= nExtraColumns {
			table.extraValColOrdinals = table.extraValColOrdinals[:nExtraColumns]
		} else {
			table.extraValColOrdinals = make([]int, nExtraColumns)
		}
		// Note that for extraValDirections we only need to make sure that the
		// slice has the correct length set since the ASC direction is the zero
		// value and we don't modify the elements of this slice.
		if cap(table.extraValDirections) >= nExtraColumns {
			table.extraValDirections = table.extraValDirections[:nExtraColumns]
		} else {
			table.extraValDirections = make([]descpb.IndexDescriptor_Direction, nExtraColumns)
		}

		extraValColOrdinals := table.extraValColOrdinals
		_ = extraValColOrdinals[nExtraColumns-1]
		for i := 0; i < nExtraColumns; i++ {
			id := table.index.GetKeySuffixColumnID(i)
			idx, ok := tableArgs.ColIdxMap.Get(id)
			if ok {
				//gcassert:bce
				extraValColOrdinals[i] = idx
			} else {
				//gcassert:bce
				extraValColOrdinals[i] = -1
			}
		}
	}

	// Keep track of the maximum keys per row to accommodate a
	// limitHint when StartScan is invoked.
	var err error
	rf.maxKeysPerRow, err = table.desc.KeysPerRow(table.index.GetID())
	if err != nil {
		return err
	}

	_ = table.desc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		id := family.ID
		if id > table.maxColumnFamilyID {
			table.maxColumnFamilyID = id
		}
		return nil
	})

	rf.table = table
	rf.accountingHelper.Init(allocator, rf.table.typs)

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
//
// The fetcher takes ownership of the spans slice - it can modify the slice and
// will perform the memory accounting accordingly. The caller can only reuse the
// spans slice after the fetcher has been closed (which happens when the fetcher
// emits the first zero batch), and if the caller does, it becomes responsible
// for the memory accounting.
func (rf *cFetcher) StartScan(
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
		firstBatchLimit = rowinfra.KeyLimit(int(limitHint) * rf.maxKeysPerRow)
	}

	f, err := row.NewKVFetcher(
		ctx,
		txn,
		spans,
		bsHeader,
		rf.reverse,
		batchBytesLimit,
		firstBatchLimit,
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.lockTimeout,
		rf.kvFetcherMemAcc,
		forceProductionKVBatchSize,
	)
	if err != nil {
		return err
	}
	rf.fetcher = f
	rf.machine.lastRowPrefix = nil
	rf.machine.limitHint = int(limitHint)
	rf.machine.state[0] = stateResetBatch
	rf.machine.state[1] = stateInitFetch
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

func (rf *cFetcher) setEstimatedRowCount(estimatedRowCount uint64) {
	rf.estimatedRowCount = estimatedRowCount
}

// setNextKV sets the next KV to process to the input KV. needsCopy, if true,
// causes the input kv to be deep copied. needsCopy should be set to true if
// the input KV is pointing to the last KV of a batch, so that the batch can
// be garbage collected before fetching the next one.
// gcassert:inline
func (rf *cFetcher) setNextKV(kv roachpb.KeyValue, needsCopy bool) {
	if !needsCopy {
		rf.machine.nextKV = kv
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
	rf.machine.nextKV = kvCopy
}

// NextBatch processes keys until we complete one batch of rows (subject to the
// limit hint and the memory limit while being max coldata.BatchSize() in
// length), which are returned in columnar format as a coldata.Batch. The batch
// contains one Vec per table column, regardless of the index used; columns that
// are not needed (as per neededCols) are filled with nulls. The Batch should
// not be modified and is only valid until the next call. When there are no more
// rows, the Batch.Length is 0.
func (rf *cFetcher) NextBatch(ctx context.Context) (coldata.Batch, error) {
	for {
		if debugState {
			log.Infof(ctx, "State %s", rf.machine.state[0])
		}
		switch rf.machine.state[0] {
		case stateInvalid:
			return nil, errors.New("invalid fetcher state")
		case stateInitFetch:
			moreKVs, kv, finalReferenceToBatch, err := rf.fetcher.NextKV(ctx, rf.mvccDecodeStrategy)
			if err != nil {
				return nil, rf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				rf.machine.state[0] = stateEmitLastBatch
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
				lcs := rf.fetcher.span.LongestCommonPrefix()
				// parse lcs into stuff
				key, matches, err := rowenc.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
					rf.table.desc, rf.table.info.index, rf.table.info.keyValTypes,
					rf.table.keyVals, rf.table.info.indexColumnDirs, kv.Key[rf.table.info.knownPrefixLength:],
				)
				if err != nil {
					// This is expected - the longest common prefix of the keyspan might
					// end half way through a key. Suppress the error and set the actual
					// LCS we'll use later to the decodable components of the key.
				}
				}
			*/

			rf.setNextKV(kv, finalReferenceToBatch)
			rf.machine.state[0] = stateDecodeFirstKVOfRow

		case stateResetBatch:
			rf.resetBatch()
			rf.shiftState()
		case stateDecodeFirstKVOfRow:
			// Reset MVCC metadata for the table, since this is the first KV of a row.
			rf.table.rowLastModified = hlc.Timestamp{}

			// foundNull is set when decoding a new index key for a row finds a NULL value
			// in the index key. This is used when decoding unique secondary indexes in order
			// to tell whether they have extra columns appended to the key.
			var foundNull bool
			if rf.mustDecodeIndexKey {
				if debugState {
					log.Infof(ctx, "decoding first key %s", rf.machine.nextKV.Key)
				}
				var (
					key []byte
					err error
				)
				// For unique secondary indexes on tables with multiple column
				// families, we must check all columns for NULL values in order
				// to determine whether a KV belongs to the same row as the
				// previous KV or a different row.
				checkAllColsForNull := rf.table.isSecondaryIndex && rf.table.index.IsUnique() && rf.table.desc.NumFamilies() != 1
				key, foundNull, rf.scratch, err = colencoding.DecodeKeyValsToCols(
					&rf.table.da,
					&rf.machine.colvecs,
					rf.machine.rowIdx,
					rf.table.indexColOrdinals,
					checkAllColsForNull,
					rf.table.keyValTypes,
					rf.table.indexColumnDirs,
					nil, /* unseen */
					rf.machine.nextKV.Key[rf.table.knownPrefixLength:],
					rf.table.invertedColOrdinal,
					rf.scratch,
				)
				if err != nil {
					return nil, err
				}
				prefix := rf.machine.nextKV.Key[:len(rf.machine.nextKV.Key)-len(key)]
				rf.machine.lastRowPrefix = prefix
			} else {
				prefixLen, err := keys.GetRowPrefixLength(rf.machine.nextKV.Key)
				if err != nil {
					return nil, err
				}
				rf.machine.lastRowPrefix = rf.machine.nextKV.Key[:prefixLen]
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
			if foundNull && rf.table.isSecondaryIndex && rf.table.index.IsUnique() && rf.table.desc.NumFamilies() != 1 {
				// We get the remaining bytes after the computed prefix, and then
				// slice off the extra encoded columns from those bytes. We calculate
				// how many bytes were sliced away, and then extend lastRowPrefix
				// by that amount.
				prefixLen := len(rf.machine.lastRowPrefix)
				remainingBytes := rf.machine.nextKV.Key[prefixLen:]
				origRemainingBytesLen := len(remainingBytes)
				for i := 0; i < rf.table.index.NumKeySuffixColumns(); i++ {
					var err error
					// Slice off an extra encoded column from remainingBytes.
					remainingBytes, err = rowenc.SkipTableKey(remainingBytes)
					if err != nil {
						return nil, err
					}
				}
				rf.machine.lastRowPrefix = rf.machine.nextKV.Key[:prefixLen+(origRemainingBytesLen-len(remainingBytes))]
			}

			familyID, err := rf.getCurrentColumnFamilyID()
			if err != nil {
				return nil, err
			}
			rf.machine.remainingValueColsByIdx.CopyFrom(rf.table.neededValueColsByIdx)
			// Process the current KV's value component.
			if err := rf.processValue(ctx, familyID); err != nil {
				return nil, err
			}
			// Update the MVCC values for this row.
			if rf.table.rowLastModified.Less(rf.machine.nextKV.Value.Timestamp) {
				rf.table.rowLastModified = rf.machine.nextKV.Value.Timestamp
			}
			// If the table has only one column family, then the next KV will
			// always belong to a different row than the current KV.
			if rf.table.desc.NumFamilies() == 1 {
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateInitFetch
				continue
			}
			// If the table has more than one column family, then the next KV
			// may belong to the same row as the current KV.
			rf.machine.state[0] = stateFetchNextKVWithUnfinishedRow

		case stateFetchNextKVWithUnfinishedRow:
			moreKVs, kv, finalReferenceToBatch, err := rf.fetcher.NextKV(ctx, rf.mvccDecodeStrategy)
			if err != nil {
				return nil, rf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				// No more data. Finalize the row and exit.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateEmitLastBatch
				continue
			}
			// TODO(jordan): if nextKV returns newSpan = true, set the new span
			// prefix and indicate that it needs decoding.
			rf.setNextKV(kv, finalReferenceToBatch)
			if debugState {
				log.Infof(ctx, "decoding next key %s", rf.machine.nextKV.Key)
			}

			// TODO(yuzefovich): optimize this prefix check by skipping logical
			// longest common span prefix.
			if !bytes.HasPrefix(kv.Key[rf.table.knownPrefixLength:], rf.machine.lastRowPrefix[rf.table.knownPrefixLength:]) {
				// The kv we just found is from a different row.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateDecodeFirstKVOfRow
				continue
			}

			familyID, err := rf.getCurrentColumnFamilyID()
			if err != nil {
				return nil, err
			}

			// Process the current KV's value component.
			if err := rf.processValue(ctx, familyID); err != nil {
				return nil, err
			}

			// Update the MVCC values for this row.
			if rf.table.rowLastModified.Less(rf.machine.nextKV.Value.Timestamp) {
				rf.table.rowLastModified = rf.machine.nextKV.Value.Timestamp
			}

			if familyID == rf.table.maxColumnFamilyID {
				// We know the row can't have any more keys, so finalize the row.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateInitFetch
			} else {
				// Continue with current state.
				rf.machine.state[0] = stateFetchNextKVWithUnfinishedRow
			}

		case stateFinalizeRow:
			// Populate the timestamp system column if needed. We have to do it
			// on a per row basis since each row can be modified at a different
			// time.
			if rf.table.timestampOutputIdx != noOutputColumn {
				rf.machine.timestampCol[rf.machine.rowIdx] = tree.TimestampToDecimal(rf.table.rowLastModified)
			}

			// We're finished with a row. Fill the row in with nulls if
			// necessary, perform the memory accounting for the row, bump the
			// row index, emit the batch if necessary, and move to the next
			// state.
			if err := rf.fillNulls(); err != nil {
				return nil, err
			}
			// Note that we haven't set the tableoid value (if that system
			// column is requested) yet, but it is ok for the purposes of the
			// memory accounting - oids are fixed length values and, thus, have
			// already been accounted for when the batch was allocated.
			rf.accountingHelper.AccountForSet(rf.machine.rowIdx)
			rf.machine.rowIdx++
			rf.shiftState()

			var emitBatch bool
			if rf.maxCapacity == 0 && rf.accountingHelper.Allocator.Used() >= rf.memoryLimit {
				rf.maxCapacity = rf.machine.rowIdx
			}
			if rf.machine.rowIdx >= rf.machine.batch.Capacity() ||
				(rf.maxCapacity > 0 && rf.machine.rowIdx >= rf.maxCapacity) ||
				(rf.machine.limitHint > 0 && rf.machine.rowIdx >= rf.machine.limitHint) {
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
				rf.machine.limitHint -= rf.machine.rowIdx
			}

			if emitBatch {
				rf.pushState(stateResetBatch)
				rf.finalizeBatch()
				return rf.machine.batch, nil
			}

		case stateEmitLastBatch:
			rf.machine.state[0] = stateFinished
			rf.finalizeBatch()
			return rf.machine.batch, nil

		case stateFinished:
			// Close the fetcher eagerly so that its memory could be GCed.
			rf.Close(ctx)
			return coldata.ZeroBatch, nil
		}
	}
}

// shiftState shifts the state queue to the left, removing the first element and
// clearing the last element.
func (rf *cFetcher) shiftState() {
	copy(rf.machine.state[:2], rf.machine.state[1:])
	rf.machine.state[2] = stateInvalid
}

func (rf *cFetcher) pushState(state fetcherState) {
	copy(rf.machine.state[1:], rf.machine.state[:2])
	rf.machine.state[0] = state
}

// getDatumAt returns the converted datum object at the given (colIdx, rowIdx).
// This function is meant for tracing and should not be used in hot paths.
func (rf *cFetcher) getDatumAt(colIdx int, rowIdx int) tree.Datum {
	res := []tree.Datum{nil}
	colconv.ColVecToDatumAndDeselect(res, rf.machine.colvecs.Vecs[colIdx], 1 /* length */, []int{rowIdx}, &rf.table.da)
	return res[0]
}

// writeDecodedCols writes the stringified representation of the decoded columns
// specified by colOrdinals. -1 in colOrdinals indicates that a column wasn't
// actually decoded (this is represented as "?" in the result). separator is
// inserted between each two consequent decoded column values (but not before
// the first one).
func (rf *cFetcher) writeDecodedCols(buf *strings.Builder, colOrdinals []int, separator byte) {
	for i, idx := range colOrdinals {
		if i > 0 {
			buf.WriteByte(separator)
		}
		if idx != -1 {
			buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx).String())
		} else {
			buf.WriteByte('?')
		}
	}
}

// processValue processes the state machine's current value component, setting
// columns in the rowIdx'th tuple in the current batch depending on what data
// is found in the current value component.
func (rf *cFetcher) processValue(ctx context.Context, familyID descpb.FamilyID) (err error) {
	table := rf.table

	var prettyKey, prettyValue string
	if rf.traceKV {
		defer func() {
			if err == nil {
				log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyValue)
			}
		}()

		var buf strings.Builder
		buf.WriteByte('/')
		buf.WriteString(rf.table.desc.GetName())
		buf.WriteByte('/')
		buf.WriteString(rf.table.index.GetName())
		buf.WriteByte('/')
		rf.writeDecodedCols(&buf, rf.table.indexColOrdinals, '/')
		prettyKey = buf.String()
	}

	if len(table.cols) == 0 {
		// We don't need to decode any values.
		return nil
	}

	val := rf.machine.nextKV.Value
	if !table.isSecondaryIndex || table.index.GetEncodingType() == descpb.PrimaryIndexEncoding {
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
			prettyKey, prettyValue, err = rf.processValueBytes(ctx, table, tupleBytes, prettyKey)
		default:
			var family *descpb.ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(familyID)
			if err != nil {
				return scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}
			prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, family, prettyKey)
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

			if table.isSecondaryIndex && table.index.IsUnique() {
				// This is a unique secondary index; decode the extra
				// column values from the value.
				valueBytes, _, rf.scratch, err = colencoding.DecodeKeyValsToCols(
					&table.da,
					&rf.machine.colvecs,
					rf.machine.rowIdx,
					table.extraValColOrdinals,
					false, /* checkAllColsForNull */
					table.extraTypes,
					table.extraValDirections,
					&rf.machine.remainingValueColsByIdx,
					valueBytes,
					rf.table.invertedColOrdinal,
					rf.scratch,
				)
				if err != nil {
					return scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				if rf.traceKV && len(table.extraValColOrdinals) > 0 {
					var buf strings.Builder
					buf.WriteByte('/')
					rf.writeDecodedCols(&buf, table.extraValColOrdinals, '/')
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
			prettyKey, prettyValue, err = rf.processValueBytes(
				ctx, table, valueBytes, prettyKey,
			)
			if err != nil {
				return scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}
	}

	if rf.traceKV && prettyValue == "" {
		prettyValue = tree.DNull.String()
	}

	return nil
}

// processValueSingle processes the given value (of column
// family.DefaultColumnID), setting values in rf.machine.colvecs accordingly.
// The key is only used for logging.
func (rf *cFetcher) processValueSingle(
	ctx context.Context,
	table *cTableInfo,
	family *descpb.ColumnFamilyDescriptor,
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

	if idx, ok := table.ColIdxMap.Get(colID); ok {
		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.cols[idx].GetName())
		}
		val := rf.machine.nextKV.Value
		if len(val.RawBytes) == 0 {
			return prettyKey, "", nil
		}
		typ := rf.table.typs[idx]
		err := colencoding.UnmarshalColumnValueToCol(
			&table.da, &rf.machine.colvecs, idx, rf.machine.rowIdx, typ, val,
		)
		if err != nil {
			return "", "", err
		}
		rf.machine.remainingValueColsByIdx.Remove(idx)

		if rf.traceKV {
			prettyValue = rf.getDatumAt(idx, rf.machine.rowIdx).String()
		}
		if row.DebugRowFetch {
			log.Infof(ctx, "Scan %s -> %v", rf.machine.nextKV.Key, "?")
		}
		return prettyKey, prettyValue, nil
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if row.DebugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", rf.machine.nextKV.Key, colID)
	}
	return prettyKey, prettyValue, nil
}

func (rf *cFetcher) processValueBytes(
	ctx context.Context, table *cTableInfo, valueBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if rf.traceKV {
		if rf.machine.prettyValueBuf == nil {
			rf.machine.prettyValueBuf = &bytes.Buffer{}
		}
		rf.machine.prettyValueBuf.Reset()
	}

	// Composite columns that are key encoded in the value (like the pk columns
	// in a unique secondary index) have gotten removed from the set of
	// remaining value columns. So, we need to add them back in here in case
	// they have full value encoded composite values.
	rf.table.compositeIndexColOrdinals.ForEach(func(i int) {
		rf.machine.remainingValueColsByIdx.Add(i)
	})

	var (
		colIDDiff      uint32
		lastColID      descpb.ColumnID
		dataOffset     int
		typ            encoding.Type
		lastColIDIndex int
	)
	// Continue reading data until there's none left or we've finished
	// populating the data for all of the requested columns.
	for len(valueBytes) > 0 && rf.machine.remainingValueColsByIdx.Len() > 0 {
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
				log.Infof(ctx, "Scan %s -> [%d] (skipped)", rf.machine.nextKV.Key, colID)
			}
			continue
		}

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.cols[vecIdx].GetName())
		}

		valueBytes, err = colencoding.DecodeTableValueToCol(
			&table.da, &rf.machine.colvecs, vecIdx, rf.machine.rowIdx, typ,
			dataOffset, rf.table.typs[vecIdx], valueBytes,
		)
		if err != nil {
			return "", "", err
		}
		rf.machine.remainingValueColsByIdx.Remove(vecIdx)
		if rf.traceKV {
			dVal := rf.getDatumAt(vecIdx, rf.machine.rowIdx)
			if _, err := fmt.Fprintf(rf.machine.prettyValueBuf, "/%v", dVal.String()); err != nil {
				return "", "", err
			}
		}
	}
	if rf.traceKV {
		prettyValue = rf.machine.prettyValueBuf.String()
	}
	return prettyKey, prettyValue, nil
}

func (rf *cFetcher) fillNulls() error {
	table := rf.table
	if rf.machine.remainingValueColsByIdx.Empty() {
		return nil
	}
	for i, ok := rf.machine.remainingValueColsByIdx.Next(0); ok; i, ok = rf.machine.remainingValueColsByIdx.Next(i + 1) {
		// Composite index columns may have a key but no value. Ignore them so we
		// don't incorrectly mark them as null.
		if table.compositeIndexColOrdinals.Contains(i) {
			continue
		}
		if !table.cols[i].IsNullable() {
			var indexColValues strings.Builder
			rf.writeDecodedCols(&indexColValues, table.indexColOrdinals, ',')
			return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
				"non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
				table.desc.GetName(), table.cols[i].GetName(), table.index.GetName(),
				strings.Join(table.index.IndexDesc().KeyColumnNames, ","), indexColValues.String()))
		}
		rf.machine.colvecs.Nulls[i].SetNull(rf.machine.rowIdx)
	}
	return nil
}

func (rf *cFetcher) finalizeBatch() {
	// Populate the tableoid system column for the whole batch if necessary.
	if rf.table.oidOutputIdx != noOutputColumn {
		id := rf.table.desc.GetID()
		for i := 0; i < rf.machine.rowIdx; i++ {
			// Note that we don't need to update the memory accounting because
			// oids are fixed length values and have already been accounted for
			// when finalizing each row.
			rf.machine.tableoidCol.Set(i, rf.table.da.NewDOid(tree.MakeDOid(tree.DInt(id))))
		}
	}
	rf.machine.batch.SetLength(rf.machine.rowIdx)
	rf.machine.rowIdx = 0
}

// getCurrentColumnFamilyID returns the column family id of the key in
// rf.machine.nextKV.Key.
func (rf *cFetcher) getCurrentColumnFamilyID() (descpb.FamilyID, error) {
	// If the table only has 1 column family, and its ID is 0, we know that the
	// key has to be the 0th column family.
	if rf.table.maxColumnFamilyID == 0 {
		return 0, nil
	}
	// The column family is encoded in the final bytes of the key. The last
	// byte of the key is the length of the column family id encoding
	// itself. See encoding.md for more details, and see MakeFamilyKey for
	// the routine that performs this encoding.
	var id uint64
	_, id, err := encoding.DecodeUvarintAscending(rf.machine.nextKV.Key[len(rf.machine.lastRowPrefix):])
	if err != nil {
		return 0, scrub.WrapError(scrub.IndexKeyDecodingError, err)
	}
	return descpb.FamilyID(id), nil
}

// convertFetchError converts an error generated during a key-value fetch to a
// storage error that will propagate through the exec subsystem unchanged. The
// error may also undergo a mapping to make it more user friendly for SQL
// consumers.
func (rf *cFetcher) convertFetchError(ctx context.Context, err error) error {
	err = row.ConvertFetchError(ctx, rf, err)
	err = colexecerror.NewStorageError(err)
	return err
}

// KeyToDesc implements the KeyToDescTranslator interface. The implementation is
// used by convertFetchError.
func (rf *cFetcher) KeyToDesc(key roachpb.Key) (catalog.TableDescriptor, bool) {
	if len(key) < rf.table.knownPrefixLength {
		return nil, false
	}
	nIndexCols := rf.table.index.NumKeyColumns() + rf.table.index.NumKeySuffixColumns()
	tableKeyVals := make([]rowenc.EncDatum, nIndexCols)
	_, _, err := rowenc.DecodeKeyVals(
		rf.table.keyValTypes,
		tableKeyVals,
		rf.table.indexColumnDirs,
		key[rf.table.knownPrefixLength:],
	)
	if err != nil {
		return nil, false
	}
	return rf.table.desc, true
}

var cFetcherPool = sync.Pool{
	New: func() interface{} {
		return &cFetcher{}
	},
}

func (rf *cFetcher) Release() {
	rf.accountingHelper.Release()
	if rf.table != nil {
		rf.table.Release()
	}
	colvecs := rf.machine.colvecs
	colvecs.Reset()
	*rf = cFetcher{
		scratch: rf.scratch[:0],
	}
	rf.machine.colvecs = colvecs
	cFetcherPool.Put(rf)
}

func (rf *cFetcher) Close(ctx context.Context) {
	if rf != nil && rf.fetcher != nil {
		rf.fetcher.Close(ctx)
		rf.fetcher = nil
	}
}
