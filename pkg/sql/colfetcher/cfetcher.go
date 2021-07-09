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
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type cTableInfo struct {
	// -- Fields initialized once --

	// Used to determine whether a key retrieved belongs to the span we
	// want to scan.
	spans            roachpb.Spans
	desc             catalog.TableDescriptor
	index            catalog.Index
	isSecondaryIndex bool
	indexColumnDirs  []descpb.IndexDescriptor_Direction

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []catalog.Column

	// The ordered list of ColumnIDs that are required.
	neededColsList []int

	// The set of required value-component column ordinals in the table.
	neededValueColsByIdx util.FastIntSet

	// The set of ordinals of the columns that are **not** required. cFetcher
	// creates an output batch that includes all columns in cols, yet only
	// needed columns are actually populated. The vectors at positions in
	// notNeededColOrdinals will be set to have all null values.
	notNeededColOrdinals []int

	// Map used to get the index for columns in cols.
	// It's kept as a pointer so we don't have to re-allocate to sort it each
	// time.
	colIdxMap *colIdxMap

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColOrdinals []int
	// allIndexColOrdinals is the same as indexColOrdinals but
	// does not contain any -1's. It is meant to be used only in logging.
	allIndexColOrdinals []int

	// The set of column ordinals which are both composite and part of the index
	// key.
	compositeIndexColOrdinals util.FastIntSet

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	extraValColOrdinals []int
	// allExtraValColOrdinals is the same as extraValColOrdinals but
	// does not contain any -1's. It is meant to be used only in logging.
	allExtraValColOrdinals []int

	// invertedColOrdinal is a column index (into cols), indicating the virtual
	// inverted column; -1 if there is no virtual inverted column or we don't
	// need the value for that column.
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
	// timestampOutputIdx controls at what row ordinal to write the timestamp.
	timestampOutputIdx int

	// Fields for outputting the tableoid system column.
	oidOutputIdx int

	keyValTypes []*types.T
	extraTypes  []*types.T

	da rowenc.DatumAlloc
}

var _ execinfra.Releasable = &cTableInfo{}

var cTableInfoPool = sync.Pool{
	New: func() interface{} {
		return &cTableInfo{
			colIdxMap: &colIdxMap{},
		}
	},
}

func newCTableInfo() *cTableInfo {
	return cTableInfoPool.Get().(*cTableInfo)
}

// Release implements the execinfra.Releasable interface.
func (c *cTableInfo) Release() {
	// Note that all slices are being reused, but there is no need to deeply
	// reset them since all of the slices are of Go native types.
	c.colIdxMap.ords = c.colIdxMap.ords[:0]
	c.colIdxMap.vals = c.colIdxMap.vals[:0]
	*c = cTableInfo{
		colIdxMap:              c.colIdxMap,
		keyValTypes:            c.keyValTypes[:0],
		extraTypes:             c.extraTypes[:0],
		neededColsList:         c.neededColsList[:0],
		notNeededColOrdinals:   c.notNeededColOrdinals[:0],
		indexColOrdinals:       c.indexColOrdinals[:0],
		allIndexColOrdinals:    c.allIndexColOrdinals[:0],
		extraValColOrdinals:    c.extraValColOrdinals[:0],
		allExtraValColOrdinals: c.allExtraValColOrdinals[:0],
	}
	cTableInfoPool.Put(c)
}

// colIdxMap is a "map" that contains the ordinal in cols for each ColumnID
// in the table to fetch. This map is used to figure out what index within a
// row a particular value-component column goes into. Value-component columns
// are encoded with a column id prefix, with the guarantee that within any
// given row, the column ids are always increasing. Because of this guarantee,
// we can store this map as two sorted lists that the fetcher keeps an index
// into, giving fast access during decoding.
//
// It implements sort.Interface to be sortable on vals, while keeping ords
// matched up to the order of vals.
type colIdxMap struct {
	// vals is the sorted list of descpb.ColumnIDs in the table to fetch.
	vals descpb.ColumnIDs
	// colIdxOrds is the list of ordinals in cols for each column in colIdxVals.
	// The ith entry in colIdxOrds is the ordinal within cols for the ith column
	// in colIdxVals.
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

func (m colIdxMap) get(c descpb.ColumnID) (int, bool) {
	for i, v := range m.vals {
		if v == c {
			return m.ords[i], true
		}
	}
	return 0, false
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
//      res, err := rf.nextBatch()
//      // Handle err
//      if res.colBatch.Length() == 0 {
//         // Done
//         break
//      }
//      // Process res.colBatch
//   }
type cFetcher struct {
	// table is the table that's configured for fetching.
	table *cTableInfo

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

	// lockStrength represents the row-level locking mode to use when fetching rows.
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
	mvccDecodeStrategy row.MVCCDecodingStrategy

	// fetcher is the underlying fetcher that provides KVs.
	fetcher *row.KVFetcher

	// estimatedRowCount is the optimizer-derived number of expected rows that
	// this fetch will produce, if non-zero.
	estimatedRowCount uint64

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
		// seekPrefix is the prefix to seek to in stateSeekPrefix.
		seekPrefix roachpb.Key

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

		// colvecs is a slice of the ColVecs within batch, pulled out to avoid
		// having to call batch.Vec too often in the tight loop.
		colvecs []coldata.Vec

		// timestampCol is the underlying ColVec for the timestamp output column,
		// or nil if the timestamp column was not requested. It is pulled out from
		// colvecs to avoid having to cast the vec to decimal on every write.
		timestampCol []apd.Decimal
		// tableoidCol is the same as timestampCol but for the tableoid system column.
		tableoidCol coldata.DatumVec
	}

	typs        []*types.T
	allocator   *colmem.Allocator
	memoryLimit int64

	// adapter is a utility struct that helps with memory accounting.
	adapter struct {
		ctx   context.Context
		batch coldata.Batch
		err   error
	}
}

func (rf *cFetcher) resetBatch(timestampOutputIdx, tableOidOutputIdx int) {
	var reallocated bool
	var minCapacity int
	if rf.machine.limitHint > 0 && (rf.estimatedRowCount == 0 || uint64(rf.machine.limitHint) < rf.estimatedRowCount) {
		// If we have a limit hint, and either
		//   1) we don't have an estimate, or
		//   2) we have a soft limit,
		// use the hint to size the batch. Note that if it exceeds
		// coldata.BatchSize, ResetMaybeReallocate will chop it down.
		minCapacity = rf.machine.limitHint
	} else {
		// Otherwise, use the estimate. Note that if the estimate is not
		// present, it'll be 0 and ResetMaybeReallocate will allocate the
		// initial batch of capacity 1 which is the esired behavior.
		//
		// We need to transform our rf.estimatedRowCount, which is a uint64,
		// into an int. We have to be careful: if we just cast it directly, a
		// giant estimate will wrap around and become negative.
		if rf.estimatedRowCount > uint64(coldata.BatchSize()) {
			minCapacity = coldata.BatchSize()
		} else {
			minCapacity = int(rf.estimatedRowCount)
		}
	}
	rf.machine.batch, reallocated = rf.allocator.ResetMaybeReallocate(
		rf.typs, rf.machine.batch, minCapacity, rf.memoryLimit,
	)
	if reallocated {
		rf.machine.colvecs = rf.machine.batch.ColVecs()
		// Pull out any requested system column output vecs.
		if timestampOutputIdx != noOutputColumn {
			rf.machine.timestampCol = rf.machine.colvecs[timestampOutputIdx].Decimal()
		}
		if tableOidOutputIdx != noOutputColumn {
			rf.machine.tableoidCol = rf.machine.colvecs[tableOidOutputIdx].Datum()
		}
	}
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *cFetcher) Init(
	codec keys.SQLCodec,
	allocator *colmem.Allocator,
	memoryLimit int64,
	reverse bool,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	tables ...row.FetcherTableArgs,
) error {
	rf.allocator = allocator
	rf.memoryLimit = memoryLimit
	if len(tables) == 0 {
		return errors.AssertionFailedf("no tables to fetch from")
	}

	rf.reverse = reverse
	rf.lockStrength = lockStrength
	rf.lockWaitPolicy = lockWaitPolicy

	if len(tables) > 1 {
		return errors.New("multiple tables not supported in cfetcher")
	}

	tableArgs := tables[0]
	table := newCTableInfo()
	nCols := tableArgs.ColIdxMap.Len()
	if cap(table.colIdxMap.vals) < nCols {
		table.colIdxMap.vals = make(descpb.ColumnIDs, 0, nCols)
		table.colIdxMap.ords = make([]int, 0, nCols)
	}
	colDescriptors := tableArgs.Cols
	for i := range colDescriptors {
		//gcassert:bce
		id := colDescriptors[i].GetID()
		table.colIdxMap.vals = append(table.colIdxMap.vals, id)
		table.colIdxMap.ords = append(table.colIdxMap.ords, tableArgs.ColIdxMap.GetDefault(id))
	}
	sort.Sort(table.colIdxMap)
	*table = cTableInfo{
		spans:                  tableArgs.Spans,
		desc:                   tableArgs.Desc,
		colIdxMap:              table.colIdxMap,
		index:                  tableArgs.Index,
		isSecondaryIndex:       tableArgs.IsSecondaryIndex,
		cols:                   colDescriptors,
		neededColsList:         table.neededColsList[:0],
		indexColOrdinals:       table.indexColOrdinals[:0],
		allIndexColOrdinals:    table.allIndexColOrdinals[:0],
		extraValColOrdinals:    table.extraValColOrdinals[:0],
		allExtraValColOrdinals: table.allExtraValColOrdinals[:0],
		timestampOutputIdx:     noOutputColumn,
		oidOutputIdx:           noOutputColumn,
	}

	if cap(rf.typs) < len(colDescriptors) {
		rf.typs = make([]*types.T, len(colDescriptors))
	} else {
		rf.typs = rf.typs[:len(colDescriptors)]
	}
	typs := rf.typs
	_ = typs[len(colDescriptors)-1]
	for i := range colDescriptors {
		//gcassert:bce
		typs[i] = colDescriptors[i].GetType()
	}

	var err error

	var neededCols util.FastIntSet
	numNeededCols := tableArgs.ValNeededForCol.Len()
	// Scan through the entire columns map to see which columns are required and
	// which columns are not needed (the latter will be set to all null values).
	if cap(table.neededColsList) < numNeededCols {
		table.neededColsList = make([]int, 0, numNeededCols)
	}
	if cap(table.notNeededColOrdinals) < len(colDescriptors)-numNeededCols {
		table.notNeededColOrdinals = make([]int, 0, len(colDescriptors)-numNeededCols)
	}
	for i := range colDescriptors {
		//gcassert:bce
		col := colDescriptors[i].GetID()
		idx := tableArgs.ColIdxMap.GetDefault(col)
		if tableArgs.ValNeededForCol.Contains(idx) {
			// The idx-th column is required.
			neededCols.Add(int(col))
			table.neededColsList = append(table.neededColsList, int(col))
			// Set up extra metadata for system columns, if this is a system column.
			switch colinfo.GetSystemColumnKindFromColumnID(col) {
			case descpb.SystemColumnKind_MVCCTIMESTAMP:
				table.timestampOutputIdx = idx
				rf.mvccDecodeStrategy = row.MVCCDecodingRequired
			case descpb.SystemColumnKind_TABLEOID:
				table.oidOutputIdx = idx
			}
		} else {
			table.notNeededColOrdinals = append(table.notNeededColOrdinals, idx)
		}
	}
	sort.Ints(table.neededColsList)
	sort.Ints(table.notNeededColOrdinals)

	table.knownPrefixLength = len(rowenc.MakeIndexKeyPrefix(codec, table.desc, table.index.GetID()))

	var indexColumnIDs []descpb.ColumnID
	indexColumnIDs, table.indexColumnDirs = catalog.FullIndexColumnIDs(table.index)

	compositeColumnIDs := util.MakeFastIntSet()
	for i := 0; i < table.index.NumCompositeColumns(); i++ {
		id := table.index.GetCompositeColumnID(i)
		compositeColumnIDs.Add(int(id))
	}

	table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()

	// If system columns are requested, they are present in ValNeededForCol.
	// However, we don't want to include them in neededValueColsByIdx, because
	// the handling of system columns is separate from the standard value
	// decoding process.
	if table.timestampOutputIdx != noOutputColumn {
		table.neededValueColsByIdx.Remove(table.timestampOutputIdx)
	}
	if table.oidOutputIdx != noOutputColumn {
		table.neededValueColsByIdx.Remove(table.oidOutputIdx)
	}

	neededIndexCols := 0
	nIndexCols := len(indexColumnIDs)
	if cap(table.indexColOrdinals) >= nIndexCols {
		table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
	} else {
		table.indexColOrdinals = make([]int, nIndexCols)
	}
	if cap(table.allIndexColOrdinals) >= nIndexCols {
		table.allIndexColOrdinals = table.allIndexColOrdinals[:nIndexCols]
	} else {
		table.allIndexColOrdinals = make([]int, nIndexCols)
	}
	indexColOrdinals := table.indexColOrdinals
	_ = indexColOrdinals[len(indexColumnIDs)-1]
	allIndexColOrdinals := table.allIndexColOrdinals
	_ = allIndexColOrdinals[len(indexColumnIDs)-1]
	for i, id := range indexColumnIDs {
		colIdx, ok := tableArgs.ColIdxMap.Get(id)
		//gcassert:bce
		allIndexColOrdinals[i] = colIdx
		if ok && neededCols.Contains(int(id)) {
			//gcassert:bce
			indexColOrdinals[i] = colIdx
			neededIndexCols++
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
			if neededCols.Contains(int(id)) {
				return errors.AssertionFailedf("needed column %d not in colIdxMap", id)
			}
		}
	}
	table.invertedColOrdinal = -1
	if table.index.GetType() == descpb.IndexDescriptor_INVERTED {
		id := table.index.InvertedColumnID()
		colIdx, ok := tableArgs.ColIdxMap.Get(id)
		if ok && neededCols.Contains(int(id)) {
			table.invertedColOrdinal = colIdx
		} else if neededCols.Contains(int(id)) {
			return errors.AssertionFailedf("needed column %d not in colIdxMap", id)
		}
	}
	// Unique secondary indexes contain the extra column IDs as part of
	// the value component. We process these separately, so we need to know
	// what extra columns are composite or not.
	if table.isSecondaryIndex && table.index.IsUnique() {
		for i := 0; i < table.index.NumKeySuffixColumns(); i++ {
			id := table.index.GetKeySuffixColumnID(i)
			colIdx, ok := tableArgs.ColIdxMap.Get(id)
			if ok && neededCols.Contains(int(id)) {
				if compositeColumnIDs.Contains(int(id)) {
					table.compositeIndexColOrdinals.Add(colIdx)
				} else {
					table.neededValueColsByIdx.Remove(colIdx)
				}
			}
		}
	}

	// - If there are interleaves, we need to read the index key in order to
	//   determine whether this row is actually part of the index we're scanning.
	// - If there are needed columns from the index key, we need to read it.
	//
	// Otherwise, we can completely avoid decoding the index key.
	if neededIndexCols > 0 || table.index.NumInterleavedBy() > 0 || table.index.NumInterleaveAncestors() > 0 {
		rf.mustDecodeIndexKey = true
	}

	if table.isSecondaryIndex {
		colIDs := table.index.CollectKeyColumnIDs()
		colIDs.UnionWith(table.index.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(table.index.CollectKeySuffixColumnIDs())
		for i := range colDescriptors {
			//gcassert:bce
			id := colDescriptors[i].GetID()
			if neededCols.Contains(int(id)) && !colIDs.Contains(id) {
				return errors.Errorf("requested column %s not in index", colDescriptors[i].GetName())
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

		if cap(table.allExtraValColOrdinals) >= nExtraColumns {
			table.allExtraValColOrdinals = table.allExtraValColOrdinals[:nExtraColumns]
		} else {
			table.allExtraValColOrdinals = make([]int, nExtraColumns)
		}

		extraValColOrdinals := table.extraValColOrdinals
		_ = extraValColOrdinals[nExtraColumns-1]
		allExtraValColOrdinals := table.allExtraValColOrdinals
		_ = allExtraValColOrdinals[nExtraColumns-1]
		for i := 0; i < nExtraColumns; i++ {
			id := table.index.GetKeySuffixColumnID(i)
			idx := tableArgs.ColIdxMap.GetDefault(id)
			//gcassert:bce
			allExtraValColOrdinals[i] = idx
			if neededCols.Contains(int(id)) {
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
	keysPerRow, err := table.desc.KeysPerRow(table.index.GetID())
	if err != nil {
		return err
	}
	if keysPerRow > rf.maxKeysPerRow {
		rf.maxKeysPerRow = keysPerRow
	}

	_ = table.desc.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		id := family.ID
		if id > table.maxColumnFamilyID {
			table.maxColumnFamilyID = id
		}
		return nil
	})

	rf.table = table
	// Change the allocation size to be the same as the capacity of the batch
	// we allocated above.
	rf.table.da.AllocSize = coldata.BatchSize()

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *cFetcher) StartScan(
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

	// Note that we pass a nil memMonitor here, because the cfetcher does its own
	// memory accounting.
	f, err := row.NewKVFetcher(
		txn,
		spans,
		rf.reverse,
		limitBatches,
		firstBatchLimit,
		rf.lockStrength,
		rf.lockWaitPolicy,
		nil, /* memMonitor */
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

// fetcherState is the state enum for nextBatch.
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
	//   3. interleave detected?
	//      - set skip prefix
	//      -> seekPrefix(decodeFirstKVOfRow)
	//   4. parse value into row buffer.
	//   5. 1-cf or secondary index?
	//     -> doneRow(initFetch)
	//   else:
	//     -> fetchNextKVWithUnfinishedRow
	stateDecodeFirstKVOfRow

	// stateSeekPrefix is the state of skipping all keys that sort before
	// (or after, in the case of a reverse scan) a prefix. s.machine.seekPrefix
	// must be set to the prefix to seek to. state[1] must be set, and seekPrefix
	// will transition to that state once it finds the first key with that prefix.
	//   1. fetch next kv into nextKV buffer
	//   2. kv doesn't match seek prefix?
	//     -> seekPrefix
	//   else:
	//     -> nextState
	stateSeekPrefix

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
	//   6. interleave detected?
	//     - set skip prefix
	//     -> finalizeRow(seekPrefix(decodeFirstKVOfRow))
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

	// stateFinished is the end state of the state machine - it causes nextBatch
	// to return empty batches forever.
	stateFinished
)

// Turn this on to enable super verbose logging of the fetcher state machine.
const debugState = false

// NextBatch is nextBatch with the addition of memory accounting.
func (rf *cFetcher) NextBatch(ctx context.Context) (coldata.Batch, error) {
	rf.adapter.ctx = ctx
	rf.allocator.PerformOperation(
		rf.machine.colvecs,
		rf.nextAdapter,
	)
	return rf.adapter.batch, rf.adapter.err
}

func (rf *cFetcher) nextAdapter() {
	rf.adapter.batch, rf.adapter.err = rf.nextBatch(rf.adapter.ctx)
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

// nextBatch processes keys until we complete one batch of rows,
// coldata.BatchSize() in length, which are returned in columnar format as a
// coldata.Batch. The batch contains one Vec per table column, regardless of
// the index used; columns that are not needed (as per neededCols) are empty.
// The Batch should not be modified and is only valid until the next call.
// When there are no more rows, the Batch.Length is 0.
func (rf *cFetcher) nextBatch(ctx context.Context) (coldata.Batch, error) {
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
			rf.resetBatch(rf.table.timestampOutputIdx, rf.table.oidOutputIdx)
			rf.shiftState()
		case stateDecodeFirstKVOfRow:
			// Reset MVCC metadata for the table, since this is the first KV of a row.
			rf.table.rowLastModified = hlc.Timestamp{}

			// foundNull is set when decoding a new index key for a row finds a NULL value
			// in the index key. This is used when decoding unique secondary indexes in order
			// to tell whether they have extra columns appended to the key.
			var foundNull bool
			if rf.mustDecodeIndexKey || rf.traceKV {
				if debugState {
					log.Infof(ctx, "decoding first key %s", rf.machine.nextKV.Key)
				}
				var (
					key     []byte
					matches bool
					err     error
				)
				indexOrds := rf.table.indexColOrdinals
				if rf.traceKV {
					indexOrds = rf.table.allIndexColOrdinals
				}
				key, matches, foundNull, err = colencoding.DecodeIndexKeyToCols(
					&rf.table.da,
					rf.machine.colvecs,
					rf.machine.rowIdx,
					rf.table.desc,
					rf.table.index,
					indexOrds,
					rf.table.keyValTypes,
					rf.table.indexColumnDirs,
					rf.machine.nextKV.Key[rf.table.knownPrefixLength:],
					rf.table.invertedColOrdinal,
				)
				if err != nil {
					return nil, err
				}
				if !matches {
					// We found an interleave. Set our skip prefix.
					seekPrefix := rf.machine.nextKV.Key[:len(key)+rf.table.knownPrefixLength]
					if debugState {
						log.Infof(ctx, "setting seek prefix to %s", seekPrefix)
					}
					rf.machine.seekPrefix = seekPrefix
					rf.machine.state[0] = stateSeekPrefix
					rf.machine.state[1] = stateDecodeFirstKVOfRow
					continue
				}
				prefix := rf.machine.nextKV.Key[:len(rf.machine.nextKV.Key)-len(key)]
				rf.machine.lastRowPrefix = prefix
			} else {
				// If mustDecodeIndexKey was false, we can't possibly have an
				// interleaved row on our hands, so we can figure out our row prefix
				// without parsing any keys by using GetRowPrefixLength.
				prefixLen, err := keys.GetRowPrefixLength(rf.machine.nextKV.Key)
				if err != nil {
					return nil, err
				}
				rf.machine.lastRowPrefix = rf.machine.nextKV.Key[:prefixLen]
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
			prettyKey, prettyVal, err := rf.processValue(ctx, familyID)
			if err != nil {
				return nil, err
			}
			if rf.traceKV {
				log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
			}
			// Update the MVCC values for this row.
			if rf.table.rowLastModified.Less(rf.machine.nextKV.Value.Timestamp) {
				rf.table.rowLastModified = rf.machine.nextKV.Value.Timestamp
			}
			if rf.table.desc.NumFamilies() == 1 {
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateInitFetch
				continue
			}
			rf.machine.state[0] = stateFetchNextKVWithUnfinishedRow
		case stateSeekPrefix:
			// Note: seekPrefix is only used for interleaved tables.
			for {
				moreKVs, kv, finalReferenceToBatch, err := rf.fetcher.NextKV(ctx, rf.mvccDecodeStrategy)
				if err != nil {
					return nil, rf.convertFetchError(ctx, err)
				}
				if debugState {
					log.Infof(ctx, "found kv %s, seeking to prefix %s", kv.Key, rf.machine.seekPrefix)
				}
				if !moreKVs {
					// We ran out of data, so ignore whatever our next state was going to
					// be and emit the final batch.
					rf.machine.state[1] = stateEmitLastBatch
					break
				}
				// The order we perform the comparison in depends on whether we are
				// performing a reverse scan or not. If we are performing a reverse
				// scan, then we want to seek until we find a key less than seekPrefix.
				var comparison int
				if rf.reverse {
					comparison = bytes.Compare(rf.machine.seekPrefix, kv.Key)
				} else {
					comparison = bytes.Compare(kv.Key, rf.machine.seekPrefix)
				}
				// TODO(jordan): if nextKV returns newSpan = true, set the new span
				//  prefix and indicate that it needs decoding.
				if comparison >= 0 {
					rf.setNextKV(kv, finalReferenceToBatch)
					break
				}
			}
			rf.shiftState()

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

			// TODO(jordan): optimize this prefix check by skipping span prefix.
			if !bytes.HasPrefix(kv.Key, rf.machine.lastRowPrefix) {
				// The kv we just found is from a different row.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateDecodeFirstKVOfRow
				continue
			}

			key := kv.Key[len(rf.machine.lastRowPrefix):]
			_, foundInterleave := encoding.DecodeIfInterleavedSentinel(key)

			if foundInterleave {
				// The key we just found isn't relevant to the current row, so finalize
				// the current row, then skip all KVs with the current interleave prefix.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateSeekPrefix
				rf.machine.state[2] = stateDecodeFirstKVOfRow
				continue
			}

			familyID, err := rf.getCurrentColumnFamilyID()
			if err != nil {
				return nil, err
			}

			// Process the current KV's value component.
			prettyKey, prettyVal, err := rf.processValue(ctx, familyID)
			if err != nil {
				return nil, err
			}
			if rf.traceKV {
				log.VEventf(ctx, 2, "fetched: %s -> %s", prettyKey, prettyVal)
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
			// Populate any system columns in the output.
			if rf.table.timestampOutputIdx != noOutputColumn {
				rf.machine.timestampCol[rf.machine.rowIdx] = tree.TimestampToDecimal(rf.table.rowLastModified)
			}
			if rf.table.oidOutputIdx != noOutputColumn {
				rf.machine.tableoidCol.Set(rf.machine.rowIdx, tree.NewDOid(tree.DInt(rf.table.desc.GetID())))
			}

			// We're finished with a row. Bump the row index, fill the row in with
			// nulls if necessary, emit the batch if necessary, and move to the next
			// state.
			if err := rf.fillNulls(); err != nil {
				return nil, err
			}
			rf.machine.rowIdx++
			rf.shiftState()

			var emitBatch bool
			if rf.machine.rowIdx >= rf.machine.batch.Capacity() ||
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
	colconv.ColVecToDatumAndDeselect(res, rf.machine.colvecs[colIdx], 1 /* length */, []int{rowIdx}, &rf.table.da)
	return res[0]
}

// processValue processes the state machine's current value component, setting
// columns in the rowIdx'th tuple in the current batch depending on what data
// is found in the current value component.
// If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *cFetcher) processValue(
	ctx context.Context, familyID descpb.FamilyID,
) (prettyKey string, prettyValue string, err error) {
	table := rf.table

	if rf.traceKV {
		var buf strings.Builder
		buf.WriteByte('/')
		buf.WriteString(rf.table.desc.GetName())
		buf.WriteByte('/')
		buf.WriteString(rf.table.index.GetName())
		for _, idx := range rf.table.allIndexColOrdinals {
			buf.WriteByte('/')
			if idx != -1 {
				buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx).String())
			} else {
				buf.WriteByte('?')
			}
		}
		prettyKey = buf.String()
	}

	if len(table.neededColsList) == 0 {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = tree.DNull.String()
		}
		return prettyKey, prettyValue, nil
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
			tupleBytes, err := val.GetTuple()
			if err != nil {
				return "", "", err
			}
			prettyKey, prettyValue, err = rf.processValueTuple(ctx, table, tupleBytes, prettyKey)
			if err != nil {
				return "", "", err
			}
		default:
			var family *descpb.ColumnFamilyDescriptor
			family, err = table.desc.FindFamilyByID(familyID)
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexKeyDecodingError, err)
			}

			prettyKey, prettyValue, err = rf.processValueSingle(ctx, table, family, prettyKey)
			if err != nil {
				return "", "", err
			}
		}
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
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
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}

			if table.isSecondaryIndex && table.index.IsUnique() {
				// This is a unique secondary index; decode the extra
				// column values from the value.
				var err error
				extraColOrds := table.extraValColOrdinals
				if rf.traceKV {
					extraColOrds = table.allExtraValColOrdinals
				}
				valueBytes, _, err = colencoding.DecodeKeyValsToCols(
					&table.da,
					rf.machine.colvecs,
					rf.machine.rowIdx,
					extraColOrds,
					table.extraTypes,
					nil,
					&rf.machine.remainingValueColsByIdx,
					valueBytes,
					rf.table.invertedColOrdinal,
				)
				if err != nil {
					return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				if rf.traceKV {
					var buf strings.Builder
					for j := range table.extraTypes {
						idx := table.allExtraValColOrdinals[j]
						buf.WriteByte('/')
						buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx).String())
					}
					prettyValue = buf.String()
				}
			}
		case roachpb.ValueType_TUPLE:
			valueBytes, err = val.GetTuple()
			if err != nil {
				return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
			}
		}

		if len(valueBytes) > 0 {
			prettyKey, prettyValue, err = rf.processValueBytes(
				ctx, table, valueBytes, prettyKey,
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

	var needDecode bool
	if rf.traceKV {
		needDecode = true
	} else {
		for i := range table.neededColsList {
			if table.neededColsList[i] == int(colID) {
				needDecode = true
				break
			}
		}
	}

	if needDecode {
		if idx, ok := table.colIdxMap.get(colID); ok {
			if rf.traceKV {
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.DeletableColumns()[idx].GetName())
			}
			val := rf.machine.nextKV.Value
			if len(val.RawBytes) == 0 {
				return prettyKey, "", nil
			}
			typ := rf.typs[idx]
			err := colencoding.UnmarshalColumnValueToCol(
				&table.da, rf.machine.colvecs[idx], rf.machine.rowIdx, typ, val,
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
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if row.DebugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", rf.machine.nextKV.Key, colID)
	}
	return "", "", nil
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
		colIDDiff          uint32
		lastColID          descpb.ColumnID
		dataOffset         int
		typ                encoding.Type
		lastColIDIndex     int
		lastNeededColIndex int
	)
	for len(valueBytes) > 0 && rf.machine.remainingValueColsByIdx.Len() > 0 {
		_, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + descpb.ColumnID(colIDDiff)
		lastColID = colID
		var colIsNeeded bool
		for ; lastNeededColIndex < len(table.neededColsList); lastNeededColIndex++ {
			nextNeededColID := table.neededColsList[lastNeededColIndex]
			if nextNeededColID == int(colID) {
				colIsNeeded = true
				break
			} else if nextNeededColID > int(colID) {
				break
			}
		}
		if !colIsNeeded {
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
		idx := -1
		for ; lastColIDIndex < len(table.colIdxMap.vals); lastColIDIndex++ {
			if table.colIdxMap.vals[lastColIDIndex] == colID {
				idx = table.colIdxMap.ords[lastColIDIndex]
				break
			}
		}
		if idx == -1 {
			return "", "", errors.Errorf("missing colid %d", colID)
		}

		if rf.traceKV {
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.DeletableColumns()[idx].GetName())
		}

		vec := rf.machine.colvecs[idx]

		valTyp := rf.typs[idx]
		valueBytes, err = colencoding.DecodeTableValueToCol(
			&table.da, vec, rf.machine.rowIdx, typ, dataOffset, valTyp, valueBytes,
		)
		if err != nil {
			return "", "", err
		}
		rf.machine.remainingValueColsByIdx.Remove(idx)
		if rf.traceKV {
			dVal := rf.getDatumAt(idx, rf.machine.rowIdx)
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

// processValueTuple processes the given values (of columns family.ColumnIDs),
// setting values in the rf.row accordingly. The key is only used for logging.
func (rf *cFetcher) processValueTuple(
	ctx context.Context, table *cTableInfo, tupleBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	return rf.processValueBytes(ctx, table, tupleBytes, prettyKeyPrefix)
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
			var indexColValues []string
			for _, idx := range table.indexColOrdinals {
				if idx != -1 {
					indexColValues = append(indexColValues, rf.getDatumAt(idx, rf.machine.rowIdx).String())
				} else {
					indexColValues = append(indexColValues, "?")
				}
				return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
					"non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.desc.GetName(), table.cols[i].GetName(), table.index.GetName(),
					strings.Join(table.index.IndexDesc().KeyColumnNames, ","), strings.Join(indexColValues, ",")))
			}
		}
		rf.machine.colvecs[i].Nulls().SetNull(rf.machine.rowIdx)
	}
	return nil
}

func (rf *cFetcher) finalizeBatch() {
	// We need to set all values in "not needed" vectors to nulls because if the
	// batch is materialized (i.e. values are converted to datums), the
	// conversion of unset values might encounter an error.
	for _, notNeededIdx := range rf.table.notNeededColOrdinals {
		rf.machine.colvecs[notNeededIdx].Nulls().SetNulls()
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
	_, ok, _, err := rowenc.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
		rf.table.desc,
		rf.table.index,
		rf.table.keyValTypes,
		tableKeyVals,
		rf.table.indexColumnDirs,
		key[rf.table.knownPrefixLength:],
	)
	if !ok || err != nil {
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
	rf.table.Release()
	*rf = cFetcher{
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: rf.typs[:0],
	}
	cFetcherPool.Put(rf)
}
