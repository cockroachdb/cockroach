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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
	"github.com/cockroachdb/errors"
)

type cTableInfo struct {
	// -- Fields initialized once --

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
	//
	// Note that if the tracing is enabled on the cFetcher (traceKV == true),
	// then values for all columns are needed and, thus, there will be no -1 in
	// indexColOrdinals.
	indexColOrdinals []int

	// The set of column ordinals which are both composite and part of the index
	// key.
	compositeIndexColOrdinals util.FastIntSet

	// One number per column coming from the "key suffix" that is part of the
	// value; each number is a column index (into cols); -1 if we don't need the
	// value for that column.
	//
	// The "key suffix" columns are only used for secondary indexes:
	// - for non-unique indexes, these columns are appended to the key (and will
	// be included in indexColOrdinals instead);
	// - for unique indexes, these columns are stored in the value (unless the
	// key contains a NULL value: then the extra columns are appended to the key
	// to unique-ify it).
	//
	// Note that if the tracing is enabled on the cFetcher (traceKV == true),
	// then values for all columns are needed and, thus, there will be no -1 in
	// extraValColOrdinals.
	extraValColOrdinals []int

	// invertedColOrdinal is a column index (into cols), indicating the inverted
	// column; -1 if there is no inverted column or we don't need the value for
	// that column.
	invertedColOrdinal int

	// checkAllColsForNull indicates whether all columns need to be checked
	// for nullity (even if those columns don't need to be decoded).
	checkAllColsForNull bool

	// maxColumnFamilyID is the maximum possible family id for the configured
	// table.
	maxColumnFamilyID descpb.FamilyID

	// keyPreambleLength is the number of bytes in the index key prefix
	// corresponding to the table id and the index id.
	keyPreambleLength int
	// knownPrefixLength is the length of LLCP (logical longest common prefix)
	// of the key span that we're currently fetching from. This will always be
	// at least keyPreambleLength but can be more when some index key columns
	// have fixed values for the whole key span.
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
	// extraValDirections contains len(extraTypes) ASC directions. This will
	// only be used for unique secondary indexes.
	extraValDirections []descpb.IndexDescriptor_Direction

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
		colIdxMap:            c.colIdxMap,
		keyValTypes:          c.keyValTypes[:0],
		extraTypes:           c.extraTypes[:0],
		extraValDirections:   c.extraValDirections[:0],
		neededColsList:       c.neededColsList[:0],
		notNeededColOrdinals: c.notNeededColOrdinals[:0],
		indexColOrdinals:     c.indexColOrdinals[:0],
		extraValColOrdinals:  c.extraValColOrdinals[:0],
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
//      res, err := rf.NextBatch()
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

	// True if the index key must be decoded. This is only false if there are no
	// needed columns and the tracing is not enabled.
	mustDecodeIndexKey bool

	// lockStrength represents the row-level locking mode to use when fetching rows.
	lockStrength descpb.ScanLockingStrength

	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy descpb.ScanLockingWaitPolicy

	// lockTimeout specifies the maximum amount of time that the fetcher will
	// wait while attempting to acquire a lock on a key or while blocking on an
	// existing lock in order to perform a non-locking read on a key.
	lockTimeout time.Duration

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when initializing the fetcher.
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
		// of a new row or not. It should never include the column family ID.
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

	// llcpState tracks the state about LLCP of the current span.
	//
	// LLCP (the logical longest common prefix) of the span is the longest
	// common prefix that contains only full key components. For example, the
	// keys /Table/53/1/foo/bar/10 and /Table/53/1/foo/bop/10 would have LLCP of
	// /Table/53/1/foo, even though they share a b prefix of the next key, since
	// that prefix isn't a complete key component.
	llcpState struct {
		// outputBatchStartIdx is the index of the first row in the batch that
		// corresponds to the key span currently being tracked by llcpState.
		outputBatchStartIdx int
		// commonValues are vectors of length 1 containing the decoded index key
		// columns from LLCP.
		commonValues []coldata.Vec
		// needCopyingFromCommon if true indicates that we need to copy
		// commonValues into the batch.
		needCopyingFromCommon bool
		// foundNull indicates whether there is a NULL value in LLCP of the key
		// span.
		foundNull bool
		// numConstIndexKeyCols is the length of LLCP in the number of table
		// columns.
		numConstIndexKeyCols int
	}

	typs             []*types.T
	accountingHelper colmem.SetAccountingHelper
	memoryLimit      int64

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
		rf.typs, rf.machine.batch, minDesiredCapacity, rf.memoryLimit,
	)
	if reallocated {
		rf.machine.colvecs = rf.machine.batch.ColVecs()
		// Pull out any requested system column output vecs.
		if rf.table.timestampOutputIdx != noOutputColumn {
			rf.machine.timestampCol = rf.machine.colvecs[rf.table.timestampOutputIdx].Decimal()
		}
		if rf.table.oidOutputIdx != noOutputColumn {
			rf.machine.tableoidCol = rf.machine.colvecs[rf.table.oidOutputIdx].Datum()
		}
		// Change the allocation size to be the same as the capacity of the
		// batch we allocated above.
		rf.table.da.AllocSize = rf.machine.batch.Capacity()
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
	lockTimeout time.Duration,
	tableArgs row.FetcherTableArgs,
	traceKV bool,
) error {
	rf.memoryLimit = memoryLimit
	rf.reverse = reverse
	rf.lockStrength = lockStrength
	rf.lockWaitPolicy = lockWaitPolicy
	rf.lockTimeout = lockTimeout
	rf.traceKV = traceKV

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
		desc:                tableArgs.Desc,
		colIdxMap:           table.colIdxMap,
		index:               tableArgs.Index,
		isSecondaryIndex:    tableArgs.IsSecondaryIndex,
		cols:                colDescriptors,
		neededColsList:      table.neededColsList[:0],
		indexColOrdinals:    table.indexColOrdinals[:0],
		extraValColOrdinals: table.extraValColOrdinals[:0],
		timestampOutputIdx:  noOutputColumn,
		oidOutputIdx:        noOutputColumn,
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

	table.keyPreambleLength = len(rowenc.MakeIndexKeyPrefix(codec, table.desc, table.index.GetID()))
	table.knownPrefixLength = table.keyPreambleLength

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

	nIndexCols := len(indexColumnIDs)
	if cap(table.indexColOrdinals) >= nIndexCols {
		table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
	} else {
		table.indexColOrdinals = make([]int, nIndexCols)
	}
	indexColOrdinals := table.indexColOrdinals
	_ = indexColOrdinals[len(indexColumnIDs)-1]
	for i, id := range indexColumnIDs {
		colIdx, ok := tableArgs.ColIdxMap.Get(id)
		if (ok && neededCols.Contains(int(id))) || rf.traceKV {
			//gcassert:bce
			indexColOrdinals[i] = colIdx
			rf.mustDecodeIndexKey = true
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
		if (ok && neededCols.Contains(int(id))) || rf.traceKV {
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
			if (ok && neededCols.Contains(int(id))) || rf.traceKV {
				if compositeColumnIDs.Contains(int(id)) {
					table.compositeIndexColOrdinals.Add(colIdx)
				} else {
					table.neededValueColsByIdx.Remove(colIdx)
				}
			}
		}
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
			idx := tableArgs.ColIdxMap.GetDefault(id)
			if neededCols.Contains(int(id)) || rf.traceKV {
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
	// For unique secondary indexes on tables with multiple column families, we
	// must check all columns for NULL values in order to determine whether a KV
	// belongs to the same row as the previous KV or a different row.
	rf.table.checkAllColsForNull = rf.table.isSecondaryIndex && rf.table.index.IsUnique() && rf.table.desc.NumFamilies() != 1
	rf.accountingHelper.Init(allocator, rf.typs, rf.table.notNeededColOrdinals)

	return nil
}

// StartScan initializes and starts the key-value scan. Can be used multiple
// times.
func (rf *cFetcher) StartScan(
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
		// The limitHint is a row limit, but each row could be made up
		// of more than one key. We take the maximum possible keys
		// per row out of all the table rows we could potentially
		// scan over.
		firstBatchLimit = rowinfra.KeyLimit(int(limitHint) * rf.maxKeysPerRow)
		// We need an extra key to make sure we form the last row.
		firstBatchLimit++
	}

	// Note that we pass a nil memMonitor here, because the cfetcher does its own
	// memory accounting.
	f, err := row.NewKVFetcher(
		txn,
		spans,
		bsHeader,
		rf.reverse,
		batchBytesLimit,
		firstBatchLimit,
		rf.lockStrength,
		rf.lockWaitPolicy,
		rf.lockTimeout,
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

// handleNewSpan checks whether the current KV came from a new key span and
// updates the LLCP state if so. It also fills in the constant values from LLCP
// of the previous key span if a new key span is encountered.
// - newRowIdx must be the index of the first row that belongs to the new span.
func (rf *cFetcher) handleNewSpan(ctx context.Context, newSpan roachpb.Span, newRowIdx int) error {
	if len(newSpan.Key) == 0 {
		// We're still processing the old key span, so there is nothing to do.
		return nil
	}
	if len(newSpan.EndKey) == 0 {
		// We have a point lookup, so there is no point in calculating LLCP.
		rf.resetLLCP()
		return nil
	}

	// We have a new span. Populate constant index key column values from the
	// previous span if applicable.
	if rf.llcpState.needCopyingFromCommon && rf.mustDecodeIndexKey {
		// Copy over the decoded values from LLCP of the previous key span.
		for _, colIdx := range rf.table.indexColOrdinals[:rf.llcpState.numConstIndexKeyCols] {
			if colIdx != -1 {
				coldata.ExpandValue(
					rf.machine.colvecs[colIdx],
					rf.llcpState.commonValues[colIdx],
					rf.llcpState.outputBatchStartIdx,
					newRowIdx,
					0, /* srcIdx */
				)
			}
		}
		rf.llcpState.needCopyingFromCommon = false
	}

	// Skip the preamble right away.
	key := newSpan.Key[rf.table.keyPreambleLength:]
	endKey := newSpan.EndKey[rf.table.keyPreambleLength:]

	// Find the length of LLCP.
	commonPrefixLength := 0
	rf.llcpState.numConstIndexKeyCols = 0
	for len(key) > 0 && len(endKey) > 0 && rf.llcpState.numConstIndexKeyCols < len(rf.table.indexColOrdinals) {
		startElementLength, err := encoding.PeekLength(key)
		if err != nil {
			break
		}
		endElementLength, err := encoding.PeekLength(endKey)
		if err != nil {
			break
		}
		if startElementLength != endElementLength {
			break
		}
		if !bytes.Equal(key[:startElementLength], endKey[:startElementLength]) {
			break
		}
		colIdx := rf.table.indexColOrdinals[rf.llcpState.numConstIndexKeyCols]
		if colIdx != -1 {
			if colinfo.CanHaveCompositeKeyEncoding(rf.typs[colIdx]) {
				// If we need to decode this index key column and it has a composite
				// encoding, we stop LLCP before this column.
				// TODO(yuzefovich): lift this restriction.
				break
			}
			if rf.table.invertedColOrdinal == colIdx {
				// Let's not deal with the inverted columns.
				break
			}
		}
		commonPrefixLength += startElementLength
		rf.llcpState.numConstIndexKeyCols++
		key = key[startElementLength:]
		endKey = endKey[startElementLength:]
	}

	if commonPrefixLength == 0 || rf.llcpState.numConstIndexKeyCols == len(rf.table.indexColOrdinals) {
		// We either have no constant values for the index key columns or a
		// point scan.
		rf.resetLLCP()
		return nil
	}

	rf.table.knownPrefixLength = rf.table.keyPreambleLength + commonPrefixLength
	if rf.mustDecodeIndexKey {
		if cap(rf.llcpState.commonValues) < len(rf.typs) {
			rf.llcpState.commonValues = make([]coldata.Vec, len(rf.typs))
		} else {
			rf.llcpState.commonValues = rf.llcpState.commonValues[:len(rf.typs)]
		}

		// Prepare commonValues vectors so that we can decode the constant
		// values into them.
		for _, colIdx := range rf.table.indexColOrdinals[:rf.llcpState.numConstIndexKeyCols] {
			if colIdx != -1 {
				if rf.llcpState.commonValues[colIdx] == nil {
					rf.llcpState.commonValues[colIdx] = rf.accountingHelper.Allocator.NewMemColumn(rf.typs[colIdx], 1 /* capacity */)
				} else {
					rf.llcpState.commonValues[colIdx].Nulls().UnsetNulls()
					if rf.llcpState.commonValues[colIdx].IsBytesLike() {
						coldata.Reset(rf.llcpState.commonValues[colIdx])
					}
				}
			}
		}

		if debugState {
			log.Infof(
				ctx, "decoding constant values of LLCP (num constant columns = %d), key %s",
				rf.llcpState.numConstIndexKeyCols, rf.machine.nextKV.Key,
			)
		}

		var err error
		_, rf.llcpState.foundNull, err = colencoding.DecodeKeyValsToCols(
			&rf.table.da,
			rf.llcpState.commonValues,
			0, /* idx */
			rf.table.indexColOrdinals[:rf.llcpState.numConstIndexKeyCols],
			rf.table.checkAllColsForNull,
			rf.table.keyValTypes[:rf.llcpState.numConstIndexKeyCols],
			rf.table.indexColumnDirs[:rf.llcpState.numConstIndexKeyCols],
			nil, /* unseen */
			newSpan.Key[rf.table.keyPreambleLength:rf.table.knownPrefixLength],
			rf.table.invertedColOrdinal,
		)
		if err != nil {
			return scrub.WrapError(scrub.IndexKeyDecodingError, err)
		}
		rf.llcpState.outputBatchStartIdx = newRowIdx
		// If the tracing is enabled, then we'll be copying the common values on
		// a per-row basis in order to print out the correct logs.
		rf.llcpState.needCopyingFromCommon = !rf.traceKV
	}
	return nil
}

// resetLLCP resets the cFetcher when LLCP only contains the table id and the
// index id.
func (rf *cFetcher) resetLLCP() {
	rf.table.knownPrefixLength = rf.table.keyPreambleLength
	rf.llcpState.needCopyingFromCommon = false
	rf.llcpState.foundNull = false
	rf.llcpState.numConstIndexKeyCols = 0
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
			moreKVs, kv, finalReferenceToBatch, newSpan, err := rf.fetcher.NextKV(ctx, rf.mvccDecodeStrategy)
			if err != nil {
				return nil, rf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				rf.machine.state[0] = stateEmitLastBatch
				continue
			}
			rf.setNextKV(kv, finalReferenceToBatch)
			if err = rf.handleNewSpan(ctx, newSpan, rf.machine.rowIdx); err != nil {
				return nil, err
			}
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
				key, foundNull, err = colencoding.DecodeKeyValsToCols(
					&rf.table.da,
					rf.machine.colvecs,
					rf.machine.rowIdx,
					rf.table.indexColOrdinals[rf.llcpState.numConstIndexKeyCols:],
					rf.table.checkAllColsForNull,
					rf.table.keyValTypes[rf.llcpState.numConstIndexKeyCols:],
					rf.table.indexColumnDirs[rf.llcpState.numConstIndexKeyCols:],
					nil, /* unseen */
					rf.machine.nextKV.Key[rf.table.knownPrefixLength:],
					rf.table.invertedColOrdinal,
				)
				if err != nil {
					return nil, err
				}
				foundNull = foundNull || rf.llcpState.foundNull
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
			if foundNull && rf.table.checkAllColsForNull {
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
			moreKVs, kv, finalReferenceToBatch, newSpan, err := rf.fetcher.NextKV(ctx, rf.mvccDecodeStrategy)
			if err != nil {
				return nil, rf.convertFetchError(ctx, err)
			}
			if !moreKVs {
				// No more data. Finalize the row and exit.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateEmitLastBatch
				continue
			}
			rf.setNextKV(kv, finalReferenceToBatch)
			var foundNewRow bool
			if len(kv.Key) < rf.table.knownPrefixLength {
				foundNewRow = true
			} else {
				foundNewRow = !bytes.HasPrefix(kv.Key[rf.table.knownPrefixLength:], rf.machine.lastRowPrefix[rf.table.knownPrefixLength:])
			}
			newRowIdx := rf.machine.rowIdx
			if foundNewRow {
				// If the KV we've just fetched belongs to a new key span, then
				// we need to increment newRowIdx. Normally this happens in
				// stateFinalizeRow, but we have to handle the new span now and
				// let handleNewSpan() copy the values into the batch, including
				// the current rf.machine.rowIdx.
				newRowIdx++
			}
			if err = rf.handleNewSpan(ctx, newSpan, newRowIdx); err != nil {
				return nil, err
			}

			if foundNewRow {
				// The kv we just found is from a different row.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateDecodeFirstKVOfRow
				continue
			}

			if debugState {
				log.Infof(ctx, "decoding next key %s", rf.machine.nextKV.Key)
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
			// Populate any system columns in the output.
			if rf.table.timestampOutputIdx != noOutputColumn {
				rf.machine.timestampCol[rf.machine.rowIdx] = tree.TimestampToDecimal(rf.table.rowLastModified)
			}
			if rf.table.oidOutputIdx != noOutputColumn {
				rf.machine.tableoidCol.Set(rf.machine.rowIdx, tree.NewDOid(tree.DInt(rf.table.desc.GetID())))
			}

			// We're finished with a row. Fill the row in with nulls if
			// necessary, perform the memory accounting for the row, bump the
			// row index, emit the batch if necessary, and move to the next
			// state.
			if err := rf.fillNulls(); err != nil {
				return nil, err
			}
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
		// Note that because rf.traceKV is true, rf.table.indexColOrdinals will
		// not include any -1, so idx values will all be valid.
		for i, idx := range rf.table.indexColOrdinals {
			if i < rf.llcpState.numConstIndexKeyCols {
				// We have to pro-actively copy the common value for this
				// column.
				coldata.ExpandValue(
					rf.machine.colvecs[idx],
					rf.llcpState.commonValues[idx],
					rf.machine.rowIdx,
					rf.machine.rowIdx+1,
					0, /* srcIdx */
				)
			}
			buf.WriteByte('/')
			buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx).String())
		}
		prettyKey = buf.String()
	}

	if len(table.neededColsList) == 0 {
		// We don't need to decode any values.
		if rf.traceKV {
			prettyValue = tree.DNull.String()
		}
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
				valueBytes, _, err = colencoding.DecodeKeyValsToCols(
					&table.da,
					rf.machine.colvecs,
					rf.machine.rowIdx,
					table.extraValColOrdinals,
					false, /* checkAllColsForNull */
					table.extraTypes,
					table.extraValDirections,
					&rf.machine.remainingValueColsByIdx,
					valueBytes,
					rf.table.invertedColOrdinal,
				)
				if err != nil {
					return scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
				}
				if rf.traceKV {
					var buf strings.Builder
					for _, idx := range table.extraValColOrdinals {
						buf.WriteByte('/')
						buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx).String())
					}
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
			}
			return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
				"non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
				table.desc.GetName(), table.cols[i].GetName(), table.index.GetName(),
				strings.Join(table.index.IndexDesc().KeyColumnNames, ","), strings.Join(indexColValues, ",")))
		}
		rf.machine.colvecs[i].Nulls().SetNull(rf.machine.rowIdx)
	}
	return nil
}

func (rf *cFetcher) finalizeBatch() {
	if rf.llcpState.needCopyingFromCommon && rf.mustDecodeIndexKey {
		for _, colIdx := range rf.table.indexColOrdinals[:rf.llcpState.numConstIndexKeyCols] {
			if colIdx != -1 {
				coldata.ExpandValue(
					rf.machine.colvecs[colIdx],
					rf.llcpState.commonValues[colIdx],
					rf.llcpState.outputBatchStartIdx,
					rf.machine.rowIdx,
					0, /* srcIdx */
				)
			}
		}
	}

	// We need to set all values in "not needed" vectors to nulls because if the
	// batch is materialized (i.e. values are converted to datums), the
	// conversion of unset values might encounter an error.
	for _, notNeededIdx := range rf.table.notNeededColOrdinals {
		rf.machine.colvecs[notNeededIdx].Nulls().SetNulls()
	}
	rf.machine.batch.SetLength(rf.machine.rowIdx)
	rf.machine.rowIdx = 0
	rf.llcpState.outputBatchStartIdx = 0
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
	if len(key) < rf.table.keyPreambleLength {
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
		key[rf.table.keyPreambleLength:],
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
	rf.accountingHelper.Release()
	rf.table.Release()
	oldCommonValues := rf.llcpState.commonValues
	for i := range oldCommonValues {
		oldCommonValues[i] = nil
	}
	*rf = cFetcher{
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: rf.typs[:0],
	}
	rf.llcpState.commonValues = oldCommonValues[:0]
	cFetcherPool.Put(rf)
}

type cFetcherArgs struct {
	visibility        execinfrapb.ScanVisibility
	lockingStrength   descpb.ScanLockingStrength
	lockingWaitPolicy descpb.ScanLockingWaitPolicy
	hasSystemColumns  bool
	reverse           bool
	memoryLimit       int64
	estimatedRowCount uint64
}

// initCFetcher extracts common logic for operators in the colfetcher package
// that need to use cFetcher operators.
func initCFetcher(
	flowCtx *execinfra.FlowCtx,
	allocator *colmem.Allocator,
	desc catalog.TableDescriptor,
	index catalog.Index,
	neededCols util.FastIntSet,
	colIdxMap catalog.TableColMap,
	virtualCol catalog.Column,
	args cFetcherArgs,
) (*cFetcher, error) {
	fetcher := cFetcherPool.Get().(*cFetcher)
	fetcher.setEstimatedRowCount(args.estimatedRowCount)

	tableArgs := row.FetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: !index.Primary(),
		ValNeededForCol:  neededCols,
	}

	tableArgs.InitCols(desc, args.visibility, args.hasSystemColumns, virtualCol)

	if err := fetcher.Init(
		flowCtx.Codec(), allocator, args.memoryLimit, args.reverse, args.lockingStrength,
		args.lockingWaitPolicy, flowCtx.EvalCtx.SessionData().LockTimeout, tableArgs, flowCtx.TraceKV,
	); err != nil {
		return nil, err
	}

	return fetcher, nil
}
