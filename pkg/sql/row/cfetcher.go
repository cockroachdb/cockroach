// Copyright 2018 The Cockroach Authors.
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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colencoding"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	desc             *sqlbase.ImmutableTableDescriptor
	index            *sqlbase.IndexDescriptor
	isSecondaryIndex bool
	indexColumnDirs  []sqlbase.IndexDescriptor_Direction

	// The table columns to use for fetching, possibly including ones currently in
	// schema changes.
	cols []sqlbase.ColumnDescriptor

	// The exec types corresponding to the table columns in cols.
	typs []types.T

	// The ordered list of ColumnIDs that are required.
	neededColsList []int

	// The set of required value-component column ordinals in the table.
	neededValueColsByIdx util.FastIntSet

	// Map used to get the index for columns in cols.
	colIdxMap colIdxMap

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	indexColOrdinals []int

	// The set of column ordinals which are both composite and part of the index
	// key.
	compositeIndexColOrdinals util.FastIntSet

	// One value per column that is part of the key; each value is a column
	// index (into cols); -1 if we don't need the value for that column.
	extraValColOrdinals []int
	// allExtraValColOrdinals is the same as extraValColOrdinals but
	// does not contain any -1's. It is meant to be used only in logging.
	allExtraValColOrdinals []int

	// maxColumnFamilyID is the maximum possible family id for the configured
	// table.
	maxColumnFamilyID sqlbase.FamilyID

	// knownPrefixLength is the number of bytes in the index key prefix this
	// Fetcher is configured for. The index key prefix is the table id, index
	// id pair at the start of the key.
	knownPrefixLength int

	keyValTypes []semtypes.T
	extraTypes  []semtypes.T

	da sqlbase.DatumAlloc
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
	// vals is the sorted list of sqlbase.ColumnIDs in the table to fetch.
	vals sqlbase.ColumnIDs
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

func (m colIdxMap) get(c sqlbase.ColumnID) (int, bool) {
	for i, v := range m.vals {
		if v == c {
			return m.ords[i], true
		}
	}
	return 0, false
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
	// table is the table that's configured for fetching.
	table cTableInfo

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

	// returnRangeInfo, if set, causes the underlying kvBatchFetcher to return
	// information about the ranges descriptors/leases uses in servicing the
	// requests. This has some cost, so it's only enabled by DistSQL when this
	// info is actually useful for correcting the plan (e.g. not for the PK-side
	// of an index-join).
	// If set, GetRangesInfo() can be used to retrieve the accumulated info.
	returnRangeInfo bool

	// traceKV indicates whether or not session tracing is enabled. It is set
	// when beginning a new scan.
	traceKV bool

	// fetcher is the underlying fetcher that provides KVs.
	fetcher kvFetcher

	// machine contains fields that get updated during the run of the fetcher.
	machine struct {
		// state is the queue of next states of the state machine. The 0th entry
		// is the next state.
		state [3]fetcherState
		// rowIdx is always set to the ordinal of the row we're currently writing to
		// within the current batch. It's incremented as soon as we detect that a row
		// is finished.
		rowIdx uint16
		// curSpan is the current span that the kv fetcher just returned data from.
		curSpan roachpb.Span
		// nextKV is the kv to process next.
		nextKV roachpb.KeyValue
		// seekPrefix is the prefix to seek to in stateSeekPrefix.
		seekPrefix roachpb.Key

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
	}
}

// Init sets up a Fetcher for a given table and index. If we are using a
// non-primary index, tables.ValNeededForCol can only refer to columns in the
// index.
func (rf *CFetcher) Init(
	reverse,
	returnRangeInfo bool, isCheck bool, tables ...FetcherTableArgs,
) error {
	if len(tables) == 0 {
		return errors.AssertionFailedf("no tables to fetch from")
	}

	rf.reverse = reverse
	rf.returnRangeInfo = returnRangeInfo

	if len(tables) > 1 {
		return errors.New("multiple tables not supported in cfetcher")
	}

	tableArgs := tables[0]
	oldTable := rf.table

	m := colIdxMap{
		vals: make(sqlbase.ColumnIDs, 0, len(tableArgs.ColIdxMap)),
		ords: make([]int, 0, len(tableArgs.ColIdxMap)),
	}
	for k, v := range tableArgs.ColIdxMap {
		m.vals = append(m.vals, k)
		m.ords = append(m.ords, v)
	}
	sort.Sort(m)
	colDescriptors := tableArgs.Cols
	typs := make([]types.T, len(colDescriptors))
	for i := range typs {
		typs[i] = conv.FromColumnType(&colDescriptors[i].Type)
		if typs[i] == types.Unhandled {
			return errors.Errorf("unhandled type %+v", &colDescriptors[i].Type)
		}
	}
	table := cTableInfo{
		spans:            tableArgs.Spans,
		desc:             tableArgs.Desc,
		colIdxMap:        m,
		index:            tableArgs.Index,
		isSecondaryIndex: tableArgs.IsSecondaryIndex,
		cols:             colDescriptors,
		typs:             typs,

		// These slice fields might get re-allocated below, so reslice them from
		// the old table here in case they've got enough capacity already.
		indexColOrdinals:       oldTable.indexColOrdinals[:0],
		extraValColOrdinals:    oldTable.extraValColOrdinals[:0],
		allExtraValColOrdinals: oldTable.allExtraValColOrdinals[:0],
	}
	rf.machine.batch = coldata.NewMemBatch(typs)
	rf.machine.colvecs = rf.machine.batch.ColVecs()

	var err error

	var neededCols util.FastIntSet
	// Scan through the entire columns map to see which columns are
	// required.
	table.neededColsList = make([]int, 0, tableArgs.ValNeededForCol.Len())
	for col, idx := range tableArgs.ColIdxMap {
		if tableArgs.ValNeededForCol.Contains(idx) {
			// The idx-th column is required.
			neededCols.Add(int(col))
			table.neededColsList = append(table.neededColsList, int(col))
		}
	}
	sort.Ints(table.neededColsList)

	table.knownPrefixLength = len(sqlbase.MakeIndexKeyPrefix(table.desc.TableDesc(), table.index.ID))

	var indexColumnIDs []sqlbase.ColumnID
	indexColumnIDs, table.indexColumnDirs = table.index.FullColumnIDs()

	compositeColumnIDs := util.MakeFastIntSet()
	for _, id := range table.index.CompositeColumnIDs {
		compositeColumnIDs.Add(int(id))
	}

	table.neededValueColsByIdx = tableArgs.ValNeededForCol.Copy()
	neededIndexCols := 0
	nIndexCols := len(indexColumnIDs)
	if cap(table.indexColOrdinals) >= nIndexCols {
		table.indexColOrdinals = table.indexColOrdinals[:nIndexCols]
	} else {
		table.indexColOrdinals = make([]int, nIndexCols)
	}
	for i, id := range indexColumnIDs {
		colIdx, ok := tableArgs.ColIdxMap[id]
		if ok {
			table.indexColOrdinals[i] = colIdx
			if neededCols.Contains(int(id)) {
				neededIndexCols++
				// A composite column might also have a value encoding which must be
				// decoded. Others can be removed from neededValueColsByIdx.
				if compositeColumnIDs.Contains(int(id)) {
					table.compositeIndexColOrdinals.Add(colIdx)
				} else {
					table.neededValueColsByIdx.Remove(colIdx)
				}
			}
		} else {
			table.indexColOrdinals[i] = -1
			if neededCols.Contains(int(id)) {
				return errors.AssertionFailedf("needed column %d not in colIdxMap", id)
			}
		}

		// - If there are interleaves, we need to read the index key in order to
		//   determine whether this row is actually part of the index we're scanning.
		// - If there are needed columns from the index key, we need to read it.
		//
		// Otherwise, we can completely avoid decoding the index key.
		if neededIndexCols > 0 || len(table.index.InterleavedBy) > 0 || len(table.index.Interleave.Ancestors) > 0 {
			rf.mustDecodeIndexKey = true
		}

		if table.isSecondaryIndex {
			for i := range table.cols {
				if neededCols.Contains(int(table.cols[i].ID)) && !table.index.ContainsColumnID(table.cols[i].ID) {
					return fmt.Errorf("requested column %s not in index", table.cols[i].Name)
				}
			}
		}

		// Prepare our index key vals slice.
		table.keyValTypes, err = sqlbase.GetColumnTypes(table.desc.TableDesc(), indexColumnIDs)
		if err != nil {
			return err
		}
		if cHasExtraCols(&table) {
			// Unique secondary indexes have a value that is the
			// primary index key.
			// Primary indexes only contain ascendingly-encoded
			// values. If this ever changes, we'll probably have to
			// figure out the directions here too.
			table.extraTypes, err = sqlbase.GetColumnTypes(table.desc.TableDesc(), table.index.ExtraColumnIDs)
			nExtraColumns := len(table.index.ExtraColumnIDs)
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

			for i, id := range table.index.ExtraColumnIDs {
				table.allExtraValColOrdinals[i] = tableArgs.ColIdxMap[id]
				if neededCols.Contains(int(id)) {
					table.extraValColOrdinals[i] = tableArgs.ColIdxMap[id]
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

		for i := range table.desc.Families {
			id := table.desc.Families[i].ID
			if id > table.maxColumnFamilyID {
				table.maxColumnFamilyID = id
			}
		}

		rf.table = table
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

	f, err := makeKVBatchFetcher(txn, spans, rf.reverse, limitBatches, firstBatchLimit, rf.returnRangeInfo)
	if err != nil {
		return err
	}
	rf.machine.lastRowPrefix = nil
	rf.fetcher = newKVFetcher(&f)
	rf.machine.state[0] = stateInitFetch
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
	//   3. interleave detected?
	//      - set skip prefix
	//      -> seekPrefix(decodeFirstKVOfRow)
	//   4. parse value into row buffer.
	//   5. 1-cf or secondary index?
	//     -> doneRow(initFetch)
	//   else:
	//     -> fetchNextKVWithUnfinishedRow
	stateDecodeFirstKVOfRow

	// stateSeekPrefix is the state of skipping all keys that sort before a
	// prefix. s.machine.seekPrefix must be set to the prefix to seek to.
	// state[1] must be set, and seekPrefix will transition to that state once it
	// finds the first key with that prefix.
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

	// stateFinished is the end state of the state machine - it causes NextBatch
	// to return empty batches forever.
	stateFinished
)

// Turn this on to enable super verbose logging of the fetcher state machine.
const debugState = false

// NextBatch processes keys until we complete one batch of rows,
// coldata.BatchSize in length, which are returned in columnar format as an
// exec.Batch. The batch contains one Vec per table column, regardless of the
// index used; columns that are not needed (as per neededCols) are empty. The
// Batch should not be modified and is only valid until the next call.
// When there are no more rows, the Batch.Length is 0.
func (rf *CFetcher) NextBatch(ctx context.Context) (coldata.Batch, error) {
	for {
		if debugState {
			log.Infof(ctx, "State %s", rf.machine.state[0])
		}
		switch rf.machine.state[0] {
		case stateInvalid:
			return nil, errors.New("invalid fetcher state")
		case stateInitFetch:
			moreKeys, kv, newSpan, err := rf.fetcher.nextKV(ctx)
			if err != nil {
				return nil, exec.NewStorageError(err)
			}
			if !moreKeys {
				rf.machine.state[0] = stateEmitLastBatch
				continue
			}
			if newSpan {
				rf.machine.curSpan = rf.fetcher.span
				// TODO(jordan): parse the logical longest common prefix of the span
				// into a buffer. The logical longest common prefix is the longest
				// common prefix that contains only full key components. For example,
				// the keys /Table/53/1/foo/bar/10 and /Table/53/1/foo/bop/10 would
				// have LLCS of /Table/53/1/foo, even though they share a b prefix of
				// the next key, since that prefix isn't a complete key component.
				/*
					lcs := rf.fetcher.span.LongestCommonPrefix()
					// parse lcs into stuff
					key, matches, err := sqlbase.DecodeIndexKeyWithoutTableIDIndexIDPrefix(
						rf.table.desc, rf.table.info.index, rf.table.info.keyValTypes,
						rf.table.keyVals, rf.table.info.indexColumnDirs, kv.Key[rf.table.info.knownPrefixLength:],
					)
					if err != nil {
						// This is expected - the longest common prefix of the keyspan might
						// end half way through a key. Suppress the error and set the actual
						// LCS we'll use later to the decodable components of the key.
					}
				*/
			}

			rf.machine.nextKV = kv
			rf.machine.state[0] = stateDecodeFirstKVOfRow

		case stateResetBatch:
			for i := range rf.machine.colvecs {
				rf.machine.colvecs[i].Nulls().UnsetNulls()
			}
			rf.machine.batch.SetSelection(false)
			rf.shiftState()
		case stateDecodeFirstKVOfRow:
			if rf.mustDecodeIndexKey || rf.traceKV {
				if debugState {
					log.Infof(ctx, "Decoding first key %s", rf.machine.nextKV.Key)
				}
				key, matches, err := colencoding.DecodeIndexKeyToCols(
					rf.machine.colvecs,
					rf.machine.rowIdx,
					rf.table.desc,
					rf.table.index,
					rf.table.indexColOrdinals,
					rf.table.keyValTypes,
					rf.table.indexColumnDirs,
					rf.machine.nextKV.Key[rf.table.knownPrefixLength:],
				)
				if err != nil {
					return nil, err
				}
				if !matches {
					// We found an interleave. Set our skip prefix.
					seekPrefix := rf.machine.nextKV.Key[:len(key)+rf.table.knownPrefixLength]
					if debugState {
						log.Infof(ctx, "Setting seek prefix to %s", seekPrefix)
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
			if rf.table.isSecondaryIndex || len(rf.table.desc.Families) == 1 {
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateInitFetch
				continue
			}
			rf.machine.state[0] = stateFetchNextKVWithUnfinishedRow
		case stateSeekPrefix:
			for {
				moreRows, kv, _, err := rf.fetcher.nextKV(ctx)
				if err != nil {
					return nil, exec.NewStorageError(err)
				}
				if debugState {
					log.Infof(ctx, "found kv %s, seeking to prefix %s", kv.Key, rf.machine.seekPrefix)
				}
				if !moreRows {
					// We ran out of data, so ignore whatever our next state was going to
					// be and emit the final batch.
					rf.machine.state[1] = stateEmitLastBatch
					break
				}
				// TODO(jordan): if nextKV returns newSpan = true, set the new span
				// prefix and indicate that it needs decoding.
				if bytes.Compare(kv.Key, rf.machine.seekPrefix) >= 0 {
					rf.machine.nextKV = kv
					break
				}
			}
			rf.shiftState()

		case stateFetchNextKVWithUnfinishedRow:
			moreKVs, kv, _, err := rf.fetcher.nextKV(ctx)
			if err != nil {
				return nil, exec.NewStorageError(err)
			}
			if !moreKVs {
				// No more data. Finalize the row and exit.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateEmitLastBatch
				continue
			}
			// TODO(jordan): if nextKV returns newSpan = true, set the new span
			// prefix and indicate that it needs decoding.
			rf.machine.nextKV = kv
			if debugState {
				log.Infof(ctx, "Decoding next key %s", rf.machine.nextKV.Key)
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

			if familyID == rf.table.maxColumnFamilyID {
				// We know the row can't have any more keys, so finalize the row.
				rf.machine.state[0] = stateFinalizeRow
				rf.machine.state[1] = stateInitFetch
			} else {
				// Continue with current state.
				rf.machine.state[0] = stateFetchNextKVWithUnfinishedRow
			}

		case stateFinalizeRow:
			// We're finished with a row. Bump the row index, fill the row in with
			// nulls if necessary, emit the batch if necessary, and move to the next
			// state.
			if err := rf.fillNulls(); err != nil {
				return nil, err
			}
			rf.machine.rowIdx++
			rf.shiftState()
			if rf.machine.rowIdx >= coldata.BatchSize {
				rf.pushState(stateResetBatch)
				rf.machine.batch.SetLength(rf.machine.rowIdx)
				rf.machine.rowIdx = 0
				return rf.machine.batch, nil
			}

		case stateEmitLastBatch:
			rf.machine.state[0] = stateFinished
			rf.machine.batch.SetLength(rf.machine.rowIdx)
			rf.machine.rowIdx = 0
			return rf.machine.batch, nil

		case stateFinished:
			rf.machine.batch.SetLength(0)
			return rf.machine.batch, nil
		}
	}
}

// shiftState shifts the state queue to the left, removing the first element and
// clearing the last element.
func (rf *CFetcher) shiftState() {
	copy(rf.machine.state[:2], rf.machine.state[1:])
	rf.machine.state[2] = stateInvalid
}

func (rf *CFetcher) pushState(state fetcherState) {
	copy(rf.machine.state[1:], rf.machine.state[:2])
	rf.machine.state[0] = state
}

// getDatumAt returns the converted datum object at the given (colIdx, rowIdx).
// This function is meant for tracing and should not be used in hot paths.
func (rf *CFetcher) getDatumAt(colIdx int, rowIdx uint16, typ semtypes.T) tree.Datum {
	if rf.machine.colvecs[colIdx].Nulls().NullAt(rowIdx) {
		return tree.DNull
	}
	return exec.PhysicalTypeColElemToDatum(rf.machine.colvecs[colIdx], rowIdx, rf.table.da, typ)
}

// processValue processes the state machine's current value component, setting
// columns in the rowIdx'th tuple in the current batch depending on what data
// is found in the current value component.
// If debugStrings is true, returns pretty printed key and value
// information in prettyKey/prettyValue (otherwise they are empty strings).
func (rf *CFetcher) processValue(
	ctx context.Context, familyID sqlbase.FamilyID,
) (prettyKey string, prettyValue string, err error) {
	table := &rf.table

	if rf.traceKV {
		var buf strings.Builder
		buf.WriteByte('/')
		buf.WriteString(rf.table.desc.Name)
		buf.WriteByte('/')
		buf.WriteString(rf.table.index.Name)
		for _, idx := range rf.table.indexColOrdinals {
			buf.WriteByte('/')
			if idx != -1 {
				buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx, rf.table.cols[idx].Type).String())
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
	if !table.isSecondaryIndex {
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
			var family *sqlbase.ColumnFamilyDescriptor
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
		valueBytes, err := val.GetBytes()
		if err != nil {
			return "", "", scrub.WrapError(scrub.IndexValueDecodingError, err)
		}

		if cHasExtraCols(table) {
			// This is a unique secondary index; decode the extra
			// column values from the value.
			var err error
			if rf.traceKV {
				valueBytes, err = colencoding.DecodeKeyValsToCols(
					rf.machine.colvecs,
					rf.machine.rowIdx,
					table.allExtraValColOrdinals,
					table.extraTypes,
					nil,
					&rf.machine.remainingValueColsByIdx,
					valueBytes,
				)
			} else {
				valueBytes, err = colencoding.DecodeKeyValsToCols(
					rf.machine.colvecs,
					rf.machine.rowIdx,
					table.extraValColOrdinals,
					table.extraTypes,
					nil,
					&rf.machine.remainingValueColsByIdx,
					valueBytes,
				)
			}
			if err != nil {
				return "", "", scrub.WrapError(scrub.SecondaryIndexKeyExtraValueDecodingError, err)
			}
			if rf.traceKV {
				var buf strings.Builder
				for j := range table.extraTypes {
					idx := table.allExtraValColOrdinals[j]
					buf.WriteByte('/')
					buf.WriteString(rf.getDatumAt(idx, rf.machine.rowIdx, rf.table.cols[idx].Type).String())
				}
				prettyValue = buf.String()
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
func (rf *CFetcher) processValueSingle(
	ctx context.Context,
	table *cTableInfo,
	family *sqlbase.ColumnFamilyDescriptor,
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
				prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
			}
			val := rf.machine.nextKV.Value
			if len(val.RawBytes) == 0 {
				return prettyKey, "", nil
			}
			typ := &table.cols[idx].Type
			err := colencoding.UnmarshalColumnValueToCol(rf.machine.colvecs[idx], rf.machine.rowIdx, typ, val)
			if err != nil {
				return "", "", err
			}
			rf.machine.remainingValueColsByIdx.Remove(idx)

			if rf.traceKV {
				prettyValue = rf.getDatumAt(idx, rf.machine.rowIdx, *typ).String()
			}
			if debugRowFetch {
				log.Infof(ctx, "Scan %s -> %v", rf.machine.nextKV.Key, "?")
			}
			return prettyKey, prettyValue, nil
		}
	}

	// No need to unmarshal the column value. Either the column was part of
	// the index key or it isn't needed.
	if debugRowFetch {
		log.Infof(ctx, "Scan %s -> [%d] (skipped)", rf.machine.nextKV.Key, colID)
	}
	return "", "", nil
}

func (rf *CFetcher) processValueBytes(
	ctx context.Context, table *cTableInfo, valueBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	prettyKey = prettyKeyPrefix
	if rf.traceKV {
		if rf.machine.prettyValueBuf == nil {
			rf.machine.prettyValueBuf = &bytes.Buffer{}
		}
		rf.machine.prettyValueBuf.Reset()
	}

	var (
		colIDDiff              uint32
		lastColID              sqlbase.ColumnID
		typeOffset, dataOffset int
		typ                    encoding.Type
		lastColIDIndex         int
		lastNeededColIndex     int
	)
	for len(valueBytes) > 0 && rf.machine.remainingValueColsByIdx.Len() > 0 {
		typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
		if err != nil {
			return "", "", err
		}
		colID := lastColID + sqlbase.ColumnID(colIDDiff)
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
			if debugRowFetch {
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
			prettyKey = fmt.Sprintf("%s/%s", prettyKey, table.desc.Columns[idx].Name)
		}

		vec := rf.machine.colvecs[idx]

		valTyp := &table.cols[idx].Type
		valueBytes, err = colencoding.DecodeTableValueToCol(vec, rf.machine.rowIdx, typ, dataOffset, valTyp,
			valueBytes[typeOffset:])
		if err != nil {
			return "", "", err
		}
		rf.machine.remainingValueColsByIdx.Remove(idx)
		if rf.traceKV {
			dVal := rf.getDatumAt(idx, rf.machine.rowIdx, *valTyp)
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
func (rf *CFetcher) processValueTuple(
	ctx context.Context, table *cTableInfo, tupleBytes []byte, prettyKeyPrefix string,
) (prettyKey string, prettyValue string, err error) {
	return rf.processValueBytes(ctx, table, tupleBytes, prettyKeyPrefix)
}

func (rf *CFetcher) fillNulls() error {
	table := &rf.table
	if rf.machine.remainingValueColsByIdx.Empty() {
		return nil
	}
	for i, ok := rf.machine.remainingValueColsByIdx.Next(0); ok; i, ok = rf.machine.remainingValueColsByIdx.Next(i + 1) {
		// Composite index columns may have a key but no value. Ignore them so we
		// don't incorrectly mark them as null.
		if table.compositeIndexColOrdinals.Contains(i) {
			continue
		}
		if !table.cols[i].Nullable {
			var indexColValues []string
			for _, idx := range table.indexColOrdinals {
				if idx != -1 {
					indexColValues = append(indexColValues, rf.getDatumAt(idx, rf.machine.rowIdx, rf.table.cols[idx].Type).String())
				} else {
					indexColValues = append(indexColValues, "?")
				}
				return scrub.WrapError(scrub.UnexpectedNullValueError, errors.Errorf(
					"Non-nullable column \"%s:%s\" with no value! Index scanned was %q with the index key columns (%s) and the values (%s)",
					table.desc.Name, table.cols[i].Name, table.index.Name,
					strings.Join(table.index.ColumnNames, ","), strings.Join(indexColValues, ",")))
			}
		}
		rf.machine.colvecs[i].Nulls().SetNull(rf.machine.rowIdx)
	}
	return nil
}

// GetRangesInfo returns information about the ranges where the rows came from.
// The RangeInfo's are deduped and not ordered.
func (rf *CFetcher) GetRangesInfo() []roachpb.RangeInfo {
	return rf.fetcher.getRangesInfo()
}

// getCurrentColumnFamilyID returns the column family id of the key in
// rf.machine.nextKV.Key.
func (rf *CFetcher) getCurrentColumnFamilyID() (sqlbase.FamilyID, error) {
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
	return sqlbase.FamilyID(id), nil
}
