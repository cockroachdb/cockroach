// Copyright 2016 The Cockroach Authors.
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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TableLookupsByID maps table IDs to looked up descriptors or, for tables that
// exist but are not yet public/leasable, entries with just the IsAdding flag.
type TableLookupsByID map[ID]TableLookup

// TableLookup is the value type of TableLookupsByID: An optional table
// descriptor, populated when the table is public/leasable, and an IsAdding
// flag.
type TableLookup struct {
	Table    *TableDescriptor
	IsAdding bool
}

// FKCheck indicates a kind of FK check (delete, insert, or both).
type FKCheck int

const (
	// CheckDeletes checks if rows reference a changed value.
	CheckDeletes FKCheck = iota
	// CheckInserts checks if a new/changed value references an existing row.
	CheckInserts
	// CheckUpdates checks all references (CheckDeletes+CheckInserts).
	CheckUpdates
)

// NoLookup can be used to not perform any lookups during a TablesNeededForFKs
// function call.
func NoLookup(ctx context.Context, tableID ID) (TableLookup, error) {
	return TableLookup{}, nil
}

type tableLookupQueueElement struct {
	ID    ID
	usage FKCheck
}

type tableLookupQueue []tableLookupQueueElement

func (q *tableLookupQueue) enqueue(tableID ID, usage FKCheck) {
	*q = append((*q), tableLookupQueueElement{ID: tableID, usage: usage})
}

func (q *tableLookupQueue) dequeue() (ID, FKCheck, bool) {
	if len(*q) == 0 {
		return 0, 0, false
	}
	elem := (*q)[0]
	*q = (*q)[1:]
	return elem.ID, elem.usage, true
}

// TablesNeededForFKs populates a map of TableLookupsByID for all the
// TableDescriptors that might be needed when performing FK checking for delete
// and/or insert operations. It uses the passed in lookup function to perform
// the actual lookup.
// TODO(Bram): Right now this is performing a transitive closure over all
// foreign keys that touch this table. This is overly broad and should match
// the algorithm described the cascade rfc.
func TablesNeededForFKs(
	ctx context.Context,
	table TableDescriptor,
	usage FKCheck,
	lookup func(ctx context.Context, tableID ID) (TableLookup, error),
) (TableLookupsByID, error) {
	tableLookups := make(TableLookupsByID)
	tableLookups[table.ID] = TableLookup{&table, false}
	var queue tableLookupQueue
	queue.enqueue(table.ID, usage)
	for {
		tableID, curUsage, exists := queue.dequeue()
		if !exists {
			break
		}
		var curTable *TableDescriptor
		if tableLookup, exists := tableLookups[tableID]; exists {
			if tableLookup.IsAdding {
				continue
			}
			curTable = tableLookup.Table
		} else {
			tableLookup, err := lookup(ctx, tableID)
			if err != nil {
				return nil, err
			}
			tableLookups[tableID] = tableLookup
			if tableLookup.IsAdding {
				continue
			}
			curTable = tableLookup.Table
		}
		if curTable == nil {
			continue
		}
		for _, idx := range curTable.AllNonDropIndexes() {
			if curUsage != CheckDeletes && idx.ForeignKey.IsSet() {
				if _, exists := tableLookups[idx.ForeignKey.Table]; !exists {
					queue.enqueue(idx.ForeignKey.Table, CheckDeletes)
				}
			}
			if curUsage != CheckInserts {
				for _, ref := range idx.ReferencedBy {
					if _, exists := tableLookups[ref.Table]; !exists {
						queue.enqueue(ref.Table, CheckDeletes)
					}
				}
			}
		}
	}
	return tableLookups, nil
}

// spanKVFetcher is an kvFetcher that returns a set slice of kvs.
type spanKVFetcher struct {
	kvs []roachpb.KeyValue
}

// nextKV implements the kvFetcher interface.
func (f *spanKVFetcher) nextKV(ctx context.Context) (bool, roachpb.KeyValue, error) {
	if len(f.kvs) == 0 {
		return false, roachpb.KeyValue{}, nil
	}
	kv := f.kvs[0]
	f.kvs = f.kvs[1:]
	return true, kv, nil
}

// getRangesInfo implements the kvFetcher interface.
func (f *spanKVFetcher) getRangesInfo() []roachpb.RangeInfo {
	panic("getRangesInfo() called on spanKVFetcher")
}

// fkBatchChecker accumulates foreign key checks and sends them out as a single
// kv batch on demand. Checks are accumulated in order - the first failing check
// will be the one that produces an error report.
type fkBatchChecker struct {
	batch roachpb.BatchRequest
	// batchIdxToFk maps the index of the check request/response in the kv batch
	// to the baseFKHelper that created it.
	batchIdxToFk []*baseFKHelper
	txn          *client.Txn
}

func (f *fkBatchChecker) reset() {
	f.batch.Reset()
	f.batchIdxToFk = f.batchIdxToFk[:0]
}

// addCheck adds a check for the given row and baseFKHelper to the batch.
func (f *fkBatchChecker) addCheck(row tree.Datums, source *baseFKHelper) error {
	span, err := source.spanForValues(row)
	if err != nil {
		return err
	}
	r := roachpb.RequestUnion{}
	scan := roachpb.ScanRequest{Span: span}
	r.MustSetInner(&scan)
	f.batch.Requests = append(f.batch.Requests, r)
	f.batchIdxToFk = append(f.batchIdxToFk, source)
	return nil
}

// runCheck sends the accumulated batch of foreign key checks to kv, given the
// old and new values of the row being modified. Either oldRow or newRow can
// be set to nil in the case of an insert or a delete, respectively.
// A pgerror.CodeForeignKeyViolationError is returned if a foreign key violation
// is detected, corresponding to the first foreign key that was violated in
// order of addition.
func (f *fkBatchChecker) runCheck(
	ctx context.Context, oldRow tree.Datums, newRow tree.Datums,
) error {
	if len(f.batch.Requests) == 0 {
		return nil
	}
	defer f.reset()

	br, err := f.txn.Send(ctx, f.batch)
	if err != nil {
		return err.GoError()
	}

	fetcher := spanKVFetcher{}
	for i, resp := range br.Responses {
		fk := f.batchIdxToFk[i]

		fetcher.kvs = resp.GetInner().(*roachpb.ScanResponse).Rows
		if err := fk.rf.StartScanFrom(ctx, &fetcher); err != nil {
			return err
		}
		switch fk.dir {
		case CheckInserts:
			// If we're inserting, then there's a violation if the scan found nothing.
			if fk.rf.kvEnd {
				fkValues := make(tree.Datums, fk.prefixLen)
				for valueIdx, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
					fkValues[valueIdx] = newRow[fk.ids[colID]]
				}
				return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
					"foreign key violation: value %s not found in %s@%s %s",
					fkValues, fk.searchTable.Name, fk.searchIdx.Name, fk.searchIdx.ColumnNames[:fk.prefixLen])
			}
		case CheckDeletes:
			// If we're deleting, then there's a violation if the scan found something.
			if !fk.rf.kvEnd {
				if oldRow == nil {
					return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
						"foreign key violation: non-empty columns %s referenced in table %q",
						fk.writeIdx.ColumnNames[:fk.prefixLen], fk.searchTable.Name)
				}
				fkValues := make(tree.Datums, fk.prefixLen)
				for valueIdx, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
					fkValues[valueIdx] = oldRow[fk.ids[colID]]
				}
				return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
					"foreign key violation: values %v in columns %s referenced in table %q",
					fkValues, fk.writeIdx.ColumnNames[:fk.prefixLen], fk.searchTable.Name)
			}
		default:
			log.Fatalf(ctx, "impossible case: baseFKHelper has dir=%v", fk.dir)
		}
	}
	return nil
}

type fkInsertHelper struct {
	// fks maps index id to slice of baseFKHelper, the outgoing foreign keys for
	// each index. These slices will have at most one entry, since there can be
	// at most one outgoing foreign key per index. We use this data structure
	// instead of a one-to-one map for consistency with the other insert helpers.
	fks map[IndexID][]baseFKHelper

	checker *fkBatchChecker
}

var errSkipUnusedFK = errors.New("no columns involved in FK included in writer")

func makeFKInsertHelper(
	txn *client.Txn,
	table TableDescriptor,
	otherTables TableLookupsByID,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
) (fkInsertHelper, error) {
	h := fkInsertHelper{
		checker: &fkBatchChecker{
			txn: txn,
		},
	}
	for _, idx := range table.AllNonDropIndexes() {
		if idx.ForeignKey.IsSet() {
			fk, err := makeBaseFKHelper(txn, otherTables, idx, idx.ForeignKey, colMap, alloc, CheckInserts)
			if err == errSkipUnusedFK {
				continue
			}
			if err != nil {
				return h, err
			}
			if h.fks == nil {
				h.fks = make(map[IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}
	return h, nil
}

func (h fkInsertHelper) checkAll(ctx context.Context, row tree.Datums) error {
	if len(h.fks) == 0 {
		return nil
	}
	for idx := range h.fks {
		if err := h.checkIdx(ctx, idx, row); err != nil {
			return err
		}
	}
	return h.checker.runCheck(ctx, nil, row)
}

func (h fkInsertHelper) checkIdx(ctx context.Context, idx IndexID, row tree.Datums) error {
	fks := h.fks
	for i, fk := range fks[idx] {
		nulls := true
		for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
			found, ok := fk.ids[colID]
			if !ok {
				panic(fmt.Sprintf("fk ids (%v) missing column id %d", fk.ids, colID))
			}
			nulls = nulls && row[found] == tree.DNull
		}
		if nulls {
			continue
		}

		if err := h.checker.addCheck(row, &(fks[idx][i])); err != nil {
			return err
		}
	}
	return nil
}

// CollectSpans implements the FkSpanCollector interface.
func (h fkInsertHelper) CollectSpans() roachpb.Spans {
	return collectSpansWithFKMap(h.fks)
}

// CollectSpansForValues implements the FkSpanCollector interface.
func (h fkInsertHelper) CollectSpansForValues(values tree.Datums) (roachpb.Spans, error) {
	return collectSpansForValuesWithFKMap(h.fks, values)
}

type fkDeleteHelper struct {
	fks         map[IndexID][]baseFKHelper
	cascadeFKs  []cascadeHelper
	otherTables TableLookupsByID
	alloc       *DatumAlloc

	checker *fkBatchChecker
}

func makeFKDeleteHelper(
	txn *client.Txn,
	table TableDescriptor,
	otherTables TableLookupsByID,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
) (fkDeleteHelper, error) {
	h := fkDeleteHelper{
		otherTables: otherTables,
		alloc:       alloc,
		checker: &fkBatchChecker{
			txn: txn,
		},
	}
	for _, idx := range table.AllNonDropIndexes() {
		for _, ref := range idx.ReferencedBy {
			if otherTables[ref.Table].IsAdding {
				// We can assume that a table being added but not yet public is empty,
				// and thus does not need to be checked for FK violations.
				continue
			}
			fk, err := makeBaseFKHelper(txn, otherTables, idx, ref, colMap, alloc, CheckDeletes)
			if err == errSkipUnusedFK {
				continue
			}
			if err != nil {
				return h, err
			}
			ch, err := makeCascadeHelper(txn, ref, otherTables, alloc)
			if err != nil {
				return h, err
			}
			idxDesc, _ := otherTables[ref.Table].Table.FindIndexByID(ref.Index)
			if h.fks == nil {
				h.fks = make(map[IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
			if idxDesc.ForeignKey.OnDelete == ForeignKeyReference_CASCADE {
				h.cascadeFKs = append(h.cascadeFKs, ch)
			}
		}
	}
	return h, nil
}

func (h fkDeleteHelper) cascadeAll(ctx context.Context, row tree.Datums, traceKV bool) error {
	if len(h.cascadeFKs) == 0 {
		return nil
	}
	for _, ch := range h.cascadeFKs {
		if err := ch.delete(ctx, row, traceKV); err != nil {
			return err
		}
	}
	return nil
}

func (h fkDeleteHelper) checkAll(ctx context.Context, row tree.Datums) error {
	if len(h.fks) == 0 {
		return nil
	}
	for idx := range h.fks {
		if err := h.checkIdx(ctx, idx, row); err != nil {
			return err
		}
	}
	return h.checker.runCheck(ctx, row, nil /* newRow */)
}

func (h fkDeleteHelper) checkIdx(ctx context.Context, idx IndexID, row tree.Datums) error {
	for i := range h.fks[idx] {
		if err := h.checker.addCheck(row, &h.fks[idx][i]); err != nil {
			return err
		}
	}
	return nil
}

// CollectSpans implements the FkSpanCollector interface.
func (h fkDeleteHelper) CollectSpans() roachpb.Spans {
	return collectSpansWithFKMap(h.fks)
}

// CollectSpansForValues implements the FkSpanCollector interface.
func (h fkDeleteHelper) CollectSpansForValues(values tree.Datums) (roachpb.Spans, error) {
	return collectSpansForValuesWithFKMap(h.fks, values)
}

type fkUpdateHelper struct {
	inbound  fkDeleteHelper // Check old values are not referenced.
	outbound fkInsertHelper // Check rows referenced by new values still exist.

	checker *fkBatchChecker
}

func makeFKUpdateHelper(
	txn *client.Txn,
	table TableDescriptor,
	otherTables TableLookupsByID,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
) (fkUpdateHelper, error) {
	ret := fkUpdateHelper{}
	var err error
	if ret.inbound, err = makeFKDeleteHelper(txn, table, otherTables, colMap, alloc); err != nil {
		return ret, err
	}
	ret.outbound, err = makeFKInsertHelper(txn, table, otherTables, colMap, alloc)
	ret.outbound.checker = ret.inbound.checker
	ret.checker = ret.inbound.checker
	return ret, err
}

func (fks fkUpdateHelper) checkIdx(
	ctx context.Context, idx IndexID, oldValues, newValues tree.Datums,
) error {
	if err := fks.inbound.checkIdx(ctx, idx, oldValues); err != nil {
		return err
	}
	return fks.outbound.checkIdx(ctx, idx, newValues)
}

// CollectSpans implements the FkSpanCollector interface.
func (fks fkUpdateHelper) CollectSpans() roachpb.Spans {
	inboundReads := fks.inbound.CollectSpans()
	outboundReads := fks.outbound.CollectSpans()
	return append(inboundReads, outboundReads...)
}

// CollectSpansForValues implements the FkSpanCollector interface.
func (fks fkUpdateHelper) CollectSpansForValues(values tree.Datums) (roachpb.Spans, error) {
	inboundReads, err := fks.inbound.CollectSpansForValues(values)
	if err != nil {
		return nil, err
	}
	outboundReads, err := fks.outbound.CollectSpansForValues(values)
	if err != nil {
		return nil, err
	}
	return append(inboundReads, outboundReads...), nil
}

type baseFKHelper struct {
	txn          *client.Txn
	rf           MultiRowFetcher
	searchTable  *TableDescriptor // the table being searched (for err msg)
	searchIdx    *IndexDescriptor // the index that must (not) contain a value
	prefixLen    int
	writeIdx     IndexDescriptor  // the index we want to modify
	searchPrefix []byte           // prefix of keys in searchIdx
	ids          map[ColumnID]int // col IDs
	dir          FKCheck          // direction of check
}

func makeBaseFKHelper(
	txn *client.Txn,
	otherTables TableLookupsByID,
	writeIdx IndexDescriptor,
	ref ForeignKeyReference,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
	dir FKCheck,
) (baseFKHelper, error) {
	b := baseFKHelper{txn: txn, writeIdx: writeIdx, searchTable: otherTables[ref.Table].Table, dir: dir}
	if b.searchTable == nil {
		return b, errors.Errorf("referenced table %d not in provided table map %+v", ref.Table, otherTables)
	}
	b.searchPrefix = MakeIndexKeyPrefix(b.searchTable, ref.Index)
	searchIdx, err := b.searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return b, err
	}
	b.prefixLen = len(searchIdx.ColumnIDs)
	if len(writeIdx.ColumnIDs) < b.prefixLen {
		b.prefixLen = len(writeIdx.ColumnIDs)
	}
	b.searchIdx = searchIdx
	tableArgs := MultiRowFetcherTableArgs{
		Desc:             b.searchTable,
		Index:            b.searchIdx,
		ColIdxMap:        ColIDtoRowIndexFromCols(b.searchTable.Columns),
		IsSecondaryIndex: b.searchIdx.ID != b.searchTable.PrimaryIndex.ID,
		Cols:             b.searchTable.Columns,
	}
	err = b.rf.Init(false /* reverse */, false /* returnRangeInfo */, alloc, tableArgs)
	if err != nil {
		return b, err
	}

	b.ids = make(map[ColumnID]int, len(writeIdx.ColumnIDs))
	nulls := true
	for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
		if found, ok := colMap[writeColID]; ok {
			b.ids[searchIdx.ColumnIDs[i]] = found
			nulls = false
		} else if !nulls {
			return b, errors.Errorf("missing value for column %q in multi-part foreign key", writeIdx.ColumnNames[i])
		}
	}
	if nulls {
		return b, errSkipUnusedFK
	}
	return b, nil
}

func (f baseFKHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	var key roachpb.Key
	if values != nil {
		keyBytes, _, err := EncodePartialIndexKey(
			f.searchTable, f.searchIdx, f.prefixLen, f.ids, values, f.searchPrefix)
		if err != nil {
			return roachpb.Span{}, err
		}
		key = roachpb.Key(keyBytes)
	} else {
		key = roachpb.Key(f.searchPrefix)
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

func (f baseFKHelper) span() roachpb.Span {
	key := roachpb.Key(f.searchPrefix)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}
}

// FkSpanCollector can collect the spans that foreign key validation will touch.
type FkSpanCollector interface {
	CollectSpans() roachpb.Spans
	CollectSpansForValues(values tree.Datums) (roachpb.Spans, error)
}

var _ FkSpanCollector = fkInsertHelper{}
var _ FkSpanCollector = fkDeleteHelper{}
var _ FkSpanCollector = fkUpdateHelper{}

func collectSpansWithFKMap(fks map[IndexID][]baseFKHelper) roachpb.Spans {
	var reads roachpb.Spans
	for idx := range fks {
		for _, fk := range fks[idx] {
			reads = append(reads, fk.span())
		}
	}
	return reads
}

func collectSpansForValuesWithFKMap(
	fks map[IndexID][]baseFKHelper, values tree.Datums,
) (roachpb.Spans, error) {
	var reads roachpb.Spans
	for idx := range fks {
		for _, fk := range fks[idx] {
			read, err := fk.spanForValues(values)
			if err != nil {
				return nil, err
			}
			reads = append(reads, read)
		}
	}
	return reads, nil
}

type cascadeHelper struct {
	txn          *client.Txn
	table        *TableDescriptor // the table being searched for rows to cascade
	index        *IndexDescriptor // the fk index that will contain the entires to cascade
	referencedRF MultiRowFetcher  // row fetcher used to look up primary keys within index
	alloc        *DatumAlloc
	indexPrefix  []byte           // prefix of keys in index
	indexColIDs  map[ColumnID]int // ColIDtoRowIndex for the index
	tablesByID   TableLookupsByID

	// The following are only populated when a delete is required.
	rowDeleter         RowDeleter      // row deleter used to perform the actual deletes, note that this call is recursive
	deleteRF           MultiRowFetcher // row fetcher used to lookup rows for deletion
	primaryIndexPrefix []byte          // prefix of keys in the primary index
}

func makeCascadeHelper(
	txn *client.Txn, ref ForeignKeyReference, tablesByID TableLookupsByID, alloc *DatumAlloc,
) (cascadeHelper, error) {
	table := tablesByID[ref.Table].Table
	index, err := table.FindIndexByID(ref.Index)
	if err != nil {
		return cascadeHelper{}, err
	}
	helper := cascadeHelper{
		txn:         txn,
		table:       table,
		index:       index,
		alloc:       alloc,
		indexPrefix: MakeIndexKeyPrefix(table, ref.Index),
		tablesByID:  tablesByID,
	}

	// Create the indexColIDs by creating a map of the index columns. This is used
	// when creating the index span.
	helper.indexColIDs = make(map[ColumnID]int, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		helper.indexColIDs[colID] = i
	}

	// To find out which rows need to be deleted, the primary keys for those rows
	// need to be fetched from the foreign key referencing index.
	var colDesc []ColumnDescriptor
	for _, id := range table.PrimaryIndex.ColumnIDs {
		cDesc, err := table.FindColumnByID(id)
		if err != nil {
			return cascadeHelper{}, err
		}
		colDesc = append(colDesc, *cDesc)
	}
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(colDesc)-1)

	isSecondary := table.PrimaryIndex.ID != index.ID
	// TODO(bram): If this is a primary index, we can skip this step entirely and
	// jump right to the fetcher using the values from oldRow as the primary key.
	if err := helper.referencedRF.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		alloc,
		MultiRowFetcherTableArgs{
			Desc:             table,
			Index:            index,
			ColIdxMap:        ColIDtoRowIndexFromCols(colDesc),
			IsSecondaryIndex: isSecondary,
			Cols:             colDesc,
			ValNeededForCol:  valNeededForCol,
		},
	); err != nil {
		return cascadeHelper{}, err
	}

	return helper, nil
}

// addRowDeleter creates the row deleter and primary index row fetcher. Sadly,
// this cannot be done eagerly or we end up looping forever.
// TODO(bram): Store and retrieve all of these values instead of regenerating
// them.
func (c *cascadeHelper) addRowDeleter() error {
	c.primaryIndexPrefix = MakeIndexKeyPrefix(c.table, c.table.PrimaryIndex.ID)

	// Create the row deleter. The row deleter is needed early to know which
	// columns are required in the row fetcher.
	var err error
	c.rowDeleter, err = MakeRowDeleter(
		c.txn,
		c.table,
		c.tablesByID,
		nil,  /* requestedCol */
		true, /* checkFKs */
		c.alloc,
	)
	if err != nil {
		return err
	}

	// Create the row fetcher that will retrive the rows
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(c.rowDeleter.FetchCols)-1)
	tableArgs := MultiRowFetcherTableArgs{
		Desc:             c.table,
		Index:            &c.table.PrimaryIndex,
		ColIdxMap:        c.rowDeleter.FetchColIDtoRowIndex,
		IsSecondaryIndex: false,
		Cols:             c.rowDeleter.FetchCols,
		ValNeededForCol:  valNeededForCol,
	}
	return c.deleteRF.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		c.alloc,
		tableArgs,
	)
}

// spanForIndexValues creates a span against an index to extra the primary keys
// needed for cascading.
func (c cascadeHelper) spanForIndexValues(values tree.Datums) (roachpb.Span, error) {
	var key roachpb.Key
	if values != nil {
		keyBytes, _, err := EncodePartialIndexKey(
			c.table, c.index, len(c.index.ColumnIDs), c.indexColIDs, values, c.indexPrefix)
		if err != nil {
			return roachpb.Span{}, err
		}
		key = roachpb.Key(keyBytes)
	} else {
		key = roachpb.Key(c.indexPrefix)
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// spanForPKValues creates a span against the primary index of a table and is
// used to fetch rows for cascading. This requires that the row deleter has
// already been created.
func (c cascadeHelper) spanForPKValues(values tree.Datums) (roachpb.Span, error) {
	keyBytes, _, err := EncodePartialIndexKey(
		c.table,
		&c.table.PrimaryIndex,
		len(c.table.PrimaryIndex.ColumnIDs),
		c.rowDeleter.FetchColIDtoRowIndex,
		values,
		c.primaryIndexPrefix,
	)
	if err != nil {
		return roachpb.Span{}, err
	}
	key := roachpb.Key(keyBytes)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

func (c cascadeHelper) delete(ctx context.Context, oldRow tree.Datums, traceKV bool) error {
	// Create the span to search for index values.
	// TODO(bram): This initial index lookup can be skipped if the index is the
	// primary index.
	span, err := c.spanForIndexValues(oldRow)
	if err != nil {
		return err
	}
	req := roachpb.BatchRequest{}
	req.Add(&roachpb.ScanRequest{Span: span})
	br, roachErr := c.txn.Send(ctx, req)
	if roachErr != nil {
		return roachErr.GoError()
	}

	// Find all primary keys that need to be deleted.
	var primaryKeysToDel []tree.Datums
	for _, resp := range br.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := c.referencedRF.StartScanFrom(ctx, &fetcher); err != nil {
			return err
		}
		for !c.referencedRF.kvEnd {
			primaryKey, _, _, err := c.referencedRF.NextRowDecoded(ctx)
			if err != nil {
				return err
			}
			// Make a copy of the primary key because the datum struct is reused in
			// the row fetcher.
			primaryKey = append(tree.Datums(nil), primaryKey...)
			primaryKeysToDel = append(primaryKeysToDel, primaryKey)
			log.Warningf(ctx, "******** pks to delete: %+v", primaryKey)
		}
	}

	// Early exit if no rows need to be deleted.
	if len(primaryKeysToDel) == 0 {
		return nil
	}

	if err := c.addRowDeleter(); err != nil {
		return err
	}

	// Create a batch request to get all the spans of the primary keys that need
	// to be deleted.
	pkLookupReq := roachpb.BatchRequest{}
	for _, primaryKey := range primaryKeysToDel {
		pkSpan, err := c.spanForPKValues(primaryKey)
		if err != nil {
			return err
		}
		pkLookupReq.Add(&roachpb.ScanRequest{Span: pkSpan})
	}
	pkResp, roachErr := c.txn.Send(ctx, pkLookupReq)
	if roachErr != nil {
		return roachErr.GoError()
	}

	// Fetch the rows for deletion.
	var rowsToDelete []tree.Datums
	for _, resp := range pkResp.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := c.deleteRF.StartScanFrom(ctx, &fetcher); err != nil {
			return err
		}
		for !c.deleteRF.kvEnd {
			rowToDelete, _, _, err := c.deleteRF.NextRowDecoded(ctx)
			if err != nil {
				return err
			}
			// Make a copy of the rowToDelete because the datum struct is reused in
			// the row fetcher.
			rowToDelete = append(tree.Datums(nil), rowToDelete...)
			rowsToDelete = append(rowsToDelete, rowToDelete)
			log.Warningf(ctx, "******** row to delete: %+v", rowToDelete)
		}
	}

	// Finally delete the rows.
	// TODO(bram): If the row was already deleted, this should not be a failure.
	// TODO(bram): Don't run the fk checks at the start of the delete, but gather
	// the checks and add them back into the batch checker at the end.
	// TODO(bram): Perhaps we should limit this depth to avoid panics.
	deleteBatch := c.txn.NewBatch()
	for _, row := range rowsToDelete {
		if err := c.rowDeleter.DeleteRow(ctx, deleteBatch, row, traceKV); err != nil {
			return err
		}
	}
	return c.txn.Run(ctx, deleteBatch)
}
