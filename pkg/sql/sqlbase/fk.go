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
		if err := checkIdx(ctx, h.checker, h.fks, idx, row); err != nil {
			return err
		}
	}
	return h.checker.runCheck(ctx, nil, row)
}

// CollectSpans implements the FkSpanCollector interface.
func (h fkInsertHelper) CollectSpans() roachpb.Spans {
	return collectSpansWithFKMap(h.fks)
}

// CollectSpansForValues implements the FkSpanCollector interface.
func (h fkInsertHelper) CollectSpansForValues(values tree.Datums) (roachpb.Spans, error) {
	return collectSpansForValuesWithFKMap(h.fks, values)
}

func checkIdx(
	ctx context.Context,
	checker *fkBatchChecker,
	fks map[IndexID][]baseFKHelper,
	idx IndexID,
	row tree.Datums,
) error {
	for i, fk := range fks[idx] {
		nulls := true
		for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
			found, ok := fk.ids[colID]
			if !ok {
				panic(fmt.Sprintf("fk ids (%v) missing column id %d", fk.ids, colID))
			}
			if row[found] != tree.DNull {
				nulls = false
				break
			}
		}
		if nulls {
			continue
		}
		if err := checker.addCheck(row, &fks[idx][i]); err != nil {
			return err
		}
	}
	return nil
}

type fkDeleteHelper struct {
	fks         map[IndexID][]baseFKHelper
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
				return fkDeleteHelper{}, err
			}
			if h.fks == nil {
				h.fks = make(map[IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}
	return h, nil
}

func (h fkDeleteHelper) checkAll(ctx context.Context, row tree.Datums) error {
	if len(h.fks) == 0 {
		return nil
	}
	for idx := range h.fks {
		if err := checkIdx(ctx, h.checker, h.fks, idx, row); err != nil {
			return err
		}
	}
	return h.checker.runCheck(ctx, row, nil /* newRow */)
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
	if err := checkIdx(ctx, fks.checker, fks.inbound.fks, idx, oldValues); err != nil {
		return err
	}
	return checkIdx(ctx, fks.checker, fks.outbound.fks, idx, newValues)
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
