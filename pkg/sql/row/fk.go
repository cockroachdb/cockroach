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

package row

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// ID is an alias for sqlbase.ID.
type ID = sqlbase.ID

// TableLookupsByID maps table IDs to looked up descriptors or, for tables that
// exist but are not yet public/leasable, entries with just the IsAdding flag.
type TableLookupsByID map[ID]TableLookup

// TableLookup is the value type of TableLookupsByID: An optional table
// descriptor, populated when the table is public/leasable, and an IsAdding
// flag.
// This also includes an optional CheckHelper for the table.
type TableLookup struct {
	Table       *sqlbase.ImmutableTableDescriptor
	IsAdding    bool
	CheckHelper *sqlbase.CheckHelper
}

// TableLookupFunction is the function type used by TablesNeededForFKs that will
// perform the actual lookup.
type TableLookupFunction func(context.Context, ID) (TableLookup, error)

// NoLookup can be used to not perform any lookups during a TablesNeededForFKs
// function call.
func NoLookup(_ context.Context, _ ID) (TableLookup, error) {
	return TableLookup{}, nil
}

// CheckPrivilegeFunction is the function type used by TablesNeededForFKs that will
// check the privileges of the current user to access specific tables.
type CheckPrivilegeFunction func(context.Context, sqlbase.DescriptorProto, privilege.Kind) error

// NoCheckPrivilege can be used to not perform any privilege checks during a
// TablesNeededForFKs function call.
func NoCheckPrivilege(_ context.Context, _ sqlbase.DescriptorProto, _ privilege.Kind) error {
	return nil
}

// FKCheck indicates a kind of FK check (delete, insert, or both).
type FKCheck int

const (
	// CheckDeletes checks if rows reference a changed value.
	CheckDeletes FKCheck = iota
	// CheckInserts checks if a new value references an existing row.
	CheckInserts
	// CheckUpdates checks all references (CheckDeletes+CheckInserts).
	CheckUpdates
)

type tableLookupQueueElement struct {
	tableLookup TableLookup
	usage       FKCheck
}

type tableLookupQueue struct {
	queue          []tableLookupQueueElement
	alreadyChecked map[ID]map[FKCheck]struct{}
	tableLookups   TableLookupsByID
	lookup         TableLookupFunction
	checkPrivilege CheckPrivilegeFunction
	analyzeExpr    sqlbase.AnalyzeExprFunction
}

func (tl *TableLookup) addCheckHelper(
	ctx context.Context, analyzeExpr sqlbase.AnalyzeExprFunction,
) error {
	if analyzeExpr == nil {
		return nil
	}
	tableName := tree.MakeUnqualifiedTableName(tree.Name(tl.Table.Name))
	tl.CheckHelper = &sqlbase.CheckHelper{}
	return tl.CheckHelper.Init(ctx, analyzeExpr, &tableName, tl.Table)
}

func (q *tableLookupQueue) getTable(ctx context.Context, tableID ID) (TableLookup, error) {
	if tableLookup, exists := q.tableLookups[tableID]; exists {
		return tableLookup, nil
	}
	tableLookup, err := q.lookup(ctx, tableID)
	if err != nil {
		return TableLookup{}, err
	}
	if !tableLookup.IsAdding && tableLookup.Table != nil {
		if err := q.checkPrivilege(ctx, tableLookup.Table, privilege.SELECT); err != nil {
			return TableLookup{}, err
		}
		if err := tableLookup.addCheckHelper(ctx, q.analyzeExpr); err != nil {
			return TableLookup{}, err
		}
	}
	q.tableLookups[tableID] = tableLookup
	return tableLookup, nil
}

func (q *tableLookupQueue) enqueue(ctx context.Context, tableID ID, usage FKCheck) error {
	// Lookup the table.
	tableLookup, err := q.getTable(ctx, tableID)
	if err != nil {
		return err
	}
	// Don't enqueue if lookup returns an empty tableLookup. This just means that
	// there is no need to walk any further.
	if tableLookup.Table == nil {
		return nil
	}
	// Only enqueue checks that haven't been performed yet.
	if alreadyCheckByTableID, exists := q.alreadyChecked[tableID]; exists {
		if _, existsInner := alreadyCheckByTableID[usage]; existsInner {
			return nil
		}
	} else {
		q.alreadyChecked[tableID] = make(map[FKCheck]struct{})
	}
	q.alreadyChecked[tableID][usage] = struct{}{}
	// If the table is being added, there's no need to check it.
	if tableLookup.IsAdding {
		return nil
	}
	switch usage {
	// Insert has already been checked when the table is fetched.
	case CheckDeletes:
		if err := q.checkPrivilege(ctx, tableLookup.Table, privilege.DELETE); err != nil {
			return err
		}
	case CheckUpdates:
		if err := q.checkPrivilege(ctx, tableLookup.Table, privilege.UPDATE); err != nil {
			return err
		}
	}
	(*q).queue = append((*q).queue, tableLookupQueueElement{tableLookup: tableLookup, usage: usage})
	return nil
}

func (q *tableLookupQueue) dequeue() (TableLookup, FKCheck, bool) {
	if len((*q).queue) == 0 {
		return TableLookup{}, 0, false
	}
	elem := (*q).queue[0]
	(*q).queue = (*q).queue[1:]
	return elem.tableLookup, elem.usage, true
}

// TablesNeededForFKs populates a map of TableLookupsByID for all the
// TableDescriptors that might be needed when performing FK checking for delete
// and/or insert operations. It uses the passed in lookup function to perform
// the actual lookup. The AnalyzeExpr function, if provided, is used to
// initialize the CheckHelper, and this requires that the TableLookupFunction
// and CheckPrivilegeFunction are provided and not just placeholder functions
// as well. If an operation may include a cascading operation then the
// CheckHelpers are required.
func TablesNeededForFKs(
	ctx context.Context,
	table *sqlbase.ImmutableTableDescriptor,
	usage FKCheck,
	lookup TableLookupFunction,
	checkPrivilege CheckPrivilegeFunction,
	analyzeExpr sqlbase.AnalyzeExprFunction,
) (TableLookupsByID, error) {
	queue := tableLookupQueue{
		tableLookups:   make(TableLookupsByID),
		alreadyChecked: make(map[ID]map[FKCheck]struct{}),
		lookup:         lookup,
		checkPrivilege: checkPrivilege,
		analyzeExpr:    analyzeExpr,
	}
	// Add the passed in table descriptor to the table lookup.
	baseTableLookup := TableLookup{Table: table}
	if err := baseTableLookup.addCheckHelper(ctx, analyzeExpr); err != nil {
		return nil, err
	}
	queue.tableLookups[table.ID] = baseTableLookup
	if err := queue.enqueue(ctx, table.ID, usage); err != nil {
		return nil, err
	}
	for {
		tableLookup, curUsage, exists := queue.dequeue()
		if !exists {
			return queue.tableLookups, nil
		}
		// If the table descriptor is nil it means that there was no actual lookup
		// performed. Meaning there is no need to walk any secondary relationships
		// and the table descriptor lookup will happen later.
		if tableLookup.IsAdding || tableLookup.Table == nil {
			continue
		}
		for _, idx := range tableLookup.Table.AllNonDropIndexes() {
			if curUsage == CheckInserts || curUsage == CheckUpdates {
				if idx.ForeignKey.IsSet() {
					if _, err := queue.getTable(ctx, idx.ForeignKey.Table); err != nil {
						return nil, err
					}
				}
			}
			if curUsage == CheckDeletes || curUsage == CheckUpdates {
				for _, ref := range idx.ReferencedBy {
					// The table being referenced is required to know the relationship, so
					// fetch it here.
					referencedTableLookup, err := queue.getTable(ctx, ref.Table)
					if err != nil {
						return nil, err
					}
					// Again here if the table descriptor is nil it means that there was
					// no actual lookup performed. Meaning there is no need to walk any
					// secondary relationships.
					if referencedTableLookup.IsAdding || referencedTableLookup.Table == nil {
						continue
					}
					referencedIdx, err := referencedTableLookup.Table.FindIndexByID(ref.Index)
					if err != nil {
						return nil, err
					}
					if curUsage == CheckDeletes {
						var nextUsage FKCheck
						switch referencedIdx.ForeignKey.OnDelete {
						case sqlbase.ForeignKeyReference_CASCADE:
							nextUsage = CheckDeletes
						case sqlbase.ForeignKeyReference_SET_DEFAULT, sqlbase.ForeignKeyReference_SET_NULL:
							nextUsage = CheckUpdates
						default:
							// There is no need to check any other relationships.
							continue
						}
						if err := queue.enqueue(ctx, referencedTableLookup.Table.ID, nextUsage); err != nil {
							return nil, err
						}
					} else {
						// curUsage == CheckUpdates
						if referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_CASCADE ||
							referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_DEFAULT ||
							referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_NULL {
							if err := queue.enqueue(
								ctx, referencedTableLookup.Table.ID, CheckUpdates,
							); err != nil {
								return nil, err
							}
						}
					}
				}
			}
		}
	}
}

// SpanKVFetcher is an kvBatchFetcher that returns a set slice of kvs.
type SpanKVFetcher struct {
	KVs []roachpb.KeyValue
}

// nextBatch implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if len(f.KVs) == 0 {
		return false, nil, nil, roachpb.Span{}, nil
	}
	res := f.KVs
	f.KVs = nil
	return true, res, nil, roachpb.Span{}, nil
}

// getRangesInfo implements the kvBatchFetcher interface.
func (f *SpanKVFetcher) getRangesInfo() []roachpb.RangeInfo {
	panic("getRangesInfo() called on SpanKVFetcher")
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
func (f *fkBatchChecker) addCheck(
	ctx context.Context, row tree.Datums, source *baseFKHelper, traceKV bool,
) error {
	span, err := source.spanForValues(row)
	if err != nil {
		return err
	}
	r := roachpb.RequestUnion{}
	scan := roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
	}
	if traceKV {
		log.VEventf(ctx, 2, "FKScan %s", span)
	}
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

	fetcher := SpanKVFetcher{}
	for i, resp := range br.Responses {
		fk := f.batchIdxToFk[i]
		fetcher.KVs = resp.GetInner().(*roachpb.ScanResponse).Rows
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
					"foreign key violation: value %s not found in %s@%s %s (txn=%s)",
					fkValues, fk.searchTable.Name, fk.searchIdx.Name, fk.searchIdx.ColumnNames[:fk.prefixLen], f.txn.ID())
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
	fks map[sqlbase.IndexID][]baseFKHelper

	checker *fkBatchChecker
}

var errSkipUnusedFK = errors.New("no columns involved in FK included in writer")

func makeFKInsertHelper(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables TableLookupsByID,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
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
				h.fks = make(map[sqlbase.IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}
	return h, nil
}

func (h fkInsertHelper) addAllIdxChecks(ctx context.Context, row tree.Datums, traceKV bool) error {
	if len(h.fks) == 0 {
		return nil
	}
	for idx := range h.fks {
		if err := checkIdx(ctx, h.checker, h.fks, idx, row, traceKV); err != nil {
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

func checkIdx(
	ctx context.Context,
	checker *fkBatchChecker,
	fks map[sqlbase.IndexID][]baseFKHelper,
	idx sqlbase.IndexID,
	row tree.Datums,
	traceKV bool,
) error {
outer:
	for i, fk := range fks[idx] {
		// See https://github.com/cockroachdb/cockroach/issues/20305 or
		// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
		// different composite foreign key matching methods.
		switch fk.ref.Match {
		case sqlbase.ForeignKeyReference_SIMPLE:
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return pgerror.NewAssertionErrorf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if row[found] == tree.DNull {
					continue outer
				}
			}
			if err := checker.addCheck(ctx, row, &fks[idx][i], traceKV); err != nil {
				return err
			}
		case sqlbase.ForeignKeyReference_FULL:
			var nulls, notNulls bool
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return pgerror.NewAssertionErrorf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if row[found] == tree.DNull {
					nulls = true
				} else {
					notNulls = true
				}
				if nulls && notNulls {
					// TODO(bram): expand this error to show more details.
					return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
						"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
						row, fk.ref.Name,
					)
				}
			}
			// Never check references for MATCH FULL that are all nulls.
			if nulls {
				continue
			}
			if err := checker.addCheck(ctx, row, &fks[idx][i], traceKV); err != nil {
				return err
			}
		default:
			return pgerror.NewAssertionErrorf("unknown composite key match type: %v", fk.ref.Match)
		}
	}
	return nil
}

type fkDeleteHelper struct {
	fks         map[sqlbase.IndexID][]baseFKHelper
	otherTables TableLookupsByID
	alloc       *sqlbase.DatumAlloc

	checker *fkBatchChecker
}

func makeFKDeleteHelper(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables TableLookupsByID,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
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
				h.fks = make(map[sqlbase.IndexID][]baseFKHelper)
			}
			h.fks[idx.ID] = append(h.fks[idx.ID], fk)
		}
	}
	return h, nil
}

func (h fkDeleteHelper) addAllIdxChecks(ctx context.Context, row tree.Datums, traceKV bool) error {
	if len(h.fks) == 0 {
		return nil
	}
	for idx := range h.fks {
		if err := checkIdx(ctx, h.checker, h.fks, idx, row, traceKV); err != nil {
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

	indexIDsToCheck map[sqlbase.IndexID]struct{} // List of Index IDs to check

	checker *fkBatchChecker
}

func makeFKUpdateHelper(
	txn *client.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables TableLookupsByID,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkUpdateHelper, error) {
	ret := fkUpdateHelper{
		indexIDsToCheck: make(map[sqlbase.IndexID]struct{}),
	}
	var err error
	if ret.inbound, err = makeFKDeleteHelper(txn, table, otherTables, colMap, alloc); err != nil {
		return ret, err
	}
	ret.outbound, err = makeFKInsertHelper(txn, table, otherTables, colMap, alloc)
	ret.outbound.checker = ret.inbound.checker
	ret.checker = ret.inbound.checker
	return ret, err
}

func (fks fkUpdateHelper) addCheckForIndex(
	indexID sqlbase.IndexID, descriptorType sqlbase.IndexDescriptor_Type,
) {
	if descriptorType == sqlbase.IndexDescriptor_FORWARD {
		fks.indexIDsToCheck[indexID] = struct{}{}
	}
}

func (fks fkUpdateHelper) addIndexChecks(
	ctx context.Context, oldValues, newValues tree.Datums, traceKV bool,
) error {
	for indexID := range fks.indexIDsToCheck {
		if err := checkIdx(ctx, fks.checker, fks.inbound.fks, indexID, oldValues, traceKV); err != nil {
			return err
		}
		if err := checkIdx(ctx, fks.checker, fks.outbound.fks, indexID, newValues, traceKV); err != nil {
			return err
		}
	}
	return nil
}

func (fks fkUpdateHelper) hasFKs() bool {
	return len(fks.inbound.fks) > 0 || len(fks.outbound.fks) > 0
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
	rf           Fetcher
	searchTable  *sqlbase.ImmutableTableDescriptor // the table being searched (for err msg)
	searchIdx    *sqlbase.IndexDescriptor          // the index that must (not) contain a value
	prefixLen    int
	writeIdx     sqlbase.IndexDescriptor  // the index we want to modify
	searchPrefix []byte                   // prefix of keys in searchIdx
	ids          map[sqlbase.ColumnID]int // col IDs
	dir          FKCheck                  // direction of check
	ref          sqlbase.ForeignKeyReference
}

func makeBaseFKHelper(
	txn *client.Txn,
	otherTables TableLookupsByID,
	writeIdx sqlbase.IndexDescriptor,
	ref sqlbase.ForeignKeyReference,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
	dir FKCheck,
) (baseFKHelper, error) {
	b := baseFKHelper{
		txn:         txn,
		writeIdx:    writeIdx,
		searchTable: otherTables[ref.Table].Table,
		dir:         dir,
		ref:         ref,
	}
	if b.searchTable == nil {
		return b, errors.Errorf("referenced table %d not in provided table map %+v", ref.Table, otherTables)
	}
	b.searchPrefix = sqlbase.MakeIndexKeyPrefix(b.searchTable.TableDesc(), ref.Index)
	searchIdx, err := b.searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return b, err
	}
	b.prefixLen = len(searchIdx.ColumnIDs)
	if len(writeIdx.ColumnIDs) < b.prefixLen {
		b.prefixLen = len(writeIdx.ColumnIDs)
	}
	b.searchIdx = searchIdx
	tableArgs := FetcherTableArgs{
		Desc:             b.searchTable,
		Index:            b.searchIdx,
		ColIdxMap:        b.searchTable.ColumnIdxMap(),
		IsSecondaryIndex: b.searchIdx.ID != b.searchTable.PrimaryIndex.ID,
		Cols:             b.searchTable.Columns,
	}
	err = b.rf.Init(false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, alloc, tableArgs)
	if err != nil {
		return b, err
	}

	// See https://github.com/cockroachdb/cockroach/issues/20305 or
	// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
	// different composite foreign key matching methods.
	b.ids = make(map[sqlbase.ColumnID]int, len(writeIdx.ColumnIDs))
	switch ref.Match {
	case sqlbase.ForeignKeyReference_SIMPLE:
		for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
			if found, ok := colMap[writeColID]; ok {
				b.ids[searchIdx.ColumnIDs[i]] = found
			} else {
				return b, errSkipUnusedFK
			}
		}
		return b, nil
	case sqlbase.ForeignKeyReference_FULL:
		var missingColumns []string
		for i, writeColID := range writeIdx.ColumnIDs[:b.prefixLen] {
			if found, ok := colMap[writeColID]; ok {
				b.ids[searchIdx.ColumnIDs[i]] = found
			} else {
				missingColumns = append(missingColumns, writeIdx.ColumnNames[i])
			}
		}
		switch len(missingColumns) {
		case 0:
			return b, nil
		case 1:
			return b, errors.Errorf("missing value for column %q in multi-part foreign key", missingColumns[0])
		case b.prefixLen:
			// All the columns are nulls, don't check the foreign key.
			return b, errSkipUnusedFK
		default:
			sort.Strings(missingColumns)
			return b, errors.Errorf("missing values for columns %q in multi-part foreign key", missingColumns)
		}
	default:
		return baseFKHelper{}, pgerror.NewAssertionErrorf(
			"unknown composite key match type: %v", ref.Match,
		)
	}
}

func (f baseFKHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	var key roachpb.Key
	if values != nil {
		span, _, err := sqlbase.EncodePartialIndexSpan(
			f.searchTable.TableDesc(), f.searchIdx, f.prefixLen, f.ids, values, f.searchPrefix)
		return span, err
	}
	key = roachpb.Key(f.searchPrefix)
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

func collectSpansWithFKMap(fks map[sqlbase.IndexID][]baseFKHelper) roachpb.Spans {
	var reads roachpb.Spans
	for idx := range fks {
		for _, fk := range fks[idx] {
			reads = append(reads, fk.span())
		}
	}
	return reads
}

func collectSpansForValuesWithFKMap(
	fks map[sqlbase.IndexID][]baseFKHelper, values tree.Datums,
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
