// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

type tableEventType uint64

//go:generate stringer --type tableEventType --trimprefix tableEvent

const (
	tableEventUnknown tableEventType = iota
	tableEventAddColumnNoBackfill
	tableEventAddColumnWithBackfill
	tableEventDropColumn
	tableEventTruncate
	tableEventPrimaryKeyChange
	tableEventLocalityRegionalByRowChange
	tableEventAddHiddenColumn
	numEventTypes int = iota
)

type tableEventTypeSet uint64

var (
	defaultTableEventFilter = tableEventFilter{
		tableEventDropColumn:                  false,
		tableEventAddColumnWithBackfill:       false,
		tableEventAddColumnNoBackfill:         true,
		tableEventUnknown:                     true,
		tableEventPrimaryKeyChange:            false,
		tableEventLocalityRegionalByRowChange: false,
		tableEventAddHiddenColumn:             true,
	}

	columnChangeTableEventFilter = tableEventFilter{
		tableEventDropColumn:                  false,
		tableEventAddColumnWithBackfill:       false,
		tableEventAddColumnNoBackfill:         false,
		tableEventUnknown:                     true,
		tableEventPrimaryKeyChange:            false,
		tableEventLocalityRegionalByRowChange: false,
		tableEventAddHiddenColumn:             true,
	}

	schemaChangeEventFilters = map[changefeedbase.SchemaChangeEventClass]tableEventFilter{
		changefeedbase.OptSchemaChangeEventClassDefault:      defaultTableEventFilter,
		changefeedbase.OptSchemaChangeEventClassColumnChange: columnChangeTableEventFilter,
	}
)

// Contains returns true if the receiver includes the given event
// types.
func (e tableEventTypeSet) Contains(event tableEventType) bool {
	return e&event.mask() != 0
}

func (e tableEventType) mask() tableEventTypeSet {
	if e == 0 {
		return 0
	}
	return 1 << (e - 1)
}

// Clear returns a new tableEventType with the given event types
// cleared.
func (e tableEventTypeSet) Clear(event tableEventType) tableEventTypeSet {
	return e & (^event.mask())
}

func (e tableEventTypeSet) empty() bool { return e == 0 }

func (e tableEventTypeSet) String() string {
	if e.empty() {
		return tableEventUnknown.String()
	}
	var strs []string
	for et := tableEventType(1); int(et) < numEventTypes; et++ {
		if e.Contains(et) {
			strs = append(strs, et.String())
		}
	}
	return strings.Join(strs, "|")
}

func classifyTableEvent(e TableEvent) tableEventTypeSet {
	var et tableEventTypeSet
	for _, c := range []struct {
		eventType tableEventType
		predicate func(event TableEvent) bool
	}{
		{tableEventPrimaryKeyChange, primaryKeyChanged},
		{tableEventAddColumnWithBackfill, newVisibleColumnBackfillComplete},
		{tableEventAddHiddenColumn, newHiddenColumnBackfillComplete},
		{tableEventAddColumnNoBackfill, newVisibleColumnNoBackfill},
		{tableEventDropColumn, hasNewVisibleColumnDropBackfillMutation},
		{tableEventTruncate, tableTruncated},
		{tableEventLocalityRegionalByRowChange, regionalByRowChanged},
	} {
		if c.predicate(e) {
			et |= c.eventType.mask()
		}
	}
	return et
}

// typeFilters indicates whether a table event of a given type should be
// permitted by the filter.
type tableEventFilter map[tableEventType]bool

func (filter tableEventFilter) shouldFilter(
	ctx context.Context, e TableEvent, targets changefeedbase.Targets,
) (bool, error) {
	et := classifyTableEvent(e)

	// Truncation events are not ignored and return an error.
	if et.Contains(tableEventTruncate) {
		return false, errors.Errorf(`"%s" was truncated`, e.Before.GetName())
	}

	if et.empty() {
		shouldFilter, ok := filter[tableEventUnknown]
		if !ok {
			return false, errors.AssertionFailedf("policy does not specify how to handle event type %v", et)
		}
		return shouldFilter, nil
	}

	shouldFilter := true
	for filterEvent, filterPolicy := range filter {
		if et.Contains(filterEvent) && !filterPolicy {
			// Apply changefeed target-specific filters.
			// In some cases, a drop column event should be filtered out.
			// For example, we may be dropping a column which is not
			// monitored by the changefeed.
			if filterEvent == tableEventDropColumn {
				sf, err := shouldFilterDropColumnEvent(e, targets)
				if err != nil {
					return false, err
				}
				shouldFilter = sf && shouldFilter
			} else if filterEvent == tableEventAddColumnNoBackfill || filterEvent == tableEventAddColumnWithBackfill {
				sf, err := shouldFilterAddColumnEvent(e, targets)
				if err != nil {
					return false, err
				}
				shouldFilter = sf && shouldFilter
			} else {
				shouldFilter = false
			}
		}
		et = et.Clear(filterEvent)
	}
	if et > 0 {
		return false, errors.AssertionFailedf("policy does not specify how to handle event (unhandled event types: %v)", et)
	}
	return shouldFilter, nil
}

// shouldFilterDropColumnEvent decides if we should filter out a drop column event.
func shouldFilterDropColumnEvent(e TableEvent, targets changefeedbase.Targets) (bool, error) {
	watched, err := droppedColumnIsWatched(e, targets)
	if err != nil {
		return false, err
	}
	return !watched, nil
}

// shouldFilterAddColumnEvent decides if we should filter out an add column event.
func shouldFilterAddColumnEvent(e TableEvent, targets changefeedbase.Targets) (bool, error) {
	watched, err := addedColumnIsWatched(e, targets)
	if err != nil {
		return false, err
	}
	return !watched, nil
}

// Returns true if the changefeed targets a column which has a drop mutation inside the table event.
func droppedColumnIsWatched(e TableEvent, targets changefeedbase.Targets) (bool, error) {
	// If no column families are specified, then all columns are targeted.
	specifiedColumnFamiliesForTable := targets.GetSpecifiedColumnFamilies(e.Before.GetID())
	if len(specifiedColumnFamiliesForTable) == 0 {
		return true, nil
	}

	var watchedColumnIDs intsets.Fast
	if err := e.Before.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		if _, ok := specifiedColumnFamiliesForTable[family.Name]; ok {
			for _, columnID := range family.ColumnIDs {
				watchedColumnIDs.Add(int(columnID))
			}
		}
		return nil
	}); err != nil {
		return false, err
	}

	for _, m := range e.After.AllMutations() {
		if m.AsColumn() == nil || m.AsColumn().IsHidden() {
			continue
		}
		if m.Dropped() && m.WriteAndDeleteOnly() && watchedColumnIDs.Contains(int(m.AsColumn().GetID())) {
			return true, nil
		}
	}

	return false, nil
}

// Returns true if the changefeed targets a column to be added will be added to a watched column family.
func addedColumnIsWatched(e TableEvent, targets changefeedbase.Targets) (bool, error) {
	// If no column families are specified, then all columns are targeted.
	specifiedColumnFamiliesForTable := targets.GetSpecifiedColumnFamilies(e.Before.GetID())
	if len(specifiedColumnFamiliesForTable) == 0 {
		return true, nil
	}

	if len(e.Before.VisibleColumns()) >= len(e.After.VisibleColumns()) {
		return false, nil
	}

	var beforeCols intsets.Fast
	for _, col := range e.Before.VisibleColumns() {
		beforeCols.Add(int(col.GetID()))
	}
	var addedCols intsets.Fast
	for _, col := range e.After.VisibleColumns() {
		colID := int(col.GetID())
		if !beforeCols.Contains(colID) {
			addedCols.Add(colID)
		}
	}

	res := false
	if err := e.Before.ForeachFamily(func(family *descpb.ColumnFamilyDescriptor) error {
		if _, ok := specifiedColumnFamiliesForTable[family.Name]; ok {
			for _, colID := range family.ColumnIDs {
				if addedCols.Contains(int(colID)) {
					res = true
					return nil
				}
			}
		}
		return nil
	}); err != nil {
		return false, err
	}

	return res, nil
}

func hasNewVisibleColumnDropBackfillMutation(e TableEvent) (res bool) {
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropVisibleColumnMutationExists(e.Before) && dropVisibleColumnMutationExists(e.After)
}

func dropVisibleColumnMutationExists(desc catalog.TableDescriptor) bool {
	for _, m := range desc.AllMutations() {
		if m.AsColumn() == nil || m.AsColumn().IsHidden() {
			continue
		}
		if m.Dropped() && m.WriteAndDeleteOnly() {
			return true
		}
	}
	return false
}

func newVisibleColumnBackfillComplete(e TableEvent) (res bool) {
	// TODO(ajwerner): What is the case where the before has a backfill mutation
	// and the After doesn't? What about other queued mutations?
	return len(e.Before.VisibleColumns()) < len(e.After.VisibleColumns()) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}

func newHiddenColumnBackfillComplete(e TableEvent) (res bool) {
	return len(e.Before.VisibleColumns()) == len(e.After.VisibleColumns()) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}

func newVisibleColumnNoBackfill(e TableEvent) (res bool) {
	return len(e.Before.VisibleColumns()) < len(e.After.VisibleColumns()) &&
		!e.Before.HasColumnBackfillMutation()
}

func pkChangeMutationExists(desc catalog.TableDescriptor) bool {
	for _, m := range desc.AllMutations() {
		if m.Adding() && m.AsPrimaryKeySwap() != nil {
			return true
		}
	}
	// For declarative schema changer check if we are trying to add
	// a primary index.
	if desc.GetDeclarativeSchemaChangerState() != nil {
		for idx, target := range desc.GetDeclarativeSchemaChangerState().Targets {
			if target.GetPrimaryIndex() != nil &&
				desc.GetDeclarativeSchemaChangerState().CurrentStatuses[idx] != scpb.Status_PUBLIC {
				return true
			}
		}
	}
	return false
}

func tableTruncated(e TableEvent) bool {
	// A table was truncated if the primary index has changed, but an ALTER
	// PRIMARY KEY statement was not performed. TRUNCATE operates by creating
	// a new set of indexes for the table, including a new primary index.
	return e.Before.GetPrimaryIndexID() != e.After.GetPrimaryIndexID() && !pkChangeMutationExists(e.Before)
}

func primaryKeyChanged(e TableEvent) bool {
	return e.Before.GetPrimaryIndexID() != e.After.GetPrimaryIndexID() &&
		pkChangeMutationExists(e.Before)
}

func regionalByRowChanged(e TableEvent) bool {
	return e.Before.IsLocalityRegionalByRow() != e.After.IsLocalityRegionalByRow()
}

func hasNewPrimaryIndexWithNoVisibleColumnChanges(
	e TableEvent, targets changefeedbase.Targets,
) bool {
	before, after := e.Before.GetPrimaryIndex(), e.After.GetPrimaryIndex()
	// Check if there is a change in the primary key.
	if before.GetID() == after.GetID() ||
		before.NumKeyColumns() != after.NumKeyColumns() {
		return false
	}
	for i, n := 0, before.NumKeyColumns(); i < n; i++ {
		if before.GetKeyColumnID(i) != after.GetKeyColumnID(i) {
			return false
		}
	}

	// Check other columns.
	targetFamilies := targets.GetSpecifiedColumnFamilies(e.Before.GetID())
	hasSpecificColumnTargets := len(targetFamilies) > 0
	collectPublicStoredColumns := func(
		idx catalog.Index, tab catalog.TableDescriptor,
	) (cols catalog.TableColSet) {

		// Generate a set of watched columns if the targets contains specific columns.
		var targetedCols intsets.Fast
		if hasSpecificColumnTargets {
			err := tab.ForeachFamily(func(fam *descpb.ColumnFamilyDescriptor) error {
				if _, ok := targetFamilies[fam.Name]; ok {
					for _, colID := range fam.ColumnIDs {
						targetedCols.Add(int(colID))
					}
				}
				return nil
			})
			if err != nil {
				panic(err)
			}
		}

		for i, n := 0, idx.NumPrimaryStoredColumns(); i < n; i++ {
			colID := idx.GetStoredColumnID(i)
			col := catalog.FindColumnByID(tab, colID)

			// If specific columns are targeted, then only consider the column if it is targeted.
			if col.Public() && (!hasSpecificColumnTargets || targetedCols.Contains(int(col.GetID()))) {
				cols.Add(colID)
			}
		}
		return cols
	}
	storedBefore := collectPublicStoredColumns(before, e.Before)
	storedAfter := collectPublicStoredColumns(after, e.After)
	return storedBefore.Len() == storedAfter.Len() &&
		storedBefore.Difference(storedAfter).Empty()
}

// IsPrimaryIndexChange returns true if the event corresponds to a change
// in the primary index. It also returns whether the primary index change
// corresponds to any change in the visible column set or key ordering
// stored by the primary key .
// This is useful because when the declarative schema changer drops a column,
// it does so by adding a new primary index with the column excluded and
// then swaps to the new primary index. The column logically disappears
// before the index swap occurs. We want to detect the case of this index
// swap and not stop changefeeds which are programmed to stop upon schema
// changes.
// If targets is non-empty, then this function will only count columns
// in target column families when determining if there are no column changes.
// Otherwise, this function will check for changes across all columns.
func IsPrimaryIndexChange(
	e TableEvent, targets changefeedbase.Targets,
) (isPrimaryIndexChange, noVisibleOrderOrColumnChanges bool) {
	isPrimaryIndexChange = classifyTableEvent(e).Contains(tableEventPrimaryKeyChange)
	if isPrimaryIndexChange {
		noVisibleOrderOrColumnChanges = hasNewPrimaryIndexWithNoVisibleColumnChanges(e, targets)
	}
	return isPrimaryIndexChange, noVisibleOrderOrColumnChanges
}

// IsOnlyPrimaryIndexChange returns to true if the event corresponds
// to a change in the primary index and _only_ a change in the primary
// index.
func IsOnlyPrimaryIndexChange(e TableEvent) bool {
	return classifyTableEvent(e) == tableEventPrimaryKeyChange.mask()
}

// IsRegionalByRowChange returns true if the event corresponds to a
// change in the table's locality to or from RegionalByRow.
func IsRegionalByRowChange(e TableEvent) bool {
	return classifyTableEvent(e).Contains(tableEventLocalityRegionalByRowChange)
}
