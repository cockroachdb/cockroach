// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
)

type tableEventType uint64

const (
	tableEventTypeUnknown             tableEventType = 0
	tableEventTypeAddColumnNoBackfill tableEventType = 1 << (iota - 1)
	tableEventTypeAddColumnWithBackfill
	tableEventTypeDropColumn
	tableEventTruncate
	tableEventPrimaryKeyChange
	tableEventLocalityRegionalByRowChange
)

var (
	defaultTableEventFilter = tableEventFilter{
		tableEventTypeDropColumn:              false,
		tableEventTypeAddColumnWithBackfill:   false,
		tableEventTypeAddColumnNoBackfill:     true,
		tableEventTypeUnknown:                 true,
		tableEventPrimaryKeyChange:            false,
		tableEventLocalityRegionalByRowChange: false,
	}

	columnChangeTableEventFilter = tableEventFilter{
		tableEventTypeDropColumn:              false,
		tableEventTypeAddColumnWithBackfill:   false,
		tableEventTypeAddColumnNoBackfill:     false,
		tableEventTypeUnknown:                 true,
		tableEventPrimaryKeyChange:            false,
		tableEventLocalityRegionalByRowChange: false,
	}

	schemaChangeEventFilters = map[changefeedbase.SchemaChangeEventClass]tableEventFilter{
		changefeedbase.OptSchemaChangeEventClassDefault:      defaultTableEventFilter,
		changefeedbase.OptSchemaChangeEventClassColumnChange: columnChangeTableEventFilter,
	}
)

// Contains returns true if the receiver includes the given event
// types.
func (e tableEventType) Contains(event tableEventType) bool {
	return e&event == event
}

// Clear returns a new tableEventType with the given event types
// cleared.
func (e tableEventType) Clear(event tableEventType) tableEventType {
	return e & (^event)
}

func classifyTableEvent(e TableEvent) tableEventType {
	et := tableEventTypeUnknown
	if primaryKeyChanged(e) {
		et = et | tableEventPrimaryKeyChange
	}

	if newColumnBackfillComplete(e) {
		et = et | tableEventTypeAddColumnWithBackfill
	}

	if newColumnNoBackfill(e) {
		et = et | tableEventTypeAddColumnNoBackfill
	}

	if hasNewColumnDropBackfillMutation(e) {
		et = et | tableEventTypeDropColumn
	}

	if tableTruncated(e) {
		et = et | tableEventTruncate
	}

	if regionalByRowChanged(e) {
		et = et | tableEventLocalityRegionalByRowChange
	}

	return et
}

// typeFilters indicates whether a table event of a given type should be
// permitted by the filter.
type tableEventFilter map[tableEventType]bool

func (filter tableEventFilter) shouldFilter(ctx context.Context, e TableEvent) (bool, error) {
	et := classifyTableEvent(e)

	// Truncation events are not ignored and return an error.
	if et.Contains(tableEventTruncate) {
		return false, errors.Errorf(`"%s" was truncated`, e.Before.GetName())
	}

	if et == tableEventTypeUnknown {
		shouldFilter, ok := filter[tableEventTypeUnknown]
		if !ok {
			return false, errors.AssertionFailedf("policy does not specify how to handle event type %v", et)
		}
		return shouldFilter, nil
	}

	shouldFilter := true
	for filterEvent, filterPolicy := range filter {
		if et.Contains(filterEvent) && !filterPolicy {
			shouldFilter = false
		}
		et = et.Clear(filterEvent)
	}
	if et > 0 {
		return false, errors.AssertionFailedf("policy does not specify how to handle event (unhandled event types: %v)", et)
	}
	return shouldFilter, nil
}

func hasNewColumnDropBackfillMutation(e TableEvent) (res bool) {
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropColumnMutationExists(e.Before) && dropColumnMutationExists(e.After)
}

func dropColumnMutationExists(desc catalog.TableDescriptor) bool {
	for _, m := range desc.AllMutations() {
		if m.AsColumn() == nil {
			continue
		}
		if m.Dropped() && m.WriteAndDeleteOnly() {
			return true
		}
	}
	return false
}

func newColumnBackfillComplete(e TableEvent) (res bool) {
	// TODO(ajwerner): What is the case where the before has a backfill mutation
	// and the After doesn't? What about other queued mutations?
	return len(e.Before.PublicColumns()) < len(e.After.PublicColumns()) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}

func newColumnNoBackfill(e TableEvent) (res bool) {
	return len(e.Before.PublicColumns()) < len(e.After.PublicColumns()) &&
		!e.Before.HasColumnBackfillMutation()
}

func pkChangeMutationExists(desc catalog.TableDescriptor) bool {
	for _, m := range desc.AllMutations() {
		if m.Adding() && m.AsPrimaryKeySwap() != nil {
			return true
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

// IsPrimaryIndexChange returns true if the event corresponds to a change
// in the primary index.
func IsPrimaryIndexChange(e TableEvent) bool {
	et := classifyTableEvent(e)
	return et.Contains(tableEventPrimaryKeyChange)
}

// IsOnlyPrimaryIndexChange returns to true if the event corresponds
// to a change in the primary index and _only_ a change in the primary
// index.
func IsOnlyPrimaryIndexChange(e TableEvent) bool {
	et := classifyTableEvent(e)
	return et == tableEventPrimaryKeyChange
}

// IsRegionalByRowChange returns true if the event corresponds to a
// change in the table's locality to or from RegionalByRow.
func IsRegionalByRowChange(e TableEvent) bool {
	et := classifyTableEvent(e)
	return et.Contains(tableEventLocalityRegionalByRowChange)
}
