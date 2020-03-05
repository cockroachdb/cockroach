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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type tableEventType int

const (
	tableEventTypeUnknown tableEventType = iota
	tableEventTypeAddColumnNoBackfill
	tableEventTypeAddColumnWithBackfill
	tableEventTypeDropColumn
)

var (
	defaultTableEventFilter = tableEventFilter{
		tableEventTypeDropColumn:            false,
		tableEventTypeAddColumnWithBackfill: false,
		tableEventTypeAddColumnNoBackfill:   true,
		tableEventTypeUnknown:               true,
	}

	columnChangeTableEventFilter = tableEventFilter{
		tableEventTypeDropColumn:            false,
		tableEventTypeAddColumnWithBackfill: false,
		tableEventTypeAddColumnNoBackfill:   false,
		tableEventTypeUnknown:               true,
	}

	schemaChangeEventFilters = map[changefeedbase.SchemaChangeEventClass]tableEventFilter{
		changefeedbase.OptSchemaChangeEventClassDefault:      defaultTableEventFilter,
		changefeedbase.OptSchemaChangeEventClassColumnChange: columnChangeTableEventFilter,
	}
)

func classifyTableEvent(e TableEvent) tableEventType {
	switch {
	case newColumnBackfillComplete(e):
		return tableEventTypeAddColumnWithBackfill
	case newColumnNoBackfill(e):
		return tableEventTypeAddColumnNoBackfill
	case hasNewColumnDropBackfillMutation(e):
		return tableEventTypeDropColumn
	default:
		return tableEventTypeUnknown
	}
}

// typeFilters indicates whether a table event of a given type should be
// permitted by the filter.
type tableEventFilter map[tableEventType]bool

func (b tableEventFilter) shouldFilter(ctx context.Context, e TableEvent) (bool, error) {
	et := classifyTableEvent(e)
	shouldFilter, ok := b[et]
	if !ok {
		return false, errors.AssertionFailedf("policy does not specify how to handle event type %v", et)
	}
	return shouldFilter, nil
}

func hasNewColumnDropBackfillMutation(e TableEvent) (res bool) {
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropMutationExists(e.Before) && dropMutationExists(e.After)
}

func dropMutationExists(desc *sqlbase.TableDescriptor) bool {
	for _, m := range desc.Mutations {
		if m.Direction == sqlbase.DescriptorMutation_DROP &&
			m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
			return true
		}
	}
	return false
}

func newColumnBackfillComplete(e TableEvent) (res bool) {
	// TODO(ajwerner): What is the case where the before has a backfill mutation
	// and the After doesn't? What about other queued mutations?
	return len(e.Before.Columns) < len(e.After.Columns) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}

func newColumnNoBackfill(e TableEvent) (res bool) {
	return len(e.Before.Columns) < len(e.After.Columns) &&
		!e.Before.HasColumnBackfillMutation()
}
