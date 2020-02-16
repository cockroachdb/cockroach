// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var defaultBackfillPolicy = backfillPolicy{
	addColumn:  true,
	dropColumn: true,
}

type backfillPolicy struct {
	addColumn  bool
	dropColumn bool
}

func (b backfillPolicy) ShouldFilter(ctx context.Context, e schemafeed.TableEvent) (bool, error) {
	interestingEvent := (b.addColumn && newColumnBackfillComplete(e)) ||
		(b.dropColumn && hasNewColumnDropBackfillMutation(e))
	return !interestingEvent, nil
}

func hasNewColumnDropBackfillMutation(e schemafeed.TableEvent) (res bool) {
	dropMutationExists := func(desc *sqlbase.TableDescriptor) bool {
		for _, m := range desc.Mutations {
			if m.Direction == sqlbase.DescriptorMutation_DROP &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return true
			}
		}
		return false
	}
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropMutationExists(e.Before) && dropMutationExists(e.After)
}

func newColumnBackfillComplete(e schemafeed.TableEvent) (res bool) {
	return len(e.Before.Columns) < len(e.After.Columns) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}
