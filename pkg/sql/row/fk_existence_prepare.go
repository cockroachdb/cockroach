// Copyright 2016 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// MakeFkMetadata populates a map of FkTableMetadata for all the
// TableDescriptors that might be needed when performing FK checking for delete
// and/or insert operations. It uses the passed in lookup function to perform
// the actual lookup. The caller is expected to create the CheckHelper for the
// given mutatedTable. However, if cascading is required, then the FK code will
// need to create additional CheckHelpers. It does this using the
// AnalyzeExprFunction, TableLookupFunction, and CheckPrivilegeFunction
// functions, so these must be provided if there's a possibility of a cascading
// operation.
func MakeFkMetadata(
	ctx context.Context,
	mutatedTable *sqlbase.ImmutableTableDescriptor,
	startUsage FKCheckType,
	tblLookupFn TableLookupFunction,
	privCheckFn CheckPrivilegeFunction,
	analyzeExprFn sqlbase.AnalyzeExprFunction,
	checkHelper *sqlbase.CheckHelper,
) (FkTableMetadata, error) {
	// Initialize the lookup queue.
	queue := tableLookupQueue{
		result:         make(FkTableMetadata),
		alreadyChecked: make(map[TableID]map[FKCheckType]struct{}),
		tblLookupFn:    tblLookupFn,
		privCheckFn:    privCheckFn,
		analyzeExprFn:  analyzeExprFn,
	}

	// Add the passed in table descriptor to the table lookup.
	//
	// This logic is very close to (*tableLookupQueue).getTable()
	// however differs in two important aspects:
	// - we are not checking the privilege; the caller
	//   will have done that given the type of SQL mutation statement.
	//   For example UPDATE wants to check the table for UPDATE privilege.
	// - we do process the mutatedTable that's given even if it is
	//   in "adding" state or non-public.
	//
	queue.result[mutatedTable.ID] = TableEntry{Desc: mutatedTable, CheckHelper: checkHelper}
	if err := queue.enqueue(ctx, mutatedTable.ID, startUsage); err != nil {
		return nil, err
	}

	// Main lookup queue.
	for {
		// Pop one unit of work.
		tableEntry, usage, hasWork := queue.dequeue()
		if !hasWork {
			return queue.result, nil
		}

		// If the table descriptor is nil it means that there was no actual lookup
		// performed. Meaning there is no need to walk any secondary relationships
		// and the table descriptor lookup will happen later.
		//
		// TODO(knz): the paragraph above is suspicious. A nil table desc
		// indicates the table is non-public. In either case it seems that
		// if there is a descriptor we ought to carry out the FK
		// work. What gives?
		if tableEntry.IsAdding || tableEntry.Desc == nil {
			continue
		}

		// Explore all the FK constraints on the table/.
		for _, idx := range tableEntry.Desc.AllNonDropIndexes() {

			if usage == CheckInserts || usage == CheckUpdates {
				// If the mutation performed is an insertion or an update,
				// we'll need to do existence checks on the referenced
				// table(s), if any.
				if idx.ForeignKey.IsSet() {
					if _, err := queue.getTable(ctx, idx.ForeignKey.Table); err != nil {
						return nil, err
					}
				}
			}

			if usage == CheckDeletes || usage == CheckUpdates {
				// If the mutaiton performed is a deletion or an update,
				// we'll need to do existence checks on the referencing
				// table(s), if any, as well as cascading actions.
				for _, ref := range idx.ReferencedBy {
					// The referencing table is required to know the relationship, so
					// fetch it here.
					referencingTableEntry, err := queue.getTable(ctx, ref.Table)
					if err != nil {
						return nil, err
					}

					// Again here if the table descriptor is nil it means that there was
					// no actual lookup performed. Meaning there is no need to walk any
					// secondary relationships.
					//
					// TODO(knz): this comment is suspicious for the same
					// reasons as above.
					if referencingTableEntry.IsAdding || referencingTableEntry.Desc == nil {
						continue
					}

					// Find the index that carries the constraint metadata.
					//
					// TODO(knz,bram): constraint metadata should not be carried
					// by index descriptors! We need to find a different way to
					// encode this.
					referencedIdx, err := referencingTableEntry.Desc.FindIndexByID(ref.Index)
					if err != nil {
						return nil, err
					}

					if usage == CheckDeletes {
						var nextUsage FKCheckType
						switch referencedIdx.ForeignKey.OnDelete {
						case sqlbase.ForeignKeyReference_CASCADE:
							nextUsage = CheckDeletes
						case sqlbase.ForeignKeyReference_SET_DEFAULT, sqlbase.ForeignKeyReference_SET_NULL:
							nextUsage = CheckUpdates
						default:
							// There is no need to check any other relationships.
							continue
						}
						if err := queue.enqueue(ctx, referencingTableEntry.Desc.ID, nextUsage); err != nil {
							return nil, err
						}
					} else {
						// curUsage == CheckUpdates
						if referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_CASCADE ||
							referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_DEFAULT ||
							referencedIdx.ForeignKey.OnUpdate == sqlbase.ForeignKeyReference_SET_NULL {
							if err := queue.enqueue(
								ctx, referencingTableEntry.Desc.ID, CheckUpdates,
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
