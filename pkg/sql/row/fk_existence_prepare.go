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

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// The utilities in this file facilitate the prepare phase of FK
// existence checks, during or at the end of logical planning, and
// before execution starts.

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
