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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
