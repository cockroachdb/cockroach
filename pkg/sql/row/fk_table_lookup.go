// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// The facilities in this file serve as interface between the FK
// planning code and the SQL schema. They provide a cache of the
// mapping between table ID and table metadata.
//
// Only the table metadata used for FK work are considered here.
// Because CASCADE actions can affect arbitrary many tables, possibly
// in cycles, the analysis algorithm to load the metadata uses a queue
// (tableLookupQueue) instead of a naive recursion.
//

//
// ------- interface between prepare and execution of FK work --------
//

// FkTableMetadata maps table IDs to looked up descriptors or, for tables that
// exist but are not yet public/leasable, entries with just the IsAdding flag.
//
// This is populated by the lookup queue (below) and used as input to
// the FK existence checkers and cascading actions.
//
// TODO(knz): the redundancy between this struct and the code in other
// packages (sql, sqlbase) is troubling! Some of this should be
// factored.
type FkTableMetadata map[TableID]TableEntry

// TableEntry is the value type of FkTableMetadata: An optional table
// descriptor, populated when the table is public/leasable, and an IsAdding
// flag.
//
// This also includes an optional CheckHelper for the table (for CHECK
// constraints). This is needed for FK work because CASCADE actions
// can modify rows, and CHECK constraints must be applied to rows
// modified by CASCADE.
type TableEntry struct {
	// Desc is the descriptor of the table. This can be nil if eg.
	// the table is not public.
	Desc *sqlbase.ImmutableTableDescriptor

	// IsAdding indicates the descriptor is being created.
	IsAdding bool

	// CheckHelper is the utility responsible for CHECK constraint
	// checks. The lookup function (see TableLookupFunction below) needs
	// not populate this field; this is populated by the lookup queue
	// below.
	CheckHelper *sqlbase.CheckHelper
}

//
// ------- table metadata lookup logic, used at start of query execution -------
//

// TableID is an alias for sqlbase.TableID (table IDs).
type TableID = sqlbase.ID

// tableLookupQueue is the facility responsible for loading all
// the table metadata used by FK work into a FkTableMetadata.
//
// The main lookup loop in MakeFkMetadata repeats as follows: run
// dequeue() once, inspects the table, queue()s zero or more FK
// constraints for further lookups. The lookup stops
// when the queue becomes empty.
type tableLookupQueue struct {
	// queue contains the remaining lookups to perform.
	queue []tableLookupQueueElement

	// alreadyChecked notes which tables / constraints have already been
	// looked up, to avoid performing the same lookup work twice.
	alreadyChecked map[TableID]map[FKCheckType]struct{}

	// result contains the result of the overall lookup work.
	result FkTableMetadata

	// tblLookupFn is used to look up individual tables by ID. This
	// is typically provided by the caller, e.g. from the functions
	// in the `sql` package.
	tblLookupFn TableLookupFunction

	// privCheckFn is used to verify a table's privileges. This is
	// typically provided by the caller, e.g. from the functions in the
	// `sql` package.
	privCheckFn CheckPrivilegeFunction

	// analyzeExprFn is used to perform semantic analysis on scalar
	// expressions. This is not used for FK work directly but needed
	// during lookup to initialize the CHECK constraint helper in each
	// TableEntry object.
	analyzeExprFn sqlbase.AnalyzeExprFunction
}

// tableLookupQueueElement describes one unit of work in the lookup
// queue.
type tableLookupQueueElement struct {
	// tableEntry is the metadata of the table to check for FK
	// constraints.
	tableEntry TableEntry

	// usage is the type of mutation for which to look up additional
	// metadata. At the top level this is the type of SQL statement
	// performing a mutation. Then when there are CASCADE clauses
	// this is used to indicate the type of CASCADE action.
	usage FKCheckType
}

// FKCheckType indicates the type of mutation that triggers FK work
// (delete, insert, or both).
type FKCheckType int

const (
	// CheckDeletes checks if rows reference a changed value.
	CheckDeletes FKCheckType = iota
	// CheckInserts checks if a new value references an existing row.
	CheckInserts
	// CheckUpdates checks all references (CheckDeletes+CheckInserts).
	CheckUpdates
)

// TableLookupFunction is the function type used by MakeFkMetadata
// that will perform the actual lookup of table metadata.
type TableLookupFunction func(context.Context, TableID) (TableEntry, error)

// NoLookup is a stub that can be used to not actually fetch metadata.
// This can be used when the FK work is initialized from a pre-populated
// FkTableMetadata map.
func NoLookup(_ context.Context, _ TableID) (TableEntry, error) {
	return TableEntry{}, nil
}

// CheckPrivilegeFunction is the function type used by MakeFkMetadata that will
// check the privileges of the current user to access specific tables.
type CheckPrivilegeFunction func(context.Context, sqlbase.DescriptorProto, privilege.Kind) error

// NoCheckPrivilege is a stub that can be used to not actually verify privileges.
// This can be used when the FK work is initialized from a pre-populated
// FkTableMetadata map.
func NoCheckPrivilege(_ context.Context, _ sqlbase.DescriptorProto, _ privilege.Kind) error {
	return nil
}

// getTable retrieves one table's metadata during FK work preparation.
// A cached TableEntry, if one exists, is reused; otherwise it is
// created and initialized.
func (q *tableLookupQueue) getTable(ctx context.Context, tableID TableID) (TableEntry, error) {
	// Do we already have an entry for this table?
	if tableEntry, exists := q.result[tableID]; exists {
		// Yes, simply reuse it.
		return tableEntry, nil
	}

	// We don't have this table yet.

	// Ask the caller to retrieve it for us.
	tableEntry, err := q.tblLookupFn(ctx, tableID)
	if err != nil {
		return TableEntry{}, err
	}
	if !tableEntry.IsAdding && tableEntry.Desc != nil {
		// If we have a real table, we need first to verify the user has permission.
		if err := q.privCheckFn(ctx, tableEntry.Desc, privilege.SELECT); err != nil {
			return TableEntry{}, err
		}

		// All is fine. Simply prepare the CHECK helper for when there are
		// CASCADE actions.
		//
		// TODO(knz): the CHECK helper is always prepared here, even when
		// there is no CASCADE work to perform. This should be moved to a
		// different place.
		checkHelper, err := sqlbase.NewEvalCheckHelper(ctx, q.analyzeExprFn, tableEntry.Desc)
		if err != nil {
			return TableEntry{}, err
		}
		tableEntry.CheckHelper = checkHelper
	}

	// Remember for next time.
	q.result[tableID] = tableEntry

	return tableEntry, nil
}

// enqueue prepares the lookup work for a given table.
func (q *tableLookupQueue) enqueue(ctx context.Context, tableID TableID, usage FKCheckType) error {
	// Lookup the table.
	tableEntry, err := q.getTable(ctx, tableID)
	if err != nil {
		return err
	}

	// Don't enqueue if lookup returns an empty tableEntry. This just means that
	// there is no need to walk any further.
	if tableEntry.Desc == nil {
		return nil
	}

	// Only enqueue checks that haven't been performed yet.
	if alreadyCheckByTableID, exists := q.alreadyChecked[tableID]; exists {
		if _, existsInner := alreadyCheckByTableID[usage]; existsInner {
			return nil
		}
	} else {
		q.alreadyChecked[tableID] = make(map[FKCheckType]struct{})
	}

	// Remember we've done this check already for later.
	q.alreadyChecked[tableID][usage] = struct{}{}

	// If the table is being added, there's no need to check it.
	if tableEntry.IsAdding {
		return nil
	}

	// Verify the user has privilege to perform the operations.
	switch usage {
	// We only need to check the privileges for CASCADE actions here:
	// the privileges related to the main mutation statement are checked
	// already in that mutation's planning code.
	// Also, there is no CASCADE action that can insert new rows.
	case CheckDeletes:
		if err := q.privCheckFn(ctx, tableEntry.Desc, privilege.DELETE); err != nil {
			return err
		}
	case CheckUpdates:
		if err := q.privCheckFn(ctx, tableEntry.Desc, privilege.UPDATE); err != nil {
			return err
		}
	}

	// Queue more lookup processing.
	(*q).queue = append((*q).queue, tableLookupQueueElement{tableEntry: tableEntry, usage: usage})

	return nil
}

// dequeue retrieves the next item in the queue (and pops it).
func (q *tableLookupQueue) dequeue() (TableEntry, FKCheckType, bool) {
	if len((*q).queue) == 0 {
		return TableEntry{}, 0, false
	}
	elem := (*q).queue[0]
	(*q).queue = (*q).queue[1:]
	return elem.tableEntry, elem.usage, true
}
