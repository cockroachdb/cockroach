// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"fmt"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

const sizeOfInt64 = int64(unsafe.Sizeof(int64(0)))

// mapEntryOverhead is a guess on how much space (in bytes)
// each item added to a map takes.
// More explanation in cockroach/pkg/sql/rowexec/aggregator.go variable
// hashAggregatorSizeOfBucketsItem
const mapEntryOverhead = 64

// alterAddFKStatements represents the column name for alter_statements in
// crdb_internal.create_statements.
const alterAddFKStatements = "alter_statements"

// alterValidateFKStatements represents the column name for validate_statements in
// crdb_internal.create_statements.
const alterValidateFKStatements = "validate_statements"

// foreignKeyValidationWarning is a warning letting the user know that
// the validate foreign key constraints may fail.
const foreignKeyValidationWarning = "-- Validate foreign key constraints. These can fail if there was unvalidated data during the SHOW CREATE ALL TABLES"

// getTopologicallySortedTableIDs returns the set of table ids sorted
// first by table id, then topologically ordered such that dependencies are
// ordered before tables that depend on them. (ie, sequences will appear before
// the table that uses the sequence).
// The tables are sorted by table id first to guarantee stable ordering.
func getTopologicallySortedTableIDs(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	dbName string,
	ts string,
	acc *mon.BoundAccount,
) ([]int64, error) {
	ids, err := getTableIDs(ctx, ie, txn, ts, dbName, acc)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}

	sizeOfMap := int64(0)
	// dependsOnIDs maps an id of a table to the ids it depends on.
	// We perform the topological sort on dependsOnIDs instead of on the
	// byID map to reduce memory usage.
	dependsOnIDs := make(map[int64][]int64)
	for _, tid := range ids {
		query := fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
		`, dbName, ts)
		it, err := ie.QueryIteratorEx(
			ctx,
			"crdb_internal.show_create_all_tables",
			txn,
			sessiondata.NoSessionDataOverride,
			query,
			tid,
		)
		if err != nil {
			return nil, err
		}

		var refs []int64
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			id := tree.MustBeDInt(it.Cur()[0])
			refs = append(refs, int64(id))
		}
		if err != nil {
			return nil, err
		}

		// Account for memory of map.
		sizeOfKeyValue := int64(unsafe.Sizeof(tid)) + int64(len(refs))*sizeOfInt64
		sizeOfMap += sizeOfKeyValue + mapEntryOverhead
		if err = acc.Grow(ctx, sizeOfKeyValue+mapEntryOverhead); err != nil {
			return nil, err
		}

		dependsOnIDs[tid] = refs
	}

	// First sort by ids to guarantee stable output.
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	// Collect transitive dependencies in topological order into collected.
	// The topological order is essential here since it captures dependencies
	// for views and sequences creation, hence simple alphabetical sort won't
	// be enough.
	var topologicallyOrderedIDs []int64

	// The sort relies on creating a new array for the ids.
	if err = acc.Grow(ctx, int64(len(ids))*sizeOfInt64); err != nil {
		return nil, err
	}
	seen := make(map[int64]struct{})
	for _, id := range ids {
		if err := topologicalSort(
			ctx, id, dependsOnIDs, seen, &topologicallyOrderedIDs, acc,
		); err != nil {
			return nil, err
		}
	}

	// Clear memory used by the seen map.
	sizeOfSeen := len(seen)
	seen = nil
	acc.Shrink(ctx, int64(sizeOfSeen)*(sizeOfInt64+mapEntryOverhead))

	// The lengths should match. This is also important for memory accounting,
	// the two arrays should have the same length.
	if len(ids) != len(topologicallyOrderedIDs) {
		return nil, errors.AssertionFailedf("show_create_all_tables_builtin failed. "+
			"len(ids):% d not equal to len(topologicallySortedIDs): %d",
			len(ids), len(topologicallyOrderedIDs))
	}

	// Shrink the memory we used for the original ids array.
	acc.Shrink(ctx, int64(len(ids))*sizeOfInt64)
	acc.Shrink(ctx, sizeOfMap)
	return topologicallyOrderedIDs, nil
}

// getTableIDs returns the set of table ids from
// crdb_internal.show_create_all_tables for a specified database.
func getTableIDs(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	ts string,
	dbName string,
	acc *mon.BoundAccount,
) ([]int64, error) {
	query := fmt.Sprintf(`
		SELECT descriptor_id
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1 
		AND is_virtual = FALSE
		AND is_temporary = FALSE
		`, dbName, ts)
	it, err := ie.QueryIteratorEx(
		ctx,
		"crdb_internal.show_create_all_tables",
		txn,
		sessiondata.NoSessionDataOverride,
		query,
		dbName,
	)
	if err != nil {
		return nil, err
	}

	var tableIDs []int64

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		tid := tree.MustBeDInt(it.Cur()[0])

		tableIDs = append(tableIDs, int64(tid))
		if err = acc.Grow(ctx, int64(unsafe.Sizeof(tid))); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return tableIDs, err
	}

	return tableIDs, nil
}

// topologicalSort sorts transitive dependencies in topological order into
// collected.
// The topological order is essential here since it captures dependencies
// for views and sequences creation, hence simple alphabetical sort won't
// be enough.
func topologicalSort(
	ctx context.Context,
	tid int64,
	dependsOnIDs map[int64][]int64,
	seen map[int64]struct{},
	collected *[]int64,
	acc *mon.BoundAccount,
) error {
	// has this table already been collected previously?
	// We need this check because a table could be traversed to multiple times
	// if it is referenced.
	// For example, if a table references itself, without this check
	// collect would infinitely recurse.
	if _, isPresent := seen[tid]; isPresent {
		return nil
	}

	// Account for memory of map.
	// The key value entry into the map is only the memory of an int64 since
	// the value stuct{}{} uses no memory.
	if err := acc.Grow(ctx, sizeOfInt64+mapEntryOverhead); err != nil {
		return err
	}
	seen[tid] = struct{}{}
	for _, dep := range dependsOnIDs[tid] {
		if err := topologicalSort(ctx, dep, dependsOnIDs, seen, collected, acc); err != nil {
			return err
		}
	}

	if err := acc.Grow(ctx, int64(unsafe.Sizeof(tid))); err != nil {
		return err
	}
	*collected = append(*collected, tid)

	return nil
}

// getCreateStatement gets the create statement to recreate a table (ignoring fks)
// for a given table id in a database.
func getCreateStatement(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, id int64, ts string, dbName string,
) (tree.Datum, error) {
	query := fmt.Sprintf(`
		SELECT
			create_nofks
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
	`, dbName, ts)
	row, err := ie.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_tables",
		txn,
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)

	if err != nil {
		return nil, err
	}
	return row[0], nil
}

// getAlterStatements gets the set of alter statements that add and validate
// foreign keys for a given table id in a database.
func getAlterStatements(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	id int64,
	ts string,
	dbName string,
	statementType string,
) (tree.Datum, error) {
	query := fmt.Sprintf(`
		SELECT
			%s
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
	`, statementType, dbName, ts)
	row, err := ie.QueryRowEx(
		ctx,
		"crdb_internal.show_create_all_tables",
		txn,
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)

	if err != nil {
		return nil, err
	}

	return row[0], nil
}
