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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type tableMetadata struct {
	ID         int64
	name       *tree.TableName
	createStmt string
	dependsOn  []int64
	alter      []string
	validate   []string
}

type tableWithSchema struct {
	schema string
	table  string
}

// showCreateAllTablesBuiltin presents CREATE TABLE statements followed by
// ALTER TABLE statements for a database. The CREATE TABLE / ALTER TABLE
// statements are sorted such that the tables are ordered topologically to
// account for dependencies and references. The output can be used to
// recreate a database.
// The reason for ALTER TABLE statements coming after all CREATE TABLE
// statements is that we want to add constraints such as foreign keys
// after all the referenced tables are created.
func showCreateAllTablesBuiltin(evalCtx *tree.EvalContext, arg tree.Datums) (tree.Datum, error) {
	mds, err := getMetadataForTablesInDB(evalCtx, arg)
	if err != nil {
		return nil, err
	}

	if len(mds) == 0 {
		return tree.NewDString(""), nil
	}

	byID := make(map[int64]tableMetadata)
	for _, md := range mds {
		byID[md.ID] = md
	}

	// First sort by name to guarantee stable output.
	sort.Slice(mds, func(i, j int) bool {
		return mds[i].name.String() < mds[j].name.String()
	})

	// Collect transitive dependencies in topological order into collected.
	// The topological order is essential here since it captures dependencies
	// for views and sequences creation, hence simple alphabetical sort won't
	// be enough.
	var collected []int64
	seen := make(map[int64]bool)
	for _, md := range mds {
		collect(md.ID, byID, seen, &collected)
	}
	// collectOrder maps a table ID to its collection index. This is needed
	// instead of just using range over collected because collected may contain
	// table IDs not present in the dump spec. It is simpler to sort mds correctly
	// to skip over these referenced-but-not-dumped tables.
	collectOrder := make(map[int64]int)
	for i, id := range collected {
		collectOrder[id] = i
	}

	// Second sort dumped tables by dependency order.
	sort.SliceStable(mds, func(i, j int) bool {
		return collectOrder[mds[i].ID] < collectOrder[mds[j].ID]
	})

	var out []string
	for _, md := range mds {
		out = append(out, md.createStmt+";\n")
	}

	hasRefs := false
	for _, md := range mds {
		for _, alter := range md.alter {
			if !hasRefs {
				hasRefs = true
			}
			out = append(out, fmt.Sprintf("%s;\n", alter))
		}
	}
	if hasRefs {
		const alterValidateMessage = `-- Validate foreign key constraints. These can fail if there was unvalidated data during the SHOW CREATE ALL TABLES`
		out = append(out, alterValidateMessage+"\n")
		for _, md := range mds {
			for _, validate := range md.validate {
				out = append(out, fmt.Sprintf("%s;\n", validate))

			}
		}
	}

	result := tree.NewDString(strings.Join(out, ""))
	return result, nil
}

// getMetadataForTablesInDB finds all the table names in a given database and
// populates the tableMetadata information for all the tables.
func getMetadataForTablesInDB(evalCtx *tree.EvalContext, arg tree.Datums) ([]tableMetadata, error) {
	tsI, err := tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
	if err != nil {
		return nil, err
	}
	ts := tsI.String()
	dbName := string(tree.MustBeDString(arg[0]))
	tableMDs, err := getTableNames(evalCtx, dbName, ts)
	if err != nil {
		return nil, err
	}

	mds := make([]tableMetadata, len(tableMDs))
	for i, dumpTable := range tableMDs {
		tableMD, err := getTableMetadata(evalCtx, dbName, dumpTable, ts)
		if err != nil {
			return nil, err
		}
		mds[i] = tableMD
	}

	return mds, nil
}

// getTableMetadata populates the metadata for a given table by querying
// crdb_internal.create_statements.
func getTableMetadata(
	evalCtx *tree.EvalContext, dbName string, table tableWithSchema, ts string,
) (tableMetadata, error) {
	tn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(table.schema), tree.Name(table.table))
	// Fetch table ID.
	query := fmt.Sprintf(`
		SELECT
			schema_name,
			descriptor_id,
			create_nofks,
			alter_statements,
			validate_statements
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
      AND schema_name = $2
			AND descriptor_name = $3
	`, dbName, ts)
	ie := evalCtx.InternalExecutor.(sqlutil.InternalExecutor)
	vals, err := ie.QueryRowEx(
		evalCtx.Context,
		"crdb_internal.show_create_all_tables",
		evalCtx.Txn,
		sessiondata.NoSessionDataOverride,
		query,
		dbName,
		table.schema,
		table.table,
	)
	if err != nil {
		return tableMetadata{}, err
	}

	if len(vals) == 0 {
		return tableMetadata{}, nil
	}

	// Check the schema to disallow dumping temp tables, views and sequences. This
	// will only be triggered if a user explicitly specifies a temp construct as
	// one of the arguments to the `cockroach dump` command. When no table names
	// are specified on the CLI, we ignore temp tables at the stage where we read
	// all table names in getTableNames.
	schemaName := string(tree.MustBeDString(vals[0]))
	if strings.HasPrefix(schemaName, sessiondata.PgTempSchemaName) {
		return tableMetadata{}, errors.Newf("cannot dump temp table %s", tn.String())
	}

	id := int64(tree.MustBeDInt(vals[1]))
	createStatement := string(tree.MustBeDString(vals[2]))
	alterStatements := extractArray(vals[3])
	validateStatements := extractArray(vals[4])

	// Get dependencies.
	query = fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
		`, dbName, ts)
	it, err := ie.QueryIteratorEx(
		evalCtx.Context,
		"crdb_internal.show_create_all_tables",
		evalCtx.Txn,
		sessiondata.NoSessionDataOverride,
		query,
		id,
	)
	if err != nil {
		return tableMetadata{}, err
	}

	var refs []int64
	var ok bool
	for ok, err = it.Next(evalCtx.Context); ok; ok, err = it.Next(evalCtx.Context) {
		id := tree.MustBeDInt(it.Cur()[0])
		refs = append(refs, int64(id))
	}
	if err != nil {
		return tableMetadata{}, err
	}
	md := tableMetadata{
		ID:         id,
		name:       &tn,
		createStmt: createStatement,
		dependsOn:  refs,
		alter:      alterStatements,
		validate:   validateStatements,
	}

	return md, nil
}

// extractArray ensures that a tree.Datum is a DArray and converts
// the DArray into a list of strings.
func extractArray(val tree.Datum) []string {
	arr := tree.MustBeDArray(val)
	res := make([]string, len(arr.Array))
	for i, v := range arr.Array {
		res[i] = string(*v.(*tree.DString))
	}
	return res
}

// getTableNames retrieves all tables names in the given database. Following
// pg_dump, we ignore all descriptors which are part of the temp schema. This
// includes tables, views and sequences.
func getTableNames(evalCtx *tree.EvalContext, dbName string, ts string) ([]tableWithSchema, error) {
	query := fmt.Sprintf(`
		SELECT schema_name, descriptor_name
		FROM "".crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1 AND schema_name NOT LIKE $2
		`, ts)
	ie := evalCtx.InternalExecutor.(sqlutil.InternalExecutor)
	it, err := ie.QueryIteratorEx(
		evalCtx.Ctx(),
		"crdb_internal.show_create_all_tables",
		evalCtx.Txn,
		sessiondata.NoSessionDataOverride,
		query,
		dbName,
		sessiondata.PgTempSchemaName+"%",
	)
	if err != nil {
		return nil, err
	}

	var tableNames []tableWithSchema

	var ok bool
	for ok, err = it.Next(evalCtx.Context); ok; ok, err = it.Next(evalCtx.Context) {
		schema := string(tree.MustBeDString(it.Cur()[0]))
		table := string(tree.MustBeDString(it.Cur()[1]))

		tableNames = append(tableNames, tableWithSchema{table: table, schema: schema})
	}
	if err != nil {
		return tableNames, err
	}

	return tableNames, nil
}

// collect maps a table id to it's tableMetadata and ensures tables are only
// mapped once.
func collect(tid int64, byID map[int64]tableMetadata, seen map[int64]bool, collected *[]int64) {
	// has this table already been collected previously?
	// We need this check because a table could be multiple times
	// if it is referenced.
	// For example, if a table references itself, without this check
	// collect would infinitely recurse.
	if seen[tid] {
		return
	}
	// no: mark it as seen.
	seen[tid] = true
	for _, dep := range byID[tid].dependsOn {
		// depth-first collection of dependencies
		collect(dep, byID, seen, collected)
	}
	// Only add it after its dependencies.
	*collected = append(*collected, tid)
}
