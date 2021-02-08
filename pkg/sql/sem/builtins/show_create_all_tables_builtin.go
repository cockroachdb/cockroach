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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type basicMetadata struct {
	ID         int64
	name       *tree.TableName
	createStmt string
	dependsOn  []int64
	kind       string // "string", "table", or "view"
	alter      []string
	validate   []string
	ts         string
}

type dumpTable struct {
	schema string
	table  string
}

func showCreateAllTablesBuiltin(evalCtx *tree.EvalContext, arg tree.Datums) (tree.Datum, error) {
	mds, err := getDumpMetaData(evalCtx, arg)
	if err != nil {
		return nil, err
	}

	if len(mds) == 0 {
		return tree.NewDString("No tables found"), nil
	}

	byID := make(map[int64]basicMetadata)
	for _, md := range mds {
		byID[md.ID] = md
	}

	// First sort by name to guarantee stable output.
	sort.Slice(mds, func(i, j int) bool {
		return mds[i].name.String() < mds[j].name.String()
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
		out = append(out, alterValidateMessage)
		for _, md := range mds {
			for _, validate := range md.validate {
				out = append(out, fmt.Sprintf("%s;\n", validate))

			}
		}
	}

	result := tree.NewDString(strings.Join(out, ""))
	return result, nil
}

func getDumpMetaData(evalCtx *tree.EvalContext, arg tree.Datums) ([]basicMetadata, error) {
	tsI, err := tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
	if err != nil {
		return nil, err
	}
	ts := tsI.String()
	dbName := string(tree.MustBeDString(arg[0]))
	dumpTables, err := getTableNames(evalCtx, dbName, ts)
	if err != nil {
		return nil, err
	}

	mds := make([]basicMetadata, len(dumpTables))
	for i, dumpTable := range dumpTables {
		basicMD, err := getBasicMetadata(evalCtx, dbName, dumpTable, ts)
		if err != nil {
			return nil, err
		}
		mds[i] = basicMD
	}

	return mds, nil
}

func getBasicMetadata(
	evalCtx *tree.EvalContext, dbName string, table dumpTable, ts string,
) (basicMetadata, error) {
	tn := tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(table.schema), tree.Name(table.table))
	// Fetch table ID.
	query := fmt.Sprintf(`
		SELECT
			schema_name,
			descriptor_id,
			create_nofks,
			descriptor_type,
			alter_statements,
			validate_statements
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
      AND schema_name = $2
			AND descriptor_name = $3
	`, dbName, ts)
	vals, err := evalCtx.InternalExecutor.QueryRow(evalCtx.Context, "getBasicMetadata", evalCtx.Txn, query, dbName, table.schema, table.table)
	if err != nil {
		return basicMetadata{}, errors.Wrap(err, "getBasicMetadata")
	}

	if len(vals) == 0 {
		return basicMetadata{}, nil
	}

	// Check the schema to disallow dumping temp tables, views and sequences. This
	// will only be triggered if a user explicitly specifies a temp construct as
	// one of the arguments to the `cockroach dump` command. When no table names
	// are specified on the CLI, we ignore temp tables at the stage where we read
	// all table names in getTableNames.
	schemaName := string(tree.MustBeDString(vals[0]))
	if strings.HasPrefix(schemaName, sessiondata.PgTempSchemaName) {
		return basicMetadata{}, errors.Newf("cannot dump temp table %s", tn.String())
	}

	idI := tree.MustBeDInt(vals[1])
	id := int64(idI)

	createStatement := string(tree.MustBeDString(vals[2]))

	kind := string(tree.MustBeDString(vals[3]))

	alterStatements := extractArray(vals[4])

	validateStatements := extractArray(vals[5])

	// Get dependencies.
	query = fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
		`, dbName, ts)
	rows, err := evalCtx.InternalExecutor.Query(evalCtx.Context, "getBasicMetadata", evalCtx.Txn, query, id)
	if err != nil {
		return basicMetadata{}, err
	}

	var refs []int64

	for _, row := range rows {
		id := tree.MustBeDInt(row[0])
		refs = append(refs, int64(id))
	}

	md := basicMetadata{
		ID:         id,
		name:       &tn,
		createStmt: createStatement,
		dependsOn:  refs,
		kind:       kind,
		alter:      alterStatements,
		validate:   validateStatements,
		ts:         ts,
	}

	return md, nil
}

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
func getTableNames(evalCtx *tree.EvalContext, dbName string, ts string) ([]dumpTable, error) {
	query := fmt.Sprintf(`
		SELECT schema_name, descriptor_name
		FROM "".crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1 AND schema_name NOT LIKE $2
		`, ts)
	rows, err := evalCtx.InternalExecutor.Query(evalCtx.Ctx(), "getTableNames", evalCtx.Txn, query, dbName, sessiondata.PgTempSchemaName+"%")
	if err != nil {
		return nil, err
	}

	var tableNames []dumpTable

	for _, row := range rows {
		schema := string(tree.MustBeDString(row[0]))
		table := string(tree.MustBeDString(row[1]))

		tableNames = append(tableNames, dumpTable{table: table, schema: schema})
	}

	return tableNames, nil
}
