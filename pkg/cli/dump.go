// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

// dumpCmd dumps SQL tables.
var dumpCmd = &cobra.Command{
	Use:   "dump [options] <database> [<table> [<table>...]]",
	Short: "dump sql tables\n",
	Long: `
Dump SQL tables of a cockroach database. If the table name
is omitted, dump all tables in the database.
`,
	RunE: MaybeDecorateGRPCError(runDump),
}

// We accept versions that are strictly newer than v2.1.0-alpha.20180416
// (hence the "-0" at the end).
var verDump = version.MustParse("v2.1.0-alpha.20180416-0")

// databasesNamesExtractor extracts list of available databases to dump
func databasesNamesExtractor(conn *sqlConn) ([]string, error) {
	var dbNames []string

	maxReservedDescID, err := driver.Int32.ConvertValue(keys.MaxReservedDescID)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(`SELECT name FROM system.namespace WHERE id > $1 AND "parentID" = 0`, []driver.Value{maxReservedDescID})
	if err != nil {
		return nil, err
	}

	vals := make([]driver.Value, 1)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		}

		if name, ok := vals[0].(string); ok {
			dbNames = append(dbNames, name)
		} else {
			return nil, fmt.Errorf("unexpected value: %T", name)
		}
	}

	// sort to get deterministic output of ordered database names
	sort.Strings(dbNames)

	return dbNames, nil
}

// runDumps performs a dump of a table or database.
//
// The approach here and its current flaws are summarized
// in https://github.com/cockroachdb/cockroach/issues/28948.
func runDump(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach dump", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.requireServerVersion(verDump); err != nil {
		return err
	}

	var dbNames []string
	if dumpCtx.dumpAll && len(args) != 0 {
		return fmt.Errorf("cannot specify --%s and a specific database at the same time", cliflags.DumpAll.Name)
	}

	if len(args) != 0 {
		dbNames = append(dbNames, args[0])
	} else if dumpCtx.dumpAll {
		dbNames, err = databasesNamesExtractor(conn)
		if err != nil {
			return err
		}
	}

	var tableNames []string
	if len(args) > 1 {
		tableNames = args[1:]
	}

	// Get the cluster timestamp at which to dump at.
	clusterTS, err := getAsOf(conn, dumpCtx.asOf)
	if err != nil {
		return err
	}

	var fullMds []basicMetadata
	w := os.Stdout

	for _, dbName := range dbNames {
		// Collect all user defined types present in this database.
		typContext, err := collectUserDefinedTypes(conn, dbName, clusterTS)
		if err != nil {
			return err
		}
		// Following pg_dump, we only dump types when dumping the database
		// is requested, not when specific tables are requested to be dumped.
		shouldDumpTypes := len(tableNames) == 0

		mds, err := getDumpMetadata(conn, dbName, tableNames, clusterTS)
		if err != nil {
			return err
		}

		if len(mds) == 0 {
			continue
		}

		byID := make(map[int64]basicMetadata)
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

		if dumpCtx.dumpAll && dumpCtx.dumpMode != dumpDataOnly {
			if _, err := fmt.Fprintf(w, "\nCREATE DATABASE IF NOT EXISTS %s;\nUSE %s;\n\n", dbName, dbName); err != nil {
				return err
			}
		}

		if shouldDumpTypes && dumpCtx.dumpMode != dumpDataOnly {
			// Only emit the settings change if there are any user defined types.
			if len(typContext.createStatements) > 0 {
				if _, err := fmt.Fprintf(w, "SET experimental_enable_enums = true;\n"); err != nil {
					return err
				}
			}
			for _, stmt := range typContext.createStatements {
				if _, err := fmt.Fprintf(w, "%s;\n\n", stmt); err != nil {
					return err
				}
			}
		}

		if dumpCtx.dumpMode != dumpDataOnly {
			for i, md := range mds {
				if i > 0 {
					fmt.Fprintln(w)
				}
				if err := dumpCreateTable(w, md); err != nil {
					return err
				}
			}
		}
		if dumpCtx.dumpMode != dumpSchemaOnly {
			for _, md := range mds {
				switch md.kind {
				case "table":
					if err := dumpTableData(w, conn, typContext, md); err != nil {
						return err
					}
				case "sequence":
					if err := dumpSequenceData(w, conn, md); err != nil {
						return err
					}
				case "view":
					continue
				default:
					panic("unknown descriptor type: " + md.kind)
				}
			}
		}
		fullMds = append(fullMds, mds...)
	}

	// Put FK ALTERs at the end.
	if dumpCtx.dumpMode != dumpDataOnly {
		hasRefs := false
		for _, md := range fullMds {
			for _, alter := range md.alter {
				if !hasRefs {
					hasRefs = true
					if _, err := w.Write([]byte("\n")); err != nil {
						return err
					}
				}
				fmt.Fprintf(w, "%s;\n", alter)
			}
		}
		if hasRefs {
			const alterValidateMessage = `-- Validate foreign key constraints. These can fail if there was unvalidated data during the dump.`
			if _, err := w.Write([]byte("\n" + alterValidateMessage + "\n")); err != nil {
				return err
			}
			for _, md := range fullMds {
				for _, validate := range md.validate {
					fmt.Fprintf(w, "%s;\n", validate)
				}
			}
		}
	}
	return nil
}

func collect(tid int64, byID map[int64]basicMetadata, seen map[int64]bool, collected *[]int64) {
	// has this table already been collected previously?
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

// tableMetadata describes one table to dump.
type tableMetadata struct {
	basicMetadata

	columnNames string
	columnTypes map[string]tree.ResolvableTypeReference
}

// dumpTypeContext acts as a collection of user defined types to resolve
// references to user defined types in the dump process.
type dumpTypeContext struct {
	typMap           map[types.UserDefinedTypeName]*types.T
	createStatements []string
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (d *dumpTypeContext) ResolveType(
	_ context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	key := types.UserDefinedTypeName{
		Name:   name.Object(),
		Schema: name.Schema(),
	}
	typ, ok := d.typMap[key]
	if !ok {
		return nil, errors.Newf("type %s not found", name.String())
	}
	return typ, nil
}

// ResolveTypeByID implements the tree.TypeReferenceResolver interface.
func (d *dumpTypeContext) ResolveTypeByID(context.Context, uint32) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in dump by ID")
}

// collectUserDefinedTypes constructs a dumpTypeContext consisting of all user
// defined types in the requested database.
func collectUserDefinedTypes(conn *sqlConn, dbName string, ts string) (*dumpTypeContext, error) {
	query := `
SELECT
	descriptor_id, schema_name, descriptor_name, create_statement, enum_members
FROM
	"".crdb_internal.create_type_statements
AS OF SYSTEM TIME %s
WHERE 
	database_name = $1
`
	rows, err := conn.Query(fmt.Sprintf(query, lex.EscapeSQLString(ts)), []driver.Value{dbName})
	if err != nil {
		// On versions before 20.2, the cluster won't have the
		// crdb_internal.create_type_statements table. If we can't find it,
		// continue with an empty type context.
		if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
			if pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedTable {
				return &dumpTypeContext{}, nil
			}
		}
		return nil, err
	}
	vals := make([]driver.Value, 5)
	var createStatements []string
	typContext := &dumpTypeContext{
		typMap: make(map[types.UserDefinedTypeName]*types.T),
	}
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// Pull out the needed values from the query.
		idI := vals[0]
		id, ok := idI.(int64)
		if !ok {
			return nil, errors.AssertionFailedf("unexpected value %T", idI)
		}
		scNameI := vals[1]
		scName, ok := scNameI.(string)
		if !ok {
			return nil, errors.AssertionFailedf("unexpected value %T", scName)
		}
		nameI := vals[2]
		name, ok := nameI.(string)
		if !ok {
			return nil, errors.AssertionFailedf("unexpected value %T", name)
		}
		createStatementI := vals[3]
		createStatement, ok := createStatementI.(string)
		if !ok {
			return nil, errors.AssertionFailedf("unexpected value %T", createStatementI)
		}
		// Remember all of the create statements.
		createStatements = append(createStatements, createStatement)
		enumMembersI := vals[4]
		enumMembersS, ok := enumMembersI.([]byte)
		if !ok {
			return nil, errors.AssertionFailedf("unexpected value %T", enumMembersI)
		}
		var enumMembers []string
		if enumMembersS != nil {
			// The driver sends back arrays as bytes, so we have to parse the array
			// if we want to access its elements.
			arr, _, err := tree.ParseDArrayFromString(
				tree.NewTestingEvalContext(serverCfg.Settings), string(enumMembersS), types.String)
			if err != nil {
				return nil, err
			}
			for _, d := range arr.Array {
				enumMembers = append(enumMembers, string(tree.MustBeDString(d)))
			}
		}

		// Based on the retrieved information, construct an entry of the correct
		// type kind in the typing context.
		switch {
		case len(enumMembers) != 0:
			typ := types.MakeEnum(uint32(id), 0 /* arrayTypeID */)
			typ.TypeMeta = types.UserDefinedTypeMetadata{
				Name: &types.UserDefinedTypeName{
					Name:   name,
					Schema: scName,
				},
				EnumData: &types.EnumMetadata{
					LogicalRepresentations: enumMembers,
					// Make an array of empty physical representations for the enum.
					// We won't need the physical representations while dumping, but enum
					// internals expect that there are as many physical representations
					// as there are logical representations.
					PhysicalRepresentations: make([][]byte, len(enumMembers)),
				},
			}
			typContext.typMap[*typ.TypeMeta.Name] = typ
		default:
			// If we don't know about this type kind, then return an error.
			return nil, errors.Newf("%q is not a supported type kind", name)
		}
	}
	typContext.createStatements = createStatements
	return typContext, nil
}

// getAsOf converts the input AS OF argument into a usable cluster timestamp,
// or returns a default if the argument was not specified.
func getAsOf(conn *sqlConn, asOf string) (string, error) {
	var clusterTS string
	if asOf == "" {
		vals, err := conn.QueryRow("SELECT cluster_logical_timestamp()", nil)
		if err != nil {
			return "", err
		}
		clusterTS = string(vals[0].([]byte))
	} else {
		// Validate the timestamp. This prevents SQL injection.
		if _, _, err := tree.ParseDTimestamp(nil, asOf, time.Nanosecond); err != nil {
			return "", err
		}
		clusterTS = asOf
	}
	return clusterTS, nil
}

// getDumpMetadata retrieves the table information for the specified table(s).
// It also retrieves the cluster timestamp at which the metadata was
// retrieved.
func getDumpMetadata(
	conn *sqlConn, dbName string, tableNames []string, clusterTS string,
) (mds []basicMetadata, err error) {
	if tableNames == nil {
		tableNames, err = getTableNames(conn, dbName, clusterTS)
		if err != nil {
			return nil, err
		}
	}

	mds = make([]basicMetadata, len(tableNames))
	for i, tableName := range tableNames {
		basicMD, err := getBasicMetadata(conn, dbName, tableName, clusterTS)
		if err != nil {
			return nil, err
		}
		mds[i] = basicMD
	}

	return mds, nil
}

// getTableNames retrieves all tables names in the given database.
func getTableNames(conn *sqlConn, dbName string, ts string) (tableNames []string, err error) {
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT descriptor_name
		FROM "".crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
		`, lex.EscapeSQLString(ts)), []driver.Value{dbName})
	if err != nil {
		return nil, err
	}

	vals := make([]driver.Value, 1)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		nameI := vals[0]
		name, ok := nameI.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", nameI)
		}
		tableNames = append(tableNames, name)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func getBasicMetadata(conn *sqlConn, dbName, tableName string, ts string) (basicMetadata, error) {
	name := tree.NewTableName(tree.Name(dbName), tree.Name(tableName))

	// Fetch table ID.
	dbNameEscaped := tree.NameString(dbName)
	vals, err := conn.QueryRow(fmt.Sprintf(`
		SELECT
			descriptor_id,
			create_nofks,
			descriptor_type,
			alter_statements,
			validate_statements
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME %s
		WHERE database_name = $1
			AND descriptor_name = $2
	`, dbNameEscaped, lex.EscapeSQLString(ts)), []driver.Value{dbName, tableName})
	if err != nil {
		if err == io.EOF {
			return basicMetadata{}, errors.Wrap(
				errors.Errorf("relation %s does not exist", tree.ErrString(name)),
				"getBasicMetadata",
			)
		}
		return basicMetadata{}, errors.Wrap(err, "getBasicMetadata")
	}
	idI := vals[0]
	id, ok := idI.(int64)
	if !ok {
		return basicMetadata{}, fmt.Errorf("unexpected value: %T", idI)
	}
	createStatementI := vals[1]
	createStatement, ok := createStatementI.(string)
	if !ok {
		return basicMetadata{}, fmt.Errorf("unexpected value: %T", createStatementI)
	}
	kindI := vals[2]
	kind, ok := kindI.(string)
	if !ok {
		return basicMetadata{}, fmt.Errorf("unexpected value: %T", kindI)
	}
	alterStatements, err := extractArray(vals[3])
	if err != nil {
		return basicMetadata{}, err
	}
	validateStatements, err := extractArray(vals[4])
	if err != nil {
		return basicMetadata{}, err
	}

	// Get dependencies.
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME %s
		WHERE descriptor_id = $1
		`, dbNameEscaped, lex.EscapeSQLString(ts)), []driver.Value{id})
	if err != nil {
		return basicMetadata{}, err
	}
	vals = make([]driver.Value, 1)

	var refs []int64
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return basicMetadata{}, err
		}
		id := vals[0].(int64)
		refs = append(refs, id)
	}
	if err := rows.Close(); err != nil {
		return basicMetadata{}, err
	}

	md := basicMetadata{
		ID:         id,
		name:       tree.NewTableName(tree.Name(dbName), tree.Name(tableName)),
		createStmt: createStatement,
		dependsOn:  refs,
		kind:       kind,
		alter:      alterStatements,
		validate:   validateStatements,
		ts:         ts,
	}

	return md, nil
}

func extractArray(val interface{}) ([]string, error) {
	b, ok := val.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected value: %T", b)
	}
	evalCtx := tree.NewTestingEvalContext(serverCfg.Settings)
	arr, _, err := tree.ParseDArrayFromString(evalCtx, string(b), types.String)
	if err != nil {
		return nil, err
	}
	res := make([]string, len(arr.Array))
	for i, v := range arr.Array {
		res[i] = string(*v.(*tree.DString))
	}
	return res, nil
}

func makeMetadataQuery(md basicMetadata, columnName string) string {
	// This query is parameterized by the column name because of
	// 2.0/2.1beta/2.1 trans-version compatibility requirements.  See
	// below for details.
	return fmt.Sprintf(`
		SELECT COLUMN_NAME, %s
		FROM %s.information_schema.columns
		AS OF SYSTEM TIME %s
		WHERE TABLE_CATALOG = $1
			AND TABLE_SCHEMA = $2
			AND TABLE_NAME = $3
			AND GENERATION_EXPRESSION = ''
		`, columnName, &md.name.CatalogName, lex.EscapeSQLString(md.ts))
}

func fetchColumnsNamesAndTypes(conn *sqlConn, md basicMetadata, noHidden bool) (*sqlRows, error) {
	query := makeMetadataQuery(md, "CRDB_SQL_TYPE")
	if noHidden {
		query = query + ` AND IS_HIDDEN = 'NO'`
	}
	rows, err := conn.Query(query,
		[]driver.Value{md.name.Catalog(), md.name.Schema(), md.name.Table()})
	if err != nil {
		// IS_HIDDEN was introduced in the first 2.1 beta. CRDB_SQL_TYPE
		// some time after that.  To ensure `cockroach dump` works across
		// versions we must try the previous forms if the first form
		// fails.
		//
		// TODO(knz): Remove this fallback logic post-2.2.
		if strings.Contains(err.Error(), "column \"crdb_sql_type\" does not exist") {
			// Pre-2.1 CRDB_SQL_HIDDEN did not exist in
			// information_schema.columns. When it does not exist,
			// information_schema.columns.data_type contains a usable SQL
			// type name instead. Use that.
			query := makeMetadataQuery(md, "DATA_TYPE")
			if noHidden {
				query = query + ` AND IS_HIDDEN = 'NO'`
			}
			rows, err = conn.Query(query,
				[]driver.Value{md.name.Catalog(), md.name.Schema(), md.name.Table()})
		}
		if strings.Contains(err.Error(), "column \"is_hidden\" does not exist") {
			// Pre-2.1 IS_HIDDEN did not exist in information_schema.columns.
			// When it does not exist, information_schema.columns only returns
			// non-hidden columns so we can still use that.
			rows, err = conn.Query(makeMetadataQuery(md, "DATA_TYPE"),
				[]driver.Value{md.name.Catalog(), md.name.Schema(), md.name.Table()})
		}
		if err != nil {
			return nil, err
		}
	}
	return rows, err
}

func constructTableMetadata(rows *sqlRows, md basicMetadata) (tableMetadata, error) {
	vals := make([]driver.Value, 2)
	coltypes := make(map[string]tree.ResolvableTypeReference)
	colnames := tree.NewFmtCtx(tree.FmtSimple)
	defer colnames.Close()
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return tableMetadata{}, err
		}
		nameI, typI := vals[0], vals[1]
		name, ok := nameI.(string)
		if !ok {
			return tableMetadata{}, fmt.Errorf("unexpected value: %T", nameI)
		}
		typ, ok := typI.(string)
		if !ok {
			return tableMetadata{}, fmt.Errorf("unexpected value: %T", typI)
		}

		// Transform the type name to an internal coltype.
		sql := fmt.Sprintf("CREATE TABLE woo (x %s)", typ)
		stmt, err := parser.ParseOne(sql)
		if err != nil {
			return tableMetadata{}, fmt.Errorf("type %s is not a valid CockroachDB type", typ)
		}
		coltypes[name] = stmt.AST.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type
		if colnames.Len() > 0 {
			colnames.WriteString(", ")
		}
		colnames.FormatName(name)
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}

	return tableMetadata{
		basicMetadata: md,

		columnNames: colnames.String(),
		columnTypes: coltypes,
	}, nil
}

func getMetadataForTable(conn *sqlConn, md basicMetadata) (tableMetadata, error) {
	// Fetch column types.
	//
	// TODO(knz): this approach is flawed, see #28948.

	rows, err := fetchColumnsNamesAndTypes(conn, md, true)
	if err != nil {
		return tableMetadata{}, err
	}

	metadata, err := constructTableMetadata(rows, md)
	if err != nil {
		return tableMetadata{}, err
	}

	if len(metadata.columnNames) == 0 {
		rows, err := fetchColumnsNamesAndTypes(conn, md, false)
		if err != nil {
			return tableMetadata{}, err
		}

		return constructTableMetadata(rows, md)
	}
	return metadata, err
}

// dumpCreateTable dumps the CREATE statement of the specified table to w.
func dumpCreateTable(w io.Writer, md basicMetadata) error {
	if _, err := w.Write([]byte(md.createStmt)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}
	return nil
}

const (
	// insertRows is the number of rows per INSERT statement.
	insertRows = 100
)

func dumpSequenceData(w io.Writer, conn *sqlConn, bmd basicMetadata) error {
	// Get sequence value.
	vals, err := conn.QueryRow(fmt.Sprintf(
		"SELECT last_value FROM %s AS OF SYSTEM TIME %s",
		bmd.name, lex.EscapeSQLString(bmd.ts),
	), nil)
	if err != nil {
		return err
	}
	seqVal := vals[0].(int64)

	// Get sequence increment.
	// TODO(knz,vilterp): This could use a shortcut via crdb_internal.
	vals2, err := conn.QueryRow(fmt.Sprintf(
		`SELECT inc
       FROM (SELECT s.seqincrement AS inc
               FROM %[1]s.pg_catalog.pg_namespace n, %[1]s.pg_catalog.pg_class c, %[1]s.pg_catalog.pg_sequence s
              WHERE n.nspname = %[2]s
                AND n.oid = c.relnamespace
                AND c.relname = %[3]s
                AND c.oid = s.seqrelid)
     AS OF SYSTEM TIME %[4]s`,
		&bmd.name.CatalogName,
		lex.EscapeSQLString(bmd.name.Schema()),
		lex.EscapeSQLString(bmd.name.Table()),
		lex.EscapeSQLString(bmd.ts),
	), nil)
	if err != nil {
		return err
	}
	seqInc := vals2[0].(int64)

	fmt.Fprintln(w)

	// Dump `setval(name, val + inc, false)`. This will cause the value to be
	// set to `(val + inc) - inc = val`, so that the next value given out by the
	// sequence will be `val`. This also avoids the minval check -- a sequence with
	// a minval of 1 will have its value saved in KV as 0, so that the next value
	// given out is 1.
	fmt.Fprintf(
		w, "SELECT setval(%s, %d, false);\n",
		lex.EscapeSQLString(tree.NameString(bmd.name.Table())), seqVal+seqInc,
	)

	return nil
}

// dumpTableData dumps the data of the specified table to w.
func dumpTableData(
	w io.Writer, conn *sqlConn, typContext tree.TypeReferenceResolver, bmd basicMetadata,
) error {
	md, err := getMetadataForTable(conn, bmd)
	if err != nil {
		return err
	}
	var collationEnv tree.CollationEnvironment
	bs := fmt.Sprintf("SELECT %s FROM %s AS OF SYSTEM TIME %s ORDER BY PRIMARY KEY %[2]s",
		md.columnNames,
		md.name,
		lex.EscapeSQLString(md.ts),
	)
	inserts := make([]string, 0, insertRows)
	rows, err := conn.Query(bs, nil)
	if err != nil {
		return err
	}
	cols := rows.Columns()
	// Make 2 []driver.Values and alternate sending them on the chan. This is
	// needed so val encoding can proceed at the same time as fetching a new
	// row. There's no benefit to having more than 2 because that's all we can
	// encode at once if we want to preserve the select order.
	var valArray [2][]driver.Value
	for i := range valArray {
		valArray[i] = make([]driver.Value, len(cols))
	}
	g := ctxgroup.WithContext(context.Background())
	valsCh := make(chan []driver.Value)
	// stringsCh receives VALUES lines and batches them before writing to the
	// output. Buffering this chan allows the val encoding to proceed during
	// writes.
	stringsCh := make(chan string, insertRows)

	g.GoCtx(func(ctx context.Context) error {
		// Fetch SQL rows and put them onto valsCh.
		defer close(valsCh)
		done := ctx.Done()
		for i := 0; ; i++ {
			vals := valArray[i%len(valArray)]
			if err := rows.Next(vals); err == io.EOF {
				return rows.Close()
			} else if err != nil {
				return err
			}
			select {
			case <-done:
				return ctx.Err()
			case valsCh <- vals:
			}
		}
	})
	g.GoCtx(func(ctx context.Context) error {
		// Convert SQL rows into VALUE strings.
		defer close(stringsCh)
		f := tree.NewFmtCtx(tree.FmtParsableNumerics)
		defer f.Close()
		done := ctx.Done()
		for vals := range valsCh {
			f.Reset()
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				if si > 0 {
					f.WriteString(", ")
				}
				ct, err := tree.ResolveType(ctx, md.columnTypes[cols[si]], typContext)
				if err != nil {
					return err
				}
				var d tree.Datum
				// TODO(knz): this approach is brittle+flawed, see #28948.
				// TODO(mjibson): can we use tree.ParseDatumStringAs here?
				switch t := sv.(type) {
				case nil:
					d = tree.DNull
				case bool:
					d = tree.MakeDBool(tree.DBool(t))
				case int64:
					d = tree.NewDInt(tree.DInt(t))
				case float64:
					d = tree.NewDFloat(tree.DFloat(t))
				case string:
					switch ct.Family() {
					case types.StringFamily:
						d = tree.NewDString(t)
					case types.CollatedStringFamily:
						d, err = tree.NewDCollatedString(t, ct.Locale(), &collationEnv)
						if err != nil {
							return err
						}
					default:
						return errors.AssertionFailedf("unknown string type %s", ct)
					}
				case []byte:
					// TODO(knz): this approach is brittle+flawed, see #28948.
					switch ct.Family() {
					case types.IntervalFamily:
						d, err = tree.ParseDInterval(string(t))
						if err != nil {
							return err
						}
					case types.BytesFamily:
						d = tree.NewDBytes(tree.DBytes(t))
					case types.UuidFamily:
						d, err = tree.ParseDUuidFromString(string(t))
						if err != nil {
							return err
						}
					case types.INetFamily:
						d, err = tree.ParseDIPAddrFromINetString(string(t))
						if err != nil {
							return err
						}
					case types.GeographyFamily:
						d, err = tree.ParseDGeography(string(t))
						if err != nil {
							return err
						}
					case types.GeometryFamily:
						d, err = tree.ParseDGeometry(string(t))
						if err != nil {
							return err
						}
					case types.JsonFamily:
						d, err = tree.ParseDJSON(string(t))
						if err != nil {
							return err
						}
					case types.ArrayFamily:
						// We can only observe ARRAY types by their [] suffix.
						d, _, err = tree.ParseDArrayFromString(
							tree.NewTestingEvalContext(serverCfg.Settings), string(t), ct.ArrayContents())
						if err != nil {
							return err
						}
					case types.StringFamily:
						// STRING types can have optional length suffixes, so only
						// examine the prefix of the type.
						d = tree.NewDString(string(t))
					case types.DecimalFamily:
						// DECIMAL types can have optional length suffixes, so only
						// examine the prefix of the type.
						d, err = tree.ParseDDecimal(string(t))
						if err != nil {
							return err
						}
					case types.OidFamily:
						var i *tree.DInt
						i, err = tree.ParseDInt(string(t))
						if err != nil {
							return err
						}
						d = tree.NewDOid(*i)
					case types.EnumFamily:
						// Enum values are streamed back in their logical representation.
						d, err = tree.MakeDEnumFromLogicalRepresentation(ct, string(t))
						if err != nil {
							return err
						}
					default:
						return errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]])
					}
				case time.Time:
					switch ct.Family() {
					case types.DateFamily:
						d, err = tree.NewDDateFromTime(t)
						if err != nil {
							return err
						}
					case types.TimeFamily:
						// pq awkwardly represents TIME as a time.Time with date 0000-01-01.
						d = tree.MakeDTime(timeofday.FromTimeAllow2400(t))
					case types.TimeTZFamily:
						d = tree.NewDTimeTZ(timetz.MakeTimeTZFromTimeAllow2400(t))
					case types.TimestampFamily:
						d, err = tree.MakeDTimestamp(t, time.Nanosecond)
						if err != nil {
							return err
						}
					case types.TimestampTZFamily:
						d, err = tree.MakeDTimestampTZ(t, time.Nanosecond)
						if err != nil {
							return err
						}
					default:
						return errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]])
					}
				default:
					return errors.Errorf("unknown field type: %T (%s)", t, cols[si])
				}
				d.Format(f)
			}
			select {
			case <-done:
				return ctx.Err()
			case stringsCh <- f.String():
			}
		}
		return nil
	})
	g.Go(func() error {
		// Batch SQL strings into groups and write to output.
		for s := range stringsCh {
			inserts = append(inserts, s)
			if len(inserts) == cap(inserts) {
				writeInserts(w, md, inserts)
				inserts = inserts[:0]
			}
		}
		if len(inserts) != 0 {
			writeInserts(w, md, inserts)
			inserts = inserts[:0]
		}
		return nil
	})
	return g.Wait()
}

func writeInserts(w io.Writer, tmd tableMetadata, inserts []string) {
	fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", &tmd.name.ObjectName, tmd.columnNames)
	for idx, values := range inserts {
		if idx > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "\n\t(%s)", values)
	}
	fmt.Fprintln(w, ";")
}
