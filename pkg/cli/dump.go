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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package cli

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
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

func runDump(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return usageAndError(cmd)
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	dbName := args[0]
	var tableNames []string
	if len(args) > 1 {
		tableNames = args[1:]
	}

	mds, ts, err := getDumpMetadata(conn, dbName, tableNames, dumpCtx.asOf)
	if err != nil {
		return err
	}

	// TODO(knz/mjibson): dump foreign key constraints and dump in
	// topological order to ensure key relationships can be verified
	// during load.

	w := os.Stdout

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
			if err := dumpTableData(w, conn, ts, md); err != nil {
				return err
			}
		}
	}
	return nil
}

// tableMetadata describes one table to dump.
type tableMetadata struct {
	name         *parser.TableName
	primaryIndex string
	numIndexCols int
	idxColNames  string
	columnNames  string
	columnTypes  map[string]string
	createStmt   string
}

// getDumpMetadata retrieves the table information for the specified table(s).
// It also retrieves the cluster timestamp at which the metadata was
// retrieved.
func getDumpMetadata(
	conn *sqlConn, dbName string, tableNames []string, asOf string,
) (mds []tableMetadata, clusterTS string, err error) {
	if asOf == "" {
		vals, err := conn.QueryRow("SELECT cluster_logical_timestamp()", nil)
		if err != nil {
			return nil, "", err
		}
		clusterTS = string(vals[0].([]byte))
	} else {
		// Validate the timestamp. This prevents SQL injection.
		if _, err := parser.ParseDTimestamp(asOf, time.Nanosecond); err != nil {
			return nil, "", err
		}
		clusterTS = asOf
	}

	if tableNames == nil {
		tableNames, err = getTableNames(conn, dbName, clusterTS)
		if err != nil {
			return nil, "", err
		}
	}

	mds = make([]tableMetadata, len(tableNames))
	for i, tableName := range tableNames {
		md, err := getMetadataForTable(conn, dbName, tableName, clusterTS)
		if err != nil {
			return nil, "", err
		}
		mds[i] = md
	}

	return mds, clusterTS, nil
}

// getTableNames retrieves all tables names in the given database.
func getTableNames(conn *sqlConn, dbName string, ts string) (tableNames []string, err error) {
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT TABLE_NAME
		FROM information_schema.tables
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
		`, ts), []driver.Value{dbName})
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

func getMetadataForTable(
	conn *sqlConn, dbName, tableName string, ts string,
) (tableMetadata, error) {
	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT COLUMN_NAME, DATA_TYPE
		FROM information_schema.columns
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
		`, ts), []driver.Value{dbName, tableName})
	if err != nil {
		return tableMetadata{}, err
	}
	vals := make([]driver.Value, 2)
	coltypes := make(map[string]string)
	var colnames bytes.Buffer
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
		coltypes[name] = typ
		if colnames.Len() > 0 {
			colnames.WriteString(", ")
		}
		parser.Name(name).Format(&colnames, parser.FmtSimple)
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}

	// Fetch the primary index name.
	vals, err = conn.QueryRow(fmt.Sprintf(`
		SELECT CONSTRAINT_NAME
		FROM information_schema.table_constraints
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
			AND CONSTRAINT_TYPE='PRIMARY KEY'
		`, ts), []driver.Value{dbName, tableName})

	var primaryIndex string

	if err != nil {
		// If the above query returns no rows, err will be set to io.EOF.
		// This indicates that there is no visible primary index in the table.
		if err != io.EOF {
			return tableMetadata{}, err
		}
	} else {
		primaryIndex = vals[0].(string)
	}

	rows, err = conn.Query(fmt.Sprintf(`
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
			AND CONSTRAINT_NAME = $3
		ORDER BY ORDINAL_POSITION
		`, ts), []driver.Value{dbName, tableName, primaryIndex})
	if err != nil {
		return tableMetadata{}, err
	}
	vals = make([]driver.Value, 1)

	var numIndexCols int
	var idxColNames bytes.Buffer
	// Find the primary index columns.
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return tableMetadata{}, err
		}
		name := vals[0].(string)
		if idxColNames.Len() > 0 {
			idxColNames.WriteString(", ")
		}
		parser.Name(name).Format(&idxColNames, parser.FmtSimple)
		numIndexCols++
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}

	name := &parser.TableName{DatabaseName: parser.Name(dbName), TableName: parser.Name(tableName)}

	vals, err = conn.QueryRow(fmt.Sprintf(`
		SELECT CREATE_TABLE
		FROM crdb_internal.tables
		AS OF SYSTEM TIME '%s'
		WHERE NAME = $1
			AND DATABASE_NAME = $2
		`, ts), []driver.Value{tableName, dbName})
	if err != nil {
		if err == io.EOF {
			return tableMetadata{}, errors.Errorf("table %s.%s does not exist", dbName, tableName)
		}
		return tableMetadata{}, err
	}
	create := vals[0].(string)

	return tableMetadata{
		name:         name,
		primaryIndex: primaryIndex,
		numIndexCols: numIndexCols,
		idxColNames:  idxColNames.String(),
		columnNames:  colnames.String(),
		columnTypes:  coltypes,
		createStmt:   create,
	}, nil
}

// dumpCreateTable dumps the CREATE statement of the specified table to w.
func dumpCreateTable(w io.Writer, md tableMetadata) error {
	if _, err := w.Write([]byte(md.createStmt)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}
	return nil
}

const (
	// limit is the number of rows to dump at a time (in each SELECT statement).
	limit = 10000
	// insertRows is the number of rows per INSERT statement.
	insertRows = 100
)

// dumpTableData dumps the data of the specified table to w.
func dumpTableData(w io.Writer, conn *sqlConn, clusterTS string, md tableMetadata) error {
	// Build the SELECT query.
	var sbuf bytes.Buffer
	if md.idxColNames == "" {
		// TODO(mjibson): remove hard coded rowid. Maybe create a crdb_internal
		// table with the information we need instead.
		md.idxColNames = "rowid"
		md.numIndexCols = 1
	}
	fmt.Fprintf(&sbuf, "SELECT %s, %s FROM %s", md.idxColNames, md.columnNames, md.name)
	if md.primaryIndex != "" {
		fmt.Fprintf(&sbuf, "@%s", parser.Name(md.primaryIndex))
	}
	fmt.Fprintf(&sbuf, " AS OF SYSTEM TIME '%s'", clusterTS)

	var wbuf bytes.Buffer
	fmt.Fprintf(&wbuf, " WHERE ROW (%s) > ROW (", md.idxColNames)
	for i := 0; i < md.numIndexCols; i++ {
		if i > 0 {
			wbuf.WriteString(", ")
		}
		fmt.Fprintf(&wbuf, "$%d", i+1)
	}
	wbuf.WriteString(")")
	// No WHERE clause first time, so add a place to inject it.
	fmt.Fprintf(&sbuf, "%%s ORDER BY %s LIMIT %d", md.idxColNames, limit)
	bs := sbuf.String()

	// pk holds the last values of the fetched primary keys
	var pk []driver.Value
	q := fmt.Sprintf(bs, "")
	inserts := make([][]string, 0, insertRows)
	for {
		rows, err := conn.Query(q, pk)
		if err != nil {
			return err
		}
		cols := rows.Columns()
		pkcols := cols[:md.numIndexCols]
		cols = cols[md.numIndexCols:]
		i := 0
		for {
			vals := make([]driver.Value, len(cols)+len(pkcols))
			if err := rows.Next(vals); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			if pk == nil {
				q = fmt.Sprintf(bs, wbuf.String())
			}
			pk = vals[:md.numIndexCols]
			vals = vals[md.numIndexCols:]
			ivals := make([]string, len(vals))
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				switch t := sv.(type) {
				case nil:
					ivals[si] = "NULL"
				case bool:
					ivals[si] = parser.MakeDBool(parser.DBool(t)).String()
				case int64:
					ivals[si] = parser.NewDInt(parser.DInt(t)).String()
				case float64:
					ivals[si] = parser.NewDFloat(parser.DFloat(t)).String()
				case string:
					ivals[si] = parser.NewDString(t).String()
				case []byte:
					switch ct := md.columnTypes[cols[si]]; ct {
					case "INTERVAL":
						ivals[si] = fmt.Sprintf("'%s'", t)
					case "BYTES":
						ivals[si] = parser.NewDBytes(parser.DBytes(t)).String()
					default:
						// STRING and DECIMAL types can have optional length
						// suffixes, so only examine the prefix of the type.
						if strings.HasPrefix(md.columnTypes[cols[si]], "STRING") {
							ivals[si] = parser.NewDString(string(t)).String()
						} else if strings.HasPrefix(md.columnTypes[cols[si]], "DECIMAL") {
							ivals[si] = string(t)
						} else {
							panic(errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]]))
						}
					}
				case time.Time:
					var d parser.Datum
					ct := md.columnTypes[cols[si]]
					switch ct {
					case "DATE":
						d = parser.NewDDateFromTime(t, time.UTC)
					case "TIMESTAMP":
						d = parser.MakeDTimestamp(t, time.Nanosecond)
					case "TIMESTAMP WITH TIME ZONE":
						d = parser.MakeDTimestampTZ(t, time.Nanosecond)
					default:
						panic(errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]]))
					}
					ivals[si] = d.String()
				default:
					panic(errors.Errorf("unknown field type: %T (%s)", t, cols[si]))
				}
			}
			inserts = append(inserts, ivals)
			i++
			if len(inserts) == cap(inserts) {
				writeInserts(w, md, inserts)
				inserts = inserts[:0]
			}
		}
		for si, sv := range pk {
			b, ok := sv.([]byte)
			if ok && strings.HasPrefix(md.columnTypes[pkcols[si]], "STRING") {
				// Primary key strings need to be converted to a go string, but not SQL
				// encoded since they aren't being written to a text file.
				pk[si] = string(b)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if len(inserts) != 0 {
			writeInserts(w, md, inserts)
			inserts = inserts[:0]
		}
		if i < limit {
			break
		}
	}
	return nil
}

func writeInserts(w io.Writer, md tableMetadata, inserts [][]string) {
	fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", md.name.TableName, md.columnNames)
	for idx, values := range inserts {
		if idx > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprint(w, "\n\t(")
		for vi, v := range values {
			if vi > 0 {
				fmt.Fprint(w, ", ")
			}
			fmt.Fprint(w, v)
		}
		fmt.Fprint(w, ")")
	}
	fmt.Fprintln(w, ";")
}
