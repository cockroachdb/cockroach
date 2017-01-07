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
	Use:   "dump [options] <database> <table>",
	Short: "dump sql tables\n",
	Long: `
Dump SQL tables of a cockroach database.
`,
	RunE:         MaybeDecorateGRPCError(runDump),
	SilenceUsage: true,
}

func runDump(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return usageAndError(cmd)
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	mds, ts, err := getDumpMetadata(conn, args)
	if err != nil {
		return err
	}

	for _, md := range mds {
		if err := DumpTable(os.Stdout, conn, ts, md); err != nil {
			return err
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
	conn *sqlConn, args []string,
) (mds []tableMetadata, clusterTS string, err error) {
	// For now we support only a single table.
	origDBName, origTableName := args[0], args[1]

	dbName := parser.Name(origDBName)
	tableName := parser.Name(origTableName)

	// Fetch all table metadata in a transaction and its time to guarantee it
	// doesn't change between the various SHOW statements.
	if err := conn.Exec("BEGIN", nil); err != nil {
		return nil, "", err
	}

	vals, err := conn.QueryRow("SELECT cluster_logical_timestamp()", nil)
	if err != nil {
		return nil, "", err
	}
	clusterTS = string(vals[0].([]byte))

	md, err := getMetadataForTable(conn, dbName, tableName)
	if err != nil {
		return nil, "", err
	}

	if err := conn.Exec("COMMIT", nil); err != nil {
		return nil, "", err
	}

	return []tableMetadata{md}, clusterTS, nil
}

func getMetadataForTable(conn *sqlConn, dbName, tableName parser.Name) (tableMetadata, error) {
	// A previous version of the code did a SELECT on system.descriptor. This
	// required the SELECT privilege to the descriptor table, which only root
	// has. Allowing non-root to do this would let users see other users' table
	// descriptors which is a problem in multi-tenancy.

	if err := conn.Exec(fmt.Sprintf("SET DATABASE = %s", dbName), nil); err != nil {
		return tableMetadata{}, err
	}

	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tableName), nil)
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

	// Primary index is always the first index returned by SHOW INDEX.
	rows, err = conn.Query(fmt.Sprintf("SHOW INDEX FROM %s", tableName), nil)
	if err != nil {
		return tableMetadata{}, err
	}
	vals = make([]driver.Value, 5)

	var numIndexCols int
	var primaryIndex string
	var idxColNames bytes.Buffer
	// Find the primary index columns.
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return tableMetadata{}, err
		}
		b, ok := vals[1].(string)
		if !ok {
			return tableMetadata{}, fmt.Errorf("unexpected value: %T", vals[1])
		}
		if primaryIndex == "" {
			primaryIndex = b
		} else if primaryIndex != b {
			break
		}
		b, ok = vals[4].(string)
		if !ok {
			return tableMetadata{}, fmt.Errorf("unexpected value: %T", vals[4])
		}
		if idxColNames.Len() > 0 {
			idxColNames.WriteString(", ")
		}
		parser.Name(b).Format(&idxColNames, parser.FmtSimple)
		numIndexCols++
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}
	if numIndexCols == 0 {
		return tableMetadata{}, fmt.Errorf("no primary key index found")
	}

	vals, err = conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", tableName), nil)
	if err != nil {
		return tableMetadata{}, err
	}
	create := vals[1].(string)

	return tableMetadata{
		name:         &parser.TableName{DatabaseName: dbName, TableName: tableName},
		primaryIndex: primaryIndex,
		numIndexCols: numIndexCols,
		idxColNames:  idxColNames.String(),
		columnNames:  colnames.String(),
		columnTypes:  coltypes,
		createStmt:   create,
	}, nil
}

// limit determines how many rows to dump at a time (in each SELECT statement).
const limit = 100

// DumpTable dumps the specified table to w.
func DumpTable(w io.Writer, conn *sqlConn, clusterTS string, md tableMetadata) error {
	// Build the SELECT query.
	var sbuf bytes.Buffer
	fmt.Fprintf(&sbuf, "SELECT %s, %s FROM %s@%s AS OF SYSTEM TIME %s",
		md.idxColNames, md.columnNames, md.name, parser.Name(md.primaryIndex), clusterTS)

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

	if _, err := w.Write([]byte(md.createStmt)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}

	// pk holds the last values of the fetched primary keys
	var pk []driver.Value
	q := fmt.Sprintf(bs, "")
	for {
		rows, err := conn.Query(q, pk)
		if err != nil {
			return err
		}
		cols := rows.Columns()
		pkcols := cols[:md.numIndexCols]
		cols = cols[md.numIndexCols:]
		inserts := make([][]string, 0, limit)
		i := 0
		for i < limit {
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
					ivals[si] = fmt.Sprintf("%s", d)
				default:
					panic(errors.Errorf("unknown field type: %T (%s)", t, cols[si]))
				}
			}
			inserts = append(inserts, ivals)
			i++
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
		if i == 0 {
			break
		}
		fmt.Fprintf(w, "\nINSERT INTO %s(%s) VALUES", md.name.TableName, md.columnNames)
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
		if i < limit {
			break
		}
	}
	return nil
}
