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

	"github.com/cockroachdb/cockroach/sql/parser"
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
	RunE:         runDump,
	SilenceUsage: true,
}

func runDump(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		mustUsage(cmd)
		return errMissingParams
	}

	conn, err := makeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	return dumpTable(os.Stdout, conn, args[0], args[1])
}

func dumpTable(w io.Writer, conn *sqlConn, origDBName, origTableName string) error {
	const limit = 100

	// Escape names since they can't be used in placeholders.
	dbname := parser.Name(origDBName).String()
	tablename := parser.Name(origTableName).String()

	if err := conn.Exec(fmt.Sprintf("SET DATABASE = %s", dbname), nil); err != nil {
		return err
	}

	// Fetch all table metadata in a transaction and its time to guarantee it
	// doesn't change between the various SHOW statements.
	if err := conn.Exec("BEGIN", nil); err != nil {
		return err
	}

	vals, err := conn.QueryRow("SELECT cluster_logical_timestamp()::int", nil)
	if err != nil {
		return err
	}
	clusterTSStart := vals[0].(int64)
	clusterTS := time.Unix(0, clusterTSStart).Format(time.RFC3339Nano)

	// A previous version of the code did a SELECT on system.descriptor. This
	// required the SELECT privilege to the descriptor table, which only root
	// has. Allowing non-root to do this would let users see other users' table
	// descriptors which is a problem in multi-tenancy.

	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tablename), nil)
	if err != nil {
		return err
	}
	vals = make([]driver.Value, 2)
	coltypes := make(map[string]string)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		name, ok := vals[0].(string)
		if !ok {
			return fmt.Errorf("unexpected value: %T", vals[1])

		}
		typ, ok := vals[1].(string)
		if !ok {
			return fmt.Errorf("unexpected value: %T", vals[4])

		}
		coltypes[name] = typ
	}
	if err := rows.Close(); err != nil {
		return err
	}

	// index holds the names, in order, of the primary key columns.
	var index []string
	// Primary index is always the first index returned by SHOW INDEX.
	rows, err = conn.Query(fmt.Sprintf("SHOW INDEX FROM %s", tablename), nil)
	if err != nil {
		return err
	}
	vals = make([]driver.Value, 5)
	var primaryIndex string
	// Find the primary index columns.
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		b, ok := vals[1].(string)
		if !ok {
			return fmt.Errorf("unexpected value: %T", vals[1])
		}
		if primaryIndex == "" {
			primaryIndex = b
		} else if primaryIndex != b {
			break
		}
		b, ok = vals[4].(string)
		if !ok {
			return fmt.Errorf("unexpected value: %T", vals[4])
		}
		index = append(index, parser.Name(b).String())
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(index) == 0 {
		return fmt.Errorf("no primary key index found")
	}
	indexes := strings.Join(index, ", ")

	// Build the SELECT query.
	var sbuf bytes.Buffer
	fmt.Fprintf(&sbuf, "SELECT %s, * FROM %s@%s AS OF SYSTEM TIME '%s'", indexes, tablename, primaryIndex, clusterTS)

	var wbuf bytes.Buffer
	fmt.Fprintf(&wbuf, " WHERE ROW (%s) > ROW (", indexes)
	for i := range index {
		if i > 0 {
			wbuf.WriteString(", ")
		}
		fmt.Fprintf(&wbuf, "$%d", i+1)
	}
	wbuf.WriteString(")")
	// No WHERE clause first time, so add a place to inject it.
	fmt.Fprintf(&sbuf, "%%s ORDER BY %s LIMIT %d", indexes, limit)
	bs := sbuf.String()

	vals, err = conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", tablename), nil)
	if err != nil {
		return err
	}
	create := vals[1].(string)
	if _, err := w.Write([]byte(create)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}

	if err := conn.Exec("COMMIT", nil); err != nil {
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
		pkcols := cols[:len(index)]
		cols = cols[len(index):]
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
			pk = vals[:len(index)]
			vals = vals[len(index):]
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
					switch ct := coltypes[cols[si]]; ct {
					case "INTERVAL":
						ivals[si] = fmt.Sprintf("'%s'", t)
					case "BYTES":
						ivals[si] = parser.NewDBytes(parser.DBytes(t)).String()
					default:
						// STRING and DECIMAL types can have optional length
						// suffixes, so only examine the prefix of the type.
						if strings.HasPrefix(coltypes[cols[si]], "STRING") {
							ivals[si] = parser.NewDString(string(t)).String()
						} else if strings.HasPrefix(coltypes[cols[si]], "DECIMAL") {
							ivals[si] = string(t)
						} else {
							panic(errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], coltypes[cols[si]]))
						}
					}
				case time.Time:
					var d parser.Datum
					ct := coltypes[cols[si]]
					switch ct {
					case "DATE":
						d = parser.NewDDateFromTime(t, time.UTC)
					case "TIMESTAMP":
						d = parser.MakeDTimestamp(t, time.Nanosecond)
					case "TIMESTAMP WITH TIME ZONE":
						d = parser.MakeDTimestampTZ(t, time.Nanosecond)
					default:
						panic(errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], coltypes[cols[si]]))
					}
					ivals[si] = fmt.Sprintf("'%s'", d)
				default:
					panic(errors.Errorf("unknown field type: %T (%s)", t, cols[si]))
				}
			}
			inserts = append(inserts, ivals)
			i++
		}
		for si, sv := range pk {
			b, ok := sv.([]byte)
			if ok && strings.HasPrefix(coltypes[pkcols[si]], "STRING") {
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
		fmt.Fprintf(w, "\nINSERT INTO %s VALUES", tablename)
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
