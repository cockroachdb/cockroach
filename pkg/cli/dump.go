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

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/sqlutil"
)

// dumpCmd dumps SQL tables.
var dumpCmd = &cobra.Command{
	Use:   "dump [options] <database> <table>",
	Short: "dump sql tables\n",
	Long: `
Dump SQL tables of a cockroach database.
`,
	RunE:         maybeDecorateGRPCError(runDump),
	SilenceUsage: true,
}

func runDump(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return usageAndError(cmd)
	}

	conn, err := makeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	return dumpTable(os.Stdout, conn, args[0], args[1])
}

// fetchColTypes returns the types of the columns of a particular table.
func fetchColTypes(conn *sqlConn, tablename string) (_ map[string]string, retErr error) {
	// A previous version of the code did a SELECT on system.descriptor. This
	// required the SELECT privilege to the descriptor table, which only root
	// has. Allowing non-root to do this would let users see other users' table
	// descriptors which is a problem in multi-tenancy.

	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tablename), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = rows.Close()
		if retErr == nil {
			retErr = err
		}
	}()
	coltypes := make(map[string]string)
	for rows.Next() {
		vals, err := rows.ScanRaw()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		name, ok := vals[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", vals[1])

		}
		typ, ok := vals[1].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", vals[4])

		}
		coltypes[name] = typ
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return coltypes, nil
}

// fetchPKInfo returns the name and the columns of the primary key.
func fetchPKInfo(conn *sqlConn, tablename string) (_ string, _ []string, retErr error) {
	// index holds the names, in order, of the primary key columns.
	var index []string
	// Primary index is always the first index returned by SHOW INDEX.
	rows, err := conn.Query(fmt.Sprintf("SHOW INDEX FROM %s", tablename), nil)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		err = rows.Close()
		if retErr == nil {
			retErr = err
		}
	}()
	var primaryIndex string
	// Find the primary index columns.
	for rows.Next() {
		vals, err := rows.ScanRaw()
		if err == io.EOF {
			break
		} else if err != nil {
			return "", nil, err
		}
		b, ok := vals[1].(string)
		if !ok {
			return "", nil, fmt.Errorf("unexpected value: %T", vals[1])
		}
		if primaryIndex == "" {
			primaryIndex = b
		} else if primaryIndex != b {
			break
		}
		b, ok = vals[4].(string)
		if !ok {
			return "", nil, fmt.Errorf("unexpected value: %T", vals[4])
		}
		index = append(index, parser.Name(b).String())
	}
	if err := rows.Close(); err != nil {
		return "", nil, err
	}
	if len(index) == 0 {
		return "", nil, fmt.Errorf("no primary key index found")
	}
	return primaryIndex, index, nil
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

	vals, err := conn.QueryRow("SELECT cluster_logical_timestamp()", nil)
	if err != nil {
		return err
	}
	clusterTS := string(vals[0].([]byte))

	coltypes, err := fetchColTypes(conn, tablename)
	if err != nil {
		return err
	}

	primaryIndex, index, err := fetchPKInfo(conn, tablename)
	if err != nil {
		return err
	}
	indexes := strings.Join(index, ", ")

	// Build the SELECT query. Note that the index columns are selected separately
	// first, and then also included in the '*' part.
	var selectBuf bytes.Buffer
	fmt.Fprintf(&selectBuf, "SELECT %s, * FROM %s@%s AS OF SYSTEM TIME %s", indexes, tablename, primaryIndex, clusterTS)

	var whereBuf bytes.Buffer
	fmt.Fprintf(&whereBuf, " WHERE ROW (%s) > ROW (", indexes)
	for i := range index {
		if i > 0 {
			whereBuf.WriteString(", ")
		}
		fmt.Fprintf(&whereBuf, "$%d", i+1)
	}
	whereBuf.WriteString(")")
	// No WHERE clause first time, so add a place to inject it.
	fmt.Fprintf(&selectBuf, "%%s ORDER BY %s LIMIT %d", indexes, limit)
	selectTemplate := selectBuf.String()
	queryWithoutWhere := fmt.Sprintf(selectTemplate, "")
	queryWithWhere := fmt.Sprintf(selectTemplate, whereBuf.String())

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
	first := true
	for {
		var q string
		if first {
			first = false
			q = queryWithoutWhere
		} else {
			q = queryWithWhere
		}
		rows, err := conn.Query(q, pk)
		if err != nil {
			return err
		}
		pk, err = dumpBatch(w, rows, tablename, index, coltypes, limit)
		if err != nil {
			return err
		}
		if pk == nil {
			break
		}
	}
	return nil
}

// dumpBatch generates an INSERT statements into `tablename` containing values
// for all the rows in `rows`. It returns the values corresponding to the PK of
// the last row, so that the next iteration can resume from there.
//
// limit is the number of rows expected. If the actual rows are less, then the
// return value is nil, to indicate that no further iterations are necessary.
//
// rows will be closed.
func dumpBatch(
	w io.Writer,
	rows *sqlutil.SQLRows,
	tablename string,
	pkcols []string,
	coltypes map[string]string,
	limit int,
) (_ []driver.Value, retErr error) {
	defer func() {
		err := rows.Close()
		if retErr == nil {
			retErr = err
		}
	}()
	cols := rows.Columns()[len(pkcols):]
	inserts := make([][]string, 0, 1000)
	i := 0
	var pk []driver.Value
	for rows.Next() {
		vals, err := rows.ScanRaw()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		pk = vals[:len(pkcols)]
		vals = vals[len(pkcols):]
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
	if i == 0 {
		// Signal no more rows.
		return nil, nil
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
		// Signal no more rows.
		return nil, nil
	}
	return pk, nil
}
