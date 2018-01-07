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

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
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

	byID := make(map[int64]tableMetadata)
	for _, md := range mds {
		byID[md.ID] = md
	}

	// First sort by name to guarantee stable output.
	sort.Slice(mds, func(i, j int) bool {
		return mds[i].name.String() < mds[j].name.String()
	})

	// Collect transitive dependencies in topological order into collected.
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
			if md.isView {
				continue
			}
			if err := dumpTableData(w, conn, ts, md); err != nil {
				return err
			}
		}
	}
	return nil
}

func collect(tid int64, byID map[int64]tableMetadata, seen map[int64]bool, collected *[]int64) {
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

// tableMetadata describes one table to dump.
type tableMetadata struct {
	ID          int64
	name        *tree.TableName
	columnNames string
	columnTypes map[string]string
	createStmt  string
	dependsOn   []int64
	isView      bool
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
		if _, err := tree.ParseDTimestamp(asOf, time.Nanosecond); err != nil {
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
		FROM "".information_schema.tables
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
	name := &tree.TableName{DatabaseName: tree.Name(dbName), TableName: tree.Name(tableName)}

	// Fetch table ID.
	dbNameStr := tree.NameString(dbName)
	vals, err := conn.QueryRow(fmt.Sprintf(`
		SELECT table_id
		FROM %s.crdb_internal.tables
		AS OF SYSTEM TIME '%s'
		WHERE DATABASE_NAME = $1
			AND NAME = $2
		`, dbNameStr, ts), []driver.Value{dbName, tableName})
	if err != nil {
		if err == io.EOF {
			return tableMetadata{}, errors.Errorf("relation %s does not exist", name)
		}
		return tableMetadata{}, err
	}
	tableID := vals[0].(int64)

	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT COLUMN_NAME, DATA_TYPE
		FROM "".information_schema.columns
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
		`, ts), []driver.Value{dbName, tableName})
	if err != nil {
		return tableMetadata{}, err
	}
	vals = make([]driver.Value, 2)
	coltypes := make(map[string]string)
	colnames := tree.NewFmtCtxWithBuf(tree.FmtSimple)
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
		coltypes[name] = typ
		if colnames.Len() > 0 {
			colnames.WriteString(", ")
		}
		colnames.FormatName(name)
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}

	vals, err = conn.QueryRow(fmt.Sprintf(`
		SELECT create_statement, descriptor_type = 'view'
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_name = $1
			AND database_name = $2
		`, dbNameStr, ts), []driver.Value{tableName, dbName})
	if err != nil {
		return tableMetadata{}, err
	}
	create := vals[0].(string)
	descType := vals[1].(bool)

	rows, err = conn.Query(fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_id = $1
		`, dbNameStr, ts), []driver.Value{tableID})
	if err != nil {
		return tableMetadata{}, err
	}
	vals = make([]driver.Value, 1)

	var refs []int64
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return tableMetadata{}, err
		}
		id := vals[0].(int64)
		refs = append(refs, id)
	}
	if err := rows.Close(); err != nil {
		return tableMetadata{}, err
	}

	return tableMetadata{
		ID:          tableID,
		name:        name,
		columnNames: colnames.String(),
		columnTypes: coltypes,
		createStmt:  create,
		dependsOn:   refs,
		isView:      descType,
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
	// insertRows is the number of rows per INSERT statement.
	insertRows = 100
)

// dumpTableData dumps the data of the specified table to w.
func dumpTableData(w io.Writer, conn *sqlConn, clusterTS string, md tableMetadata) error {
	bs := fmt.Sprintf("SELECT * FROM %s AS OF SYSTEM TIME '%s' ORDER BY PRIMARY KEY %[1]s",
		md.name,
		clusterTS,
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
	g, ctx := errgroup.WithContext(context.Background())
	done := ctx.Done()
	valsCh := make(chan []driver.Value)
	// stringsCh receives VALUES lines and batches them before writing to the
	// output. Buffering this chan allows the val encoding to proceed during
	// writes.
	stringsCh := make(chan string, insertRows)

	g.Go(func() error {
		// Fetch SQL rows and put them onto valsCh.
		defer close(valsCh)
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
	g.Go(func() error {
		// Convert SQL rows into VALUE strings.
		defer close(stringsCh)
		f := tree.NewFmtCtxWithBuf(tree.FmtParsable)
		defer f.Close()
		for vals := range valsCh {
			f.Reset()
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				if si > 0 {
					f.WriteString(", ")
				}
				var d tree.Datum
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
					d = tree.NewDString(t)
				case []byte:
					switch ct := md.columnTypes[cols[si]]; ct {
					case "INTERVAL":
						d, err = tree.ParseDInterval(string(t))
						if err != nil {
							return err
						}
					case "BYTES":
						d = tree.NewDBytes(tree.DBytes(t))
					case "UUID":
						d, err = tree.ParseDUuidFromString(string(t))
						if err != nil {
							return err
						}
					case "INET":
						d, err = tree.ParseDIPAddrFromINetString(string(t))
						if err != nil {
							return err
						}
					case "JSON":
						d, err = tree.ParseDJSON(string(t))
						if err != nil {
							return err
						}
					default:
						// STRING and DECIMAL types can have optional length
						// suffixes, so only examine the prefix of the type.
						// In addition, we can only observe ARRAY types by their [] suffix.
						if strings.HasSuffix(md.columnTypes[cols[si]], "[]") {
							typ := strings.TrimRight(md.columnTypes[cols[si]], "[]")
							elemType, err := tree.StringToColType(typ)
							if err != nil {
								return err
							}
							d, err = tree.ParseDArrayFromString(tree.NewTestingEvalContext(), string(t), elemType)
							if err != nil {
								return err
							}
						} else if strings.HasPrefix(md.columnTypes[cols[si]], "STRING") {
							d = tree.NewDString(string(t))
						} else if strings.HasPrefix(md.columnTypes[cols[si]], "DECIMAL") {
							d, err = tree.ParseDDecimal(string(t))
							if err != nil {
								return err
							}
						} else {
							return errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]])
						}
					}
				case time.Time:
					ct := md.columnTypes[cols[si]]
					switch ct {
					case "DATE":
						d = tree.NewDDateFromTime(t, time.UTC)
					case "TIME":
						// pq awkwardly represents TIME as a time.Time with date 0000-01-01.
						d = tree.MakeDTime(timeofday.FromTime(t))
					case "TIMESTAMP":
						d = tree.MakeDTimestamp(t, time.Nanosecond)
					case "TIMESTAMP WITH TIME ZONE":
						d = tree.MakeDTimestampTZ(t, time.Nanosecond)
					default:
						return errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], md.columnTypes[cols[si]])
					}
				default:
					return errors.Errorf("unknown field type: %T (%s)", t, cols[si])
				}
				d.Format(&f.FmtCtx)
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

func writeInserts(w io.Writer, md tableMetadata, inserts []string) {
	fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", &md.name.TableName, md.columnNames)
	for idx, values := range inserts {
		if idx > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "\n\t(%s)", values)
	}
	fmt.Fprintln(w, ";")
}
