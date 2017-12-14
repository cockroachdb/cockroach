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
	"bytes"
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

	mds, ts, err := getDumpablesForNames(conn, dbName, tableNames, dumpCtx.asOf)
	if err != nil {
		return err
	}

	byID := make(map[int64]dumper)
	for _, md := range mds {
		byID[md.GetID()] = md
	}

	// First sort by name to guarantee stable output.
	sort.Slice(mds, func(i, j int) bool {
		return mds[i].GetName().String() < mds[j].GetName().String()
	})

	// Collect transitive dependencies in topological order into collected.
	var collected []int64
	seen := make(map[int64]bool)
	for _, md := range mds {
		collect(md.GetID(), byID, seen, &collected)
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
		return collectOrder[mds[i].GetID()] < collectOrder[mds[j].GetID()]
	})

	w := os.Stdout

	if dumpCtx.dumpMode != dumpDataOnly {
		for i, md := range mds {
			if i > 0 {
				fmt.Fprintln(w)
			}
			if err := dumpCreateStmt(md, w); err != nil {
				return err
			}
		}
	}
	if dumpCtx.dumpMode != dumpSchemaOnly {
		for _, md := range mds {
			if err := md.DumpData(w, conn, ts); err != nil {
				return err
			}
		}
	}
	return nil
}

func collect(tid int64, byID map[int64]dumper, seen map[int64]bool, collected *[]int64) {
	// has this table already been collected previously?
	if seen[tid] {
		return
	}
	if _, ok := byID[tid]; !ok {
		return
	}
	// no: mark it as seen.
	seen[tid] = true
	for _, dep := range byID[tid].GetDependsOn() {
		// depth-first collection of dependencies
		collect(dep, byID, seen, collected)
	}
	// Only add it after its dependencies.
	*collected = append(*collected, tid)
}

type dumper interface {
	GetID() int64
	GetName() *tree.TableName
	GetDependsOn() []int64
	GetCreateStmt() string

	DumpData(w io.Writer, conn *sqlConn, clusterTS string) error
}

// sequenceMetadata describes one sequence to dump.
type sequenceMetadata struct {
	ID         int64
	name       *tree.TableName
	createStmt string
}

func (smd *sequenceMetadata) GetID() int64             { return smd.ID }
func (smd *sequenceMetadata) GetName() *tree.TableName { return smd.name }
func (smd *sequenceMetadata) GetDependsOn() []int64    { return []int64{} }
func (smd *sequenceMetadata) GetCreateStmt() string    { return smd.createStmt }

var _ dumper = &sequenceMetadata{}

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

func (tmd *tableMetadata) GetID() int64             { return tmd.ID }
func (tmd *tableMetadata) GetName() *tree.TableName { return tmd.name }
func (tmd *tableMetadata) GetDependsOn() []int64    { return tmd.dependsOn }
func (tmd *tableMetadata) GetCreateStmt() string    { return tmd.createStmt }

// getDumpablesForNames retrieves the table information for the specified table(s).
// It also retrieves the cluster timestamp at which the metadata was
// retrieved.
func getDumpablesForNames(
	conn *sqlConn, dbName string, dumpableNames []string, asOf string,
) (mds []dumper, clusterTS string, err error) {
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

	if dumpableNames == nil {
		dumpableNames, err = getDumpableNames(conn, dbName, clusterTS)
		if err != nil {
			return nil, "", err
		}
	}

	mds = make([]dumper, len(dumpableNames))

	for i, tableName := range dumpableNames {
		md, err := getMetadataForDumpable(conn, dbName, tableName, clusterTS)
		if err != nil {
			return nil, "", err
		}
		mds[i] = md
	}

	return mds, clusterTS, nil
}

func getDumpableNames(conn *sqlConn, dbName string, ts string) (dumpableNames []string, err error) {
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT c.relname
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n
		ON c.relnamespace = n.oid
		AS OF SYSTEM TIME %s
		WHERE n.nspname = $1
		AND c.relkind IN ('S', 'r', 'v')
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
		name, ok := nameI.([]uint8)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", nameI)
		}
		dumpableNames = append(dumpableNames, string([]byte(name)))
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return dumpableNames, nil
}

func getMetadataForDumpable(conn *sqlConn, dbName, dumpableName string, ts string) (dumper, error) {
	// Determine what it is.
	query := fmt.Sprintf(`
		SELECT descriptor_type
		FROM "".crdb_internal.create_statements
		AS OF SYSTEM TIME '%s'
		WHERE database_name = $1
		AND descriptor_name = $2
	`, ts)
	vals, err := conn.QueryRow(query, []driver.Value{dbName, dumpableName})
	if err != nil {
		if err == io.EOF {
			return nil, errors.Errorf("relation %s.%s does not exist", dbName, dumpableName)
		}
		return nil, err
	}
	kindI := vals[0]
	kind, ok := kindI.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected value: %T", kindI)
	}
	switch kind {
	case "sequence":
		res, err := getMetadataForSequence(conn, dbName, dumpableName, ts)
		return res, err
	case "table", "view":
		res, err := getMetadataForTable(conn, dbName, dumpableName, ts)
		return res, err
	default:
		panic(fmt.Sprintf("unknown kind: %s", kind))
	}
}

func getMetadataForTable(
	conn *sqlConn, dbName, tableName string, ts string,
) (*tableMetadata, error) {
	name := &tree.TableName{DatabaseName: tree.Name(dbName), TableName: tree.Name(tableName)}

	// Fetch table ID.
	vals, err := conn.QueryRow(fmt.Sprintf(`
		SELECT table_id
		FROM %s.crdb_internal.tables
		AS OF SYSTEM TIME '%s'
		WHERE DATABASE_NAME = $1
			AND NAME = $2
		`, tree.Name(dbName).String(), ts), []driver.Value{dbName, tableName})
	if err != nil {
		if err == io.EOF {
			return nil, errors.Errorf("relation %s does not exist", name)
		}
		return nil, err
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
		return nil, err
	}
	vals = make([]driver.Value, 2)
	coltypes := make(map[string]string)
	var colnames bytes.Buffer
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		nameI, typI := vals[0], vals[1]
		name, ok := nameI.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", nameI)
		}
		typ, ok := typI.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", typI)
		}
		coltypes[name] = typ
		if colnames.Len() > 0 {
			colnames.WriteString(", ")
		}
		tree.FormatNode(&colnames, tree.FmtSimple, tree.Name(name))
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	vals, err = conn.QueryRow(fmt.Sprintf(`
		SELECT create_statement, descriptor_type = 'view'
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_name = $1
			AND database_name = $2
		`, tree.Name(dbName).String(), ts), []driver.Value{tableName, dbName})
	if err != nil {
		return nil, err
	}
	create := vals[0].(string)
	descType := vals[1].(bool)

	rows, err = conn.Query(fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_id = $1
		`, tree.Name(dbName).String(), ts), []driver.Value{tableID})
	if err != nil {
		return nil, err
	}
	vals = make([]driver.Value, 1)

	var refs []int64
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		id := vals[0].(int64)
		refs = append(refs, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	return &tableMetadata{
		ID:          tableID,
		name:        name,
		columnNames: colnames.String(),
		columnTypes: coltypes,
		createStmt:  create,
		dependsOn:   refs,
		isView:      descType,
	}, nil
}

func getMetadataForSequence(
	conn *sqlConn, dbName, sequenceName string, ts string,
) (*sequenceMetadata, error) {
	vals, err := conn.QueryRow(fmt.Sprintf(`
		SELECT descriptor_id, create_statement
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME '%s'
		WHERE database_name = $1
			AND descriptor_name = $2
		`, tree.Name(dbName).String(), ts), []driver.Value{dbName, sequenceName})
	if err != nil {
		if err == io.EOF {
			return nil, errors.Errorf("relation %s does not exist", sequenceName)
		}
		return nil, err
	}
	name := &tree.TableName{DatabaseName: tree.Name(dbName), TableName: tree.Name(sequenceName)}
	ID := vals[0].(int64)
	createStmt := vals[1].(string)
	return &sequenceMetadata{
		ID:         ID,
		name:       name,
		createStmt: createStmt,
	}, nil
}

func dumpCreateStmt(d dumper, w io.Writer) error {
	if _, err := w.Write([]byte(d.GetCreateStmt())); err != nil {
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

// DumpData implements the dumper interface.
func (tmd *tableMetadata) DumpData(w io.Writer, conn *sqlConn, clusterTS string) error {
	if tmd.isView {
		return nil
	}

	bs := fmt.Sprintf("SELECT * FROM %s AS OF SYSTEM TIME '%s' ORDER BY PRIMARY KEY %[1]s",
		tmd.name,
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
		var ivals bytes.Buffer
		for vals := range valsCh {
			ivals.Reset()
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				if si > 0 {
					ivals.WriteString(", ")
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
					switch ct := tmd.columnTypes[cols[si]]; ct {
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
						if strings.HasSuffix(tmd.columnTypes[cols[si]], "[]") {
							typ := strings.TrimRight(tmd.columnTypes[cols[si]], "[]")
							elemType, err := tree.StringToColType(typ)
							if err != nil {
								return err
							}
							d, err = tree.ParseDArrayFromString(tree.NewTestingEvalContext(), string(t), elemType)
							if err != nil {
								return err
							}
						} else if strings.HasPrefix(tmd.columnTypes[cols[si]], "STRING") {
							d = tree.NewDString(string(t))
						} else if strings.HasPrefix(tmd.columnTypes[cols[si]], "DECIMAL") {
							d, err = tree.ParseDDecimal(string(t))
							if err != nil {
								return err
							}
						} else {
							return errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], tmd.columnTypes[cols[si]])
						}
					}
				case time.Time:
					ct := tmd.columnTypes[cols[si]]
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
						return errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], tmd.columnTypes[cols[si]])
					}
				default:
					return errors.Errorf("unknown field type: %T (%s)", t, cols[si])
				}
				d.Format(&ivals, tree.FmtParsable)
			}
			select {
			case <-done:
				return ctx.Err()
			case stringsCh <- ivals.String():
			}
		}
		return nil
	})
	g.Go(func() error {
		// Batch SQL strings into groups and write to output.
		for s := range stringsCh {
			inserts = append(inserts, s)
			if len(inserts) == cap(inserts) {
				writeInserts(w, tmd, inserts)
				inserts = inserts[:0]
			}
		}
		if len(inserts) != 0 {
			writeInserts(w, tmd, inserts)
			inserts = inserts[:0]
		}
		return nil
	})
	return g.Wait()
}

func writeInserts(w io.Writer, md *tableMetadata, inserts []string) {
	fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", md.name.TableName, md.columnNames)
	for idx, values := range inserts {
		if idx > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "\n\t(%s)", values)
	}
	fmt.Fprintln(w, ";")
}

func (smd *sequenceMetadata) DumpData(w io.Writer, conn *sqlConn, clusterTS string) error {
	// TODO(vilterp): dump setval(name, val, false) here
	return nil
}
